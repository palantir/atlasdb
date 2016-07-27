/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import javax.annotation.Nullable;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.transaction.api.Transaction.TransactionType;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.annotation.Modified;
import com.palantir.common.base.ClosableIterator;

/**
 * Sweeps one individual table.
 *
 * @author jweel
 */
public class SweepTaskRunnerImpl implements SweepTaskRunner {
    private static final Logger log = LoggerFactory.getLogger(SweepTaskRunnerImpl.class);
    private static final Set<Long> INVALID_TIMESTAMPS = ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP);

    private final TransactionManager txManager;
    private final KeyValueService keyValueService;
    private final Supplier<Long> unreadableTimestampSupplier;
    private final Supplier<Long> immutableTimestampSupplier;
    private final TransactionService transactionService;
    private final SweepStrategyManager sweepStrategyManager;
    private final Collection<Follower> followers;

    public SweepTaskRunnerImpl(TransactionManager txManager,
                           KeyValueService keyValueService,
                           Supplier<Long> unreadableTimestampSupplier,
                           Supplier<Long> immutableTimestampSupplier,
                           TransactionService transactionService,
                           SweepStrategyManager sweepStrategyManager,
                           Collection<Follower> followers) {
        this.txManager = txManager;
        this.keyValueService = keyValueService;
        this.unreadableTimestampSupplier = unreadableTimestampSupplier;
        this.immutableTimestampSupplier = immutableTimestampSupplier;
        this.transactionService = transactionService;
        this.sweepStrategyManager = sweepStrategyManager;
        this.followers = followers;
    }

    @Override
    public SweepResults run(TableReference tableRef, int batchSize, @Nullable byte[] startRow) {
        Preconditions.checkNotNull(tableRef);
        Preconditions.checkState(!AtlasDbConstants.hiddenTables.contains(tableRef));

        if (tableRef.getQualifiedName().startsWith(AtlasDbConstants.NAMESPACE_PREFIX)) {
                // this happens sometimes; I think it's because some places in the code can
                // start this sweeper without doing the full normally ordered KVSModule startup.
                // I did check and sweep.stats did contain the FQ table name for all of the tables,
                // so it is at least broken in some way that still allows namespaced tables to eventually be swept.
                log.warn("The sweeper should not be run on tables passed through namespace mapping.");
                return SweepResults.createEmptySweepResult(0L);
        }
        if (keyValueService.getMetadataForTable(tableRef).length == 0) {
            log.warn("The sweeper tried to sweep table '{}', but the table does not exist. Skipping table.", tableRef);
            return SweepResults.createEmptySweepResult(0L);
        }

        // Earliest start timestamp of any currently open transaction, with two caveats:
        // (1) unreadableTimestamps are calculated via wall-clock time, and so may not be correct
        //     under pathological clock conditions
        // (2) immutableTimestamps do not account for locks have timed out after checking their locks;
        //     such a transaction may have a start timestamp less than the immutableTimestamp, and it
        //     could still get successfully committed (its commit timestamp may or may not be less than
        //     the immutableTimestamp
        // Note that this is fine, because we'll either
        // (1) force old readers to abort (if they read a garbage collection sentinel), or
        // (2) force old writers to retry (note that we must roll back any uncommitted transactions that
        //     we encounter
        SweepStrategy sweepStrategy = MoreObjects.firstNonNull(sweepStrategyManager.get().get(tableRef), SweepStrategy.CONSERVATIVE);
        if (sweepStrategy == SweepStrategy.NOTHING) {
            // This sweep strategy makes transaction table truncation impossible
            return SweepResults.createEmptySweepResult(0L);
        }

        startRow = MoreObjects.firstNonNull(startRow, PtBytes.EMPTY_BYTE_ARRAY);
        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(startRow)
                .batchHint(batchSize)
                .build();

        SweepStrategySweeper strategySweeper = getStrategySweeperFor(sweepStrategy);

        long sweepTimestamp = strategySweeper.getSweepTimestamp();


        try (ClosableIterator<RowResult<Value>> valueResults = strategySweeper.getValues(tableRef, rangeRequest, sweepTimestamp);
             ClosableIterator<RowResult<Set<Long>>> rowResults = strategySweeper.getCellTimestamps(tableRef, rangeRequest, sweepTimestamp)) {
            List<RowResult<Set<Long>>> rowResultTimestamps = ImmutableList.copyOf(Iterators.limit(rowResults, batchSize));
            PeekingIterator<RowResult<Value>> peekingValues = Iterators.peekingIterator(valueResults);
            Multimap<Cell, Long> rowTimestamps = getTimestampsFromRowResults(rowResultTimestamps, strategySweeper);
            SweepCellsAndSentinels sweepCellsAndSentinels = getStartTimestampsPerRowToSweep(rowTimestamps, peekingValues, sweepTimestamp, strategySweeper);
            Multimap<Cell, Long> startTimestampsToSweepPerCell = sweepCellsAndSentinels.startTimestampsToSweepPerCell();
            sweepCells(tableRef, startTimestampsToSweepPerCell, sweepCellsAndSentinels.sentinelsToAdd());
            byte[] nextRow = rowResultTimestamps.size() < batchSize ? null :
                RangeRequests.getNextStartRow(false, Iterables.getLast(rowResultTimestamps).getRowName());
            return new SweepResults(nextRow, rowResultTimestamps.size(), startTimestampsToSweepPerCell.size(), sweepTimestamp);
        }
    }

    private SweepStrategySweeper getStrategySweeperFor(SweepStrategy sweepStrategy) {
        switch (sweepStrategy) {
            case NOTHING:
                return new NothingSweepStrategySweeper();
            case CONSERVATIVE:
                return new ConservativeSweepStrategySweeper(keyValueService, immutableTimestampSupplier, unreadableTimestampSupplier);
            case THOROUGH:
                return new ThoroughSweepStrategySweeper(keyValueService, immutableTimestampSupplier);
            default:
                throw new IllegalArgumentException("Unknown sweep strategy: " + sweepStrategy);
        }
    }

    @Override
    public long getSweepTimestamp(SweepStrategy sweepStrategy) {
        return getStrategySweeperFor(sweepStrategy).getSweepTimestamp();
    }

    @VisibleForTesting
    static Multimap<Cell, Long> getTimestampsFromRowResults(List<RowResult<Set<Long>>> cellsToSweep, SweepStrategySweeper strategySweeper) {
        Set<Long> timestampsToIgnore = strategySweeper.getTimestampsToIgnore();
        ImmutableMultimap.Builder<Cell, Long> cellTsMappings = ImmutableMultimap.builder();
        for (RowResult<Set<Long>> rowResult : cellsToSweep) {
            for (Map.Entry<Cell, Set<Long>> entry : rowResult.getCells()) {
                cellTsMappings.putAll(entry.getKey(), Sets.difference(entry.getValue(), timestampsToIgnore));
            }
        }
        return cellTsMappings.build();
    }

    @VisibleForTesting
    SweepCellsAndSentinels getStartTimestampsPerRowToSweep(
            Multimap<Cell, Long> startTimestampsPerCell,
            PeekingIterator<RowResult<Value>> values,
            long sweepTimestamp,
            SweepStrategySweeper strategySweeper) {
        Multimap<Cell, Long> startTimestampsToSweepPerCell = HashMultimap.create();
        Set<Cell> sentinelsToAdd = Sets.newHashSet();

        Map<Long, Long> startTsToCommitTs = transactionService.get(startTimestampsPerCell.values());
        for (Map.Entry<Cell, Collection<Long>> entry : startTimestampsPerCell.asMap().entrySet()) {
            Cell cell = entry.getKey();
            Collection<Long> timestamps = entry.getValue();
            boolean sweepLastCommitted = isLatestValueEmpty(cell, values);
            SweepTimestampsAndSentinels sweepTimestampsAndSentinels = getTimestampsToSweep(
                    cell,
                    timestamps,
                    startTsToCommitTs,
                    sweepTimestamp,
                    sweepLastCommitted,
                    strategySweeper);
            startTimestampsToSweepPerCell.putAll(cell, sweepTimestampsAndSentinels.timestamps());
            sentinelsToAdd.addAll(sweepTimestampsAndSentinels.sentinelsToAdd());
        }
        return SweepCellsAndSentinels.of(startTimestampsToSweepPerCell, sentinelsToAdd);
    }

    private boolean isLatestValueEmpty(Cell cell, PeekingIterator<RowResult<Value>> values) {
        while (values.hasNext()) {
            RowResult<Value> result = values.peek();
            int comparison = UnsignedBytes.lexicographicalComparator().compare(cell.getRowName(), result.getRowName());
            if (comparison == 0) {
                Value matchingValue = result.getColumns().get(cell.getColumnName());
                return matchingValue != null && matchingValue.getContents().length == 0;
            } else if (comparison < 0) {
                return false;
            } else {
                values.next();
            }
        }
        return false;
    }

    private SweepTimestampsAndSentinels getTimestampsToSweep(Cell cell,
                                                             Collection<Long> startTimestamps,
                                                             @Modified Map<Long, Long> startTsToCommitTs,
                                                             long sweepTimestamp,
                                                             boolean sweepLastCommitted,
                                                             SweepStrategySweeper strategySweeper) {
        Set<Long> uncommittedTimestamps = Sets.newHashSet();
        SortedSet<Long> committedTimestampsToSweep = Sets.newTreeSet();
        long maxStartTs = TransactionConstants.FAILED_COMMIT_TS;
        boolean maxStartTsIsCommitted = false;
        for (long startTs : startTimestamps) {
            long commitTs = ensureCommitTimestampExists(startTs, startTsToCommitTs);

            if (startTs > maxStartTs && commitTs < sweepTimestamp) {
                maxStartTs = startTs;
                maxStartTsIsCommitted = commitTs != TransactionConstants.FAILED_COMMIT_TS;
            }
            // Note: there could be an open transaction whose start timestamp is equal to
            // sweepTimestamp; thus we want to sweep all cells such that:
            // (1) their commit timestamp is less than sweepTimestamp
            // (2) their start timestamp is NOT the greatest possible start timestamp
            //     passing condition (1)
            if (commitTs > 0 && commitTs < sweepTimestamp) {
                committedTimestampsToSweep.add(startTs);
            } else if (commitTs == TransactionConstants.FAILED_COMMIT_TS) {
                uncommittedTimestamps.add(startTs);
            }
        }

        if (committedTimestampsToSweep.isEmpty()) {
            return SweepTimestampsAndSentinels.of(uncommittedTimestamps, ImmutableSet.of());
        }

        Set<Cell> sentinelsToAdd = Sets.newHashSet();
        if (strategySweeper.shouldAddSentinels() && committedTimestampsToSweep.size() > 1) {
            // We need to add a sentinel if we are removing a committed value
            sentinelsToAdd.add(cell);
        }

        if (sweepLastCommitted && maxStartTsIsCommitted) {
            Set<Long> sweepTimestamps = Sets.union(uncommittedTimestamps, committedTimestampsToSweep);
            return SweepTimestampsAndSentinels.of(sweepTimestamps, sentinelsToAdd);
        }
        Set<Long> sweepTimestamps = Sets.union(
                uncommittedTimestamps,
                committedTimestampsToSweep.subSet(0L, committedTimestampsToSweep.last()));
        return SweepTimestampsAndSentinels.of(sweepTimestamps, sentinelsToAdd);
    }

    private long ensureCommitTimestampExists(Long startTs, @Modified Map<Long, Long> startTsToCommitTs) {
        Long commitTs = startTsToCommitTs.get(startTs);
        if (commitTs == null) {
            // Roll back this transaction (note that rolling back arbitrary transactions
            // can never cause correctness issues, only liveness issues)
            try {
                // TODO: carrino: use the batched version of putUnlessExists when it is available.
                transactionService.putUnlessExists(startTs, TransactionConstants.FAILED_COMMIT_TS);
            } catch (KeyAlreadyExistsException e) {
                String msg = "Could not roll back transaction with start timestamp " + startTs + "; either" +
                        " it was already rolled back (by a different transaction), or it committed successfully" +
                        " before we could roll it back.";
                log.error("This isn't a bug but it should be very infrequent. " + msg,
                        new TransactionFailedRetriableException(msg, e));
            }
            commitTs = transactionService.get(startTs);
            Validate.notNull(commitTs);
            startTsToCommitTs.put(startTs, commitTs);
        }
        return commitTs;
    }

    @VisibleForTesting
    void sweepCells(
            TableReference tableRef,
            Multimap<Cell, Long> cellTsPairsToSweep,
            Set<Cell> sentinelsToAdd) {
        if (cellTsPairsToSweep.isEmpty()) {
            return;
        }

        for (Follower follower : followers) {
            follower.run(txManager, tableRef, cellTsPairsToSweep.keySet(), TransactionType.HARD_DELETE);
        }
        if (!sentinelsToAdd.isEmpty()) {
            keyValueService.addGarbageCollectionSentinelValues(
                    tableRef,
                    sentinelsToAdd);
        }
        keyValueService.delete(tableRef, cellTsPairsToSweep);
    }
}
