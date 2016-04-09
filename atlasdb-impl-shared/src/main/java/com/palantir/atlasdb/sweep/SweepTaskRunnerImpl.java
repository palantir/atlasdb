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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Follower;
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
import com.palantir.common.annotation.Output;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;

/**
 * Sweeps one individual table.
 *
 * @author jweel
 */
public class SweepTaskRunnerImpl implements SweepTaskRunner {
    private static final Logger log = LoggerFactory.getLogger(SweepTaskRunnerImpl.class);
    private static final Set<Long> invalidTimestamps = ImmutableSet.of(Value.INVALID_VALUE_TIMESTAMP);

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
        SweepStrategy sweepStrategy = sweepStrategyManager.get().get(tableRef);
        if (sweepStrategy == null) {
            sweepStrategy = SweepStrategy.CONSERVATIVE;
        } else if (sweepStrategy == SweepStrategy.NOTHING) {
            // This sweep strategy makes transaction table truncation impossible
            return SweepResults.createEmptySweepResult(0L);
        }
        if (startRow == null) {
            startRow = new byte[0];
        }
        RangeRequest rangeRequest = RangeRequest.builder().startRowInclusive(startRow).batchHint(batchSize).build();

        long sweepTimestamp = getSweepTimestamp(sweepStrategy);

        ClosableIterator<RowResult<Value>> valueResults;
        if (sweepStrategy == SweepStrategy.CONSERVATIVE) {
            valueResults = ClosableIterators.wrap(ImmutableList.<RowResult<Value>>of().iterator());
        } else {
            valueResults = keyValueService.getRange(tableRef, rangeRequest, sweepTimestamp);
        }

        ClosableIterator<RowResult<Set<Long>>> rowResults =
                keyValueService.getRangeOfTimestamps(tableRef, rangeRequest, sweepTimestamp);

        try {
            List<RowResult<Set<Long>>> rowResultTimestamps = ImmutableList.copyOf(Iterators.limit(rowResults, batchSize));
            PeekingIterator<RowResult<Value>> peekingValues = Iterators.peekingIterator(valueResults);
            Set<Cell> sentinelsToAdd = Sets.newHashSet();
            Multimap<Cell, Long> rowTimestamps = getTimestampsFromRowResults(rowResultTimestamps, sweepStrategy);
            Multimap<Cell, Long> cellTsPairsToSweep = getCellTsPairsToSweep(rowTimestamps, peekingValues, sweepTimestamp, sweepStrategy, sentinelsToAdd);
            sweepCells(tableRef, cellTsPairsToSweep, sentinelsToAdd);
            byte[] nextRow = rowResultTimestamps.size() < batchSize ? null :
                RangeRequests.getNextStartRow(false, Iterables.getLast(rowResultTimestamps).getRowName());
            return new SweepResults(nextRow, rowResultTimestamps.size(), cellTsPairsToSweep.size(), sweepTimestamp);
        } finally {
            rowResults.close();
            valueResults.close();
        }
    }

    @Override
    public long getSweepTimestamp(SweepStrategy sweepStrategy) {
        if (sweepStrategy == SweepStrategy.CONSERVATIVE) {
            return Math.min(unreadableTimestampSupplier.get(), immutableTimestampSupplier.get());
        } else {
            return immutableTimestampSupplier.get();
        }
    }

    private Multimap<Cell, Long> getTimestampsFromRowResults(List<RowResult<Set<Long>>> cellsToSweep,
                                                             SweepStrategy sweepStrategy) {
        Multimap<Cell, Long> cellTsMappings = HashMultimap.create();
        for (RowResult<Set<Long>> rowResult : cellsToSweep) {
            for (Map.Entry<Cell, Set<Long>> entry : rowResult.getCells()) {
                if (sweepStrategy == SweepStrategy.CONSERVATIVE) {
                    cellTsMappings.putAll(entry.getKey(), Sets.difference(entry.getValue(), invalidTimestamps));
                } else {
                    cellTsMappings.putAll(entry.getKey(), entry.getValue());
                }
            }
        }
        return cellTsMappings;
    }

    private Multimap<Cell, Long> getCellTsPairsToSweep(Multimap<Cell, Long> cellTsMappings,
                                                       PeekingIterator<RowResult<Value>> values,
                                                       long sweepTimestamp,
                                                       SweepStrategy sweepStrategy,
                                                       @Output Set<Cell> sentinelsToAdd) {
        Multimap<Cell, Long> cellTsMappingsToSweep = HashMultimap.create();

        Map<Long, Long> startTsToCommitTs = transactionService.get(cellTsMappings.values());
        for (Map.Entry<Cell, Collection<Long>> entry : cellTsMappings.asMap().entrySet()) {
            Cell cell = entry.getKey();
            Collection<Long> timestamps = entry.getValue();
            boolean sweepLastCommitted = isLatestValueEmpty(cell, values);
            Iterable<? extends Long> timestampsToSweep = getTimestampsToSweep(
                    cell,
                    timestamps,
                    startTsToCommitTs,
                    sentinelsToAdd,
                    sweepTimestamp,
                    sweepLastCommitted,
                    sweepStrategy);
            cellTsMappingsToSweep.putAll(entry.getKey(), timestampsToSweep);
        }
        return cellTsMappingsToSweep;
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

    private Set<Long> getTimestampsToSweep(Cell cell,
                                           Collection<Long> timestamps /* start timestamps */,
                                           @Modified Map<Long, Long> startTsToCommitTs,
                                           @Output Set<Cell> sentinelsToAdd,
                                           long sweepTimestamp,
                                           boolean sweepLastCommitted,
                                           SweepStrategy sweepStrategy) {
        Set<Long> uncommittedTimestamps = Sets.newHashSet();
        SortedSet<Long> committedTimestampsToSweep = Sets.newTreeSet();
        long maxStartTs = TransactionConstants.FAILED_COMMIT_TS;
        boolean maxStartTsIsCommitted = false;
        for (long startTs : timestamps) {
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
            return uncommittedTimestamps;
        }

        if (sweepStrategy == SweepStrategy.CONSERVATIVE && committedTimestampsToSweep.size() > 1) {
            // We need to add a sentinel if we are removing a committed value
            sentinelsToAdd.add(cell);
        }

        if (sweepLastCommitted && maxStartTsIsCommitted) {
            return Sets.union(uncommittedTimestamps, committedTimestampsToSweep);
        }
        return Sets.union(
                uncommittedTimestamps,
                committedTimestampsToSweep.subSet(0L, committedTimestampsToSweep.last()));
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

    private void sweepCells(TableReference tableRef,
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
