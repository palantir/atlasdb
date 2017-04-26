/*
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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Equivalence;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableFromIterable;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;

/**
 * Sweeps one individual table.
 *
 * @author jweel
 */
public class SweepTaskRunner {
    private static final Logger log = LoggerFactory.getLogger(SweepTaskRunner.class);

    private final KeyValueService keyValueService;
    private final LongSupplier unreadableTimestampSupplier;
    private final LongSupplier immutableTimestampSupplier;
    private final TransactionService transactionService;
    private final SweepStrategyManager sweepStrategyManager;
    private final CellsSweeper cellsSweeper;

    public SweepTaskRunner(
            KeyValueService keyValueService,
            LongSupplier unreadableTimestampSupplier,
            LongSupplier immutableTimestampSupplier,
            TransactionService transactionService,
            SweepStrategyManager sweepStrategyManager,
            CellsSweeper cellsSweeper) {
        this.keyValueService = keyValueService;
        this.unreadableTimestampSupplier = unreadableTimestampSupplier;
        this.immutableTimestampSupplier = immutableTimestampSupplier;
        this.transactionService = transactionService;
        this.sweepStrategyManager = sweepStrategyManager;
        this.cellsSweeper = cellsSweeper;
    }

    /**
     * Represents the type of run to be conducted by the sweep runner.
     */
    private enum RunType {
        /**
         * A DRY run is expect to not mutate any tables (with the exception of the sweep.progress table)
         * but will determine and report back all the values that *would* have been swept.
         */
        DRY,

        /**
         * A FULL run will execute all followers / sentinel additions / and deletions on the
         * cells that qualify for sweeping.
         */
        FULL
    }

    public SweepResults dryRun(TableReference tableRef,
            int rowBatchSize,
            int cellBatchSize,
            @Nullable byte[] startRow) {
        return runInternal(tableRef, rowBatchSize, cellBatchSize, startRow, RunType.DRY);
    }

    public SweepResults run(TableReference tableRef, int rowBatchSize, int cellBatchSize, @Nullable byte[] startRow) {
        return runInternal(tableRef, rowBatchSize, cellBatchSize, startRow, RunType.FULL);
    }

    public long getConservativeSweepTimestamp() {
        return Sweeper.CONSERVATIVE.getSweepTimestampSupplier().getSweepTimestamp(
                unreadableTimestampSupplier, immutableTimestampSupplier);
    }

    private SweepResults runInternal(
            TableReference tableRef,
            int rowBatchSize,
            int cellBatchSize,
            @Nullable byte[] nullableStartRow,
            RunType runType) {
        Preconditions.checkNotNull(tableRef, "tableRef cannot be null");
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
        SweepStrategy sweepStrategy = sweepStrategyManager.get().getOrDefault(tableRef, SweepStrategy.CONSERVATIVE);
        Optional<Sweeper> sweeper = Sweeper.of(sweepStrategy);
        if (!sweeper.isPresent()) {
            return SweepResults.createEmptySweepResult(0L);
        }
        return doRun(tableRef, rowBatchSize, cellBatchSize, nullableStartRow, runType, sweeper.get());
    }

    private SweepResults doRun(TableReference tableRef,
                               int rowBatchSize,
                               int cellBatchSize,
                               @Nullable byte[] nullableStartRow,
                               RunType runType,
                               Sweeper sweeper) {
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
        long sweepTs = sweeper.getSweepTimestampSupplier().getSweepTimestamp(
                unreadableTimestampSupplier, immutableTimestampSupplier);

        byte[] startRow = MoreObjects.firstNonNull(nullableStartRow, PtBytes.EMPTY_BYTE_ARRAY);
        RangeRequest range = RangeRequest.builder()
                .startRowInclusive(startRow)
                .batchHint(rowBatchSize)
                .build();

        try (ClosableIterator<RowResult<Value>> valueResults = getValues(tableRef, range, sweepTs, sweeper);
             ClosableIterator<RowResult<Set<Long>>> rowResults = keyValueService.getRangeOfTimestamps(
                     tableRef, range, sweepTs)) {
            EquivalenceCountingIterator<RowResult<Set<Long>>> rowResultTimestamps =
                    new EquivalenceCountingIterator<>(rowResults, rowBatchSize, sameRowEquivalence());
            PeekingIterator<RowResult<Value>> peekingValues = Iterators.peekingIterator(valueResults);

            BatchingVisitable<CellAndTimestamps> cellsAndTimestamps = BatchingVisitableFromIterable
                    .create(getTimestampsFromRowResultsIterator(() -> rowResultTimestamps));

            final AtomicInteger totalCellsSwept = new AtomicInteger(0);
            cellsAndTimestamps.batchAccept(
                    cellBatchSize,
                    thisBatch -> {
                        CellsAndTimestamps thisBatchCells = CellsAndTimestamps.fromCellAndTimestampsList(thisBatch);
                        int cellsSwept = sweepForCells(thisBatchCells,
                                tableRef,
                                sweeper,
                                sweepTs,
                                peekingValues,
                                runType);
                        totalCellsSwept.addAndGet(cellsSwept);
                        return true;
                    });

            byte[] nextRow = rowResultTimestamps.size() < rowBatchSize ? null :
                    RangeRequests.getNextStartRow(false, rowResultTimestamps.lastItem().getRowName());
            return SweepResults.builder()
                    .previousStartRow(Optional.fromNullable(startRow))
                    .nextStartRow(Optional.fromNullable(nextRow))
                    .cellsExamined(rowResultTimestamps.size())
                    .cellsDeleted(totalCellsSwept.get())
                    .sweptTimestamp(sweepTs)
                    .build();
        }
    }

    private ClosableIterator<RowResult<Value>> getValues(TableReference tableRef,
                                                         RangeRequest range,
                                                         long sweepTs,
                                                         Sweeper sweeper) {
        if (sweeper.shouldSweepLastCommitted()) {
            return keyValueService.getRange(tableRef, range, sweepTs);
        } else {
            return ClosableIterators.emptyImmutableClosableIterator();
        }
    }

    public static Equivalence<RowResult<Set<Long>>> sameRowEquivalence() {
        return new Equivalence<RowResult<Set<Long>>>() {
            @Override
            protected boolean doEquivalent(RowResult<Set<Long>> fst, RowResult<Set<Long>> snd) {
                return Arrays.equals(fst.getRowName(), snd.getRowName());
            }

            @Override
            protected int doHash(RowResult<Set<Long>> setRowResult) {
                return 0;
            }
        };
    }

    private int sweepForCells(
            CellsAndTimestamps currentBatch,
            TableReference tableRef,
            Sweeper sweeper,
            long sweepTs,
            PeekingIterator<RowResult<Value>> peekingValues,
            RunType runType) {
        CellsAndTimestamps currentBatchWithoutIgnoredTimestamps =
                currentBatch.withoutIgnoredTimestamps(sweeper.getTimestampsToIgnore());

        CellsToSweep cellsToSweep = getStartTimestampsPerRowToSweep(
                currentBatchWithoutIgnoredTimestamps, peekingValues, sweepTs, sweeper);

        Multimap<Cell, Long> startTimestampsToSweepPerCell = cellsToSweep.timestampsAsMultimap();

        if (runType == RunType.FULL) {
            cellsSweeper.sweepCells(tableRef, startTimestampsToSweepPerCell, cellsToSweep.allSentinels());
        }

        return startTimestampsToSweepPerCell.size();
    }

    private static Iterator<CellAndTimestamps> getTimestampsFromRowResultsIterator(
            Iterable<RowResult<Set<Long>>> cellsToSweep) {
        return StreamSupport.stream(cellsToSweep.spliterator(), false)
                .flatMap(SweepTaskRunner::rowToCellAndTimestampStream)
                .iterator();
    }

    private static Stream<CellAndTimestamps> rowToCellAndTimestampStream(RowResult<Set<Long>> rowResult) {
        Set<Map.Entry<Cell, Set<Long>>> cellsInRow = ImmutableSet.copyOf(rowResult.getCells());
        return cellsInRow.stream()
                .map(SweepTaskRunner::convertToCellAndTimestamps);
    }

    private static CellAndTimestamps convertToCellAndTimestamps(
            Map.Entry<Cell, Set<Long>> entry) {
        return CellAndTimestamps.of(entry.getKey(), entry.getValue());
    }

    @VisibleForTesting
    CellsToSweep getStartTimestampsPerRowToSweep(
            CellsAndTimestamps startTimestampsPerCell,
            PeekingIterator<RowResult<Value>> values,
            long sweepTimestamp,
            Sweeper sweeper) {

        LoadingCache<Long, Long> startTsToCommitTs = CacheBuilder.newBuilder()
                .build(new StartTsToCommitTsCacheLoader(transactionService));

        // Needed because calling transactionService.get(<EMPTY>) is weird (it logs that it is empty too).
        Set<Long> allStartTimestamps = startTimestampsPerCell.getAllTimestampValues();
        if (!allStartTimestamps.isEmpty()) {
            startTsToCommitTs.putAll(transactionService.get(allStartTimestamps));
        }

        ImmutableCellsToSweep.Builder builder = ImmutableCellsToSweep.builder();
        for (CellAndTimestamps cellAndTimestamps : startTimestampsPerCell.cellAndTimestampsList()) {
            Cell cell = cellAndTimestamps.cell();
            Collection<Long> timestamps = cellAndTimestamps.timestamps();
            boolean sweepLastCommitted = isLatestValueEmpty(cell, values);
            CellToSweep cellToSweep = getTimestampsToSweep(
                    cell,
                    timestamps,
                    startTsToCommitTs,
                    sweepTimestamp,
                    sweepLastCommitted,
                    sweeper);
            builder.addCellToSweepList(cellToSweep);
        }
        return builder.build();
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

    private CellToSweep getTimestampsToSweep(
            Cell cell,
            Collection<Long> startTimestamps,
            LoadingCache<Long, Long> startTsToCommitTs,
            long sweepTimestamp,
            boolean sweepLastCommitted,
            Sweeper sweeper) {
        Set<Long> uncommittedTimestamps = new HashSet<>();
        SortedSet<Long> commitedTssToSweep = new TreeSet<>();
        long maxStartTs = TransactionConstants.FAILED_COMMIT_TS;
        boolean maxStartTsIsCommitted = false;
        for (long startTs : startTimestamps) {
            long commitTs = startTsToCommitTs.getUnchecked(startTs);

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
                commitedTssToSweep.add(startTs);
            } else if (commitTs == TransactionConstants.FAILED_COMMIT_TS) {
                uncommittedTimestamps.add(startTs);
            }
        }

        Set<Long> sweepTimestamps = commitedTssToSweep.isEmpty() || (sweepLastCommitted && maxStartTsIsCommitted)
                ? Sets.union(uncommittedTimestamps, commitedTssToSweep)
                : Sets.union(uncommittedTimestamps, commitedTssToSweep.subSet(0L, commitedTssToSweep.last()));

        boolean needsSentinel = sweeper.shouldAddSentinels() && commitedTssToSweep.size() > 1;

        return CellToSweep.of(cell, sweepTimestamps, needsSentinel);
    }
}
