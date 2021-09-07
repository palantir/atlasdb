/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.sweep.CellsToSweepPartitioningIterator.ExaminedCellLimit;
import com.palantir.atlasdb.sweep.metrics.LegacySweepMetrics;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;
import com.palantir.atlasdb.table.description.SweepStrategy;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.ClosableIterator;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * Sweeps one individual table.
 */
public class SweepTaskRunner {
    private static final SafeLogger log = SafeLoggerFactory.get(SweepTaskRunner.class);

    private final KeyValueService keyValueService;
    private final SpecialTimestampsSupplier specialTimestampsSupplier;
    private final SweepStrategyManager sweepStrategyManager;
    private final CellsSweeper cellsSweeper;
    private final Optional<LegacySweepMetrics> metricsManager;
    private final CommitTsCache commitTsCache;
    private final BooleanSupplier skipCellVersion =
            () -> ThreadLocalRandom.current().nextInt(100) == 0;

    public SweepTaskRunner(
            KeyValueService keyValueService,
            LongSupplier unreadableTimestampSupplier,
            LongSupplier immutableTimestampSupplier,
            TransactionService transactionService,
            SweepStrategyManager sweepStrategyManager,
            CellsSweeper cellsSweeper) {
        this(
                keyValueService,
                unreadableTimestampSupplier,
                immutableTimestampSupplier,
                transactionService,
                sweepStrategyManager,
                cellsSweeper,
                null);
    }

    public SweepTaskRunner(
            KeyValueService keyValueService,
            LongSupplier unreadableTsSupplier,
            LongSupplier immutableTsSupplier,
            TransactionService transactionService,
            SweepStrategyManager sweepStrategyManager,
            CellsSweeper cellsSweeper,
            LegacySweepMetrics metricsManager) {
        this.keyValueService = keyValueService;
        this.specialTimestampsSupplier = new SpecialTimestampsSupplier(unreadableTsSupplier, immutableTsSupplier);
        this.sweepStrategyManager = sweepStrategyManager;
        this.cellsSweeper = cellsSweeper;
        this.metricsManager = Optional.ofNullable(metricsManager);
        this.commitTsCache = CommitTsCache.create(transactionService);
    }

    /**
     * Represents the type of run to be conducted by the sweep runner.
     */
    public enum RunType {
        /**
         * A DRY run is expect to not mutate any tables (with the exception of the sweep.progress table)
         * but will determine and report back all the values that *would* have been swept.
         */
        DRY,

        /**
         * A FULL run will execute all followers / sentinel additions / and deletions on the
         * cells that qualify for sweeping.
         */
        FULL,

        /**
         * A WAS_CONSERVATIVE_NOW_THOROUGH run is a FULL run that will intentionally retain ~1% of entries for cells
         * to avoid issues that could arise due to too many consecutive deletes in the underlying KVS from sweep
         * potentially deleting many consecutive legacy sentinels.
         */
        WAS_CONSERVATIVE_NOW_THOROUGH
    }

    public SweepResults dryRun(TableReference tableRef, SweepBatchConfig batchConfig, byte[] startRow) {
        return runInternal(tableRef, batchConfig, startRow, RunType.DRY);
    }

    public SweepResults run(TableReference tableRef, SweepBatchConfig batchConfig, byte[] startRow, RunType runType) {
        return runInternal(tableRef, batchConfig, startRow, runType);
    }

    public long getConservativeSweepTimestamp() {
        return Sweeper.CONSERVATIVE.getSweepTimestamp(specialTimestampsSupplier);
    }

    private SweepResults runInternal(
            TableReference tableRef, SweepBatchConfig batchConfig, byte[] startRow, RunType runType) {

        Preconditions.checkNotNull(tableRef, "tableRef cannot be null");
        Preconditions.checkState(!AtlasDbConstants.HIDDEN_TABLES.contains(tableRef));

        if (tableRef.getQualifiedName().startsWith(AtlasDbConstants.NAMESPACE_PREFIX)) {
            // this happens sometimes; I think it's because some places in the code can
            // start this sweeper without doing the full normally ordered KVSModule startup.
            // I did check and sweep.stats did contain the FQ table name for all of the tables,
            // so it is at least broken in some way that still allows namespaced tables to eventually be swept.
            log.warn("The sweeper should not be run on tables passed through namespace mapping.");
            return SweepResults.createEmptySweepResultWithNoMoreToSweep();
        }
        byte[] tableMeta = keyValueService.getMetadataForTable(tableRef);
        if (tableMeta.length == 0) {
            log.warn(
                    "The sweeper tried to sweep table '{}', but the table does not exist. Skipping table.",
                    LoggingArgs.tableRef("tableRef", tableRef));
            return SweepResults.createEmptySweepResultWithNoMoreToSweep();
        }
        SweepStrategy sweepStrategy = SweepStrategy.from(
                TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(tableMeta).getSweepStrategy());
        Optional<Sweeper> maybeSweeper = sweepStrategy.getSweeperStrategy().map(Sweeper::of);
        return maybeSweeper
                .map(sweeper -> doRun(tableRef, batchConfig, startRow, runType, sweeper))
                .orElseGet(SweepResults::createEmptySweepResultWithNoMoreToSweep);
    }

    private SweepResults doRun(
            TableReference tableRef, SweepBatchConfig batchConfig, byte[] startRow, RunType runType, Sweeper sweeper) {
        Stopwatch watch = Stopwatch.createStarted();
        long timeSweepStarted = System.currentTimeMillis();
        log.info(
                "Beginning iteration of sweep for table {} starting at row {}",
                LoggingArgs.tableRef(tableRef),
                UnsafeArg.of("startRow", PtBytes.encodeHexString(startRow)));
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
        long sweepTs = sweeper.getSweepTimestamp(specialTimestampsSupplier);
        CandidateCellForSweepingRequest request = ImmutableCandidateCellForSweepingRequest.builder()
                .startRowInclusive(startRow)
                .batchSizeHint(batchConfig.candidateBatchSize())
                .maxTimestampExclusive(sweepTs)
                .shouldCheckIfLatestValueIsEmpty(sweeper.shouldSweepLastCommitted())
                .shouldDeleteGarbageCollectionSentinels(!sweeper.shouldAddSentinels())
                .build();

        SweepableCellFilter sweepableCellFilter = new SweepableCellFilter(commitTsCache, sweeper, sweepTs);
        try (ClosableIterator<List<CandidateCellForSweeping>> candidates =
                keyValueService.getCandidateCellsForSweeping(tableRef, request)) {
            ExaminedCellLimit limit = new ExaminedCellLimit(startRow, batchConfig.maxCellTsPairsToExamine());
            Iterator<BatchOfCellsToSweep> batchesToSweep =
                    getBatchesToSweep(candidates, batchConfig, sweepableCellFilter, limit);
            long totalCellTsPairsExamined = 0;
            long totalCellTsPairsDeleted = 0;

            byte[] lastRow = startRow;
            while (batchesToSweep.hasNext()) {
                BatchOfCellsToSweep batch = batchesToSweep.next();

                /*
                 * At this point cells were merged in batches of at least deleteBatchSize blocks per batch. Therefore we
                 * expect most batches to have slightly more than deleteBatchSize blocks. Partitioning such batches with
                 * deleteBatchSize as a limit results in a small second batch, which is bad for performance reasons.
                 * Therefore, deleteBatchSize is doubled.
                 */
                long cellsDeleted = sweepBatch(tableRef, batch.cells(), runType, 2 * batchConfig.deleteBatchSize());
                totalCellTsPairsDeleted += cellsDeleted;

                long cellsExamined = batch.numCellTsPairsExamined();
                totalCellTsPairsExamined += cellsExamined;

                metricsManager.ifPresent(manager -> manager.updateCellsExaminedDeleted(cellsExamined, cellsDeleted));

                lastRow = batch.lastCellExamined().getRowName();
            }
            return SweepResults.builder()
                    .previousStartRow(Optional.of(startRow))
                    .nextStartRow(Arrays.equals(startRow, lastRow) ? Optional.empty() : Optional.of(lastRow))
                    .cellTsPairsExamined(totalCellTsPairsExamined)
                    .staleValuesDeleted(totalCellTsPairsDeleted)
                    .minSweptTimestamp(sweepTs)
                    .timeInMillis(watch.elapsed(TimeUnit.MILLISECONDS))
                    .timeSweepStarted(timeSweepStarted)
                    .build();
        }
    }

    /**
     * Returns batches with at least batchConfig.deleteBatchSize blocks per batch.
     */
    private Iterator<BatchOfCellsToSweep> getBatchesToSweep(
            Iterator<List<CandidateCellForSweeping>> candidates,
            SweepBatchConfig batchConfig,
            SweepableCellFilter sweepableCellFilter,
            ExaminedCellLimit limit) {
        Iterator<BatchOfCellsToSweep> cellsToSweep = Iterators.transform(
                Iterators.filter(candidates, list -> !list.isEmpty()), sweepableCellFilter::getCellsToSweep);
        return new CellsToSweepPartitioningIterator(cellsToSweep, batchConfig.deleteBatchSize(), limit);
    }

    /**
     * Returns the number of blocks - (cell, timestamp) pairs - that were deleted.
     */
    private int sweepBatch(TableReference tableRef, List<CellToSweep> batch, RunType runType, int deleteBatchSize) {
        int numberOfSweptCells = 0;

        Multimap<Cell, Long> currentBatch = ArrayListMultimap.create();
        List<Cell> currentBatchSentinels = new ArrayList<>();

        for (CellToSweep cell : batch) {
            if (cell.needsSentinel()) {
                currentBatchSentinels.add(cell.cell());
            }

            // Taking an immutable copy is done here to allow for faster sublist extraction.
            List<Long> currentCellTimestamps = ImmutableList.copyOf(cell.sortedTimestamps());

            if (currentBatch.size() + currentCellTimestamps.size() < deleteBatchSize) {
                addCurrentCellTimestamps(currentBatch, cell.cell(), currentCellTimestamps, runType);
            } else {
                while (currentBatch.size() + currentCellTimestamps.size() >= deleteBatchSize) {
                    int numberOfTimestampsForThisBatch = deleteBatchSize - currentBatch.size();

                    addCurrentCellTimestamps(
                            currentBatch,
                            cell.cell(),
                            currentCellTimestamps.subList(0, numberOfTimestampsForThisBatch),
                            runType);

                    if (runType != RunType.DRY) {
                        cellsSweeper.sweepCells(tableRef, currentBatch, currentBatchSentinels);
                    }

                    numberOfSweptCells += currentBatch.size();
                    currentBatch.clear();
                    currentBatchSentinels.clear();
                    currentCellTimestamps =
                            currentCellTimestamps.subList(numberOfTimestampsForThisBatch, currentCellTimestamps.size());
                }
                addCurrentCellTimestamps(currentBatch, cell.cell(), currentCellTimestamps, runType);
            }
        }

        if (!currentBatch.isEmpty() && runType == RunType.FULL) {
            cellsSweeper.sweepCells(tableRef, currentBatch, currentBatchSentinels);
        }
        numberOfSweptCells += currentBatch.size();

        return numberOfSweptCells;
    }

    private Multimap<Cell, Long> addCurrentCellTimestamps(
            Multimap<Cell, Long> currentBatch, Cell currentCell, List<Long> currentCellTimestamps, RunType runType) {
        if (runType == RunType.WAS_CONSERVATIVE_NOW_THOROUGH) {
            currentCellTimestamps.stream()
                    .filter(_ignore -> !skipCellVersion.getAsBoolean())
                    .forEach(timestamp -> currentBatch.put(currentCell, timestamp));
        } else {
            currentBatch.putAll(currentCell, currentCellTimestamps);
        }
        return currentBatch;
    }
}
