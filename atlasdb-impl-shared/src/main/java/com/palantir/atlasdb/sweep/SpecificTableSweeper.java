/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.priority.ImmutableUpdateSweepPriority;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStore;
import com.palantir.atlasdb.sweep.progress.ImmutableSweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgressStore;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.time.Clock;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class SpecificTableSweeper {
    private static final Logger log = LoggerFactory.getLogger(SpecificTableSweeper.class);
    private final LockAwareTransactionManager txManager;
    private final KeyValueService kvs;
    private final SweepTaskRunner sweepRunner;
    private final Supplier<SweepBatchConfig> sweepBatchConfig;
    private final SweepPriorityStore sweepPriorityStore;
    private final SweepProgressStore sweepProgressStore;
    private final BackgroundSweeperPerformanceLogger sweepPerfLogger;
    private final SweepMetrics sweepMetrics;
    private final Clock wallClock;


    @VisibleForTesting
    SpecificTableSweeper(
            LockAwareTransactionManager txManager,
            KeyValueService kvs,
            SweepTaskRunner sweepRunner,
            Supplier<SweepBatchConfig> sweepBatchConfig,
            SweepPriorityStore sweepPriorityStore,
            SweepProgressStore sweepProgressStore,
            BackgroundSweeperPerformanceLogger sweepPerfLogger,
            SweepMetrics sweepMetrics,
            Clock wallclock) {
        this.txManager = txManager;
        this.kvs = kvs;
        this.sweepRunner = sweepRunner;
        this.sweepBatchConfig = sweepBatchConfig;
        this.sweepPriorityStore = sweepPriorityStore;
        this.sweepProgressStore = sweepProgressStore;
        this.sweepPerfLogger = sweepPerfLogger;
        this.sweepMetrics = sweepMetrics;
        this.wallClock = wallclock;
    }

    public static SpecificTableSweeper create(
            LockAwareTransactionManager txManager,
            KeyValueService kvs,
            SweepTaskRunner sweepRunner,
            Supplier<SweepBatchConfig> sweepBatchConfig,
            SweepTableFactory tableFactory,
            BackgroundSweeperPerformanceLogger sweepPerfLogger,
            SweepMetrics sweepMetrics) {
        SweepProgressStore sweepProgressStore = new SweepProgressStore(kvs, tableFactory);
        SweepPriorityStore sweepPriorityStore = new SweepPriorityStore(tableFactory);
        return new SpecificTableSweeper(txManager, kvs, sweepRunner,
                sweepBatchConfig, sweepPriorityStore, sweepProgressStore, sweepPerfLogger,
                sweepMetrics,
                System::currentTimeMillis);
    }

    public LockAwareTransactionManager getTxManager() {
        return txManager;
    }

    public KeyValueService getKvs() {
        return kvs;
    }

    public SweepTaskRunner getSweepRunner() {
        return sweepRunner;
    }

    public Supplier<SweepBatchConfig> getSweepBatchConfig() {
        return sweepBatchConfig;
    }

    public SweepPriorityStore getSweepPriorityStore() {
        return sweepPriorityStore;
    }

    public SweepProgressStore getSweepProgressStore() {
        return sweepProgressStore;
    }

    public SweepMetrics getSweepMetrics() {
        return sweepMetrics;
    }

    void runOnceForTable(TableToSweep tableToSweep,
            Optional<SweepBatchConfig> newSweepBatchConfig,
            boolean saveSweepResults) {
        Stopwatch watch = Stopwatch.createStarted();
        TableReference tableRef = tableToSweep.getTableRef();
        byte[] startRow = tableToSweep.getStartRow();
        SweepBatchConfig batchConfig = newSweepBatchConfig.orElse(getAdjustedBatchConfig());
        try {
            SweepResults results = sweepRunner.run(
                    tableRef,
                    batchConfig,
                    startRow);
            long elapsedMillis = watch.elapsed(TimeUnit.MILLISECONDS);
            log.info("Swept successfully.",
                    LoggingArgs.tableRef("tableRef", tableRef),
                    UnsafeArg.of("startRow", startRowToHex(startRow)),
                    SafeArg.of("unique cells swept", results.getCellTsPairsExamined()),
                    SafeArg.of("deletion count", results.getStaleValuesDeleted()),
                    SafeArg.of("time taken", elapsedMillis),
                    SafeArg.of("last swept timestamp", results.getSweptTimestamp()));
            sweepPerfLogger.logSweepResults(
                    SweepPerformanceResults.builder()
                            .sweepResults(results)
                            .tableName(tableRef.getQualifiedName())
                            .elapsedMillis(elapsedMillis)
                            .build());
            if (saveSweepResults) {
                saveSweepResults(tableToSweep, results);
            }
        } catch (RuntimeException e) {
            // Error logged at a higher log level above.
            log.info("Failed to sweep.",
                    LoggingArgs.tableRef("tableRef", tableRef),
                    UnsafeArg.of("startRow", startRowToHex(startRow)),
                    SafeArg.of("batchConfig", batchConfig));
            throw e;
        }
    }

    private SweepBatchConfig getAdjustedBatchConfig() {
        SweepBatchConfig baseConfig = sweepBatchConfig.get();
        return ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(
                        BackgroundSweeperImpl.adjustBatchParameter(baseConfig.maxCellTsPairsToExamine()))
                .candidateBatchSize(BackgroundSweeperImpl.adjustBatchParameter(baseConfig.candidateBatchSize()))
                .deleteBatchSize(BackgroundSweeperImpl.adjustBatchParameter(baseConfig.deleteBatchSize()))
                .build();
    }

    private static String startRowToHex(@Nullable byte[] row) {
        if (row == null) {
            return "0";
        } else {
            return PtBytes.encodeHexString(row);
        }
    }


    private void saveSweepResults(TableToSweep tableToSweep, SweepResults currentIteration) {
        long staleValuesDeleted = tableToSweep.getStaleValuesDeletedPreviously()
                + currentIteration.getStaleValuesDeleted();
        long cellsExamined = tableToSweep.getCellsExaminedPreviously() + currentIteration.getCellTsPairsExamined();
        long minimumSweptTimestamp = Math.min(
                tableToSweep.getPreviousMinimumSweptTimestamp().orElse(Long.MAX_VALUE),
                currentIteration.getSweptTimestamp());
        SweepResults cumulativeResults = SweepResults.builder()
                .staleValuesDeleted(staleValuesDeleted)
                .cellTsPairsExamined(cellsExamined)
                .sweptTimestamp(minimumSweptTimestamp)
                .nextStartRow(currentIteration.getNextStartRow())
                .build();
        if (currentIteration.getNextStartRow().isPresent()) {
            saveIntermediateSweepResults(tableToSweep, cumulativeResults);
        } else {
            saveFinalSweepResults(tableToSweep, cumulativeResults);
            performInternalCompactionIfNecessary(tableToSweep.getTableRef(), cumulativeResults);
            log.debug("Finished sweeping.",
                    LoggingArgs.tableRef("tableRef", tableToSweep.getTableRef()),
                    SafeArg.of("unique cells examined count", cellsExamined),
                    SafeArg.of("stale values deleted count", staleValuesDeleted));
            sweepProgressStore.clearProgress();
        }
    }

    private void saveIntermediateSweepResults(TableToSweep tableToSweep, SweepResults results) {
        Preconditions.checkArgument(results.getNextStartRow().isPresent(),
                "Next start row should be present when saving intermediate results!");
        txManager.runTaskWithRetry((TxTask) tx -> {
            if (!tableToSweep.hasPreviousProgress()) {
                // This is the first set of results being written for this table.
                sweepPriorityStore.update(
                        tx,
                        tableToSweep.getTableRef(),
                        ImmutableUpdateSweepPriority.builder().newWriteCount(0L).build());
            }
            SweepProgress newProgress = ImmutableSweepProgress.builder()
                    .tableRef(tableToSweep.getTableRef())
                    .staleValuesDeleted(results.getStaleValuesDeleted())
                    .cellTsPairsExamined(results.getCellTsPairsExamined())
                    //noinspection OptionalGetWithoutIsPresent // covered by precondition above
                    .startRow(results.getNextStartRow().get())
                    .minimumSweptTimestamp(results.getSweptTimestamp())
                    .build();
            sweepProgressStore.saveProgress(tx, newProgress);
            return null;
        });
    }

    private void performInternalCompactionIfNecessary(TableReference tableRef, SweepResults results) {
        if (results.getStaleValuesDeleted() > 0) {
            Stopwatch watch = Stopwatch.createStarted();
            kvs.compactInternally(tableRef);
            long elapsedMillis = watch.elapsed(TimeUnit.MILLISECONDS);
            log.debug("Finished performing compactInternally on {} in {} ms.",
                    LoggingArgs.tableRef("tableRef", tableRef),
                    SafeArg.of("elapsedMillis", elapsedMillis));
            sweepPerfLogger.logInternalCompaction(
                    SweepCompactionPerformanceResults.builder()
                            .tableName(tableRef.getQualifiedName())
                            .cellsDeleted(results.getStaleValuesDeleted())
                            .cellsExamined(results.getCellTsPairsExamined())
                            .elapsedMillis(elapsedMillis)
                            .build());
        }
    }

    private void saveFinalSweepResults(TableToSweep tableToSweep, SweepResults sweepResults) {
        txManager.runTaskWithRetry((TxTask) tx -> {
            ImmutableUpdateSweepPriority.Builder update = ImmutableUpdateSweepPriority.builder()
                    .newStaleValuesDeleted(sweepResults.getStaleValuesDeleted())
                    .newCellTsPairsExamined(sweepResults.getCellTsPairsExamined())
                    .newLastSweepTimeMillis(wallClock.getTimeMillis())
                    .newMinimumSweptTimestamp(sweepResults.getSweptTimestamp());
            if (!tableToSweep.hasPreviousProgress()) {
                // This is the first (and only) set of results being written for this table.
                update.newWriteCount(0L);
            }
            sweepPriorityStore.update(tx, tableToSweep.getTableRef(), update.build());
            return null;
        });

        sweepMetrics.examinedCells(sweepResults.getCellTsPairsExamined());
        sweepMetrics.deletedCells(sweepResults.getStaleValuesDeleted());
    }
}
