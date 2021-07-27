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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.metrics.LegacySweepMetrics;
import com.palantir.atlasdb.sweep.priority.ImmutableUpdateSweepPriority;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStore;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStoreImpl;
import com.palantir.atlasdb.sweep.progress.ImmutableSweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgressStore;
import com.palantir.atlasdb.sweep.progress.SweepProgressStoreImpl;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.time.Clock;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpecificTableSweeper {
    private static final Logger log = LoggerFactory.getLogger(SpecificTableSweeper.class);
    private final TransactionManager txManager;
    private final KeyValueService kvs;
    private final SweepTaskRunner sweepRunner;
    private final SweepPriorityStore sweepPriorityStore;
    private final SweepProgressStore sweepProgressStore;
    private final BackgroundSweeperPerformanceLogger sweepPerfLogger;
    private final LegacySweepMetrics sweepMetrics;
    private final Clock wallClock;

    @VisibleForTesting
    SpecificTableSweeper(
            TransactionManager txManager,
            KeyValueService kvs,
            SweepTaskRunner sweepRunner,
            SweepPriorityStore sweepPriorityStore,
            SweepProgressStore sweepProgressStore,
            BackgroundSweeperPerformanceLogger sweepPerfLogger,
            LegacySweepMetrics sweepMetrics,
            Clock wallclock) {
        this.txManager = txManager;
        this.kvs = kvs;
        this.sweepRunner = sweepRunner;
        this.sweepPriorityStore = sweepPriorityStore;
        this.sweepProgressStore = sweepProgressStore;
        this.sweepPerfLogger = sweepPerfLogger;
        this.sweepMetrics = sweepMetrics;
        this.wallClock = wallclock;
    }

    public static SpecificTableSweeper create(
            TransactionManager txManager,
            KeyValueService kvs,
            SweepTaskRunner sweepRunner,
            SweepTableFactory tableFactory,
            BackgroundSweeperPerformanceLogger sweepPerfLogger,
            LegacySweepMetrics sweepMetrics,
            boolean initializeAsync) {
        SweepProgressStore sweepProgressStore = SweepProgressStoreImpl.create(kvs, initializeAsync);
        SweepPriorityStore sweepPriorityStore = SweepPriorityStoreImpl.create(kvs, tableFactory, initializeAsync);
        return new SpecificTableSweeper(
                txManager,
                kvs,
                sweepRunner,
                sweepPriorityStore,
                sweepProgressStore,
                sweepPerfLogger,
                sweepMetrics,
                System::currentTimeMillis);
    }

    public boolean isInitialized() {
        return sweepProgressStore.isInitialized() && sweepPriorityStore.isInitialized();
    }

    public TransactionManager getTxManager() {
        return txManager;
    }

    public KeyValueService getKvs() {
        return kvs;
    }

    public SweepTaskRunner getSweepRunner() {
        return sweepRunner;
    }

    public SweepPriorityStore getSweepPriorityStore() {
        return sweepPriorityStore;
    }

    public SweepProgressStore getSweepProgressStore() {
        return sweepProgressStore;
    }

    void runOnceAndSaveResults(TableToSweep tableToSweep, SweepBatchConfig batchConfig) {
        TableReference tableRef = tableToSweep.getTableRef();
        byte[] startRow = tableToSweep.getStartRow();

        SweepResults results = runOneIteration(tableRef, startRow, batchConfig);
        processSweepResults(tableToSweep, results);
    }

    SweepResults runOneIteration(TableReference tableRef, byte[] startRow, SweepBatchConfig batchConfig) {
        try {
            SweepResults results = sweepRunner.run(tableRef, batchConfig, startRow);
            logSweepPerformance(tableRef, startRow, results);

            return results;
        } catch (RuntimeException e) {
            // This error may be logged on some paths above, but I prefer to log defensively.
            logSweepError(tableRef, startRow, batchConfig, e);
            throw e;
        }
    }

    private void logSweepPerformance(TableReference tableRef, byte[] startRow, SweepResults results) {
        log.info(
                "Analyzed {} cell+timestamp pairs"
                        + " from table {}"
                        + " starting at row {}"
                        + " and deleted {} stale values"
                        + " in {} ms"
                        + " up to timestamp {}.",
                SafeArg.of("cellTs pairs examined", results.getCellTsPairsExamined()),
                LoggingArgs.tableRef("tableRef", tableRef),
                UnsafeArg.of("startRow", startRowToHex(startRow)),
                SafeArg.of("cellTs pairs deleted", results.getStaleValuesDeleted()),
                SafeArg.of("time taken", results.getTimeInMillis()),
                SafeArg.of("last swept timestamp", results.getMinSweptTimestamp()));

        SweepPerformanceResults performanceResults = SweepPerformanceResults.builder()
                .sweepResults(results)
                .tableName(tableRef.getQualifiedName())
                .elapsedMillis(results.getTimeInMillis())
                .build();

        sweepPerfLogger.logSweepResults(performanceResults);
    }

    private void logSweepError(
            TableReference tableRef, byte[] startRow, SweepBatchConfig config, RuntimeException exception) {
        log.info(
                "Failed to sweep table {}"
                        + " at row {}"
                        + " with candidate batch size {},"
                        + " delete batch size {},"
                        + " and {} cell+timestamp pairs to examine.",
                LoggingArgs.tableRef("tableRef", tableRef),
                UnsafeArg.of("startRow", startRowToHex(startRow)),
                SafeArg.of("candidateBatchSize", config.candidateBatchSize()),
                SafeArg.of("deleteBatchSize", config.deleteBatchSize()),
                SafeArg.of("maxCellTsPairsToExamine", config.maxCellTsPairsToExamine()),
                exception);
    }

    private void processSweepResults(TableToSweep tableToSweep, SweepResults currentIteration) {
        updateTimeMetricsOneIteration(
                currentIteration.getTimeInMillis(), currentIteration.getTimeElapsedSinceStartedSweeping());

        SweepResults cumulativeResults = getCumulativeSweepResults(tableToSweep, currentIteration);

        if (currentIteration.getNextStartRow().isPresent()) {
            saveIntermediateSweepResults(tableToSweep, cumulativeResults);
        } else {
            processFinishedSweep(tableToSweep, cumulativeResults);
        }
    }

    private static SweepResults getCumulativeSweepResults(TableToSweep tableToSweep, SweepResults currentIteration) {
        return tableToSweep.getPreviousSweepResults().accumulateWith(currentIteration);
    }

    private void saveIntermediateSweepResults(TableToSweep tableToSweep, SweepResults results) {
        Preconditions.checkArgument(
                results.getNextStartRow().isPresent(),
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
                    .startColumn(PtBytes.toBytes("unused"))
                    .minimumSweptTimestamp(results.getMinSweptTimestamp())
                    .timeInMillis(results.getTimeInMillis())
                    .startTimeInMillis(results.getTimeSweepStarted())
                    .build();
            sweepProgressStore.saveProgress(newProgress);
            return null;
        });
    }

    private void processFinishedSweep(TableToSweep tableToSweep, SweepResults cumulativeResults) {
        saveFinalSweepResults(tableToSweep, cumulativeResults);
        log.info(
                "Finished sweeping table {}. Examined {} cell+timestamp pairs, deleted {} stale values. Time taken "
                        + "sweeping: {} ms, time elapsed since sweep first started on this table: {} ms.",
                LoggingArgs.tableRef("tableRef", tableToSweep.getTableRef()),
                SafeArg.of("cellTs pairs examined", cumulativeResults.getCellTsPairsExamined()),
                SafeArg.of("cellTs pairs deleted", cumulativeResults.getStaleValuesDeleted()),
                SafeArg.of("time sweeping table", cumulativeResults.getTimeInMillis()),
                SafeArg.of("time elapsed", cumulativeResults.getTimeElapsedSinceStartedSweeping()));
        tableToSweep.getSweepLock().close();
        sweepProgressStore.clearProgress(tableToSweep.getTableRef());
    }

    private void saveFinalSweepResults(TableToSweep tableToSweep, SweepResults finalSweepResults) {
        txManager.runTaskWithRetry((TxTask) tx -> {
            ImmutableUpdateSweepPriority.Builder update = ImmutableUpdateSweepPriority.builder()
                    .newStaleValuesDeleted(finalSweepResults.getStaleValuesDeleted())
                    .newCellTsPairsExamined(finalSweepResults.getCellTsPairsExamined())
                    .newLastSweepTimeMillis(wallClock.getTimeMillis())
                    .newMinimumSweptTimestamp(finalSweepResults.getMinSweptTimestamp());
            if (!tableToSweep.hasPreviousProgress()) {
                // This is the first (and only) set of results being written for this table.
                update.newWriteCount(0L);
            }
            sweepPriorityStore.update(tx, tableToSweep.getTableRef(), update.build());
            return null;
        });
    }

    private static String startRowToHex(@Nullable byte[] row) {
        if (row == null) {
            return "0";
        } else {
            return PtBytes.encodeHexString(row);
        }
    }

    void updateTimeMetricsOneIteration(long sweepTime, long totalTimeElapsedSweeping) {
        sweepMetrics.updateSweepTime(sweepTime, totalTimeElapsedSweeping);
    }

    void updateSweepErrorMetric() {
        sweepMetrics.sweepError();
    }
}
