/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.sweep.priority.ImmutableUpdateSweepPriority;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProviderImpl;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStore;
import com.palantir.atlasdb.sweep.progress.ImmutableSweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.sweep.progress.SweepProgressStore;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.Throwables;
import com.palantir.common.time.Clock;
import com.palantir.lock.RemoteLockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public final class BackgroundSweeperImpl implements BackgroundSweeper {
    private static final Logger log = LoggerFactory.getLogger(BackgroundSweeperImpl.class);
    private final TransactionManager txManager;
    private final RemoteLockService lockService;
    private final KeyValueService kvs;
    private final SweepProgressStore sweepProgressStore;
    private final SweepPriorityStore sweepPriorityStore;
    private final NextTableToSweepProvider nextTableToSweepProvider;
    private final SweepTaskRunner sweepRunner;
    private final Supplier<Boolean> isSweepEnabled;
    private final Supplier<Long> sweepPauseMillis;
    private final Supplier<SweepBatchConfig> sweepBatchConfig;
    private final BackgroundSweeperPerformanceLogger sweepPerfLogger;
    private final SweepMetrics sweepMetrics;
    private final PersistentLockManager persistentLockManager;
    private final Clock wallClock;

    private volatile double batchSizeMultiplier = 1.0;

    private Thread daemon;

    @VisibleForTesting
    BackgroundSweeperImpl(
            TransactionManager txManager,
            RemoteLockService lockService,
            KeyValueService kvs,
            SweepProgressStore sweepProgressStore,
            SweepPriorityStore sweepPriorityStore,
            NextTableToSweepProvider nextTableToSweepProvider,
            SweepTaskRunner sweepRunner,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            Supplier<SweepBatchConfig> sweepBatchConfig,
            BackgroundSweeperPerformanceLogger sweepPerfLogger,
            SweepMetrics sweepMetrics,
            PersistentLockManager persistentLockManager,
            Clock wallClock) {
        this.txManager = txManager;
        this.lockService = lockService;
        this.kvs = kvs;
        this.sweepProgressStore = sweepProgressStore;
        this.sweepPriorityStore = sweepPriorityStore;
        this.nextTableToSweepProvider = nextTableToSweepProvider;
        this.sweepRunner = sweepRunner;
        this.isSweepEnabled = isSweepEnabled;
        this.sweepPauseMillis = sweepPauseMillis;
        this.sweepBatchConfig = sweepBatchConfig;
        this.sweepPerfLogger = sweepPerfLogger;
        this.sweepMetrics = sweepMetrics;
        this.persistentLockManager = persistentLockManager;
        this.wallClock = wallClock;
    }

    public static BackgroundSweeperImpl create(
            LockAwareTransactionManager txManager,
            KeyValueService kvs,
            SweepTaskRunner sweepRunner,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            Supplier<SweepBatchConfig> sweepBatchConfig,
            SweepTableFactory tableFactory,
            BackgroundSweeperPerformanceLogger sweepPerfLogger,
            PersistentLockManager persistentLockManager) {
        SweepMetrics sweepMetrics = new SweepMetrics();
        SweepProgressStore sweepProgressStore = new SweepProgressStore(kvs, tableFactory);
        SweepPriorityStore sweepPriorityStore = new SweepPriorityStore(tableFactory);
        NextTableToSweepProvider nextTableToSweepProvider = new NextTableToSweepProviderImpl(kvs, sweepPriorityStore);
        return new BackgroundSweeperImpl(
                txManager,
                txManager.getLockService(),
                kvs,
                sweepProgressStore,
                sweepPriorityStore,
                nextTableToSweepProvider,
                sweepRunner,
                isSweepEnabled,
                sweepPauseMillis,
                sweepBatchConfig,
                sweepPerfLogger,
                sweepMetrics,
                persistentLockManager,
                System::currentTimeMillis);
    }

    @Override
    public synchronized void runInBackground() {
        Preconditions.checkState(daemon == null);
        daemon = new Thread(this);
        daemon.setDaemon(true);
        daemon.setName("BackgroundSweeper");
        daemon.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down persistent lock manager");
            try {
                persistentLockManager.shutdown();
                log.info("Shutdown complete!");
            } catch (Exception e) {
                log.warn("An exception occurred while shutting down. This means that we had the backup lock out when"
                        + "the shutdown was triggered, but failed to release it. If this is the case, sweep or backup"
                        + "may fail to take out the lock in future. If this happens consistently, "
                        + "consult the following documentation on how to release the dead lock: "
                        + "https://palantir.github.io/atlasdb/html/troubleshooting/index.html#clearing-the-backup-lock",
                        e);
            }
        }));
    }

    @Override
    public void run() {
        try (SweepLocks locks = createSweepLocks()) {
            // Wait a while before starting so short lived clis don't try to sweep.
            Thread.sleep(20 * (1000 + sweepPauseMillis.get()));
            log.info("Starting background sweeper.");
            while (true) {
                long millisToSleep = grabLocksAndRun(locks);
                Thread.sleep(millisToSleep);
            }
        } catch (InterruptedException e) {
            log.warn("Shutting down background sweeper. Please restart the service to rerun background sweep.");
        }
    }

    // Returns milliseconds to sleep
    @VisibleForTesting
    long grabLocksAndRun(SweepLocks locks) throws InterruptedException {
        boolean sweptSuccessfully = false;
        try {
            if (isSweepEnabled.get()) {
                locks.lockOrRefresh();
                if (locks.haveLocks()) {
                    sweptSuccessfully = runOnce();
                } else {
                    log.debug("Skipping sweep because sweep is running elsewhere.");
                }
            } else {
                log.debug("Skipping sweep because it is currently disabled.");
            }
        } catch (InsufficientConsistencyException e) {
            log.warn("Could not sweep because not all nodes of the database are online.", e);
        } catch (RuntimeException e) {
            sweepMetrics.sweepError();
            if (checkAndRepairTableDrop()) {
                log.info("The table being swept by the background sweeper was dropped, moving on...");
            } else {
                SweepBatchConfig lastBatchConfig = getAdjustedBatchConfig();
                log.warn("The background sweep job failed unexpectedly with batch config {}."
                                + " Attempting to continue with a lower batch size...",
                        SafeArg.of("cell batch size", lastBatchConfig),
                        e);
                // Cut batch size in half, always sweep at least one row (we round down).
                batchSizeMultiplier = Math.max(batchSizeMultiplier / 2, 1.5 / lastBatchConfig.candidateBatchSize());
            }
        }
        if (sweptSuccessfully) {
            batchSizeMultiplier = Math.min(1.0, batchSizeMultiplier * 1.01);
            return sweepPauseMillis.get();
        } else {
            return 20 * (1000 + sweepPauseMillis.get());
        }
    }

    @VisibleForTesting
    boolean runOnce() {
        Optional<TableToSweep> tableToSweep = getTableToSweep();
        if (!tableToSweep.isPresent()) {
            // Don't change this log statement. It's parsed by test automation code.
            log.debug("Skipping sweep because no table has enough new writes to be worth sweeping at the moment.");
            return false;
        } else {
            runOnceForTable(tableToSweep.get());
            return true;
        }
    }

    private void runOnceForTable(TableToSweep tableToSweep) {
        Stopwatch watch = Stopwatch.createStarted();
        TableReference tableRef = tableToSweep.getTableRef();
        byte[] startRow = tableToSweep.getStartRow();
        SweepBatchConfig batchConfig = getAdjustedBatchConfig();
        try {
            SweepResults results = sweepRunner.run(
                    tableRef,
                    batchConfig,
                    startRow);
            long elapsedMillis = watch.elapsed(TimeUnit.MILLISECONDS);
            log.info("Swept {} unique cells from {} starting at {}"
                            + " and performed {} deletions in {} ms"
                            + " up to timestamp {}.",
                    SafeArg.of("cellsExamined", results.getCellTsPairsExamined()),
                    UnsafeArg.of("table name", tableRef),
                    UnsafeArg.of("start row hex", startRowToHex(startRow)),
                    SafeArg.of("staleValuesDeleted", results.getStaleValuesDeleted()),
                    SafeArg.of("elapsedMillis", elapsedMillis),
                    SafeArg.of("sweptTimestamp", results.getSweptTimestamp()));
            sweepPerfLogger.logSweepResults(
                    SweepPerformanceResults.builder()
                            .sweepResults(results)
                            .tableName(tableRef.getQualifiedName())
                            .elapsedMillis(elapsedMillis)
                            .build());
            saveSweepResults(tableToSweep, results);
        } catch (RuntimeException e) {
            // Error logged at a higher log level above.
            log.debug("Failed to sweep {} with batch config {} starting from row {}",
                    UnsafeArg.of("table name", tableRef),
                    SafeArg.of("cell batch size", batchConfig),
                    UnsafeArg.of("start row hex", startRowToHex(startRow)));
            throw e;
        }
    }

    private SweepBatchConfig getAdjustedBatchConfig() {
        SweepBatchConfig baseConfig = sweepBatchConfig.get();
        return ImmutableSweepBatchConfig.builder()
                .maxCellTsPairsToExamine(adjustBatchParameter(baseConfig.maxCellTsPairsToExamine()))
                .candidateBatchSize(adjustBatchParameter(baseConfig.candidateBatchSize()))
                .deleteBatchSize(adjustBatchParameter(baseConfig.deleteBatchSize()))
                .build();
    }

    private int adjustBatchParameter(int parameterValue) {
        return Math.max(1, (int) (batchSizeMultiplier * parameterValue));
    }

    private static String startRowToHex(@Nullable byte[] row) {
        if (row == null) {
            return "0";
        } else {
            return PtBytes.encodeHexString(row);
        }
    }

    private final class TableToSweep {
        private final TableReference tableRef;
        @Nullable private final SweepProgress progress;

        TableToSweep(TableReference tableRef, SweepProgress progress) {
            this.tableRef = tableRef;
            this.progress = progress;
        }

        TableReference getTableRef() {
            return tableRef;
        }

        boolean hasPreviousProgress() {
            return progress != null;
        }

        long getStaleValuesDeletedPreviously() {
            return progress == null ? 0L : progress.staleValuesDeleted();
        }

        long getCellsExaminedPreviously() {
            return progress == null ? 0L : progress.cellTsPairsExamined();
        }

        OptionalLong getPreviousMinimumSweptTimestamp() {
            return progress == null ? OptionalLong.empty() : OptionalLong.of(progress.minimumSweptTimestamp());
        }

        byte[] getStartRow() {
            return progress == null ? PtBytes.EMPTY_BYTE_ARRAY : progress.startRow();
        }
    }

    private Optional<TableToSweep> getTableToSweep() {
        return txManager.runTaskWithRetry(new TransactionTask<Optional<TableToSweep>, RuntimeException>() {
            @Override
            public Optional<TableToSweep> execute(Transaction tx) {
                Optional<SweepProgress> progress = sweepProgressStore.loadProgress(tx);
                if (progress.isPresent()) {
                    return Optional.of(new TableToSweep(progress.get().tableRef(), progress.get()));
                } else {
                    Optional<TableReference> nextTable = nextTableToSweepProvider.chooseNextTableToSweep(
                            tx, sweepRunner.getConservativeSweepTimestamp());
                    if (nextTable.isPresent()) {
                        log.debug("Now starting to sweep {}.", UnsafeArg.of("table name", nextTable));
                        return Optional.of(new TableToSweep(nextTable.get(), null));
                    } else {
                        return Optional.empty();
                    }
                }
            }
        });
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
            log.info("Finished sweeping {}, examined {} unique cells, deleted {} stale values.",
                    UnsafeArg.of("table name", tableToSweep.getTableRef()),
                    SafeArg.of("cellsExamined", cellsExamined),
                    SafeArg.of("staleValuesDeleted", staleValuesDeleted));
            sweepProgressStore.clearProgress();
        }
    }

    private void performInternalCompactionIfNecessary(TableReference tableRef, SweepResults results) {
        if (results.getStaleValuesDeleted() > 0) {
            Stopwatch watch = Stopwatch.createStarted();
            kvs.compactInternally(tableRef);
            long elapsedMillis = watch.elapsed(TimeUnit.MILLISECONDS);
            log.debug("Finished performing compactInternally on {} in {} ms.",
                    UnsafeArg.of("table name", tableRef),
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

        sweepMetrics.examinedCells(tableToSweep.getTableRef(), sweepResults.getCellTsPairsExamined());
        sweepMetrics.deletedCells(tableToSweep.getTableRef(), sweepResults.getStaleValuesDeleted());
    }

    /**
     * Check whether the table being swept was dropped. If so, stop sweeping it and move on.
     * @return Whether the table being swept was dropped
     */
    private boolean checkAndRepairTableDrop() {
        try {
            Set<TableReference> tables = kvs.getAllTableNames();
            Optional<SweepProgress> progress = txManager.runTaskReadOnly(sweepProgressStore::loadProgress);
            if (!progress.isPresent() || tables.contains(progress.get().tableRef())) {
                return false;
            } else {
                sweepProgressStore.clearProgress();
                return true;
            }
        } catch (RuntimeException e) {
            log.error("Failed to check whether the table being swept was dropped."
                    + " Continuing under the assumption that it wasn't...", e);
            return false;
        }
    }

    @VisibleForTesting
    SweepLocks createSweepLocks() {
        return new SweepLocks(lockService);
    }

    @Override
    public synchronized void shutdown() {
        if (daemon == null) {
            return;
        }
        log.debug("Signalling background sweeper to shut down.");
        daemon.interrupt();
        try {
            daemon.join();
            daemon = null;
        } catch (InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

}
