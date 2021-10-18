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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.sweep.metrics.SweepOutcome;
import com.palantir.atlasdb.sweep.metrics.SweepOutcomeMetrics;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriorityOverrideConfig;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.LockService;
import com.palantir.lock.SingleLockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class BackgroundSweepThread implements Runnable {
    private static final SafeLogger log = SafeLoggerFactory.get(BackgroundSweepThread.class);

    public static final String TABLE_LOCK_PREFIX = "sweep table";

    private final SweepOutcomeMetrics sweepOutcomeMetrics;
    private final SpecificTableSweeper specificTableSweeper;
    private final AdjustableSweepBatchConfigSource sweepBatchConfigSource;
    private final Supplier<Long> sweepPauseMillis;
    private final Supplier<Boolean> isSweepEnabled;
    private final Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig;
    private final NextTableToSweepProvider nextTableToSweepProvider;
    private final CountDownLatch shuttingDown;
    private final LockService lockService;
    private final int threadIndex;

    private Optional<TableToSweep> currentTable = Optional.empty();

    // VisibleForTesting - used in internal test code
    public static BackgroundSweepThread createForTests(
            LockService lockService,
            NextTableToSweepProvider nextTableToSweepProvider,
            AdjustableSweepBatchConfigSource sweepBatchConfigSource,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig,
            SpecificTableSweeper specificTableSweeper,
            MetricsManager metricsManager) {
        return new BackgroundSweepThread(
                lockService,
                nextTableToSweepProvider,
                sweepBatchConfigSource,
                isSweepEnabled,
                sweepPauseMillis,
                sweepPriorityOverrideConfig,
                specificTableSweeper,
                SweepOutcomeMetrics.registerLegacy(metricsManager),
                new CountDownLatch(1),
                1);
    }

    BackgroundSweepThread(
            LockService lockService,
            NextTableToSweepProvider nextTableToSweepProvider,
            AdjustableSweepBatchConfigSource sweepBatchConfigSource,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig,
            SpecificTableSweeper specificTableSweeper,
            SweepOutcomeMetrics sweepOutcomeMetrics,
            CountDownLatch shuttingDown,
            int threadIndex) {
        this.specificTableSweeper = specificTableSweeper;
        this.sweepOutcomeMetrics = sweepOutcomeMetrics;
        this.sweepBatchConfigSource = sweepBatchConfigSource;
        this.sweepPauseMillis = sweepPauseMillis;
        this.isSweepEnabled = isSweepEnabled;
        this.sweepPriorityOverrideConfig = sweepPriorityOverrideConfig;
        this.nextTableToSweepProvider = nextTableToSweepProvider;
        this.shuttingDown = shuttingDown;
        this.lockService = lockService;
        this.threadIndex = threadIndex;
    }

    @Override
    public void run() {
        try (SingleLockService locks = createSweepLocks()) {
            // Wait a while before starting so short lived clis don't try to sweep.
            waitUntilSpecificTableSweeperIsInitialized();
            sleepFor(getBackoffTimeWhenSweepHasNotRun());
            log.info("Starting background sweeper with thread index {}", SafeArg.of("threadIndex", threadIndex));
            while (true) {
                // InterruptedException might be wrapped in RuntimeException (i.e. AtlasDbDependencyException),
                // which would be caught downstream.
                // We throw InterruptedException here to register that BackgroundSweeper was shutdown
                // on the catch block.
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("The background sweeper thread is interrupted.");
                }

                Optional<SweepOutcome> maybeOutcome = checkConfigAndRunSweep(locks);

                maybeOutcome.ifPresent(outcome -> {
                    logOutcome(outcome);
                    updateBatchSize(outcome);
                    updateMetrics(outcome);
                });

                sleepUntilNextRun(maybeOutcome);
            }
        } catch (InterruptedException e) {
            log.info("Shutting down background sweeper. Please restart the service to rerun background sweep.", e);
            closeTableLockIfHeld();
        } catch (Throwable t) {
            log.error(
                    "BackgroundSweeper failed fatally and will not rerun until restarted: {}",
                    UnsafeArg.of("message", t.getMessage()),
                    t);
            closeTableLockIfHeld();
            sweepOutcomeMetrics.registerOccurrenceOf(SweepOutcome.FATAL);
        }
    }

    private void waitUntilSpecificTableSweeperIsInitialized() throws InterruptedException {
        while (!specificTableSweeper.isInitialized()) {
            log.info("Sweep Priority Table and Sweep Progress Table are not initialized yet. If you have enabled "
                    + "asynchronous initialization, these tables are being initialized asynchronously. Background "
                    + "sweeper will start once the initialization is complete.");
            sleepFor(getBackoffTimeWhenSweepHasNotRun());
        }
    }

    private void logOutcome(SweepOutcome outcome) {
        if (outcome.equals(SweepOutcome.UNABLE_TO_ACQUIRE_LOCKS)) {
            log.info(
                    "Sweep iteration finished with outcome: {}. This means that sweep is running elsewhere. "
                            + "If the lock was in fact leaked, then it should expire within {} seconds (this can be "
                            + "overridden by defaultLockTimeoutSeconds in config), after which "
                            + "time one node should be able to grab the lock. "
                            + "If all nodes in an HA setup report this outcome for longer than expected, "
                            + "then another cluster may be connecting to the same Cassandra keyspace.",
                    SafeArg.of("sweepOutcome", outcome),
                    SafeArg.of("defaultLockTimeoutSeconds", AtlasDbConstants.DEFAULT_LOCK_TIMEOUT_SECONDS));
        } else {
            log.info("Sweep iteration finished with outcome: {}", SafeArg.of("sweepOutcome", outcome));
        }
    }

    private void updateBatchSize(SweepOutcome outcome) {
        if (outcome == SweepOutcome.SUCCESS) {
            sweepBatchConfigSource.increaseMultiplier();
        }
        if (outcome == SweepOutcome.ERROR) {
            sweepBatchConfigSource.decreaseMultiplier();
        }
    }

    private void updateMetrics(SweepOutcome outcome) {
        sweepOutcomeMetrics.registerOccurrenceOf(outcome);
    }

    private void sleepUntilNextRun(Optional<SweepOutcome> maybeOutcome) throws InterruptedException {
        sleepFor(getSleepDuration(maybeOutcome));
    }

    private Duration getSleepDuration(Optional<SweepOutcome> maybeOutcome) {
        return maybeOutcome
                .flatMap(outcome -> {
                    if (outcome == SweepOutcome.SUCCESS) {
                        return Optional.of(Duration.ofMillis(sweepPauseMillis.get()));
                    } else if (outcome == SweepOutcome.NOTHING_TO_SWEEP) {
                        return Optional.of(getBackoffTimeWhenNothingToSweep());
                    } else {
                        return Optional.empty();
                    }
                })
                .orElseGet(this::getBackoffTimeWhenSweepHasNotRun);
    }

    @VisibleForTesting
    Optional<SweepOutcome> checkConfigAndRunSweep(SingleLockService locks) throws InterruptedException {
        if (isSweepEnabled.get()) {
            return Optional.of(grabLocksAndRun(locks));
        }

        log.debug("Skipping background sweep because it is currently disabled. Note that AtlasDB automatically"
                + " disables background sweep if targeted sweep is fully enabled (i.e. both writes to the queue"
                + " and reading from the queue are enabled); if you still want to run Background Sweep under these"
                + " circumstances, please explicitly enable Background Sweep in configuration. ");
        closeTableLockIfHeld();
        return Optional.empty();
    }

    private SweepOutcome grabLocksAndRun(SingleLockService locks) throws InterruptedException {
        try {
            locks.lockOrRefresh();
            if (locks.haveLocks()) {
                return runOnce();
            } else {
                log.debug("Skipping sweep because sweep is running elsewhere.");
                closeTableLockIfHeld();
                return SweepOutcome.UNABLE_TO_ACQUIRE_LOCKS;
            }
        } catch (RuntimeException e) {
            specificTableSweeper.updateSweepErrorMetric();

            log.info("Sweep failed", e);
            return SweepOutcome.ERROR;
        }
    }

    private Duration getBackoffTimeWhenSweepHasNotRun() {
        return Duration.ofMillis(sweepPauseMillis.get()).plusSeconds(1).multipliedBy(20); // 2 minutes by default
    }

    private Duration getBackoffTimeWhenNothingToSweep() {
        return Duration.ofMinutes(10);
    }

    @VisibleForTesting
    SweepOutcome runOnce() {
        Optional<TableToSweep> tableToSweep = getTableToSweep();
        if (!tableToSweep.isPresent()) {
            // Don't change this log statement. It's parsed by test automation code.
            log.debug("Skipping sweep because no table has enough new writes to be worth sweeping at the moment.");
            return SweepOutcome.NOTHING_TO_SWEEP;
        }

        SweepBatchConfig batchConfig = sweepBatchConfigSource.getAdjustedSweepConfig();
        try {
            specificTableSweeper.runOnceAndSaveResults(tableToSweep.get(), batchConfig);
            return SweepOutcome.SUCCESS;
        } catch (InsufficientConsistencyException e) {
            log.info("Could not sweep because not all nodes of the database are online.", e);
            return SweepOutcome.NOT_ENOUGH_DB_NODES_ONLINE;
        } catch (RuntimeException e) {
            specificTableSweeper.updateSweepErrorMetric();

            return determineCauseOfFailure(e, tableToSweep.get());
        }
    }

    private Optional<TableToSweep> getTableToSweep() {
        return specificTableSweeper.getTxManager().runTaskWithRetry(tx -> {
            Optional<SweepProgress> progress = currentTable.flatMap(tableToSweep ->
                    specificTableSweeper.getSweepProgressStore().loadProgress(tableToSweep.getTableRef()));
            SweepPriorityOverrideConfig overrideConfig = sweepPriorityOverrideConfig.get();
            if (progress.map(realProgress -> shouldContinueSweepingCurrentTable(realProgress, overrideConfig))
                    .orElse(false)) {
                try {
                    // If we're here, currentTable exists and we're going to sweep it again this iteration
                    updateProgressAndRefreshLock(progress.get());
                    return currentTable;
                } catch (InterruptedException ex) {
                    log.info(
                            "Sweep lost the lock for table {}",
                            LoggingArgs.tableRef(progress.get().tableRef()),
                            ex);
                    closeTableLockIfHeld();
                    currentTable = Optional.empty();
                    // We'll fall through and choose a new table
                }
            }

            log.info("Sweep is choosing a new table to sweep.");
            closeTableLockIfHeld();
            return getNextTableToSweep(tx, overrideConfig);
        });
    }

    @SuppressWarnings("ConstantConditions") // class runs in a single thread, so this is fine
    private void updateProgressAndRefreshLock(SweepProgress progress) throws InterruptedException {
        currentTable.get().setProgress(progress);
        currentTable.get().refreshLock();
    }

    private boolean shouldContinueSweepingCurrentTable(
            SweepProgress progress, SweepPriorityOverrideConfig overrideConfig) {
        String currentTableName = progress.tableRef().getQualifiedName();
        if (overrideConfig.priorityTables().isEmpty()) {
            return !overrideConfig.blacklistTables().contains(currentTableName);
        }
        return overrideConfig.priorityTables().contains(currentTableName);
    }

    private Optional<TableToSweep> getNextTableToSweep(Transaction tx, SweepPriorityOverrideConfig overrideConfig) {
        Optional<TableToSweep> nextTableToSweep = nextTableToSweepProvider.getNextTableToSweep(
                tx, specificTableSweeper.getSweepRunner().getConservativeSweepTimestamp(), overrideConfig);

        if (nextTableToSweep.isPresent()) {
            // Check if we're resuming this table after a previous sweep
            nextTableToSweep = augmentWithProgress(nextTableToSweep.get());
            currentTable = nextTableToSweep;
        }

        return nextTableToSweep;
    }

    private Optional<TableToSweep> augmentWithProgress(TableToSweep nextTableWithoutProgress) {
        Optional<SweepProgress> sweepProgress =
                specificTableSweeper.getSweepProgressStore().loadProgress(nextTableWithoutProgress.getTableRef());

        if (sweepProgress.isPresent()) {
            TableToSweep nextTableWithProgress = TableToSweep.continueSweeping(
                    nextTableWithoutProgress.getTableRef(),
                    nextTableWithoutProgress.getSweepLock(),
                    sweepProgress.get());
            return Optional.of(nextTableWithProgress);
        }

        return Optional.of(nextTableWithoutProgress);
    }

    private SweepOutcome determineCauseOfFailure(Exception originalException, TableToSweep tableToSweep) {
        try {
            Set<TableReference> tables = specificTableSweeper.getKvs().getAllTableNames();

            if (!tables.contains(tableToSweep.getTableRef())) {
                clearSweepProgress(tableToSweep.getTableRef());
                log.info("The table being swept by the background sweeper was dropped, moving on...");
                tableToSweep.getSweepLock().close();
                return SweepOutcome.TABLE_DROPPED_WHILE_SWEEPING;
            }

            log.info(
                    "The background sweep job failed unexpectedly; will retry with a lower batch size...",
                    originalException);
            return SweepOutcome.ERROR;

        } catch (RuntimeException newE) {
            log.warn("Sweep failed", originalException);
            log.warn("Failed to check whether the table being swept was dropped. Retrying...", newE);
            return SweepOutcome.ERROR;
        }
    }

    private void clearSweepProgress(TableReference tableRef) {
        specificTableSweeper.getSweepProgressStore().clearProgress(tableRef);
    }

    private void sleepFor(Duration duration) throws InterruptedException {
        if (shuttingDown.await(duration.toNanos(), TimeUnit.NANOSECONDS)) {
            throw new InterruptedException();
        }
    }

    @VisibleForTesting
    SingleLockService createSweepLocks() {
        return SingleLockService.createSingleLockServiceWithSafeLockId(lockService, "atlas sweep " + threadIndex);
    }

    private void closeTableLockIfHeld() {
        currentTable.ifPresent(table -> table.getSweepLock().close());
    }
}
