/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriorityOverrideConfig;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.lock.LockService;
import com.palantir.lock.SingleLockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

class BackgroundSweepThread implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BackgroundSweepThread.class);
    private SweepOutcomeMetrics sweepOutcomeMetrics;
    private SpecificTableSweeper specificTableSweeper;
    private AdjustableSweepBatchConfigSource sweepBatchConfigSource;
    private Supplier<Long> sweepPauseMillis;
    private Supplier<Boolean> isSweepEnabled;
    private Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig;
    private NextTableToSweepProvider nextTableToSweepProvider;
    private CountDownLatch shuttingDown;
    private LockService lockService;

    BackgroundSweepThread(LockService lockService,
            NextTableToSweepProvider nextTableToSweepProvider,
            AdjustableSweepBatchConfigSource sweepBatchConfigSource,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig,
            SpecificTableSweeper specificTableSweeper,
            SweepOutcomeMetrics sweepOutcomeMetrics,
            CountDownLatch shuttingDown) {
        this.specificTableSweeper = specificTableSweeper;
        this.sweepOutcomeMetrics = sweepOutcomeMetrics;
        this.sweepBatchConfigSource = sweepBatchConfigSource;
        this.sweepPauseMillis = sweepPauseMillis;
        this.isSweepEnabled = isSweepEnabled;
        this.sweepPriorityOverrideConfig = sweepPriorityOverrideConfig;
        this.nextTableToSweepProvider = nextTableToSweepProvider;
        this.shuttingDown = shuttingDown;
        this.lockService = lockService;
    }

    @Override
    public void run() {
        try (SingleLockService locks = createSweepLocks()) {
            // Wait a while before starting so short lived clis don't try to sweep.
            waitUntilSpecificTableSweeperIsInitialized();
            sleepForMillis(getBackoffTimeWhenSweepHasNotRun());
            log.info("Starting background sweeper.");
            while (true) {
                // InterruptedException might be wrapped in RuntimeException (i.e. AtlasDbDependencyException),
                // which would be caught downstream.
                // We throw InterruptedException here to register that BackgroundSweeper was shutdown
                // on the catch block.
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("The background sweeper thread is interrupted.");
                }

                SweepOutcome outcome = checkConfigAndRunSweep(locks);

                log.info("Sweep iteration finished with outcome: {}",
                        SafeArg.of("sweepOutcome", outcome));

                updateBatchSize(outcome);
                updateMetrics(outcome);

                sleepUntilNextRun(outcome);
            }
        } catch (InterruptedException e) {
            log.warn(
                    "Shutting down background sweeper. Please restart the service to rerun background sweep.");
            sweepOutcomeMetrics.registerOccurrenceOf(
                    SweepOutcome.SHUTDOWN);
        } catch (Throwable t) {
            log.error("BackgroundSweeper failed fatally and will not rerun until restarted: {}",
                    UnsafeArg.of("message", t.getMessage()), t);
            sweepOutcomeMetrics.registerOccurrenceOf(
                    SweepOutcome.FATAL);
        }
    }

    private void waitUntilSpecificTableSweeperIsInitialized() throws InterruptedException {
        while (!specificTableSweeper.isInitialized()) {
            log.info("Sweep Priority Table and Sweep Progress Table are not initialized yet. If you have enabled "
                    + "asynchronous initialization, these tables are being initialized asynchronously. Background "
                    + "sweeper will start once the initialization is complete.");
            sleepForMillis(getBackoffTimeWhenSweepHasNotRun());
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

    private void sleepUntilNextRun(SweepOutcome outcome) throws InterruptedException {
        long sleepDurationMillis = getBackoffTimeWhenSweepHasNotRun();
        if (outcome == SweepOutcome.SUCCESS) {
            sleepDurationMillis = sweepPauseMillis.get();
        }
        sleepForMillis(sleepDurationMillis);
    }

    @VisibleForTesting
    SweepOutcome checkConfigAndRunSweep(SingleLockService locks) throws InterruptedException {
        if (isSweepEnabled.get()) {
            return grabLocksAndRun(locks);
        }

        log.debug("Skipping sweep because it is currently disabled.");
        return SweepOutcome.DISABLED;
    }

    private SweepOutcome grabLocksAndRun(SingleLockService locks) throws InterruptedException {
        try {
            locks.lockOrRefresh();
            if (locks.haveLocks()) {
                return runOnce();
            } else {
                log.debug("Skipping sweep because sweep is running elsewhere.");
                return SweepOutcome.UNABLE_TO_ACQUIRE_LOCKS;
            }
        } catch (RuntimeException e) {
            specificTableSweeper.updateSweepErrorMetric();

            log.error("Sweep failed", e);
            return SweepOutcome.ERROR;
        }
    }

    private long getBackoffTimeWhenSweepHasNotRun() {
        return 20 * (1000 + sweepPauseMillis.get());
    }

    @VisibleForTesting
    SweepOutcome runOnce() {
        Optional<TableToSweep> tableToSweep = getTableToSweep();
        if (!tableToSweep.isPresent()) {
            // Don't change this log statement. It's parsed by test automation code.
            log.debug(
                    "Skipping sweep because no table has enough new writes to be worth sweeping at the moment.");
            return SweepOutcome.NOTHING_TO_SWEEP;
        }

        SweepBatchConfig batchConfig = sweepBatchConfigSource.getAdjustedSweepConfig();
        try {
            specificTableSweeper.runOnceAndSaveResults(tableToSweep.get(), batchConfig);
            return SweepOutcome.SUCCESS;
        } catch (InsufficientConsistencyException e) {
            log.warn("Could not sweep because not all nodes of the database are online.", e);
            return SweepOutcome.NOT_ENOUGH_DB_NODES_ONLINE;
        } catch (RuntimeException e) {
            specificTableSweeper.updateSweepErrorMetric();

            return determineCauseOfFailure(e, tableToSweep.get());
        }
    }

    private Optional<TableToSweep> getTableToSweep() {
        return specificTableSweeper.getTxManager().runTaskWithRetry(
                tx -> {
                    Optional<SweepProgress> progress = specificTableSweeper.getSweepProgressStore().loadProgress();
                    SweepPriorityOverrideConfig overrideConfig = sweepPriorityOverrideConfig.get();
                    if (progress.map(realProgress -> shouldContinueSweepingCurrentTable(realProgress, overrideConfig))
                            .orElse(false)) {
                        return Optional.of(new TableToSweep(progress.get().tableRef(), progress));
                    } else {
                        log.info("Sweep is choosing a new table to sweep.");
                        Optional<TableReference> nextTable = getNextTableToSweep(tx, overrideConfig);
                        return nextTable.map(tableReference -> new TableToSweep(tableReference, Optional.empty()));
                    }
                });
    }

    private boolean shouldContinueSweepingCurrentTable(
            SweepProgress progress,
            SweepPriorityOverrideConfig overrideConfig) {
        String currentTableName = progress.tableRef().getQualifiedName();
        if (overrideConfig.priorityTables().isEmpty()) {
            return !overrideConfig.blacklistTables().contains(currentTableName);
        }
        return overrideConfig.priorityTables().contains(currentTableName);
    }

    private Optional<TableReference> getNextTableToSweep(Transaction tx, SweepPriorityOverrideConfig overrideConfig) {
        return nextTableToSweepProvider.getNextTableToSweep(
                        tx,
                        specificTableSweeper.getSweepRunner().getConservativeSweepTimestamp(),
                        overrideConfig);
    }

    private SweepOutcome determineCauseOfFailure(Exception originalException,
            TableToSweep tableToSweep) {
        try {
            Set<TableReference> tables = specificTableSweeper.getKvs().getAllTableNames();

            if (!tables.contains(tableToSweep.getTableRef())) {
                clearSweepProgress();
                log.info(
                        "The table being swept by the background sweeper was dropped, moving on...");
                return SweepOutcome.TABLE_DROPPED_WHILE_SWEEPING;
            }

            log.warn(
                    "The background sweep job failed unexpectedly; will retry with a lower batch size...",
                    originalException);
            return SweepOutcome.ERROR;

        } catch (RuntimeException newE) {
            log.error("Sweep failed", originalException);
            log.error("Failed to check whether the table being swept was dropped. Retrying...",
                    newE);
            return SweepOutcome.ERROR;
        }
    }

    private void clearSweepProgress() {
        specificTableSweeper.getSweepProgressStore().clearProgress();
    }

    private void sleepForMillis(long millis) throws InterruptedException {
        if (shuttingDown.await(millis, TimeUnit.MILLISECONDS)) {
            throw new InterruptedException();
        }
    }

    @VisibleForTesting
    SingleLockService createSweepLocks() {
        return SingleLockService.createSingleLockServiceWithSafeLockId(lockService, "atlas sweep");
    }
}
