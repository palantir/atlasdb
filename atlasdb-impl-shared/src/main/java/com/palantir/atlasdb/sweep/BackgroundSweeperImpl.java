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

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.Throwables;
import com.palantir.lock.LockService;
import com.palantir.lock.SingleLockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public final class BackgroundSweeperImpl implements BackgroundSweeper, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BackgroundSweeperImpl.class);

    private static final long MAX_DAEMON_CLEAN_SHUTDOWN_TIME_MILLIS = 10_000;

    private final LockService lockService;
    private final NextTableToSweepProvider nextTableToSweepProvider;
    private final AdjustableSweepBatchConfigSource sweepBatchConfigSource;
    private final Supplier<Boolean> isSweepEnabled;
    private final Supplier<Long> sweepPauseMillis;
    private final PersistentLockManager persistentLockManager;
    private final SpecificTableSweeper specificTableSweeper;

    private final SweepOutcomeMetrics sweepOutcomeMetrics = new SweepOutcomeMetrics();

    private Thread daemon;

    private final CountDownLatch shuttingDown = new CountDownLatch(1);

    @VisibleForTesting
    BackgroundSweeperImpl(
            LockService lockService,
            NextTableToSweepProvider nextTableToSweepProvider,
            AdjustableSweepBatchConfigSource sweepBatchConfigSource,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            PersistentLockManager persistentLockManager,
            SpecificTableSweeper specificTableSweeper) {
        this.lockService = lockService;
        this.nextTableToSweepProvider = nextTableToSweepProvider;
        this.sweepBatchConfigSource = sweepBatchConfigSource;
        this.isSweepEnabled = isSweepEnabled;
        this.sweepPauseMillis = sweepPauseMillis;
        this.persistentLockManager = persistentLockManager;
        this.specificTableSweeper = specificTableSweeper;
    }

    public static BackgroundSweeperImpl create(
            AdjustableSweepBatchConfigSource sweepBatchConfigSource,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            PersistentLockManager persistentLockManager,
            SpecificTableSweeper specificTableSweeper) {
        NextTableToSweepProvider nextTableToSweepProvider = NextTableToSweepProvider
                .create(specificTableSweeper.getKvs(), specificTableSweeper.getSweepPriorityStore());

        return new BackgroundSweeperImpl(
                specificTableSweeper.getTxManager().getLockService(),
                nextTableToSweepProvider,
                sweepBatchConfigSource,
                isSweepEnabled,
                sweepPauseMillis,
                persistentLockManager,
                specificTableSweeper);
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

                log.info("Sweep iteration finished with outcome: {}", SafeArg.of("sweepOutcome", outcome));

                updateBatchSize(outcome);
                updateMetrics(outcome);

                sleepUntilNextRun(outcome);
            }
        } catch (InterruptedException e) {
            log.warn("Shutting down background sweeper. Please restart the service to rerun background sweep.");
            sweepOutcomeMetrics.registerOccurrenceOf(SweepOutcome.SHUTDOWN);
        } catch (Throwable t) {
            log.error("BackgroundSweeper failed fatally and will not rerun until restarted: {}",
                    UnsafeArg.of("message", t.getMessage()), t);
            sweepOutcomeMetrics.registerOccurrenceOf(SweepOutcome.FATAL);
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
            log.debug("Skipping sweep because no table has enough new writes to be worth sweeping at the moment.");
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

    // there's a bug in older jdk8s around type inference here, don't make the same mistake two of us made
    // and try to lambda refactor this unless you live far enough in the future that this isn't an issue
    private Optional<TableToSweep> getTableToSweep() {
        return specificTableSweeper.getTxManager().runTaskWithRetry(
                tx -> {
                    Optional<SweepProgress> progress = specificTableSweeper.getSweepProgressStore().loadProgress();
                    if (progress.isPresent()) {
                        return Optional.of(new TableToSweep(progress.get().tableRef(), progress));
                    } else {
                        log.info("Sweep is choosing a new table to sweep.");
                        Optional<TableReference> nextTable = getNextTableToSweep(tx);
                        return nextTable.map(tableReference -> new TableToSweep(tableReference, Optional.empty()));
                    }
                });
    }

    private Optional<TableReference> getNextTableToSweep(Transaction tx) {
        return nextTableToSweepProvider
                .getNextTableToSweep(tx, specificTableSweeper.getSweepRunner().getConservativeSweepTimestamp());
    }

    private SweepOutcome determineCauseOfFailure(Exception originalException, TableToSweep tableToSweep) {
        try {
            Set<TableReference> tables = specificTableSweeper.getKvs().getAllTableNames();

            if (!tables.contains(tableToSweep.getTableRef())) {
                clearSweepProgress();
                log.info("The table being swept by the background sweeper was dropped, moving on...");
                return SweepOutcome.TABLE_DROPPED_WHILE_SWEEPING;
            }

            log.warn("The background sweep job failed unexpectedly; will retry with a lower batch size...",
                    originalException);
            return SweepOutcome.ERROR;

        } catch (RuntimeException newE) {
            log.error("Sweep failed", originalException);
            log.error("Failed to check whether the table being swept was dropped. Retrying...", newE);
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
        return new SingleLockService(lockService, "atlas sweep");
    }

    @Override
    public void close() {
        shutdown();
    }

    @Override
    public synchronized void shutdown() {
        sweepOutcomeMetrics.registerOccurrenceOf(SweepOutcome.SHUTDOWN);
        if (daemon == null) {
            return;
        }
        log.info("Signalling background sweeper to shut down.");
        // Interrupt the daemon, whatever lock it may be waiting on.
        daemon.interrupt();
        // Ensure we do not accidentally abort shutdown if any code incorrectly swallows InterruptedExceptions
        // on the daemon thread.
        shuttingDown.countDown();
        try {
            daemon.join(MAX_DAEMON_CLEAN_SHUTDOWN_TIME_MILLIS);
            if (daemon.isAlive()) {
                log.error("Background sweep thread failed to shut down");
            }
            daemon = null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public enum SweepOutcome {
        SUCCESS, NOTHING_TO_SWEEP, DISABLED, UNABLE_TO_ACQUIRE_LOCKS,
        NOT_ENOUGH_DB_NODES_ONLINE, TABLE_DROPPED_WHILE_SWEEPING, ERROR,
        SHUTDOWN, FATAL
    }

    private class SweepOutcomeMetrics {
        private final MetricsManager metricsManager = new MetricsManager();
        private final SlidingTimeWindowReservoir reservoir;

        private boolean shutdown;
        private boolean fatal;

        SweepOutcomeMetrics() {
            Arrays.stream(SweepOutcome.values()).forEach(outcome ->
                    metricsManager.registerMetric(BackgroundSweeperImpl.class, "outcome",
                            () -> getOutcomeCount(outcome), ImmutableMap.of("status", outcome.name())));
            reservoir = new SlidingTimeWindowReservoir(60L, TimeUnit.SECONDS);
            shutdown = false;
            fatal = false;
        }

        private Long getOutcomeCount(SweepOutcome outcome) {
            if (outcome == SweepOutcome.SHUTDOWN) {
                return shutdown ? 1L : 0L;
            }
            if (outcome == SweepOutcome.FATAL) {
                return fatal ? 1L : 0L;
            }

            return Arrays.stream(reservoir.getSnapshot().getValues())
                    .filter(l -> l == outcome.ordinal())
                    .count();
        }

        void registerOccurrenceOf(SweepOutcome outcome) {
            if (outcome == SweepOutcome.SHUTDOWN) {
                shutdown = true;
                return;
            }
            if (outcome == SweepOutcome.FATAL) {
                fatal = true;
                return;
            }

            reservoir.update(outcome.ordinal());
        }
    }
}
