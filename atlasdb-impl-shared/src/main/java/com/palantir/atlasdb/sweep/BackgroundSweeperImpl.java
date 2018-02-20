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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProviderImpl;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.base.Throwables;
import com.palantir.lock.LockService;
import com.palantir.lock.SingleLockService;
import com.palantir.logsafe.SafeArg;

public final class BackgroundSweeperImpl implements BackgroundSweeper {
    private static final Logger log = LoggerFactory.getLogger(BackgroundSweeperImpl.class);
    private final LockService lockService;
    private final NextTableToSweepProvider nextTableToSweepProvider;
    private final Supplier<Boolean> isSweepEnabled;
    private final Supplier<Long> sweepPauseMillis;
    private final PersistentLockManager persistentLockManager;
    private final SpecificTableSweeper specificTableSweeper;

    static volatile double batchSizeMultiplier = 1.0;

    private Thread daemon;

    @VisibleForTesting
    BackgroundSweeperImpl(
            LockService lockService,
            NextTableToSweepProvider nextTableToSweepProvider,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            PersistentLockManager persistentLockManager,
            SpecificTableSweeper specificTableSweeper) {
        this.lockService = lockService;
        this.nextTableToSweepProvider = nextTableToSweepProvider;
        this.isSweepEnabled = isSweepEnabled;
        this.sweepPauseMillis = sweepPauseMillis;
        this.persistentLockManager = persistentLockManager;
        this.specificTableSweeper = specificTableSweeper;
    }

    public static BackgroundSweeperImpl create(
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            PersistentLockManager persistentLockManager,
            SpecificTableSweeper specificTableSweeper) {
        NextTableToSweepProvider nextTableToSweepProvider = new NextTableToSweepProviderImpl(
                specificTableSweeper.getKvs(), specificTableSweeper.getSweepPriorityStore());
        return new BackgroundSweeperImpl(
                specificTableSweeper.getTxManager().getLockService(),
                nextTableToSweepProvider,
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
            Thread.sleep(getBackoffTimeWhenSweepHasNotRun());
            log.info("Starting background sweeper.");
            while (true) {
                long millisToSleep = checkConfigAndRunSweep(locks);
                Thread.sleep(millisToSleep);
            }
        } catch (InterruptedException e) {
            log.warn("Shutting down background sweeper. Please restart the service to rerun background sweep.");
        }
    }

    // Returns milliseconds to sleep
    @VisibleForTesting
    long checkConfigAndRunSweep(SingleLockService locks) throws InterruptedException {
        if (isSweepEnabled.get()) {
            return grabLocksAndRun(locks);
        } else {
            log.debug("Skipping sweep because it is currently disabled.");
        }
        return getBackoffTimeWhenSweepHasNotRun();
    }

    private long grabLocksAndRun(SingleLockService locks) throws InterruptedException {
        boolean sweptSuccessfully = false;
        try {
            locks.lockOrRefresh();
            if (locks.haveLocks()) {
                sweptSuccessfully = runOnce();
            } else {
                log.debug("Skipping sweep because sweep is running elsewhere.");
            }
        } catch (InsufficientConsistencyException e) {
            log.warn("Could not sweep because not all nodes of the database are online.", e);
        } catch (RuntimeException e) {
            specificTableSweeper.getSweepMetrics().sweepError();
            if (checkAndRepairTableDrop()) {
                log.info("The table being swept by the background sweeper was dropped, moving on...");
            } else {
                SweepBatchConfig lastBatchConfig = specificTableSweeper.getAdjustedBatchConfig();
                log.warn("The background sweep job failed unexpectedly with candidate batch size {},"
                                + " delete batch size {},"
                                + " and {} cell+timestamp pairs to examine."
                                + " Attempting to continue with a lower batch size...",
                        SafeArg.of("candidateBatchSize", lastBatchConfig.candidateBatchSize()),
                        SafeArg.of("deleteBatchSize", lastBatchConfig.deleteBatchSize()),
                        SafeArg.of("maxCellTsPairsToExamine", lastBatchConfig.maxCellTsPairsToExamine()),
                        e);
                // Cut batch size in half, always sweep at least one row (we round down).
                batchSizeMultiplier = Math.max(batchSizeMultiplier / 2, 1.5 / lastBatchConfig.candidateBatchSize());
            }
        }

        if (sweptSuccessfully) {
            batchSizeMultiplier = Math.min(1.0, batchSizeMultiplier * 1.01);
            return sweepPauseMillis.get();
        } else {
            return getBackoffTimeWhenSweepHasNotRun();
        }
    }

    private long getBackoffTimeWhenSweepHasNotRun() {
        return 20 * (1000 + sweepPauseMillis.get());
    }

    @VisibleForTesting
    boolean runOnce() {
        Optional<TableToSweep> tableToSweep = getTableToSweep();
        if (!tableToSweep.isPresent()) {
            // Don't change this log statement. It's parsed by test automation code.
            log.debug("Skipping sweep because no table has enough new writes to be worth sweeping at the moment.");
            return false;
        } else {
            specificTableSweeper.runOnceAndSaveResults(tableToSweep.get());
            return true;
        }
    }

    // there's a bug in older jdk8s around type inference here, don't make the same mistake two of us made
    // and try to lambda refactor this unless you live far enough in the future that this isn't an issue
    private Optional<TableToSweep> getTableToSweep() {
        return specificTableSweeper.getTxManager().runTaskWithRetry(
                new TransactionTask<Optional<TableToSweep>, RuntimeException>() {
                    @Override
                    public Optional<TableToSweep> execute(Transaction tx) {
                        Optional<SweepProgress> progress = specificTableSweeper.getSweepProgressStore().loadProgress();
                        if (progress.isPresent()) {
                            return Optional.of(new TableToSweep(progress.get().tableRef(), progress));
                        } else {
                            Optional<TableReference> nextTable = nextTableToSweepProvider.chooseNextTableToSweep(
                                    tx, specificTableSweeper.getSweepRunner().getConservativeSweepTimestamp());
                            if (nextTable.isPresent()) {
                                return Optional.of(new TableToSweep(nextTable.get(), Optional.empty()));
                            } else {
                                return Optional.empty();
                            }
                        }
                    }
                });
    }

    /**
     * Check whether the table being swept was dropped. If so, stop sweeping it and move on.
     *
     * @return Whether the table being swept was dropped
     */
    boolean checkAndRepairTableDrop() {
        try {
            Set<TableReference> tables = specificTableSweeper.getKvs().getAllTableNames();
            Optional<SweepProgress> progress = specificTableSweeper.getSweepProgressStore().loadProgress();
            if (!progress.isPresent() || tables.contains(progress.get().tableRef())) {
                return false;
            } else {
                specificTableSweeper.getSweepProgressStore().clearProgress();
                return true;
            }
        } catch (RuntimeException e) {
            log.error("Failed to check whether the table being swept was dropped."
                    + " Continuing under the assumption that it wasn't...", e);
            return false;
        }
    }

    @VisibleForTesting
    SingleLockService createSweepLocks() {
        return new SingleLockService(lockService, "atlas sweep");
    }

    @Override
    public synchronized void shutdown() {
        if (daemon == null) {
            return;
        }
        log.info("Signalling background sweeper to shut down.");
        daemon.interrupt();
        try {
            daemon.join();
            daemon = null;
        } catch (InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }
}
