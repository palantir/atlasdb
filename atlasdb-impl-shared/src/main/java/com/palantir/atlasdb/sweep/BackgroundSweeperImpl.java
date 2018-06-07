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

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriorityOverrideConfig;
import com.palantir.common.base.Throwables;
import com.palantir.lock.LockService;
import com.palantir.lock.SingleLockService;

public final class BackgroundSweeperImpl implements BackgroundSweeper, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BackgroundSweeperImpl.class);

    private static final long MAX_DAEMON_CLEAN_SHUTDOWN_TIME_MILLIS = 10_000;

    private final PersistentLockManager persistentLockManager;
    private final SweepOutcomeMetrics sweepOutcomeMetrics = new SweepOutcomeMetrics();
    private final BackgroundSweepThread backgroundSweepThread;

    private Thread daemon;

    private final CountDownLatch shuttingDown = new CountDownLatch(1);

    @VisibleForTesting
    BackgroundSweeperImpl(
            LockService lockService,
            NextTableToSweepProvider nextTableToSweepProvider,
            AdjustableSweepBatchConfigSource sweepBatchConfigSource,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig,
            PersistentLockManager persistentLockManager,
            SpecificTableSweeper specificTableSweeper) {
        this.persistentLockManager = persistentLockManager;

        this.backgroundSweepThread = new BackgroundSweepThread(lockService, nextTableToSweepProvider,
                sweepBatchConfigSource, isSweepEnabled, sweepPauseMillis, sweepPriorityOverrideConfig,
                specificTableSweeper, sweepOutcomeMetrics, shuttingDown);
    }

    public static BackgroundSweeperImpl create(
            AdjustableSweepBatchConfigSource sweepBatchConfigSource,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Long> sweepPauseMillis,
            Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig,
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
                sweepPriorityOverrideConfig,
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

    @Override
    public void run() {
        backgroundSweepThread.run();
    }

    @VisibleForTesting
    SweepOutcome checkConfigAndRunSweep(SingleLockService locks) throws InterruptedException {
        return backgroundSweepThread.checkConfigAndRunSweep(locks);
    }

    @VisibleForTesting
    SweepOutcome runOnce() {
        return backgroundSweepThread.runOnce();
    }

    @VisibleForTesting
    SingleLockService createSweepLocks() {
        return backgroundSweepThread.createSweepLocks();
    }

}
