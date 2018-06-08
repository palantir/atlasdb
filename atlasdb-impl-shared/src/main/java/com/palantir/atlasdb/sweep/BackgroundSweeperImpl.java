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

import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriorityOverrideConfig;
import com.palantir.common.base.Throwables;
import com.palantir.lock.LockService;

public final class BackgroundSweeperImpl implements BackgroundSweeper, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BackgroundSweeperImpl.class);

    private static final long MAX_DAEMON_CLEAN_SHUTDOWN_TIME_MILLIS = 10_000;

    // Thread management
    private final Supplier<Integer> sweepThreads;
    private Set<Thread> daemons;
    private final CountDownLatch shuttingDown = new CountDownLatch(1);

    // Shared between threads
    private final LockService lockService;
    private final NextTableToSweepProvider nextTableToSweepProvider;
    private final AdjustableSweepBatchConfigSource sweepBatchConfigSource;
    private final Supplier<Boolean> isSweepEnabled;
    private final Supplier<Long> sweepPauseMillis;
    private final Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig;
    private final PersistentLockManager persistentLockManager;
    private final SpecificTableSweeper specificTableSweeper;
    private final SweepOutcomeMetrics sweepOutcomeMetrics = new SweepOutcomeMetrics();

    private BackgroundSweeperImpl(
            LockService lockService,
            NextTableToSweepProvider nextTableToSweepProvider,
            AdjustableSweepBatchConfigSource sweepBatchConfigSource,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Integer> sweepThreads,
            Supplier<Long> sweepPauseMillis,
            Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig,
            PersistentLockManager persistentLockManager,
            SpecificTableSweeper specificTableSweeper) {
        this.lockService = lockService;
        this.nextTableToSweepProvider = nextTableToSweepProvider;
        this.sweepBatchConfigSource = sweepBatchConfigSource;
        this.isSweepEnabled = isSweepEnabled;
        this.sweepThreads = sweepThreads;
        this.sweepPauseMillis = sweepPauseMillis;
        this.sweepPriorityOverrideConfig = sweepPriorityOverrideConfig;
        this.persistentLockManager = persistentLockManager;
        this.specificTableSweeper = specificTableSweeper;
    }

    public static BackgroundSweeperImpl create(
            AdjustableSweepBatchConfigSource sweepBatchConfigSource,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Integer> sweepThreads,
            Supplier<Long> sweepPauseMillis,
            Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig,
            PersistentLockManager persistentLockManager,
            SpecificTableSweeper specificTableSweeper) {
        NextTableToSweepProvider nextTableToSweepProvider = NextTableToSweepProvider
                .create(specificTableSweeper.getKvs(),
                        specificTableSweeper.getTxManager().getLockService(),
                        specificTableSweeper.getSweepPriorityStore());

        return new BackgroundSweeperImpl(
                specificTableSweeper.getTxManager().getLockService(),
                nextTableToSweepProvider,
                sweepBatchConfigSource,
                isSweepEnabled,
                sweepThreads,
                sweepPauseMillis,
                sweepPriorityOverrideConfig,
                persistentLockManager,
                specificTableSweeper);
    }

    @Override
    public synchronized void runInBackground() {
        Preconditions.checkState(daemons == null);
        int numThreads = sweepThreads.get();
        daemons = Sets.newHashSetWithExpectedSize(numThreads);

        for (int idx = 1; idx <= numThreads; idx++) {
            BackgroundSweepThread backgroundSweepThread = new BackgroundSweepThread(lockService,
                    nextTableToSweepProvider,
                    sweepBatchConfigSource, isSweepEnabled, sweepPauseMillis, sweepPriorityOverrideConfig,
                    specificTableSweeper, sweepOutcomeMetrics, shuttingDown, idx);

            Thread daemon = new Thread(backgroundSweepThread);
            daemon.setDaemon(true);
            daemon.setName("BackgroundSweeper " + idx);
            daemon.start();

            daemons.add(daemon);
        }

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
        if (daemons == null) {
            return;
        }
        log.info("Signalling background sweepers to shut down.");
        // Interrupt the daemon, whatever lock it may be waiting on.
        daemons.forEach(Thread::interrupt);
        // Ensure we do not accidentally abort shutdown if any code incorrectly swallows InterruptedExceptions
        // on the daemon thread.
        shuttingDown.countDown();

        // TODO what happens if one of the daemons (but not the last one) causes this to throw?
        daemons.forEach(this::verifyInterrupted);
        daemons = null;
    }

    private void verifyInterrupted(Thread daemon) {
        try {
            daemon.join(MAX_DAEMON_CLEAN_SHUTDOWN_TIME_MILLIS);
            if (daemon.isAlive()) {
                log.error("Background sweep thread failed to shut down");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }
}
