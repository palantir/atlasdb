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

import com.google.common.collect.Sets;
import com.palantir.atlasdb.sweep.metrics.SweepOutcomeMetrics;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProvider;
import com.palantir.atlasdb.sweep.priority.SweepPriorityOverrideConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.Throwables;
import com.palantir.lock.LockService;
import com.palantir.logsafe.Preconditions;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final SpecificTableSweeper specificTableSweeper;
    private final SweepOutcomeMetrics sweepOutcomeMetrics;

    private BackgroundSweeperImpl(
            MetricsManager metricsManager,
            LockService lockService,
            NextTableToSweepProvider nextTableToSweepProvider,
            AdjustableSweepBatchConfigSource sweepBatchConfigSource,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Integer> sweepThreads,
            Supplier<Long> sweepPauseMillis,
            Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig,
            SpecificTableSweeper specificTableSweeper) {
        this.sweepOutcomeMetrics = SweepOutcomeMetrics.registerLegacy(metricsManager);
        this.lockService = lockService;
        this.nextTableToSweepProvider = nextTableToSweepProvider;
        this.sweepBatchConfigSource = sweepBatchConfigSource;
        this.isSweepEnabled = isSweepEnabled;
        this.sweepThreads = sweepThreads;
        this.sweepPauseMillis = sweepPauseMillis;
        this.sweepPriorityOverrideConfig = sweepPriorityOverrideConfig;
        this.specificTableSweeper = specificTableSweeper;
    }

    public static BackgroundSweeperImpl create(
            MetricsManager metricsManager,
            AdjustableSweepBatchConfigSource sweepBatchConfigSource,
            Supplier<Boolean> isSweepEnabled,
            Supplier<Integer> sweepThreads,
            Supplier<Long> sweepPauseMillis,
            Supplier<SweepPriorityOverrideConfig> sweepPriorityOverrideConfig,
            SpecificTableSweeper specificTableSweeper) {
        NextTableToSweepProvider nextTableToSweepProvider = NextTableToSweepProvider.create(
                specificTableSweeper.getKvs(),
                specificTableSweeper.getTxManager().getLockService(),
                specificTableSweeper.getSweepPriorityStore());

        return new BackgroundSweeperImpl(
                metricsManager,
                specificTableSweeper.getTxManager().getLockService(),
                nextTableToSweepProvider,
                sweepBatchConfigSource,
                isSweepEnabled,
                sweepThreads,
                sweepPauseMillis,
                sweepPriorityOverrideConfig,
                specificTableSweeper);
    }

    @Override
    public synchronized void runInBackground() {
        Preconditions.checkState(daemons == null);
        int numThreads = sweepThreads.get();
        daemons = Sets.newHashSetWithExpectedSize(numThreads);

        for (int idx = 1; idx <= numThreads; idx++) {
            BackgroundSweepThread backgroundSweepThread = new BackgroundSweepThread(
                    lockService,
                    nextTableToSweepProvider,
                    sweepBatchConfigSource,
                    isSweepEnabled,
                    sweepPauseMillis,
                    sweepPriorityOverrideConfig,
                    specificTableSweeper,
                    sweepOutcomeMetrics,
                    shuttingDown,
                    idx);

            Thread daemon = new Thread(backgroundSweepThread);
            daemon.setDaemon(true);
            daemon.setName("BackgroundSweeper " + idx);
            daemon.start();

            daemons.add(daemon);
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    @Override
    public synchronized void shutdown() {
        if (daemons == null) {
            return;
        }
        log.info("Signalling background sweepers to shut down.");
        // Interrupt the daemon, whatever lock it may be waiting on.
        daemons.forEach(Thread::interrupt);
        // Ensure we do not accidentally abort shutdown if any code incorrectly swallows InterruptedExceptions
        // on the daemon thread.
        shuttingDown.countDown();

        verifyDaemonsInterrupted();
        daemons = null;
    }

    private void verifyDaemonsInterrupted() {
        int interruptedThreads = 0;
        InterruptedException lastException = null;
        for (Thread daemon : daemons) {
            try {
                daemon.join(MAX_DAEMON_CLEAN_SHUTDOWN_TIME_MILLIS);
                if (daemon.isAlive()) {
                    log.error("Background sweep thread failed to shut down");
                }
            } catch (InterruptedException e) {
                interruptedThreads++;
                lastException = e;
            }
        }

        if (lastException != null) {
            Thread.currentThread().interrupt();
            RuntimeException ex =
                    new RuntimeException(interruptedThreads + " threads were interrupted.", lastException);
            throw Throwables.rewrapAndThrowUncheckedException(ex);
        }
    }
}
