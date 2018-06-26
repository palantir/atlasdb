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

package com.palantir.lock.client;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;

/**
 * Releases lock tokens from a {@link TimelockService} asynchronously.
 *
 * There is another layer of retrying below us (at the HTTP client level) for external timelock users.
 * Also, in the event we fail to unlock (e.g. because of a connection issue), locks will eventually time-out.
 * Thus not retrying is reasonably safe (as long as we can guarantee that the lock won't otherwise be refreshed).
 */
public class AsyncTimeLockUnlocker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AsyncTimeLockUnlocker.class);

    private static final Duration KICK_JOB_INTERVAL = Duration.ofSeconds(1);

    private final TimelockService timelockService;
    private final ScheduledExecutorService scheduledExecutorService;

    private final AtomicBoolean available = new AtomicBoolean(true);
    private final AtomicReference<Set<LockToken>> outstandingLockTokens;

    AsyncTimeLockUnlocker(TimelockService timelockService, ScheduledExecutorService scheduledExecutorService) {
        this(timelockService, scheduledExecutorService, new AtomicReference<>(ImmutableSet.of()));
    }

    @VisibleForTesting
    AsyncTimeLockUnlocker(TimelockService timelockService,
            ScheduledExecutorService scheduledExecutorService,
            AtomicReference<Set<LockToken>> outstandingLockTokens) {
        this.timelockService = timelockService;
        this.scheduledExecutorService = scheduledExecutorService;
        this.outstandingLockTokens = outstandingLockTokens;
        schedulePeriodicKickJob();
    }

    /**
     * Adds all provided lock tokens to a queue to eventually be scheduled for unlocking.
     * Locks in the queue are unlocked asynchronously, and users must not depend on these locks being unlocked /
     * available for other users immediately.
     *
     * @param tokens Lock tokens to schedule an unlock for.
     */
    public void enqueue(Set<LockToken> tokens) {
        outstandingLockTokens.getAndAccumulate(tokens, Sets::union);
        scheduleIfNoTaskRunning();
    }

    private void scheduleIfNoTaskRunning() {
        if (available.compareAndSet(true, false)) {
            try {
                scheduledExecutorService.submit(this::unlockOutstanding);
            } finally {
                available.set(true);
            }
        }
    }

    private void unlockOutstanding() {
        Set<LockToken> toUnlock = outstandingLockTokens.getAndSet(ImmutableSet.of());
        if (toUnlock.isEmpty()) {
            return;
        }
        try {
            timelockService.tryUnlock(toUnlock);
        } catch (Throwable t) {
            log.info("Failed to unlock lock tokens {} from timelock. They will eventually expire on their own, but if"
                    + " this message recurs frequently, it may be worth investigation.",
                    SafeArg.of("lockTokens", toUnlock),
                    t);
        }
    }

    private void schedulePeriodicKickJob() {
        // This exists to handle a specific race, where transaction A adds itself to outstandingLockTokens
        // but an already running task in transaction B has read the tokens and is trying to unlock, and
        // then no transactions follow - the tokens registered by A will not unlock.
        // Under high continuous volume of transactions, this job is not important.
        // Also, it won't affect correctness as it is basically doing an empty-set enqueue.
        scheduledExecutorService.scheduleAtFixedRate(
                this::scheduleIfNoTaskRunning, 0, KICK_JOB_INTERVAL.getSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
    }
}
