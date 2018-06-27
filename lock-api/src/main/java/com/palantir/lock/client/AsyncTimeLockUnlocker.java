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

package com.palantir.lock.client;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 * Concurrency: We want to guarantee that a token T that is enqueued is included in some call to unlockOutstanding.
 * If T can pass the compareAndSet, then T itself is scheduled. If T does not, that means there is some other
 * thread that has scheduled the task, but the task has not retrieved the reference to the set of tokens to be
 * unlocked (because it re-sets unlockIsScheduled to true first, before extracting the reference to the set).
 */
public class AsyncTimeLockUnlocker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AsyncTimeLockUnlocker.class);

    private final TimelockService timelockService;
    private final ScheduledExecutorService scheduledExecutorService;

    private final AtomicBoolean unlockIsScheduled = new AtomicBoolean(false);

    // Fairness incurs a performance penalty but we do not want to starve the actual unlocking process.
    private final VisibleReadWriteLock readWriteLock = new VisibleReadWriteLock(new ReentrantReadWriteLock(true));

    private Set<LockToken> outstandingLockTokens = Sets.newConcurrentHashSet();
    private volatile int visibilitySignal = 0;

    AsyncTimeLockUnlocker(TimelockService timelockService, ScheduledExecutorService scheduledExecutorService) {
        this.timelockService = timelockService;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    /**
     * Adds all provided lock tokens to a queue to eventually be scheduled for unlocking.
     * Locks in the queue are unlocked asynchronously, and users must not depend on these locks being unlocked /
     * unlockIsScheduled for other users immediately.
     *
     * @param tokens Lock tokens to schedule an unlock for.
     */
    public void enqueue(Set<LockToken> tokens) {
        // addAll() can run safely in parallel because the set is a concurrent set.
        readWriteLock.readLock();
        try {
            outstandingLockTokens.addAll(tokens);
            visibilitySignal = 1;
        } finally {
            readWriteLock.readUnlock();
        }

        if (unlockIsScheduled.compareAndSet(false, true)) {
            scheduledExecutorService.submit(this::unlockOutstanding);
        }
    }

    private void unlockOutstanding() {
        unlockIsScheduled.set(false);

        Set<LockToken> toUnlock = getOutstandingLockTokenSnapshot();
        if (toUnlock.isEmpty()) {
            log.info("Not unlocking, because we don't believe there are any tokens to unlock.");
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

    private Set<LockToken> getOutstandingLockTokenSnapshot() {
        // Ensure that we acquire the lock for as short as possible (i.e. only 2 writes)
        Set<LockToken> toUnlock;
        Set<LockToken> newSet = Sets.newConcurrentHashSet();
        int local = 0;

        readWriteLock.writeLock();
        try {
            System.out.println(visibilitySignal);
            toUnlock = outstandingLockTokens;
            outstandingLockTokens = newSet;
        } finally {
            readWriteLock.writeUnlock();
        }

        return toUnlock;
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
    }
}
