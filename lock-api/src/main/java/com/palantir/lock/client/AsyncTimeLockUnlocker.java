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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * thread that has scheduled the task, but the task has not yet retrieved the current contents of the queue.
 */
public class AsyncTimeLockUnlocker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AsyncTimeLockUnlocker.class);
    private static final int BACKPRESSURE = 1024;

    private final TimelockService timelockService;
    private final ExecutorService executorService;

    private final AtomicBoolean unlockIsScheduled = new AtomicBoolean(false);
    private final BlockingQueue<Set<LockToken>> outstandingLockTokens = new ArrayBlockingQueue<>(BACKPRESSURE);

    AsyncTimeLockUnlocker(TimelockService timelockService, ExecutorService executorService) {
        this.timelockService = timelockService;
        this.executorService = executorService;
    }

    /**
     * Adds all provided lock tokens to a queue to eventually be scheduled for unlocking.
     * Locks in the queue are unlocked asynchronously, and users must not depend on these locks being unlocked /
     * available for other users immediately.
     *
     * This may block, if continuing to buffer may cause memory pressure issues.
     *
     * @param tokens Lock tokens to schedule an unlock for.
     */
    public void enqueue(Set<LockToken> tokens) {
        try {
            outstandingLockTokens.put(tokens);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (unlockIsScheduled.compareAndSet(false, true)) {
            executorService.submit(this::unlockOutstanding);
        }
    }

    private void unlockOutstanding() {
        unlockIsScheduled.set(false);

        Set<LockToken> toUnlock = getOutstandingLockTokenSnapshot();
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

    private Set<LockToken> getOutstandingLockTokenSnapshot() {
        List<Set<LockToken>> drained = new ArrayList<>(outstandingLockTokens.size());
        outstandingLockTokens.drainTo(drained);
        int expected = drained.stream().mapToInt(Collection::size).sum();
        Set<LockToken> ret = new HashSet<>(expected);
        for (Set<LockToken> tokens : drained) {
            ret.addAll(tokens);
        }
        return ret;
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
