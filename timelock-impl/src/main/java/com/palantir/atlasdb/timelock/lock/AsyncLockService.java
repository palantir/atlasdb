/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockTokenV2;

public class AsyncLockService {

    private static final Logger log = LoggerFactory.getLogger(AsyncLockService.class);

    private final LockCollection locks;
    private final LockAcquirer lockAcquirer;
    private final ScheduledExecutorService reaperExecutor;
    private final HeldLocksCollection heldLocks;
    private final ImmutableTimestampTracker immutableTsTracker;

    public static AsyncLockService createDefault(ScheduledExecutorService reaperExecutor,
            ScheduledExecutorService cancellationExecutor) {
        DelayedExecutor canceller = new DelayedExecutor(cancellationExecutor, System::currentTimeMillis);
        return new AsyncLockService(
                new LockCollection(() -> new ExclusiveLock(canceller)),
                new ImmutableTimestampTracker(),
                new LockAcquirer(),
                new HeldLocksCollection(),
                reaperExecutor);
    }

    public AsyncLockService(
            LockCollection locks,
            ImmutableTimestampTracker immutableTimestampTracker,
            LockAcquirer acquirer,
            HeldLocksCollection heldLocks,
            ScheduledExecutorService reaperExecutor) {
        this.locks = locks;
        this.immutableTsTracker = immutableTimestampTracker;
        this.lockAcquirer = acquirer;
        this.heldLocks = heldLocks;
        this.reaperExecutor = reaperExecutor;

        scheduleExpiredLockReaper();
    }

    private void scheduleExpiredLockReaper() {
        reaperExecutor.scheduleAtFixedRate(() -> {
            try {
                heldLocks.removeExpired();
            } catch (Throwable t) {
                log.warn("Error while removing expired lock requests. Trying again on next iteration.", t);
            }
        }, 0, LeaseExpirationTimer.LEASE_TIMEOUT_MILLIS / 2, TimeUnit.MILLISECONDS);
    }

    public AsyncResult<LockTokenV2> lock(UUID requestId, Set<LockDescriptor> lockDescriptors, long deadlineMs) {
        return heldLocks.getExistingOrAcquire(
                requestId,
                () -> acquireLocks(requestId, lockDescriptors, deadlineMs));
    }

    public AsyncResult<LockTokenV2> lockImmutableTimestamp(UUID requestId, long timestamp) {
        return heldLocks.getExistingOrAcquire(
                requestId,
                () -> acquireImmutableTimestampLock(requestId, timestamp));
    }

    public AsyncResult<Void> waitForLocks(UUID requestId, Set<LockDescriptor> lockDescriptors, long deadlineMs) {
        OrderedLocks orderedLocks = locks.getAll(lockDescriptors);
        return lockAcquirer.waitForLocks(requestId, orderedLocks, deadlineMs);
    }

    public Optional<Long> getImmutableTimestamp() {
        return immutableTsTracker.getImmutableTimestamp();
    }

    private AsyncResult<HeldLocks> acquireLocks(UUID requestId, Set<LockDescriptor> lockDescriptors, long deadlineMs) {
        OrderedLocks orderedLocks = locks.getAll(lockDescriptors);
        return lockAcquirer.acquireLocks(requestId, orderedLocks, deadlineMs);
    }

    private AsyncResult<HeldLocks> acquireImmutableTimestampLock(UUID requestId, long timestamp) {
        AsyncLock immutableTsLock = immutableTsTracker.getLockFor(timestamp);
        return lockAcquirer.acquireLocks(requestId, OrderedLocks.fromSingleLock(immutableTsLock), Long.MAX_VALUE);
    }

    public boolean unlock(LockTokenV2 token) {
        return unlock(ImmutableSet.of(token)).contains(token);
    }

    public Set<LockTokenV2> unlock(Set<LockTokenV2> tokens) {
        return heldLocks.unlock(tokens);
    }

    public boolean refresh(LockTokenV2 token) {
        return refresh(ImmutableSet.of(token)).contains(token);
    }

    public Set<LockTokenV2> refresh(Set<LockTokenV2> tokens) {
        return heldLocks.refresh(tokens);
    }

}
