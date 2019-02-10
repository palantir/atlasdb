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
package com.palantir.atlasdb.timelock.lock;

import java.io.Closeable;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.LeasableLockToken;
import com.palantir.lock.v2.LeasableRefreshLockResponse;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockToken;

public class AsyncLockService implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AsyncLockService.class);

    private final LockCollection locks;
    private final LockAcquirer lockAcquirer;
    private final ScheduledExecutorService reaperExecutor;
    private final HeldLocksCollection heldLocks;
    private final AwaitedLocksCollection awaitedLocks;
    private final ImmutableTimestampTracker immutableTsTracker;
    private final LeadershipId leadershipId;

    public static AsyncLockService createDefault(
            LockLog lockLog,
            ScheduledExecutorService reaperExecutor,
            ScheduledExecutorService timeoutExecutor) {
        return new AsyncLockService(
                new LockCollection(),
                new ImmutableTimestampTracker(),
                new LockAcquirer(lockLog, timeoutExecutor),
                new HeldLocksCollection(),
                new AwaitedLocksCollection(),
                reaperExecutor);
    }

    public AsyncLockService(
            LockCollection locks,
            ImmutableTimestampTracker immutableTimestampTracker,
            LockAcquirer acquirer,
            HeldLocksCollection heldLocks,
            AwaitedLocksCollection awaitedLocks,
            ScheduledExecutorService reaperExecutor) {
        this.locks = locks;
        this.immutableTsTracker = immutableTimestampTracker;
        this.lockAcquirer = acquirer;
        this.heldLocks = heldLocks;
        this.awaitedLocks = awaitedLocks;
        this.reaperExecutor = reaperExecutor;
        this.leadershipId = LeadershipId.random();

        scheduleExpiredLockReaper();
    }

    private void scheduleExpiredLockReaper() {
        reaperExecutor.scheduleAtFixedRate(() -> {
            try {
                heldLocks.removeExpired();
            } catch (Throwable t) {
                log.warn("Error while removing expired lock requests. Trying again on next iteration.", t);
            }
        }, 0, LeaseExpirationTimer.LEASE_TIMEOUT.toMillis() / 2, TimeUnit.MILLISECONDS);
    }

    public AsyncResult<LeasableLockToken> lock(UUID requestId, Set<LockDescriptor> lockDescriptors, TimeLimit timeout) {
        return heldLocks.getExistingOrAcquire(
                requestId,
                () -> acquireLocks(requestId, lockDescriptors, timeout))
                .map(this::createLeasableLockToken);
    }

    public AsyncResult<LeasableLockToken> lockImmutableTimestamp(UUID requestId, long timestamp) {
        return heldLocks.getExistingOrAcquire(
                requestId,
                () -> acquireImmutableTimestampLock(requestId, timestamp))
                .map(this::createLeasableLockToken);
    }

    public AsyncResult<Void> waitForLocks(UUID requestId, Set<LockDescriptor> lockDescriptors, TimeLimit timeout) {
        return awaitedLocks.getExistingOrAwait(
                requestId,
                () -> awaitLocks(requestId, lockDescriptors, timeout));
    }

    public Optional<Long> getImmutableTimestamp() {
        return immutableTsTracker.getImmutableTimestamp();
    }

    private LeasableLockToken createLeasableLockToken(HeldLocks heldLocks) {
        return LeasableLockToken.of(heldLocks.getToken(), leaseWithStart(heldLocks.lastRefreshTime()));
    }

    private AsyncResult<HeldLocks> acquireLocks(UUID requestId, Set<LockDescriptor> lockDescriptors,
            TimeLimit timeout) {
        OrderedLocks orderedLocks = locks.getAll(lockDescriptors);
        return lockAcquirer.acquireLocks(requestId, orderedLocks, timeout);
    }

    private AsyncResult<Void> awaitLocks(UUID requestId, Set<LockDescriptor> lockDescriptors,
            TimeLimit timeout) {
        OrderedLocks orderedLocks = locks.getAll(lockDescriptors);
        return lockAcquirer.waitForLocks(requestId, orderedLocks, timeout);
    }

    private AsyncResult<HeldLocks> acquireImmutableTimestampLock(UUID requestId, long timestamp) {
        AsyncLock immutableTsLock = immutableTsTracker.getLockFor(timestamp);
        return lockAcquirer.acquireLocks(requestId, OrderedLocks.fromSingleLock(immutableTsLock), TimeLimit.zero());
    }

    public boolean unlock(LockToken token) {
        return unlock(ImmutableSet.of(token)).contains(token);
    }

    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return heldLocks.unlock(tokens);
    }

    public boolean refresh(LockToken token) {
        return refresh(ImmutableSet.of(token)).refreshedTokens().contains(token);
    }

    public LeasableRefreshLockResponse refresh(Set<LockToken> tokens) {

        NanoTime startTime = NanoTime.now();
        Set<LockToken> refreshedTokens = heldLocks.refresh(tokens);

        return LeasableRefreshLockResponse.of(
                refreshedTokens,
                leaseWithStart(startTime));
    }

    private Lease leaseWithStart(NanoTime startTime) {
        return Lease.of(leadershipId, startTime, LeaseContract.LEASE_PERIOD);
    }

    public LeaderTime identifiedTime() {
        return LeaderTime.of(leadershipId, NanoTime.now());
    }

    /**
     * Shuts down the lock service, and fails any outstanding requests with a {@link
     * com.palantir.leader.NotCurrentLeaderException}.
     */
    @Override
    public void close() {
        reaperExecutor.shutdown();
        heldLocks.failAllOutstandingRequestsWithNotCurrentLeaderException();
    }
}
