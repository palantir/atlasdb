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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.watch.LockEventProcessor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.RefreshLockResponseV2;

public class AsyncLockService implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AsyncLockService.class);

    private final LockCollection locks;
    private final LockAcquirer lockAcquirer;
    private final ScheduledExecutorService reaperExecutor;
    private final HeldLocksCollection heldLocks;
    private final AwaitedLocksCollection awaitedLocks;
    private final ImmutableTimestampTracker immutableTsTracker;
    private final LeaderClock leaderClock;

    public static AsyncLockService createDefault(
            LockLog lockLog,
            ScheduledExecutorService reaperExecutor,
            ScheduledExecutorService timeoutExecutor,
            LockEventProcessor eventProcessor) {

        LeaderClock clock = LeaderClock.create();

        return new AsyncLockService(
                new LockCollection(),
                new ImmutableTimestampTracker(),
                new LockAcquirer(lockLog, timeoutExecutor, clock, eventProcessor),
                HeldLocksCollection.create(clock),
                new AwaitedLocksCollection(),
                reaperExecutor,
                clock);
    }

    @VisibleForTesting
    AsyncLockService(
            LockCollection locks,
            ImmutableTimestampTracker immutableTimestampTracker,
            LockAcquirer acquirer,
            HeldLocksCollection heldLocks,
            AwaitedLocksCollection awaitedLocks,
            ScheduledExecutorService reaperExecutor,
            LeaderClock leaderClock) {
        this.locks = locks;
        this.immutableTsTracker = immutableTimestampTracker;
        this.lockAcquirer = acquirer;
        this.heldLocks = heldLocks;
        this.awaitedLocks = awaitedLocks;
        this.reaperExecutor = reaperExecutor;
        this.leaderClock = leaderClock;

        scheduleExpiredLockReaper();
    }

    private void scheduleExpiredLockReaper() {
        reaperExecutor.scheduleAtFixedRate(() -> {
            try {
                heldLocks.removeExpired();
            } catch (Throwable t) {
                log.warn("Error while removing expired lock requests. Trying again on next iteration.", t);
            }
        }, 0, LockLeaseContract.SERVER_LEASE_TIMEOUT.toMillis() / 2, TimeUnit.MILLISECONDS);
    }

    public AsyncResult<Leased<LockToken>> lock(UUID requestId, Set<LockDescriptor> lockDescriptors, TimeLimit timeout) {
        return heldLocks.getExistingOrAcquire(
                requestId,
                () -> acquireLocks(requestId, lockDescriptors, timeout));
    }

    public AsyncResult<Leased<LockToken>> lockImmutableTimestamp(UUID requestId, long timestamp) {
        return heldLocks.getExistingOrAcquire(
                requestId,
                () -> acquireImmutableTimestampLock(requestId, timestamp));
    }

    public AsyncResult<Void> waitForLocks(UUID requestId, Set<LockDescriptor> lockDescriptors, TimeLimit timeout) {
        return awaitedLocks.getExistingOrAwait(
                requestId,
                () -> awaitLocks(requestId, lockDescriptors, timeout));
    }

    public Optional<Long> getImmutableTimestamp() {
        return immutableTsTracker.getImmutableTimestamp();
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

    public RefreshLockResponseV2 refresh(Set<LockToken> tokens) {

        Leased<Set<LockToken>> refreshedTokens = heldLocks.refresh(tokens);

        return RefreshLockResponseV2.of(
                refreshedTokens.value(),
                refreshedTokens.lease());
    }

    public LeaderTime leaderTime() {
        return leaderClock.time();
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
