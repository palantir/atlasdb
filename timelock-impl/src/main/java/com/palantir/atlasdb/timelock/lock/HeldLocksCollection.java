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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.common.time.NanoTime;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockToken;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class HeldLocksCollection {
    @VisibleForTesting
    final ConcurrentMap<UUID, AsyncResult<HeldLocks>> heldLocksById = Maps.newConcurrentMap();

    private final LeaderClock leaderClock;

    @VisibleForTesting
    HeldLocksCollection(LeaderClock leaderClock) {
        this.leaderClock = leaderClock;
    }

    public static HeldLocksCollection create(LeaderClock leaderClock) {
        return new HeldLocksCollection(leaderClock);
    }

    public AsyncResult<Leased<LockToken>> getExistingOrAcquire(
            UUID requestId,
            Supplier<AsyncResult<HeldLocks>> lockAcquirer) {
        return heldLocksById.computeIfAbsent(
                requestId, ignored -> lockAcquirer.get())
                .map(this::createLeasableLockToken);
    }

    public Set<LockToken> unlock(Set<LockToken> tokens) {
        Set<LockToken> unlocked = filter(tokens, HeldLocks::unlockExplicitly);
        for (LockToken token : unlocked) {
            heldLocksById.remove(token.getRequestId());
        }
        return unlocked;
    }

    public Leased<Set<LockToken>> refresh(Set<LockToken> tokens) {
        Lease lease = leaseWithStart(leaderClock.time());
        return Leased.of(filter(tokens, HeldLocks::refresh), lease);
    }

    public void removeExpired() {
        heldLocksById.values().removeIf(this::shouldRemove);
    }

    public void failAllOutstandingRequestsWithNotCurrentLeaderException() {
        NotCurrentLeaderException ex = new NotCurrentLeaderException("This lock service has been closed");
        heldLocksById.values().forEach(result -> result.failIfNotCompleted(ex));
    }

    public Set<HeldLocks> locksHeld() {
        return heldLocksById.values().stream()
                .filter(AsyncResult::isCompletedSuccessfully)
                .<HeldLocks>map(AsyncResult::get)
                .collect(Collectors.toSet());
    }

    private Leased<LockToken> createLeasableLockToken(HeldLocks heldLocks) {
        return Leased.of(heldLocks.getToken(), leaseWithStart(heldLocks.lastRefreshTime()));
    }

    private Lease leaseWithStart(NanoTime startTime) {
        return leaseWithStart(LeaderTime.of(leaderClock.id(), startTime));
    }

    private Lease leaseWithStart(LeaderTime leaderTime) {
        return Lease.of(leaderTime, LockLeaseContract.CLIENT_LEASE_TIMEOUT);
    }

    private boolean shouldRemove(AsyncResult<HeldLocks> lockResult) {
        return lockResult.isFailed()
                || lockResult.isTimedOut()
                || lockResult.test(HeldLocks::unlockIfExpired);
    }

    private Set<LockToken> filter(Set<LockToken> tokens, Predicate<HeldLocks> predicate) {
        Set<LockToken> filtered = Sets.newHashSetWithExpectedSize(tokens.size());

        for (LockToken token : tokens) {
            AsyncResult<HeldLocks> lockResult = heldLocksById.get(token.getRequestId());
            if (lockResult != null && lockResult.test(predicate)) {
                filtered.add(token);
            }
        }

        return filtered;
    }
}
