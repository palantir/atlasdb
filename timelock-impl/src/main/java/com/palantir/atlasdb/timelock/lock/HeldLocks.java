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
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingService;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;

public class HeldLocks {

    private final LockLog lockLog;
    private final Collection<AsyncLock> acquiredLocks;
    private final LockToken token;
    private final LeaseExpirationTimer expirationTimer;
    private final LockWatchingService lockWatchingService;
    private final Supplier<Set<LockDescriptor>> descriptors = Suppliers.memoize(this::getLockDescriptors);

    @GuardedBy("this")
    private boolean isUnlocked = false;

    @VisibleForTesting
    HeldLocks(
            LockLog lockLog,
            Collection<AsyncLock> acquiredLocks,
            UUID requestId,
            LeaseExpirationTimer expirationTimer,
            LockWatchingService lockWatchingService) {
        this.lockLog = lockLog;
        this.acquiredLocks = acquiredLocks;
        this.token = LockToken.of(requestId);
        this.expirationTimer = expirationTimer;
        this.lockWatchingService = lockWatchingService;
    }

    public static HeldLocks create(
            LockLog lockLog,
            Collection<AsyncLock> acquiredLocks,
            UUID requestId,
            LeaderClock leaderClock,
            LockWatchingService lockWatchingService) {
        HeldLocks locks = new HeldLocks(
                lockLog,
                acquiredLocks,
                requestId,
                new LeaseExpirationTimer(() -> leaderClock.time().currentTime()),
                lockWatchingService);
        locks.registerLock();
        return locks;
    }

    private void registerLock() {
        lockWatchingService.registerLock(descriptors.get(), token);
    }

    /**
     * Unlocks if expired, and returns whether the locks are now unlocked (regardless of whether or not they were
     * unlocked as a result of calling this method).
     */
    public synchronized boolean unlockIfExpired() {
        if (expirationTimer.isExpired()) {
            if (unlockInternal()) {
                lockLog.lockExpired(token.getRequestId(), descriptors.get());
            }
        }
        return isUnlocked;
    }

    public synchronized boolean refresh() {
        if (isUnlocked) {
            return false;
        }

        expirationTimer.refresh();
        return true;
    }

    public synchronized boolean unlockExplicitly() {
        boolean successfullyUnlocked = unlockInternal();
        if (successfullyUnlocked) {
            lockLog.lockUnlocked(token.getRequestId());
        }
        return successfullyUnlocked;
    }

    private synchronized boolean unlockInternal() {
        if (isUnlocked) {
            return false;
        }
        isUnlocked = true;
        lockWatchingService.registerUnlock(descriptors.get());

        for (AsyncLock lock : acquiredLocks) {
            lock.unlock(token.getRequestId());
        }

        return true;
    }

    public LockToken getToken() {
        return token;
    }

    public UUID getRequestId() {
        return token.getRequestId();
    }

    public NanoTime lastRefreshTime() {
        return expirationTimer.lastRefreshTime();
    }

    public Collection<AsyncLock> getLocks() {
        return acquiredLocks;
    }

    private Set<LockDescriptor> getLockDescriptors() {
        return acquiredLocks.stream().map(AsyncLock::getDescriptor).collect(Collectors.toSet());
    }
}
