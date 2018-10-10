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

import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;

public class HeldLocks {

    private final LockLog lockLog;
    private final Collection<AsyncLock> acquiredLocks;
    private final LockToken token;
    private final LeaseExpirationTimer expirationTimer;

    @GuardedBy("this")
    private boolean isUnlocked = false;

    public HeldLocks(LockLog lockLog, Collection<AsyncLock> acquiredLocks, UUID requestId) {
        this(lockLog, acquiredLocks, requestId, new LeaseExpirationTimer(System::currentTimeMillis));
    }

    @VisibleForTesting
    HeldLocks(LockLog lockLog, Collection<AsyncLock> acquiredLocks,
            UUID requestId, LeaseExpirationTimer expirationTimer) {
        this.lockLog = lockLog;
        this.acquiredLocks = acquiredLocks;
        this.token = LockToken.of(requestId);
        this.expirationTimer = expirationTimer;
    }

    /**
     * Unlocks if expired, and returns whether the locks are now unlocked (regardless of whether or not they were
     * unlocked as a result of calling this method).
     */
    public synchronized boolean unlockIfExpired() {
        if (expirationTimer.isExpired()) {
            if (unlock()) {
                lockLog.lockExpired(token.getRequestId(), getLockDescriptors());
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

    public synchronized boolean unlock() {
        if (isUnlocked) {
            return false;
        }
        isUnlocked = true;

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

    @VisibleForTesting
    Collection<AsyncLock> getLocks() {
        return acquiredLocks;
    }

    private Collection<LockDescriptor> getLockDescriptors() {
        return acquiredLocks.stream()
                .map(AsyncLock::getDescriptor)
                .collect(Collectors.toList());
    }

}
