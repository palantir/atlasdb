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

import java.util.Collection;
import java.util.UUID;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.lock.v2.LockTokenV2;

public class HeldLocks {

    private final Collection<AsyncLock> acquiredLocks;
    private final LockTokenV2 token;
    private final LeaseExpirationTimer expirationTimer;

    @GuardedBy("this")
    private boolean isUnlocked = false;

    public HeldLocks(Collection<AsyncLock> acquiredLocks, UUID requestId) {
        this(acquiredLocks, requestId, new LeaseExpirationTimer(System::currentTimeMillis));
    }

    @VisibleForTesting
    HeldLocks(Collection<AsyncLock> acquiredLocks, UUID requestId, LeaseExpirationTimer expirationTimer) {
        this.acquiredLocks = acquiredLocks;
        this.token = LockTokenV2.of(requestId);
        this.expirationTimer = expirationTimer;
    }

    /**
     * Unlocks if expired, and returns whether the locks are now unlocked (regardless of whether or not they were
     * unlocked as a result of calling this method).
     */
    public synchronized boolean unlockIfExpired() {
        if (expirationTimer.isExpired()) {
            unlock();
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

    public LockTokenV2 getToken() {
        return token;
    }

    public UUID getRequestId() {
        return token.getRequestId();
    }

    @VisibleForTesting
    Collection<AsyncLock> getLocks() {
        return acquiredLocks;
    }

}
