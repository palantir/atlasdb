/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.lock.impl;

import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.palantir.lock.ExpiringToken;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockCollection;

/**
 * A set of locks held by the lock server, along with the canonical
 * {@link HeldLocksToken} or {@link HeldLocksGrant} object for these locks.
 */
@Immutable
public class HeldLocks<T extends ExpiringToken> {
    final T realToken;
    final LockCollection<? extends ClientAwareReadWriteLock> locks;

    @VisibleForTesting
    public static <T extends ExpiringToken> HeldLocks<T> of(T token,
            LockCollection<? extends ClientAwareReadWriteLock> locks) {
        return new HeldLocks<T>(token, locks);
    }

    HeldLocks(T token, LockCollection<? extends ClientAwareReadWriteLock> locks) {
        this.realToken = Preconditions.checkNotNull(token);
        this.locks = locks;
    }

    public LockCollection<? extends ClientAwareReadWriteLock> getLocks() {
        return locks;
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(getClass().getSimpleName())
                .add("realToken", realToken)
                .add("locks", locks)
                .toString();
    }
}