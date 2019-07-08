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
package com.palantir.lock;

import java.util.Set;

import javax.annotation.Nullable;

public interface RemoteLockService {
    /**
     * Attempts to acquire the requested set of locks for the given client.
     * @return null if the lock request failed
     */
    @Nullable
    LockRefreshToken lock(String client, LockRequest request) throws InterruptedException;

    /**
     * This is the same as {@link #lock(String, LockRequest)} but will return as many locks as can be acquired.
     * @return a token for the locks acquired
     */
    @Nullable
    HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request)
            throws InterruptedException;

    /**
     * Attempts to release the set of locks represented by the
     * <code>token</code> parameter. For locks which
     * have been locked multiple times reentrantly, this method decrements the
     * lock hold counts by one.
     *
     * @return <code>true</code> if the locks were unlocked by this call,
     *         <code>false</code> if the token was invalid,
     *         either because it was already unlocked, or because it expired or
     *         was converted to a lock grant
     */
    boolean unlock(LockRefreshToken token);

    /**
     * Refreshes the given lock tokens.
     *
     * @return the subset of tokens which are still valid after being refreshed
     */
    Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens);

    /**
     * Returns the minimum version ID for all locks that are currently acquired
     * (by everyone), or {@code null} if none of these active locks specified a
     * version ID in their {@link LockRequest}s.
     */
    @Nullable
    Long getMinLockedInVersionId(String client);

    /** Returns the current time in milliseconds on the server. */
    long currentTimeMillis();

    void logCurrentState();
}
