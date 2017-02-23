/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.lock;

import java.math.BigInteger;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.annotations.Beta;
import com.marathon.util.spring.CancelableServerCall;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;

/**
 * Defines the service which handles locking operations.
 *
 * @author jtamer
 */
@Beta public interface LockService extends RemoteLockService {
    /**
     * Attempts to acquire the requested set of locks. The locks are
     * acquired reentrantly as long as the lock client is not
     * {@link LockClient#ANONYMOUS}.
     *
     * @return a token for the set of locks that were acquired, or <code>null</code>
     *         if no locks were acquired
     */
    @CancelableServerCall
    @NonIdempotent
    LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException;

    /**
     * @deprecated use {@link #unlockSimple(SimpleHeldLocksToken)} instead
     * @see #unlockSimple(SimpleHeldLocksToken)
     */
    @Deprecated
    @NonIdempotent boolean unlock(HeldLocksToken token);

    @NonIdempotent boolean unlockSimple(SimpleHeldLocksToken token);

    /**
     * Unlocks the set of locks represented by the <code>token</code>
     * parameter.
     *
     * <p>If the locks were locked multiple times reentrantly, this method decrements the
     * lock hold counts by one and then "freezes" the locks so that:
     * <ul>
     * <li>the locks can no longer be refreshed</li>
     * <li>the locks cannot be converted to grants (by
     * any existing {@link HeldLocksToken})</li>
     * <li>the locks's hold counts cannot be
     * incremented</li>
     * </ul>
     * <p>Locks become "unfrozen" once their hold counts drop all the
     * way to zero and they are not being held by any clients.
     *
     * <p>If the locks
     * held by {@code token} were not locked reentrantly, then this method is
     * equivalent to {@link #unlock(HeldLocksToken)} except for error
     * conditions. Call this method against write locks only. The
     * lock client for {@code token} cannot be anonymous.
     * <p>
     * Note that it is possible for a {@code HeldLocksToken} to contain both
     * frozen and unfrozen locks. These lock tokens cannot be refreshed.
     *
     * @return <code>true</code> if the locks were unlocked by this call, or
     *         <code>false</code> if the token was invalid,
     *         either because it was already unlocked or because it expired or
     *         was converted to a lock grant
     * @throws IllegalArgumentException if any of the locks represented by
     *         {@code token} are read locks (instead of write locks), or if
     *         {@code token} is held by an anonymous lock client
     */
    @NonIdempotent boolean unlockAndFreeze(HeldLocksToken token);

    /**
     * Returns the set of all lock tokens that the given <code>client</code> is currently
     * holding, excluding any locks which are frozen.
     *
     * @return the set of valid tokens held by the given client.
     * @throws IllegalArgumentException if {@code client} is anonymous.
     */
    @Idempotent Set<HeldLocksToken> getTokens(LockClient client);

    /**
     * Refreshes the given lock tokens.
     *
     * @deprecated use {@link #refreshLockRefreshTokens(Iterable)} instead
     * @see #refreshLockRefreshTokens(Iterable)
     * @return the subset of tokens which are still valid after being refreshed.
     */
    @Deprecated
    @Idempotent Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens);

    /**
     * Refreshes the given lock grant.
     *
     * @return the new {@link HeldLocksGrant} token if refreshed successfully,
     *         or {@code null} if the grant could not be refreshed.
     */
    @Idempotent @Nullable HeldLocksGrant refreshGrant(HeldLocksGrant grant);

    /**
     * Refreshes the lock grant identified by the given grant ID.
     *
     * @return the new {@link HeldLocksGrant} token if refreshed successfully,
     *         or {@code null} if the grant could not be refreshed.
     */
    @Idempotent @Nullable HeldLocksGrant refreshGrant(BigInteger grantId);

    /**
     * Converts the given token into a lock grant. The underlying locks are not
     * released  until the grant expires. However, the locks are
     * no longer owned by this client and the given <code>token</code> is invalidated.
     * <p>
     * The client must not be holding both the read lock and the write
     * lock for any of the locks represented by <code>token</code>. Furthermore, the
     * hold count of each write lock must be exactly one. If the client has
     * opened any of the locks multiple times reentrantly, this method throws
     * <code>IllegalMonitorStateException</code>. Read locks are permitted to
     * have a hold count greater than one, as long as the corresponding write
     * lock is not also held.
     *
     * @throws IllegalArgumentException if {@code token} is invalid
     * @throws IllegalMonitorStateException if the token cannot be converted to
     *         a lock grant because of reentrancy
     */
    @NonIdempotent HeldLocksGrant convertToGrant(HeldLocksToken token);

    /**
     * Grants the specified client ownership of the locks represented by the
     * given lock grant and returns a new token for these locks. The grant
     * object is invalidated.
     *
     * @throws IllegalArgumentException if {@code grant} is invalid
     */
    @NonIdempotent HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant);

    /**
     * Grants the specified client ownership of the locks represented by the
     * given lock grant ID and returns a new token for these locks. The
     * underlying grant object is invalidated.
     *
     * @throws IllegalArgumentException if {@code grantId} is invalid
     */
    @NonIdempotent HeldLocksToken useGrant(LockClient client, BigInteger grantId);

    /**
     * This method is the same as <code>getMinLockedInVersionId(LockClient.ANONYMOUS)</code>
     * @deprecated use {@link #getMinLockedInVersionId(LockClient)} instead
     */
    @Deprecated
    @Idempotent @Nullable Long getMinLockedInVersionId();

    /**
     * Returns the minimum version ID for all locks that are currently acquired
     * (by everyone), or {@code null} if none of these active locks specified a
     * version ID in their {@link LockRequest}s.
     */
    Long getMinLockedInVersionId(LockClient client);

    /** Returns the options used to configure the lock server. */
    @Idempotent LockServerOptions getLockServerOptions();

    /** Returns the current time in milliseconds on the server. */
    @Override
    @Idempotent long currentTimeMillis();

    @Override
    @Idempotent void logCurrentState();

}
