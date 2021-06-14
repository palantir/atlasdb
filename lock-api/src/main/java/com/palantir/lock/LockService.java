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

import com.google.common.annotations.Beta;
import com.palantir.annotations.remoting.CancelableServerCall;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.logsafe.Safe;
import com.palantir.processors.AutoDelegate;
import com.palantir.proxy.annotations.Proxy;
import java.math.BigInteger;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Defines the service which handles locking operations.
 *
 * @author jtamer
 */
@Path("/lock")
@AutoDelegate
@Beta
@Proxy
public interface LockService extends RemoteLockService {
    /**
     * Attempts to acquire the requested set of locks. The locks are
     * acquired reentrantly as long as the lock client is not
     * {@link LockClient#ANONYMOUS}.
     *
     * @return a token for the set of locks that were acquired, or <code>null</code>
     *         if no locks were acquired
     */
    @POST
    @Path("lock-with-full-response/{client: .*}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @CancelableServerCall
    @NonIdempotent
    LockResponse lockWithFullLockResponse(@Safe @PathParam("client") LockClient client, LockRequest request)
            throws InterruptedException;

    /**
     * Unlocks a lock, given a provided lock token.
     *
     * @deprecated use {@link #unlockSimple(SimpleHeldLocksToken)} instead
     * @see #unlockSimple(SimpleHeldLocksToken)
     */
    @POST
    @Path("unlock-deprecated")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Deprecated
    @NonIdempotent
    boolean unlock(HeldLocksToken token);

    @POST
    @Path("unlock-simple")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    boolean unlockSimple(SimpleHeldLocksToken token);

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
    @POST
    @Path("unlock-and-freeze")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    boolean unlockAndFreeze(HeldLocksToken token);

    /**
     * Returns the set of all lock tokens that the given <code>client</code> is currently
     * holding, excluding any locks which are frozen.
     *
     * @return the set of valid tokens held by the given client
     * @throws IllegalArgumentException if {@code client} is anonymous.
     */
    @POST
    @Path("get-tokens/{client: .*}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    Set<HeldLocksToken> getTokens(@Safe @PathParam("client") LockClient client);

    /**
     * Refreshes the given lock tokens.
     *
     * @deprecated use {@link #refreshLockRefreshTokens(Iterable)} instead
     * @see #refreshLockRefreshTokens(Iterable)
     * @return the subset of tokens which are still valid after being refreshed
     */
    @POST
    @Path("refresh-tokens")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Deprecated
    @Idempotent
    Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens);

    /**
     * Refreshes the given lock grant.
     *
     * @return the new {@link HeldLocksGrant} token if refreshed successfully,
     *         or {@code null} if the grant could not be refreshed.
     */
    @POST
    @Path("refresh-grant")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Nullable
    HeldLocksGrant refreshGrant(HeldLocksGrant grant);

    /**
     * Refreshes the lock grant identified by the given grant ID.
     *
     * @return the new {@link HeldLocksGrant} token if refreshed successfully,
     *         or {@code null} if the grant could not be refreshed.
     */
    @POST
    @Path("refresh-grant-id")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    @Nullable
    HeldLocksGrant refreshGrant(BigInteger grantId);

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
    @POST
    @Path("convert-to-grant")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    HeldLocksGrant convertToGrant(HeldLocksToken token);

    /**
     * Grants the specified client ownership of the locks represented by the
     * given lock grant and returns a new token for these locks. The grant
     * object is invalidated.
     *
     * @throws IllegalArgumentException if {@code grant} is invalid
     */
    @POST
    @Path("use-grant/{client: .*}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    HeldLocksToken useGrant(@Safe @PathParam("client") LockClient client, HeldLocksGrant grant);

    /**
     * Grants the specified client ownership of the locks represented by the
     * given lock grant ID and returns a new token for these locks. The
     * underlying grant object is invalidated.
     *
     * @throws IllegalArgumentException if {@code grantId} is invalid
     */
    @POST
    @Path("use-grant-id/{client: .*}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent
    HeldLocksToken useGrant(@Safe @PathParam("client") LockClient client, BigInteger grantId);

    /**
     * This method is the same as <code>getMinLockedInVersionId(LockClient.ANONYMOUS)</code>.
     * @deprecated use {@link #getMinLockedInVersionId(LockClient)} instead
     */
    @POST
    @Path("min-locked-in-version-id")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Deprecated
    @Idempotent
    @Nullable
    Long getMinLockedInVersionId();

    /**
     * Returns the minimum version ID for all locks that are currently acquired
     * (by everyone), or {@code null} if none of these active locks specified a
     * version ID in their {@link LockRequest}s.
     */
    @POST
    @Path("min-locked-in-version-id-for-client/{client: .*}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    Long getMinLockedInVersionId(@Safe @PathParam("client") LockClient client);

    /** Returns the options used to configure the lock server. */
    @POST
    @Path("lock-server-options")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent
    LockServerOptions getLockServerOptions();

    /** Returns the current time in milliseconds on the server. */
    @POST
    @Path("current-time-millis")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Override
    @Idempotent
    long currentTimeMillis();

    @POST
    @Path("log-current-state")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Override
    @Idempotent
    void logCurrentState();
}
