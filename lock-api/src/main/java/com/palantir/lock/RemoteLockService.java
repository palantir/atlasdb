/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.lock;

import java.util.Set;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.marathon.util.spring.CancelableServerCall;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.annotation.NonIdempotent;

@Path("/lock")
public interface RemoteLockService {
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
    LockResponse lock(LockClient client, LockRequest request) throws InterruptedException;

    /**
     * This is the same as {@link #lock(LockClient, LockRequest)} but passing in {@link LockClient#ANONYMOUS}
     * as the first param.
     */
    @POST
    @Path("lock-anonymously")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    LockResponse lockAnonymously(LockRequest request) throws InterruptedException;

    /**
     * This is the same as {@link #lock(LockClient, LockRequest)} but passing in <code>LockClient.of(client)</code>
     * as the first param.
     */
    @POST
    @Path("lock/{client}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    LockResponse lockWithClient(@PathParam("client") String client, LockRequest request) throws InterruptedException;

    /**
     * Attempts to release the set of locks represented by the
     * <code>token</code> parameter. For locks which
     * have been locked multiple times reentrantly, this method decrements the
     * lock hold counts by one.
     * <p>
     * Prefer this over {@link #unlock(HeldLocksToken)} because it sends less
     * data over the wire.
     *
     * @return <code>true<code> if the locks were unlocked by this call,
     *         <code>false</code> if the token was invalid,
     *         either because it was already unlocked, or because it expired or
     *         was converted to a lock grant
     */
    @POST
    @Path("unlock")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @NonIdempotent boolean unlockSimple(SimpleHeldLocksToken token);

    /**
     * Refreshes the given lock tokens.
     * <p>
     * This method transfers less data over the wire but does the same work as {@link #refreshTokens(Iterable)}.
     * {@link HeldLocksToken} objects can be pretty heavyweight because they have a set of locks in them that
     * can be large.
     *
     * @return the subset of tokens which are still valid after being refreshed.
     */
    @POST
    @Path("refresh-lock-tokens")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens);

    /**
     * Returns the minimum version ID for all locks that are currently acquired
     * (by everyone), or {@code null} if none of these active locks specified a
     * version ID in their {@link LockRequest}s.
     */
    @POST
    @Path("min-locked-in-version")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Idempotent @Nullable Long getMinLockedInVersionId(LockClient client);

    /** Returns the current time in milliseconds on the server. */
    @POST
    @Path("current-time-millis")
    @Produces(MediaType.APPLICATION_JSON)
    @Idempotent long currentTimeMillis();

    @POST
    @Path("log-current-state")
    void logCurrentState();

    @POST
    @Path("log-current-state-inconsistent")
    void logCurrentStateInconsistent();

}
