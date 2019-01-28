/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LeaderTimeResponse;
import com.palantir.lock.v2.LeasableLockResponse;
import com.palantir.lock.v2.LeasableRefreshLockResponse;
import com.palantir.lock.v2.LeasableStartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.Safe;
import com.palantir.processors.AutoDelegate;
import com.palantir.timestamp.TimestampRange;

@Path("/timelock")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@AutoDelegate
public interface TimelockRpcClient { //needs a better name
    /**
     * Used for TimelockServices that can be initialized asynchronously (i.e. those extending
     * {@link com.palantir.async.initializer.AsyncInitializer}; other TimelockServices can keep the default
     * implementation, and return true (they're trivially fully initialized).
     *
     * @return true iff the TimelockService has been fully initialized and is ready to use
     */
    default boolean isInitialized() {
        return true;
    }

    @POST
    @Path("fresh-timestamp")
    long getFreshTimestamp();

    @POST
    @Path("fresh-timestamps")
    TimestampRange getFreshTimestamps(@Safe @QueryParam("number") int numTimestampsRequested);

    @POST
    @Path("lock-immutable-timestamp")
        // TODO (jkong): Can this be deprecated? Are there users outside of Atlas transactions?
    LockImmutableTimestampResponse lockImmutableTimestamp(IdentifiedTimeLockRequest request);

    /**
     * @deprecated Please use {@link TimelockService#startIdentifiedAtlasDbTransaction(
     *StartIdentifiedAtlasDbTransactionRequest)} instead; ignore the partition information if it is not useful for you.
     */
    @POST
    @Path("start-atlasdb-transaction")
    @Deprecated
    StartAtlasDbTransactionResponse startAtlasDbTransaction(IdentifiedTimeLockRequest request);

    @POST
    @Path("start-identified-atlasdb-transaction")
    StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction(
            StartIdentifiedAtlasDbTransactionRequest request);

    @POST
    @Path("immutable-timestamp")
    long getImmutableTimestamp();

    @POST
    @Path("lock")
    LockResponse lock(LockRequest request);

    @POST
    @Path("await-locks")
    WaitForLocksResponse waitForLocks(WaitForLocksRequest request);

    @POST
    @Path("refresh-locks")
    Set<LockToken> refreshLockLeases(Set<LockToken> tokens);

    @POST
    @Path("leasable-refresh-locks")
    LeasableRefreshLockResponse leasableRefreshLockLeases(Set<LockToken> tokens);

    @POST
    @Path("leasable-lock")
    LeasableLockResponse leasableLock(LockRequest request);

    @POST
    @Path("leasable-start-identified-atlasdb-transaction")
    LeasableStartIdentifiedAtlasDbTransactionResponse leasableStartIdentifiedAtlasDbTransaction(
            StartIdentifiedAtlasDbTransactionRequest request);

    /**
     * Releases locks associated with the set of {@link LockToken}s provided.
     * The set of tokens returned are the tokens for which the associated locks were unlocked in this call.
     * It is possible that a token that was provided is NOT in the returned set (e.g. if it expired).
     * However, in this case it is guaranteed that that token is no longer valid.
     *
     * @param tokens Tokens for which associated locks should be unlocked.
     * @return Tokens for which associated locks were unlocked
     */
    @POST
    @Path("unlock")
    Set<LockToken> unlock(Set<LockToken> tokens);

    /**
     * A version of {@link TimelockService#unlock(Set)} where one does not need to know whether the locks associated
     * with the provided tokens were successfully unlocked or not.
     *
     * In some implementations, this may be more performant than a standard unlock.
     *
     * @param tokens Tokens for which associated locks should be unlocked.
     */
    default void tryUnlock(Set<LockToken> tokens) {
        unlock(tokens);
    }

    @GET
    @Path("leader-time")
    LeaderTimeResponse getLeaderTime();

    @POST
    @Path("current-time-millis")
    long currentTimeMillis();

}
