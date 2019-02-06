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

import com.palantir.lock.v2.IdentifiedTime;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
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
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampRange;

/**
 * Interface describing timelock endpoints to be used by feign client factories to create raw clients.
 *
 * If you are adding a new endpoint that is a modified version of an existing one, please use the existing naming scheme
 * in which you add a new version number, rather than describing the additional functionality (i.e. refresh-locks-v2
 * rather than leasable-refresh-locks)
 */

@Path("/timelock")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface TimelockRpcClient {

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
     * @deprecated Please use {@link TimelockRpcClient#startAtlasDbTransactionV3(
     * StartIdentifiedAtlasDbTransactionRequest)} instead; ignore the partition information if it is not useful for you.
     */
    @POST
    @Path("start-atlasdb-transaction")
    @Deprecated
    StartAtlasDbTransactionResponse startAtlasDbTransaction(IdentifiedTimeLockRequest request);

    /**
     * @deprecated Please use {@link TimelockRpcClient#startAtlasDbTransactionV3(
     * StartIdentifiedAtlasDbTransactionRequest)} instead.
     */
    @POST
    @Path("start-identified-atlasdb-transaction")
    @Deprecated
    StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction(
            StartIdentifiedAtlasDbTransactionRequest request);

    @POST
    @Path("immutable-timestamp")
    long getImmutableTimestamp();

    /**
     * @deprecated Please use {@link TimelockRpcClient#lockV2(LockRequest) instead}.
     */
    @POST
    @Path("lock")
    @Deprecated
    LockResponse lock(LockRequest request);

    @POST
    @Path("await-locks")
    WaitForLocksResponse waitForLocks(WaitForLocksRequest request);

    /**
     * @deprecated Please use {@link TimelockRpcClient#refreshLockLeasesV2(Set)} instead}.
     */
    @POST
    @Path("refresh-locks")
    @Deprecated
    Set<LockToken> refreshLockLeases(Set<LockToken> tokens);

    @POST
    @Path("refresh-locks-v2")
    LeasableRefreshLockResponse refreshLockLeasesV2(Set<LockToken> tokens);

    @POST
    @Path("lock-v2")
    LeasableLockResponse lockV2(LockRequest request);

    @POST
    @Path("start-atlasdb-transaction-v3")
    LeasableStartIdentifiedAtlasDbTransactionResponse startAtlasDbTransactionV3(
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

    @GET
    @Path("leader-time")
    IdentifiedTime getLeaderTime();

    @POST
    @Path("current-time-millis")
    long currentTimeMillis();

}
