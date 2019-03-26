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

package com.palantir.lock.v2;

import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampRange;

/**
 * Interface describing timelock endpoints to be used by feign client factories to create raw clients.
 *
 * If you are adding a replacement for an endpoint, please version by number, e.g. a new version of
 * fresh-timestamp might be fresh-timestamp-2.
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
    LockImmutableTimestampResponse lockImmutableTimestamp(IdentifiedTimeLockRequest request);

    @POST
    @Path("start-atlasdb-transaction-v3")
    StartAtlasDbTransactionResponseV3 deprecatedStartTransaction(
            StartIdentifiedAtlasDbTransactionRequest request);

    /**
     * Returns a {@link StartTransactionResponseV4} which has a single immutable ts, and a range of timestamps to
     * be used as start timestamps.
     *
     * It is guaranteed to have at least one usable timestamp matching the partition criteria in the returned timestamp
     * range, but there is no other guarantee given. (It can be less than number of requested timestamps)
     */
    @POST
    @Path("start-atlasdb-transaction-v4")
    StartTransactionResponseV4 startTransactions(
            StartTransactionRequestV4 request);

    @POST
    @Path("immutable-timestamp")
    long getImmutableTimestamp();

    @POST
    @Path("lock-v2")
    LockResponseV2 lock(IdentifiedLockRequest request);

    @POST
    @Path("await-locks")
    WaitForLocksResponse waitForLocks(WaitForLocksRequest request);

    @POST
    @Path("refresh-locks-v2")
    RefreshLockResponseV2 refreshLockLeases(Set<LockToken> tokens);

    @GET
    @Path("leader-time")
    LeaderTime getLeaderTime();

    @POST
    @Path("unlock")
    Set<LockToken> unlock(Set<LockToken> tokens);

    @POST
    @Path("current-time-millis")
    long currentTimeMillis();

}
