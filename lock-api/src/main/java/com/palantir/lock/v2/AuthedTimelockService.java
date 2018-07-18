/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.lock.v2;

import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampRange;
import com.palantir.tokens.auth.AuthHeader;

/**
 * Defines the service which handles locking operations.
 * Compared to {@link TimelockService} this requires an AuthHeader, passed as first parameter in all the methods.
 * The auth header can be used by Timelock server to authorize the requests for each namespace.
 */

@Path("/timelock")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface AuthedTimelockService {
    /**]
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
    long getFreshTimestamp(@HeaderParam(HttpHeaders.AUTHORIZATION) AuthHeader authHeader);

    @POST
    @Path("fresh-timestamps")
    TimestampRange getFreshTimestamps(@HeaderParam(HttpHeaders.AUTHORIZATION) AuthHeader authHeader,
            @Safe @QueryParam("number") int numTimestampsRequested);

    @POST
    @Path("lock-immutable-timestamp")
    LockImmutableTimestampResponse lockImmutableTimestamp(@HeaderParam(HttpHeaders.AUTHORIZATION) AuthHeader authHeader,
            IdentifiedTimeLockRequest request);

    @POST
    @Path("start-atlasdb-transaction")
    StartAtlasDbTransactionResponse startAtlasDbTransaction(
            @HeaderParam(HttpHeaders.AUTHORIZATION) AuthHeader authHeader,
            IdentifiedTimeLockRequest request);

    @POST
    @Path("immutable-timestamp")
    long getImmutableTimestamp(@HeaderParam(HttpHeaders.AUTHORIZATION) AuthHeader authHeader);

    @POST
    @Path("lock")
    LockResponse lock(@HeaderParam(HttpHeaders.AUTHORIZATION) AuthHeader authHeader, LockRequest request);

    @POST
    @Path("await-locks")
    WaitForLocksResponse waitForLocks(@HeaderParam(HttpHeaders.AUTHORIZATION) AuthHeader authHeader,
            WaitForLocksRequest request);

    @POST
    @Path("refresh-locks")
    Set<LockToken> refreshLockLeases(@HeaderParam(HttpHeaders.AUTHORIZATION) AuthHeader authHeader,
            Set<LockToken> tokens);

    @POST
    @Path("unlock")
    Set<LockToken> unlock(@HeaderParam(HttpHeaders.AUTHORIZATION) AuthHeader authHeader,
            Set<LockToken> tokens);

    @POST
    @Path("current-time-millis")
    long currentTimeMillis(@HeaderParam(HttpHeaders.AUTHORIZATION) AuthHeader authHeader);

}
