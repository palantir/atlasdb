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
package com.palantir.atlasdb.timelock;

import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.timelock.lock.AsyncResult;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LeasableLockResponse;
import com.palantir.lock.v2.LeasableLockToken;
import com.palantir.lock.v2.LeasableRefreshLockResponse;
import com.palantir.lock.v2.LeasableStartAtlasDbTransactionResponse;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampRange;

@Path("/timelock")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class AsyncTimelockResource {
    private final LockLog lockLog;
    private final AsyncTimelockService timelock;

    public AsyncTimelockResource(LockLog lockLog, AsyncTimelockService timelock) {
        this.lockLog = lockLog;
        this.timelock = timelock;
    }

    @POST
    @Path("fresh-timestamp")
    public long getFreshTimestamp() {
        return timelock.getFreshTimestamp();
    }

    @POST
    @Path("fresh-timestamps")
    public TimestampRange getFreshTimestamps(@Safe @QueryParam("number") int numTimestampsRequested) {
        return timelock.getFreshTimestamps(numTimestampsRequested);
    }

    @POST
    @Path("lock-immutable-timestamp")
    public LockImmutableTimestampResponse lockImmutableTimestamp(IdentifiedTimeLockRequest request) {
        return timelock.lockImmutableTimestamp(request);
    }

    @POST
    @Path("start-atlasdb-transaction")
    public StartAtlasDbTransactionResponse startAtlasDbTransaction(IdentifiedTimeLockRequest request) {
        return timelock.startAtlasDbTransaction(request);
    }

    @POST
    @Path("start-identified-atlasdb-transaction")
    public StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction(
            StartIdentifiedAtlasDbTransactionRequest request) {
        return timelock.startIdentifiedAtlasDbTransaction(request).getStartTransactionResponse();
    }

    @POST
    @Path("immutable-timestamp")
    public long getImmutableTimestamp() {
        return timelock.getImmutableTimestamp();
    }

    @POST
    @Path("lock")
    public void lock(@Suspended final AsyncResponse response, LockRequest request) {
        AsyncResult<LeasableLockToken> result = timelock.lock(request);
        lockLog.registerRequest(request, result);
        result.onComplete(() -> {
            if (result.isFailed()) {
                response.resume(result.getError());
            } else if (result.isTimedOut()) {
                response.resume(LockResponse.timedOut());
            } else {
                response.resume(LockResponse.successful(result.get().token()));
            }
        });
    }

    @POST
    @Path("await-locks")
    public void waitForLocks(@Suspended final AsyncResponse response, WaitForLocksRequest request) {
        AsyncResult<Void> result = timelock.waitForLocks(request);
        lockLog.registerRequest(request, result);
        result.onComplete(() -> {
            if (result.isFailed()) {
                response.resume(result.getError());
            } else if (result.isTimedOut()) {
                response.resume(WaitForLocksResponse.timedOut());
            } else {
                response.resume(WaitForLocksResponse.successful());
            }
        });
    }

    @POST
    @Path("refresh-locks")
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return timelock.refreshLockLeases(tokens).refreshedTokens();
    }

    @POST
    @Path("lock-v2")
    public void lockV2(@Suspended final AsyncResponse response, LockRequest request) {
        AsyncResult<LeasableLockToken> result = timelock.lock(request);
        lockLog.registerRequest(request, result);
        result.onComplete(() -> {
            if (result.isFailed()) {
                response.resume(result.getError());
            } else if (result.isTimedOut()) {
                response.resume(LeasableLockResponse.timedOut());
            } else {
                response.resume(LeasableLockResponse.successful(result.get().token(), result.get().lease()));
            }
        });
    }

    @POST
    @Path("start-atlasdb-transaction-v3")
    public LeasableStartAtlasDbTransactionResponse startAtlasDbTransactionV3(
            StartIdentifiedAtlasDbTransactionRequest request) {
        return timelock.startIdentifiedAtlasDbTransaction(request);
    }

    @POST
    @Path("refresh-locks-v2")
    public LeasableRefreshLockResponse refreshLockLeasesV2(Set<LockToken> tokens) {
        return timelock.refreshLockLeases(tokens);
    }


    @GET
    @Path("leader-time")
    public LeaderTime getLeaderTime() {
        return timelock.leaderTime();
    }

    @POST
    @Path("unlock")
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return timelock.unlock(tokens);
    }

    @POST
    @Path("current-time-millis")
    public long currentTimeMillis() {
        return timelock.currentTimeMillis();
    }

}
