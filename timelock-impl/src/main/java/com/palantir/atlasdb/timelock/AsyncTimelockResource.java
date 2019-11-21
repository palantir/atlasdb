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

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import com.palantir.atlasdb.debug.LockDiagnosticInfo;
import com.palantir.atlasdb.timelock.lock.AsyncResult;
import com.palantir.atlasdb.timelock.lock.Leased;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartAtlasDbTransactionResponseV3;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartTransactionRequestV4;
import com.palantir.lock.v2.StartTransactionRequestV5;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.StartTransactionResponseV5;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.lock.watch.TimestampWithWatches;
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
    @Path("commit-timestamp")
    public TimestampWithWatches getCommitTimestampWithWatches(@Safe @QueryParam("lastKnown") OptionalLong lastVersion) {
        return timelock.getCommitTimestampWithWatches(lastVersion);
    }

    @POST
    @Path("lock-immutable-timestamp")
    public LockImmutableTimestampResponse lockImmutableTimestamp(IdentifiedTimeLockRequest request) {
        return timelock.lockImmutableTimestamp(request);
    }

    @POST
    @Path("start-atlasdb-transaction")
    public StartAtlasDbTransactionResponse deprecatedStartTransactionV1(IdentifiedTimeLockRequest request) {
        return timelock.deprecatedStartTransaction(request);
    }

    @POST
    @Path("start-identified-atlasdb-transaction")
    public StartIdentifiedAtlasDbTransactionResponse deprecatedStartTransactionV2(
            StartIdentifiedAtlasDbTransactionRequest request) {
        return timelock.startTransaction(request).toStartTransactionResponse();
    }

    @POST
    @Path("start-atlasdb-transaction-v3")
    public StartAtlasDbTransactionResponseV3 deprecatedStartTransactionV3(
            StartIdentifiedAtlasDbTransactionRequest request) {
        return timelock.startTransaction(request);
    }

    /**
     * Returns a {@link StartTransactionResponseV4} which has a single immutable ts, and a range of timestamps to
     * be used as start timestamps.
     *
     * It is guaranteed to have at least one usable timestamp matching the partition criteria in the returned timestamp
     * range, but there is no other guarantee given. (It can be less than number of requested timestamps)
     */
    @POST
    @Path("start-atlasdb-transaction-v4")
    public StartTransactionResponseV4 startTransactions(StartTransactionRequestV4 request) {
        return timelock.startTransactions(request);
    }

    @POST
    @Path("start-atlasdb-transaction-v5")
    public StartTransactionResponseV5 startTransactionsWithWatches(StartTransactionRequestV5 request) {
        return timelock.startTransactionsWithWatches(request);
    }

    @POST
    @Path("immutable-timestamp")
    public long getImmutableTimestamp() {
        return timelock.getImmutableTimestamp();
    }

    @POST
    @Path("lock")
    public void deprecatedLock(@Suspended final AsyncResponse response, IdentifiedLockRequest request) {
        AsyncResult<Leased<LockToken>> result = timelock.lock(request);
        lockLog.registerRequest(request, result);
        result.onComplete(() -> {
            if (result.isFailed()) {
                response.resume(result.getError());
            } else if (result.isTimedOut()) {
                response.resume(LockResponse.timedOut());
            } else {
                response.resume(LockResponse.successful(result.get().value()));
            }
        });
    }

    @POST
    @Path("lock-v2")
    public void lock(@Suspended final AsyncResponse response, IdentifiedLockRequest request) {
        AsyncResult<Leased<LockToken>> result = timelock.lock(request);
        lockLog.registerRequest(request, result);
        result.onComplete(() -> {
            if (result.isFailed()) {
                response.resume(result.getError());
            } else if (result.isTimedOut()) {
                response.resume(LockResponseV2.timedOut());
            } else {
                response.resume(LockResponseV2.successful(result.get().value(), result.get().lease()));
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
    public Set<LockToken> deprecatedRefreshLockLeases(Set<LockToken> tokens) {
        return timelock.refreshLockLeases(tokens).refreshedTokens();
    }

    @POST
    @Path("refresh-locks-v2")
    public RefreshLockResponseV2 refreshLockLeases(Set<LockToken> tokens) {
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

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    @POST
    @Path("do-not-use-without-explicit-atlasdb-authorisation/lock-diagnostic-config")
    public Optional<LockDiagnosticInfo> getEnhancedLockDiagnosticInfo(Set<UUID> requestIds) {
        return lockLog.getAndLogLockDiagnosticInfo(requestIds);
    }
}
