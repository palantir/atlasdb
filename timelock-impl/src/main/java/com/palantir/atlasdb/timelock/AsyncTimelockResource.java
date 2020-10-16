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
package com.palantir.atlasdb.timelock;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.debug.LockDiagnosticInfo;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.ImmutableStartTransactionResponseV4;
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
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampRange;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

@Path("/timelock")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class AsyncTimelockResource {
    private static final ExecutorService asyncResponseExecutor = PTExecutors.newFixedThreadPool(64);
    private final LockLog lockLog;
    private final AsyncTimelockService timelock;

    public AsyncTimelockResource(LockLog lockLog, AsyncTimelockService timelock) {
        this.lockLog = lockLog;
        this.timelock = timelock;
    }

    @POST
    @Path("fresh-timestamp")
    public void getFreshTimestamp(@Suspended final AsyncResponse response) {
        addJerseyCallback(
                Futures.transform(
                        timelock.getFreshTimestampsAsync(1),
                        TimestampRange::getLowerBound,
                        MoreExecutors.directExecutor()),
                response);
    }

    @POST
    @Path("fresh-timestamps")
    public void getFreshTimestamps(
            @Suspended final AsyncResponse response, @Safe @QueryParam("number") int numTimestampsRequested) {
        addJerseyCallback(timelock.getFreshTimestampsAsync(numTimestampsRequested), response);
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
    public void startTransactions(@Suspended final AsyncResponse response, StartTransactionRequestV4 request) {
        ConjureStartTransactionsRequest conjureRequest = ConjureStartTransactionsRequest.builder()
                .requestId(request.requestId())
                .requestorId(request.requestorId())
                .numTransactions(request.numTransactions())
                .build();
        addJerseyCallback(
                Futures.transform(
                        timelock.startTransactionsWithWatches(conjureRequest),
                        newResponse -> ImmutableStartTransactionResponseV4.builder()
                                .timestamps(newResponse.getTimestamps())
                                .immutableTimestamp(newResponse.getImmutableTimestamp())
                                .lease(newResponse.getLease())
                                .build(),
                        MoreExecutors.directExecutor()),
                response);
    }

    @POST
    @Path("immutable-timestamp")
    public long getImmutableTimestamp() {
        return timelock.getImmutableTimestamp();
    }

    @POST
    @Path("lock")
    public void deprecatedLock(@Suspended final AsyncResponse response, IdentifiedLockRequest request) {
        addJerseyCallback(
                Futures.transform(
                        timelock.lock(request),
                        result -> result.accept(LockResponseV2.Visitor.of(
                                success -> LockResponse.successful(success.getToken()),
                                unsuccessful -> LockResponse.timedOut())),
                        MoreExecutors.directExecutor()),
                response);
    }

    @POST
    @Path("lock-v2")
    public void lock(@Suspended final AsyncResponse response, IdentifiedLockRequest request) {
        addJerseyCallback(timelock.lock(request), response);
    }

    @POST
    @Path("await-locks")
    public void waitForLocks(@Suspended final AsyncResponse response, WaitForLocksRequest request) {
        addJerseyCallback(timelock.waitForLocks(request), response);
    }

    @POST
    @Path("refresh-locks")
    public void deprecatedRefreshLockLeases(@Suspended final AsyncResponse response, Set<LockToken> tokens) {
        addJerseyCallback(
                Futures.transform(
                        timelock.refreshLockLeases(tokens),
                        RefreshLockResponseV2::refreshedTokens,
                        MoreExecutors.directExecutor()),
                response);
    }

    @POST
    @Path("refresh-locks-v2")
    public void refreshLockLeases(@Suspended final AsyncResponse response, Set<LockToken> tokens) {
        addJerseyCallback(timelock.refreshLockLeases(tokens), response);
    }

    @GET
    @Path("leader-time")
    public void getLeaderTime(@Suspended final AsyncResponse response) {
        addJerseyCallback(timelock.leaderTime(), response);
    }

    @POST
    @Path("unlock")
    public void unlock(@Suspended final AsyncResponse response, Set<LockToken> tokens) {
        addJerseyCallback(timelock.unlock(tokens), response);
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

    private void addJerseyCallback(ListenableFuture<?> result, AsyncResponse response) {
        Futures.addCallback(
                result,
                new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object result) {
                        response.resume(result);
                    }

                    @Override
                    public void onFailure(Throwable thrown) {
                        response.resume(thrown);
                    }
                },
                asyncResponseExecutor);
    }
}
