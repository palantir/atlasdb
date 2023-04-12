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
import com.palantir.atlasdb.debug.LockDiagnosticInfo;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartAtlasDbTransactionResponseV3;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartTransactionRequestV4;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.logsafe.Safe;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

@Path("/{namespace: (?!(tl|lw)/)[a-zA-Z0-9_-]+}/timelock")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class JerseyAsyncTimelockResource {
    private static final ExecutorService asyncResponseExecutor = PTExecutors.newFixedThreadPool(64);
    private final TimelockNamespaces namespaces;

    public JerseyAsyncTimelockResource(TimelockNamespaces namespaces) {
        this.namespaces = namespaces;
    }

    @POST
    @Path("fresh-timestamp")
    public void getFreshTimestamp(@Suspended final AsyncResponse response, @PathParam("namespace") String namespace) {
        addJerseyCallback(getAsyncTimelockService(namespace).getFreshTimestampAsync(), response);
    }

    @POST
    @Path("fresh-timestamps")
    public void getFreshTimestamps(
            @Suspended final AsyncResponse response,
            @PathParam("namespace") String namespace,
            @Safe @QueryParam("number") int numTimestampsRequested) {
        addJerseyCallback(getAsyncTimelockService(namespace).getFreshTimestampsAsync(numTimestampsRequested), response);
    }

    @POST
    @Path("lock-immutable-timestamp")
    public LockImmutableTimestampResponse lockImmutableTimestamp(
            @PathParam("namespace") String namespace, IdentifiedTimeLockRequest request) {
        return getAsyncTimelockService(namespace).lockImmutableTimestamp(request);
    }

    @POST
    @Path("start-atlasdb-transaction")
    public StartAtlasDbTransactionResponse deprecatedStartTransactionV1(
            @PathParam("namespace") String namespace, IdentifiedTimeLockRequest request) {
        return getAsyncTimelockService(namespace).deprecatedStartTransaction(request);
    }

    @POST
    @Path("start-identified-atlasdb-transaction")
    public StartIdentifiedAtlasDbTransactionResponse deprecatedStartTransactionV2(
            @PathParam("namespace") String namespace, StartIdentifiedAtlasDbTransactionRequest request) {
        return getAsyncTimelockService(namespace).startTransaction(request).toStartTransactionResponse();
    }

    @POST
    @Path("start-atlasdb-transaction-v3")
    public StartAtlasDbTransactionResponseV3 deprecatedStartTransactionV3(
            @PathParam("namespace") String namespace, StartIdentifiedAtlasDbTransactionRequest request) {
        return getAsyncTimelockService(namespace).startTransaction(request);
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
    public void startTransactions(
            @Suspended final AsyncResponse response,
            @PathParam("namespace") String namespace,
            StartTransactionRequestV4 request) {
        addJerseyCallback(getAsyncTimelockService(namespace).startTransactionsAsync(request), response);
    }

    @POST
    @Path("immutable-timestamp")
    public long getImmutableTimestamp(@PathParam("namespace") String namespace) {
        return getAsyncTimelockService(namespace).getImmutableTimestamp();
    }

    @POST
    @Path("lock")
    public void deprecatedLock(
            @Suspended final AsyncResponse response,
            @PathParam("namespace") String namespace,
            IdentifiedLockRequest request) {
        addJerseyCallback(getAsyncTimelockService(namespace).deprecatedLock(request), response);
    }

    @POST
    @Path("lock-v2")
    public void lock(
            @Suspended final AsyncResponse response,
            @PathParam("namespace") String namespace,
            IdentifiedLockRequest request) {
        addJerseyCallback(getAsyncTimelockService(namespace).lock(request), response);
    }

    @POST
    @Path("await-locks")
    public void waitForLocks(
            @Suspended final AsyncResponse response,
            @PathParam("namespace") String namespace,
            WaitForLocksRequest request) {
        addJerseyCallback(getAsyncTimelockService(namespace).waitForLocks(request), response);
    }

    @POST
    @Path("refresh-locks")
    public void deprecatedRefreshLockLeases(
            @Suspended final AsyncResponse response, @PathParam("namespace") String namespace, Set<LockToken> tokens) {
        addJerseyCallback(getAsyncTimelockService(namespace).deprecatedRefreshLockLeases(tokens), response);
    }

    @POST
    @Path("refresh-locks-v2")
    public void refreshLockLeases(
            @Suspended final AsyncResponse response, @PathParam("namespace") String namespace, Set<LockToken> tokens) {
        addJerseyCallback(getAsyncTimelockService(namespace).refreshLockLeases(tokens), response);
    }

    @GET
    @Path("leader-time")
    public void getLeaderTime(@Suspended final AsyncResponse response, @PathParam("namespace") String namespace) {
        addJerseyCallback(getAsyncTimelockService(namespace).leaderTime(), response);
    }

    @POST
    @Path("unlock")
    public void unlock(
            @Suspended final AsyncResponse response, @PathParam("namespace") String namespace, Set<LockToken> tokens) {
        addJerseyCallback(getAsyncTimelockService(namespace).unlock(tokens), response);
    }

    @POST
    @Path("current-time-millis")
    public long currentTimeMillis(@PathParam("namespace") String namespace) {
        return getAsyncTimelockService(namespace).currentTimeMillis();
    }

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    @POST
    @Path("do-not-use-without-explicit-atlasdb-authorisation/lock-diagnostic-config")
    public Optional<LockDiagnosticInfo> getEnhancedLockDiagnosticInfo(
            @PathParam("namespace") String namespace, Set<UUID> requestIds) {
        return namespaces.get(namespace).getLockLog().getAndLogLockDiagnosticInfo(requestIds);
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

    private AsyncTimelockService getAsyncTimelockService(String namespace) {
        return namespaces.get(namespace).getTimelockService();
    }
}
