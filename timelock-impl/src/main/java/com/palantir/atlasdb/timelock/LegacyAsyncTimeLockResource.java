/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.conjure.java.undertow.annotations.Handle;
import com.palantir.conjure.java.undertow.annotations.HttpMethod;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.ImmutableStartTransactionResponseV4;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartAtlasDbTransactionResponseV3;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartTransactionRequestV4;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.logsafe.Safe;
import com.palantir.timestamp.TimestampRange;

/**
 * This class exists as a simple migration of the asynchronous Jersey resources in {@link TimeLockResource}.
 *
 * New endpoints should not be added here; instead, they should be defined in Conjure.
 * Namespaces should respect the regular expression (?!(tl|lw)/)[a-zA-Z0-9_-]+. Requests will still be mapped here, but
 * we do not guarantee we will service them.
 */
public final class LegacyAsyncTimeLockResource {
    private final TimelockNamespaces namespaces;

    public LegacyAsyncTimeLockResource(TimelockNamespaces namespaces) {
        this.namespaces = namespaces;
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/fresh-timestamp")
    public ListenableFuture<Long> getFreshTimestamp(@Safe @Handle.PathParam String namespace) {
        return Futures.transform(
                getTimelockService(namespace).getFreshTimestampsAsync(1),
                TimestampRange::getLowerBound,
                MoreExecutors.directExecutor());
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/fresh-timestamps")
    public ListenableFuture<TimestampRange> getFreshTimestamps(
            @Safe @Handle.PathParam String namespace,
            @Safe @Handle.QueryParam(value = "number") int numTimestampsRequested) {
        return getTimelockService(namespace).getFreshTimestampsAsync(numTimestampsRequested);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/lock-immutable-timestamp")
    public LockImmutableTimestampResponse lockImmutableTimestamp(
            @Safe @Handle.PathParam String namespace, @Handle.Body IdentifiedTimeLockRequest request) {
        return getTimelockService(namespace).lockImmutableTimestamp(request);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/start-atlasdb-transaction")
    public StartAtlasDbTransactionResponse deprecatedStartTransactionV1(
            @Safe @Handle.PathParam String namespace, @Handle.Body IdentifiedTimeLockRequest request) {
        return getTimelockService(namespace).deprecatedStartTransaction(request);
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/start-identified-atlasdb-transaction")
    public StartIdentifiedAtlasDbTransactionResponse deprecatedStartTransactionV2(
            @Safe @Handle.PathParam String namespace, @Handle.Body StartIdentifiedAtlasDbTransactionRequest request) {
        return getTimelockService(namespace).startTransaction(request).toStartTransactionResponse();
    }

    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/start-atlasdb-transaction-v3")
    public StartAtlasDbTransactionResponseV3 deprecatedStartTransactionV3(
            @Safe @Handle.PathParam String namespace, @Handle.Body StartIdentifiedAtlasDbTransactionRequest request) {
        return getTimelockService(namespace).startTransaction(request);
    }

    /**
     * Returns a {@link StartTransactionResponseV4} which has a single immutable ts, and a range of timestamps to
     * be used as start timestamps.
     *
     * It is guaranteed to have at least one usable timestamp matching the partition criteria in the returned timestamp
     * range, but there is no other guarantee given. (It can be less than number of requested timestamps)
     */
    @Handle(method = HttpMethod.POST, path = "/{namespace}/timelock/start-atlasdb-transaction-v4")
    public ListenableFuture<StartTransactionResponseV4> startTransactions(
            @Safe @Handle.PathParam String namespace, @Handle.Body StartTransactionRequestV4 request) {
        ConjureStartTransactionsRequest conjureRequest = ConjureStartTransactionsRequest.builder()
                .requestId(request.requestId())
                .requestorId(request.requestorId())
                .numTransactions(request.numTransactions())
                .build();
        return Futures.transform(
                getTimelockService(namespace).startTransactionsWithWatches(conjureRequest),
                newResponse -> ImmutableStartTransactionResponseV4.builder()
                        .timestamps(newResponse.getTimestamps())
                        .immutableTimestamp(newResponse.getImmutableTimestamp())
                        .lease(newResponse.getLease())
                        .build(),
                MoreExecutors.directExecutor());
    }

    private AsyncTimelockService getTimelockService(String namespace) {
        return namespaces.get(namespace).getTimelockService();
    }
}
