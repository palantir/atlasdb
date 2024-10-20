/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.batch;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.AsyncTimelockServiceFactory;
import com.palantir.atlasdb.timelock.api.GetMinLeasedTimestampResponses;
import com.palantir.atlasdb.timelock.api.MultiClientGetMinLeasedTimestampRequest;
import com.palantir.atlasdb.timelock.api.MultiClientGetMinLeasedTimestampResponse;
import com.palantir.atlasdb.timelock.api.MultiClientTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.MultiClientTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.TimestampLeasesResponse;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.undertow.lib.RequestContext;
import java.util.Map;
import javax.annotation.Nullable;

final class RemotingMultiClientTimestampLeaseServiceAdapter {
    private final RemotingTimestampLeaseServiceAdapter delegate;

    private RemotingMultiClientTimestampLeaseServiceAdapter(RemotingTimestampLeaseServiceAdapter delegate) {
        this.delegate = delegate;
    }

    RemotingMultiClientTimestampLeaseServiceAdapter(AsyncTimelockServiceFactory timelockServices) {
        this(new RemotingTimestampLeaseServiceAdapter(timelockServices));
    }

    ListenableFuture<MultiClientTimestampLeaseResponse> acquireTimestampLeases(
            MultiClientTimestampLeaseRequest requests, @Nullable RequestContext context) {
        Map<Namespace, ListenableFuture<TimestampLeasesResponse>> futures = KeyedStream.stream(requests.get())
                .map((namespace, request) -> delegate.acquireTimestampLeases(namespace, request, context))
                .collectToMap();

        // TODO(aalouane): clean up lease resources in cases of partial failures
        return Futures.transform(
                AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor()),
                MultiClientTimestampLeaseResponse::of,
                MoreExecutors.directExecutor());
    }

    ListenableFuture<MultiClientGetMinLeasedTimestampResponse> getMinLeasedTimestamps(
            MultiClientGetMinLeasedTimestampRequest requests, @Nullable RequestContext context) {
        Map<Namespace, ListenableFuture<GetMinLeasedTimestampResponses>> futures = KeyedStream.stream(requests.get())
                .map((namespace, request) -> delegate.getMinLeasedTimestamps(namespace, request, context))
                .collectToMap();

        return Futures.transform(
                AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor()),
                MultiClientGetMinLeasedTimestampResponse::of,
                MoreExecutors.directExecutor());
    }
}
