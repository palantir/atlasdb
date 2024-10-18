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
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.AsyncTimelockServiceFactory;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.api.GetMinLeasedTimestampRequests;
import com.palantir.atlasdb.timelock.api.GetMinLeasedTimestampResponses;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.atlasdb.timelock.api.TimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.TimestampLeaseRequests;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponses;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.undertow.lib.RequestContext;
import java.util.Map;
import javax.annotation.Nullable;

final class RemotingTimestampLeaseServiceAdapter {
    private final AsyncTimelockServiceFactory timelockServices;

    RemotingTimestampLeaseServiceAdapter(AsyncTimelockServiceFactory timelockServices) {
        this.timelockServices = timelockServices;
    }

    ListenableFuture<TimestampLeaseResponses> acquireTimestampLeases(
            Namespace namespace, TimestampLeaseRequests requests, @Nullable RequestContext context) {
        AsyncTimelockService service = getServiceForNamespace(namespace, context);

        Map<TimestampLeaseName, ListenableFuture<TimestampLeaseResponse>> futures = KeyedStream.stream(requests.get())
                .map((timestampName, request) -> acquireTimestampLease(service, timestampName, request))
                .collectToMap();

        // TODO(aalouane): clean up lease resources in cases of partial failures
        return Futures.transform(
                AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor()),
                TimestampLeaseResponses::of,
                MoreExecutors.directExecutor());
    }

    ListenableFuture<GetMinLeasedTimestampResponses> getMinLeasedTimestamps(
            Namespace namespace, GetMinLeasedTimestampRequests request, RequestContext context) {
        AsyncTimelockService service = getServiceForNamespace(namespace, context);

        Map<TimestampLeaseName, ListenableFuture<Long>> futures = KeyedStream.of(request.get())
                .map(service::getMinLeasedTimestamp)
                .collectToMap();

        return Futures.transform(
                AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor()),
                GetMinLeasedTimestampResponses::of,
                MoreExecutors.directExecutor());
    }

    private AsyncTimelockService getServiceForNamespace(Namespace namespace, @Nullable RequestContext context) {
        return timelockServices.get(namespace.get(), TimelockNamespaces.toUserAgent(context));
    }

    private static ListenableFuture<TimestampLeaseResponse> acquireTimestampLease(
            AsyncTimelockService service, TimestampLeaseName timestampName, TimestampLeaseRequest request) {
        return service.acquireTimestampLease(timestampName, request.getRequestId(), request.getNumFreshTimestamps());
    }
}
