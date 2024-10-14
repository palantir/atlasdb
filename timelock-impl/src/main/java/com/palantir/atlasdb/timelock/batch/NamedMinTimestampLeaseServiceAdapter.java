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
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.api.GenericNamedMinTimestamp;
import com.palantir.atlasdb.timelock.api.GetMinLeasedNamedTimestampRequests;
import com.palantir.atlasdb.timelock.api.GetMinLeasedNamedTimestampResponses;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseRequests;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseResponses;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.undertow.lib.RequestContext;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

final class NamedMinTimestampLeaseServiceAdapter {
    private final BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices;

    NamedMinTimestampLeaseServiceAdapter(BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        this.timelockServices = timelockServices;
    }

    ListenableFuture<NamedMinTimestampLeaseResponses> acquireNamedMinTimestampLeases(
            Namespace namespace, NamedMinTimestampLeaseRequests requests, @Nullable RequestContext context) {
        AsyncTimelockService service = getServiceForNamespace(namespace, context);

        Map<GenericNamedMinTimestamp, ListenableFuture<NamedMinTimestampLeaseResponse>> futures = KeyedStream.stream(
                        requests.get())
                .map((timestampName, request) -> acquireNamedMinTimestampLease(service, timestampName, request))
                .collectToMap();

        // TODO(aalouane): clean up lease resources in cases of partial failures
        return Futures.transform(
                AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor()),
                NamedMinTimestampLeaseResponses::of,
                MoreExecutors.directExecutor());
    }

    ListenableFuture<GetMinLeasedNamedTimestampResponses> getMinLeasedNamedTimestamps(
            Namespace namespace, GetMinLeasedNamedTimestampRequests request, RequestContext context) {
        AsyncTimelockService service = getServiceForNamespace(namespace, context);

        Map<GenericNamedMinTimestamp, ListenableFuture<Long>> futures = KeyedStream.of(request.get())
                .map(service::getMinLeasedNamedTimestamp)
                .collectToMap();

        return Futures.transform(
                AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor()),
                GetMinLeasedNamedTimestampResponses::of,
                MoreExecutors.directExecutor());
    }

    private AsyncTimelockService getServiceForNamespace(Namespace namespace, @Nullable RequestContext context) {
        return timelockServices.apply(namespace.get(), TimelockNamespaces.toUserAgent(context));
    }

    private static ListenableFuture<NamedMinTimestampLeaseResponse> acquireNamedMinTimestampLease(
            AsyncTimelockService service,
            GenericNamedMinTimestamp timestampName,
            NamedMinTimestampLeaseRequest request) {
        return service.acquireNamedMinTimestampLease(
                timestampName, request.getRequestId(), request.getNumFreshTimestamps());
    }
}
