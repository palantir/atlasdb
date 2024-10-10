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

package com.palantir.atlasdb.timelock;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.GetMinLeasedNamedTimestampRequests;
import com.palantir.atlasdb.timelock.api.GetMinLeasedNamedTimestampResponses;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseRequests;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseResponses;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.TimestampName;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.undertow.lib.RequestContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

final class NamedMinTimestampLeaseServiceAdapter {
    private final BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices;

    NamedMinTimestampLeaseServiceAdapter(BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        this.timelockServices = timelockServices;
    }

    ListenableFuture<NamedMinTimestampLeaseResponses> acquireNamedMinTimestampLeases(
            Namespace namespace, NamedMinTimestampLeaseRequests requests, @Nullable RequestContext context) {
        AsyncTimelockService service = getServiceForNamespace(namespace, context);
        List<ListenableFuture<NamedMinTimestampLeaseResponse>> futures = requests.get().stream()
                .map(request -> acquireNamedMinTimestampLease(service, request))
                .collect(Collectors.toList());
        return Futures.transform(
                Futures.allAsList(futures), NamedMinTimestampLeaseResponses::of, MoreExecutors.directExecutor());
    }

    ListenableFuture<GetMinLeasedNamedTimestampResponses> getMinLeasedNamedTimestamps(
            Namespace namespace, GetMinLeasedNamedTimestampRequests request, RequestContext context) {
        AsyncTimelockService service = getServiceForNamespace(namespace, context);

        Map<TimestampName, ListenableFuture<Long>> futures = KeyedStream.of(request.get())
                .map(service::getMinLeasedNamedTimestamp)
                .collectToMap();

        return Futures.transform(
                AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor()),
                GetMinLeasedNamedTimestampResponses::of,
                MoreExecutors.directExecutor());
    }

    private static ListenableFuture<NamedMinTimestampLeaseResponse> acquireNamedMinTimestampLease(
            AsyncTimelockService service, NamedMinTimestampLeaseRequest request) {
        return service.acquireNamedMinTimestampLease(
                request.getTimestampName(), request.getRequestId(), request.getNumFreshTimestamps());
    }

    private AsyncTimelockService getServiceForNamespace(Namespace namespace, @Nullable RequestContext context) {
        return timelockServices.apply(namespace.get(), TimelockNamespaces.toUserAgent(context));
    }
}
