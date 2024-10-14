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
import com.palantir.atlasdb.timelock.api.GetMinLeasedNamedTimestampResponses;
import com.palantir.atlasdb.timelock.api.MultiClientGetMinLeasedNamedTimestampRequest;
import com.palantir.atlasdb.timelock.api.MultiClientGetMinLeasedNamedTimestampResponse;
import com.palantir.atlasdb.timelock.api.MultiClientNamedMinTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.MultiClientNamedMinTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseResponses;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.undertow.lib.RequestContext;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

final class MultiClientMinTimestampLeaseServiceAdapter {
    private final NamedMinTimestampLeaseServiceAdapter delegate;

    private MultiClientMinTimestampLeaseServiceAdapter(NamedMinTimestampLeaseServiceAdapter delegate) {
        this.delegate = delegate;
    }

    MultiClientMinTimestampLeaseServiceAdapter(
            BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        this(new NamedMinTimestampLeaseServiceAdapter(timelockServices));
    }

    ListenableFuture<MultiClientNamedMinTimestampLeaseResponse> acquireNamedMinTimestampLease(
            MultiClientNamedMinTimestampLeaseRequest requests, @Nullable RequestContext context) {
        Map<Namespace, ListenableFuture<NamedMinTimestampLeaseResponses>> futures = KeyedStream.stream(requests.get())
                .map((namespace, request) -> delegate.acquireNamedMinTimestampLeases(namespace, request, context))
                .collectToMap();

        // TODO(aalouane): clean up lease resources in cases of partial failures
        return Futures.transform(
                AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor()),
                MultiClientNamedMinTimestampLeaseResponse::of,
                MoreExecutors.directExecutor());
    }

    ListenableFuture<MultiClientGetMinLeasedNamedTimestampResponse> getMinLeasedNamedTimestamp(
            MultiClientGetMinLeasedNamedTimestampRequest requests, @Nullable RequestContext context) {
        Map<Namespace, ListenableFuture<GetMinLeasedNamedTimestampResponses>> futures = KeyedStream.stream(
                        requests.get())
                .map((namespace, request) -> delegate.getMinLeasedNamedTimestamps(namespace, request, context))
                .collectToMap();

        return Futures.transform(
                AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor()),
                MultiClientGetMinLeasedNamedTimestampResponse::of,
                MoreExecutors.directExecutor());
    }
}
