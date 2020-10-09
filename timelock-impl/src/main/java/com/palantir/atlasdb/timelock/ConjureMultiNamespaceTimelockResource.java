/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.api.ConjureLeaderTimesRequest;
import com.palantir.atlasdb.timelock.api.ConjureLeaderTimesResponse;
import com.palantir.atlasdb.timelock.api.ConjureMultiNamespaceTimelockServiceEndpoints;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.UndertowConjureMultiNamespaceTimelockService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;

public final class ConjureMultiNamespaceTimelockResource implements UndertowConjureMultiNamespaceTimelockService {


    private final ConjureResourceExceptionHandler exceptionHandler;
    private final Function<String, AsyncTimelockService> timelockServices;

    public ConjureMultiNamespaceTimelockResource(
            RedirectRetryTargeter redirectRetryTargeter,
            Function<String, AsyncTimelockService> timelockServices) {
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockServices = timelockServices;
    }

    @Override
    public ListenableFuture<ConjureLeaderTimesResponse> leaderTime(AuthHeader authHeader,
            ConjureLeaderTimesRequest request) {
        return handleExceptions(() -> {
            Map<Namespace, ListenableFuture<LeaderTime>> individualFutures = KeyedStream.of(request.getRequests()).map(
                    namespace -> timelockServices.apply(namespace.get()).leaderTime()).collectToMap();

            return Futures.whenAllSucceed(individualFutures.values())
                    .call(() -> ConjureLeaderTimesResponse.builder()
                            .responses(KeyedStream.stream(individualFutures).map(Futures::getUnchecked).collectToMap())
                            .build(), MoreExecutors.directExecutor());
        });
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter,
            Function<String, AsyncTimelockService> timelockServices) {
        return ConjureMultiNamespaceTimelockServiceEndpoints.of(
                new ConjureMultiNamespaceTimelockResource(redirectRetryTargeter, timelockServices));
    }

    private <T> ListenableFuture<T> handleExceptions(Supplier<ListenableFuture<T>> supplier) {
        return exceptionHandler.handleExceptions(supplier);
    }
}
