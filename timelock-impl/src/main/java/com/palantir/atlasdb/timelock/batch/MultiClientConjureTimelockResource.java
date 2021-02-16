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

package com.palantir.atlasdb.timelock.batch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.ConjureResourceExceptionHandler;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockService;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockServiceEndpoints;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.UndertowMultiClientConjureTimelockService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class MultiClientConjureTimelockResource implements UndertowMultiClientConjureTimelockService {
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final Function<String, AsyncTimelockService> timelockServices;

    @VisibleForTesting
    MultiClientConjureTimelockResource(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockServices = timelockServices;
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        return MultiClientConjureTimelockServiceEndpoints.of(
                new MultiClientConjureTimelockResource(redirectRetryTargeter, timelockServices));
    }

    public static MultiClientConjureTimelockService jersey(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        return new JerseyAdapter(new MultiClientConjureTimelockResource(redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<LeaderTimes> leaderTimes(AuthHeader authHeader, Set<Namespace> namespaces) {
        List<ListenableFuture<Map.Entry<Namespace, LeaderTime>>> futures =
                namespaces.stream().map(this::getNamespacedLeaderTimes).collect(Collectors.toList());

        return handleExceptions(() -> Futures.transform(
                Futures.allAsList(futures),
                entryList -> LeaderTimes.of(ImmutableMap.copyOf(entryList)),
                MoreExecutors.directExecutor()));
    }

    @Override
    public ListenableFuture<Map<Namespace, ConjureStartTransactionsResponse>> startTransactions(
            AuthHeader authHeader, Map<Namespace, ConjureStartTransactionsRequest> requests) {
        List<ListenableFuture<Map.Entry<Namespace, ConjureStartTransactionsResponse>>> futures = KeyedStream.stream(
                        requests)
                .map(this::getStartTransactionsResponseListenableFutures)
                .values()
                .collect(Collectors.toList());
        return handleExceptions(() ->
                Futures.transform(Futures.allAsList(futures), ImmutableMap::copyOf, MoreExecutors.directExecutor()));
    }

    private ListenableFuture<Map.Entry<Namespace, ConjureStartTransactionsResponse>>
            getStartTransactionsResponseListenableFutures(
                    Namespace namespace, ConjureStartTransactionsRequest request) {
        ListenableFuture<ConjureStartTransactionsResponse> conjureStartTransactionsResponseListenableFuture =
                getServiceForNamespace(namespace).startTransactionsWithWatches(request);
        return Futures.transform(
                conjureStartTransactionsResponseListenableFuture,
                startTransactionsResponse -> Maps.immutableEntry(namespace, startTransactionsResponse),
                MoreExecutors.directExecutor());
    }

    private ListenableFuture<Map.Entry<Namespace, LeaderTime>> getNamespacedLeaderTimes(Namespace namespace) {
        ListenableFuture<LeaderTime> leaderTimeListenableFuture =
                getServiceForNamespace(namespace).leaderTime();
        return Futures.transform(
                leaderTimeListenableFuture,
                leaderTime -> Maps.immutableEntry(namespace, leaderTime),
                MoreExecutors.directExecutor());
    }

    private AsyncTimelockService getServiceForNamespace(Namespace namespace) {
        return timelockServices.apply(namespace.get());
    }

    private <T> ListenableFuture<T> handleExceptions(Supplier<ListenableFuture<T>> supplier) {
        return exceptionHandler.handleExceptions(supplier);
    }

    public static final class JerseyAdapter implements MultiClientConjureTimelockService {
        private final MultiClientConjureTimelockResource resource;

        private JerseyAdapter(MultiClientConjureTimelockResource resource) {
            this.resource = resource;
        }

        @Override
        public LeaderTimes leaderTimes(AuthHeader authHeader, Set<Namespace> namespaces) {
            return unwrap(resource.leaderTimes(authHeader, namespaces));
        }

        @Override
        public Map<Namespace, ConjureStartTransactionsResponse> startTransactions(
                AuthHeader authHeader, Map<Namespace, ConjureStartTransactionsRequest> requests) {
            return unwrap(resource.startTransactions(authHeader, requests));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }
}
