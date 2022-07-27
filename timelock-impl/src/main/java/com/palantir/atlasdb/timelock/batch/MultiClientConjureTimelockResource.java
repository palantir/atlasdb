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
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.ConjureResourceExceptionHandler;
import com.palantir.atlasdb.timelock.api.ConjureIdentifiedVersion;
import com.palantir.atlasdb.timelock.api.ConjureLockTokenV2;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockService;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockServiceEndpoints;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.UndertowMultiClientConjureTimelockService;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This implementation of multi-client batched TimeLock endpoints does not support multi-leader mode on TimeLock.
 * */
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
        return handleExceptions(() -> Futures.transform(
                Futures.allAsList(Collections2.transform(namespaces, this::getNamespacedLeaderTimes)),
                entryList -> LeaderTimes.of(ImmutableMap.copyOf(entryList)),
                MoreExecutors.directExecutor()));
    }

    @Override
    public ListenableFuture<Map<Namespace, ConjureStartTransactionsResponse>> startTransactions(
            AuthHeader authHeader, Map<Namespace, ConjureStartTransactionsRequest> requests) {
        return startTransactionsForClients(authHeader, requests);
    }

    @Override
    public ListenableFuture<Map<Namespace, GetCommitTimestampsResponse>> getCommitTimestamps(
            AuthHeader authHeader, Map<Namespace, GetCommitTimestampsRequest> requests) {
        return getCommitTimestampsForClients(authHeader, requests);
    }

    @Override
    public ListenableFuture<Map<Namespace, ConjureStartTransactionsResponse>> startTransactionsForClients(
            AuthHeader authHeader, Map<Namespace, ConjureStartTransactionsRequest> requests) {
        return handleExceptions(() -> Futures.transform(
                Futures.allAsList(Collections2.transform(
                        requests.entrySet(), e -> startTransactionsForSingleNamespace(e.getKey(), e.getValue()))),
                ImmutableMap::copyOf,
                MoreExecutors.directExecutor()));
    }

    @Override
    public ListenableFuture<Map<Namespace, GetCommitTimestampsResponse>> getCommitTimestampsForClients(
            AuthHeader authHeader, Map<Namespace, GetCommitTimestampsRequest> requests) {
        return handleExceptions(() -> Futures.transform(
                Futures.allAsList(Collections2.transform(
                        requests.entrySet(), e -> getCommitTimestampsForSingleNamespace(e.getKey(), e.getValue()))),
                ImmutableMap::copyOf,
                MoreExecutors.directExecutor()));
    }

    @Override
    public ListenableFuture<Map<Namespace, ConjureUnlockResponseV2>> unlock(
            AuthHeader authHeader, Map<Namespace, ConjureUnlockRequestV2> requests) {
        return handleExceptions(() -> Futures.transform(
                Futures.allAsList(Collections2.transform(
                        requests.entrySet(), e -> unlockForSingleNamespace(e.getKey(), e.getValue()))),
                ImmutableMap::copyOf,
                MoreExecutors.directExecutor()));
    }

    private ListenableFuture<Entry<Namespace, ConjureUnlockResponseV2>> unlockForSingleNamespace(
            Namespace namespace, ConjureUnlockRequestV2 request) {
        ListenableFuture<ConjureUnlockResponseV2> unlockResponseFuture = Futures.transform(
                getServiceForNamespace(namespace).unlock(toServerLockTokens(request.get())),
                response -> ConjureUnlockResponseV2.of(fromServerLockTokens(response)),
                MoreExecutors.directExecutor());
        return Futures.transform(
                unlockResponseFuture,
                response -> Maps.immutableEntry(namespace, response),
                MoreExecutors.directExecutor());
    }

    private static Set<LockToken> toServerLockTokens(Set<ConjureLockTokenV2> conjureLockTokenV2s) {
        ImmutableSet.Builder<LockToken> result = ImmutableSet.builder();
        for (ConjureLockTokenV2 originalLockToken : conjureLockTokenV2s) {
            result.add(LockToken.of(originalLockToken.get()));
        }
        return result.build();
    }

    private static Set<ConjureLockTokenV2> fromServerLockTokens(Set<LockToken> lockTokens) {
        ImmutableSet.Builder<ConjureLockTokenV2> result = ImmutableSet.builder();
        for (LockToken serverToken : lockTokens) {
            result.add(ConjureLockTokenV2.of(serverToken.getRequestId()));
        }
        return result.build();
    }

    private ListenableFuture<Map.Entry<Namespace, GetCommitTimestampsResponse>> getCommitTimestampsForSingleNamespace(
            Namespace namespace, GetCommitTimestampsRequest request) {
        ListenableFuture<GetCommitTimestampsResponse> commitTimestampsResponseListenableFuture = getServiceForNamespace(
                        namespace)
                .getCommitTimestamps(
                        request.getNumTimestamps(),
                        request.getLastKnownVersion().map(this::toIdentifiedVersion));
        return Futures.transform(
                commitTimestampsResponseListenableFuture,
                response -> Maps.immutableEntry(namespace, response),
                MoreExecutors.directExecutor());
    }

    private LockWatchVersion toIdentifiedVersion(ConjureIdentifiedVersion conjureIdentifiedVersion) {
        return LockWatchVersion.of(conjureIdentifiedVersion.getId(), conjureIdentifiedVersion.getVersion());
    }

    private ListenableFuture<Map.Entry<Namespace, ConjureStartTransactionsResponse>>
            startTransactionsForSingleNamespace(Namespace namespace, ConjureStartTransactionsRequest request) {
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
        @Deprecated
        public Map<Namespace, ConjureStartTransactionsResponse> startTransactions(
                AuthHeader authHeader, Map<Namespace, ConjureStartTransactionsRequest> requests) {
            return unwrap(resource.startTransactions(authHeader, requests));
        }

        @Override
        @Deprecated
        public Map<Namespace, GetCommitTimestampsResponse> getCommitTimestamps(
                AuthHeader authHeader, Map<Namespace, GetCommitTimestampsRequest> requests) {
            return unwrap(resource.getCommitTimestamps(authHeader, requests));
        }

        @Override
        public Map<Namespace, ConjureStartTransactionsResponse> startTransactionsForClients(
                AuthHeader authHeader, Map<Namespace, ConjureStartTransactionsRequest> requests) {
            return unwrap(resource.startTransactionsForClients(authHeader, requests));
        }

        @Override
        public Map<Namespace, GetCommitTimestampsResponse> getCommitTimestampsForClients(
                AuthHeader authHeader, Map<Namespace, GetCommitTimestampsRequest> requests) {
            return unwrap(resource.getCommitTimestampsForClients(authHeader, requests));
        }

        @Override
        public Map<Namespace, ConjureUnlockResponseV2> unlock(
                AuthHeader authHeader, Map<Namespace, ConjureUnlockRequestV2> requests) {
            return unwrap(resource.unlock(authHeader, requests));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }
}
