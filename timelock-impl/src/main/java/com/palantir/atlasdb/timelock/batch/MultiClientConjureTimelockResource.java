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
import com.palantir.atlasdb.timelock.TimelockNamespaces;
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
import com.palantir.atlasdb.timelock.api.MultiClientGetMinLeasedTimestampRequest;
import com.palantir.atlasdb.timelock.api.MultiClientGetMinLeasedTimestampResponse;
import com.palantir.atlasdb.timelock.api.MultiClientTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.MultiClientTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.UndertowMultiClientConjureTimelockService;
import com.palantir.conjure.java.undertow.lib.RequestContext;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * This implementation of multi-client batched TimeLock endpoints does not support multi-leader mode on TimeLock.
 * */
public final class MultiClientConjureTimelockResource implements UndertowMultiClientConjureTimelockService {
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices;

    @VisibleForTesting
    MultiClientConjureTimelockResource(
            RedirectRetryTargeter redirectRetryTargeter,
            BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockServices = timelockServices;
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter,
            BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        return MultiClientConjureTimelockServiceEndpoints.of(
                new MultiClientConjureTimelockResource(redirectRetryTargeter, timelockServices));
    }

    public static MultiClientConjureTimelockService jersey(
            RedirectRetryTargeter redirectRetryTargeter,
            BiFunction<String, Optional<String>, AsyncTimelockService> timelockServices) {
        return new JerseyAdapter(new MultiClientConjureTimelockResource(redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<LeaderTimes> leaderTimes(
            AuthHeader authHeader, Set<Namespace> namespaces, @Nullable RequestContext context) {
        return handleExceptions(() -> Futures.transform(
                Futures.allAsList(
                        Collections2.transform(namespaces, namespace -> getNamespacedLeaderTimes(namespace, context))),
                entryList -> LeaderTimes.of(ImmutableMap.copyOf(entryList)),
                MoreExecutors.directExecutor()));
    }

    @Override
    public ListenableFuture<Map<Namespace, ConjureStartTransactionsResponse>> startTransactions(
            AuthHeader authHeader,
            Map<Namespace, ConjureStartTransactionsRequest> requests,
            @Nullable RequestContext context) {
        return startTransactionsForClients(authHeader, requests, context);
    }

    @Override
    public ListenableFuture<Map<Namespace, GetCommitTimestampsResponse>> getCommitTimestamps(
            AuthHeader authHeader,
            Map<Namespace, GetCommitTimestampsRequest> requests,
            @Nullable RequestContext context) {
        return getCommitTimestampsForClients(authHeader, requests, context);
    }

    @Override
    public ListenableFuture<Map<Namespace, ConjureStartTransactionsResponse>> startTransactionsForClients(
            AuthHeader authHeader,
            Map<Namespace, ConjureStartTransactionsRequest> requests,
            @Nullable RequestContext context) {
        return handleExceptions(() -> Futures.transform(
                Futures.allAsList(Collections2.transform(
                        requests.entrySet(),
                        e -> startTransactionsForSingleNamespace(e.getKey(), e.getValue(), context))),
                ImmutableMap::copyOf,
                MoreExecutors.directExecutor()));
    }

    @Override
    public ListenableFuture<Map<Namespace, GetCommitTimestampsResponse>> getCommitTimestampsForClients(
            AuthHeader authHeader,
            Map<Namespace, GetCommitTimestampsRequest> requests,
            @Nullable RequestContext context) {
        return handleExceptions(() -> Futures.transform(
                Futures.allAsList(Collections2.transform(
                        requests.entrySet(),
                        e -> getCommitTimestampsForSingleNamespace(e.getKey(), e.getValue(), context))),
                ImmutableMap::copyOf,
                MoreExecutors.directExecutor()));
    }

    @Override
    public ListenableFuture<Map<Namespace, ConjureUnlockResponseV2>> unlock(
            AuthHeader authHeader, Map<Namespace, ConjureUnlockRequestV2> requests, @Nullable RequestContext context) {
        return handleExceptions(() -> Futures.transform(
                Futures.allAsList(Collections2.transform(
                        requests.entrySet(), e -> unlockForSingleNamespace(e.getKey(), e.getValue(), context))),
                ImmutableMap::copyOf,
                MoreExecutors.directExecutor()));
    }

    @Override
    public ListenableFuture<MultiClientTimestampLeaseResponse> acquireTimestampLease(
            AuthHeader authHeader, MultiClientTimestampLeaseRequest requests, @Nullable RequestContext context) {
        // TODO(aalouane): implement
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ListenableFuture<MultiClientGetMinLeasedTimestampResponse> getMinLeasedTimestamp(
            AuthHeader authHeader, MultiClientGetMinLeasedTimestampRequest requests, @Nullable RequestContext context) {
        // TODO(aalouane): implement
        throw new UnsupportedOperationException("Not implemented yet");
    }

    private ListenableFuture<Entry<Namespace, ConjureUnlockResponseV2>> unlockForSingleNamespace(
            Namespace namespace, ConjureUnlockRequestV2 request, @Nullable RequestContext context) {
        ListenableFuture<ConjureUnlockResponseV2> unlockResponseFuture = Futures.transform(
                getServiceForNamespace(namespace, context).unlock(toServerLockTokens(request.get())),
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
            Namespace namespace, GetCommitTimestampsRequest request, @Nullable RequestContext context) {
        ListenableFuture<GetCommitTimestampsResponse> commitTimestampsResponseListenableFuture = getServiceForNamespace(
                        namespace, context)
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
            startTransactionsForSingleNamespace(
                    Namespace namespace, ConjureStartTransactionsRequest request, @Nullable RequestContext context) {
        ListenableFuture<ConjureStartTransactionsResponse> conjureStartTransactionsResponseListenableFuture =
                getServiceForNamespace(namespace, context).startTransactionsWithWatches(request);
        return Futures.transform(
                conjureStartTransactionsResponseListenableFuture,
                startTransactionsResponse -> Maps.immutableEntry(namespace, startTransactionsResponse),
                MoreExecutors.directExecutor());
    }

    private ListenableFuture<Map.Entry<Namespace, LeaderTime>> getNamespacedLeaderTimes(
            Namespace namespace, @Nullable RequestContext context) {
        ListenableFuture<LeaderTime> leaderTimeListenableFuture =
                getServiceForNamespace(namespace, context).leaderTime();
        return Futures.transform(
                leaderTimeListenableFuture,
                leaderTime -> Maps.immutableEntry(namespace, leaderTime),
                MoreExecutors.directExecutor());
    }

    private AsyncTimelockService getServiceForNamespace(Namespace namespace, @Nullable RequestContext context) {
        return timelockServices.apply(namespace.get(), TimelockNamespaces.toUserAgent(context));
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
            return unwrap(resource.leaderTimes(authHeader, namespaces, null));
        }

        @Override
        @Deprecated
        public Map<Namespace, ConjureStartTransactionsResponse> startTransactions(
                AuthHeader authHeader, Map<Namespace, ConjureStartTransactionsRequest> requests) {
            return unwrap(resource.startTransactions(authHeader, requests, null));
        }

        @Override
        @Deprecated
        public Map<Namespace, GetCommitTimestampsResponse> getCommitTimestamps(
                AuthHeader authHeader, Map<Namespace, GetCommitTimestampsRequest> requests) {
            return unwrap(resource.getCommitTimestamps(authHeader, requests, null));
        }

        @Override
        public Map<Namespace, ConjureStartTransactionsResponse> startTransactionsForClients(
                AuthHeader authHeader, Map<Namespace, ConjureStartTransactionsRequest> requests) {
            return unwrap(resource.startTransactionsForClients(authHeader, requests, null));
        }

        @Override
        public Map<Namespace, GetCommitTimestampsResponse> getCommitTimestampsForClients(
                AuthHeader authHeader, Map<Namespace, GetCommitTimestampsRequest> requests) {
            return unwrap(resource.getCommitTimestampsForClients(authHeader, requests, null));
        }

        @Override
        public Map<Namespace, ConjureUnlockResponseV2> unlock(
                AuthHeader authHeader, Map<Namespace, ConjureUnlockRequestV2> requests) {
            return unwrap(resource.unlock(authHeader, requests, null));
        }

        @Override
        public MultiClientTimestampLeaseResponse acquireTimestampLease(
                AuthHeader authHeader, MultiClientTimestampLeaseRequest requests) {
            return unwrap(resource.acquireTimestampLease(authHeader, requests, null));
        }

        @Override
        public MultiClientGetMinLeasedTimestampResponse getMinLeasedTimestamp(
                AuthHeader authHeader, MultiClientGetMinLeasedTimestampRequest requests) {
            return unwrap(resource.getMinLeasedTimestamp(authHeader, requests, null));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }
}
