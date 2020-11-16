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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.ConjureResourceExceptionHandler;
import com.palantir.atlasdb.timelock.api.ConjureIdentifiedVersion;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockService;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockServiceEndpoints;
import com.palantir.atlasdb.timelock.api.NamespacedGetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.NamespacedGetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.NamespacedLeaderTime;
import com.palantir.atlasdb.timelock.api.UndertowMultiClientConjureTimelockService;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.tokens.auth.AuthHeader;
import java.util.List;
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
        return MultiClientConjureTimelockServiceEndpoints.of(new MultiClientConjureTimelockResource(
                redirectRetryTargeter,
                timelockServices));
    }

    public static MultiClientConjureTimelockService jersey(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        return new JerseyAdapter(new MultiClientConjureTimelockResource(redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<List<NamespacedLeaderTime>> leaderTimes(AuthHeader authHeader, Set<String> namespaces) {
        List<ListenableFuture<NamespacedLeaderTime>> futures = namespaces.stream()
                .map(this::getNamespacedLeaderTimeListenableFutures)
                .collect(Collectors.toList());

        return handleExceptions(() -> Futures.allAsList(futures));
    }

    @Override
    public ListenableFuture<List<NamespacedGetCommitTimestampsResponse>> getCommitTimestamps(
            AuthHeader authHeader, List<NamespacedGetCommitTimestampsRequest> requests) {
        List<ListenableFuture<NamespacedGetCommitTimestampsResponse>> futures = requests.stream()
                .map(this::getNamespacedGetCommitTimestampsResponseListenableFutures)
                .collect(Collectors.toList());

        return handleExceptions(() -> Futures.allAsList(futures));
    }

    private ListenableFuture<NamespacedLeaderTime> getNamespacedLeaderTimeListenableFutures(String namespace) {
        ListenableFuture<LeaderTime> leaderTimeListenableFuture =
                getServiceForNamespace(namespace).leaderTime();
        return Futures.transform(
                leaderTimeListenableFuture,
                leaderTime -> NamespacedLeaderTime.of(namespace, leaderTime),
                MoreExecutors.directExecutor());
    }

    private ListenableFuture<NamespacedGetCommitTimestampsResponse>
    getNamespacedGetCommitTimestampsResponseListenableFutures(NamespacedGetCommitTimestampsRequest request) {
        ListenableFuture<GetCommitTimestampsResponse> commitTimestamps = getServiceForNamespace(request.getNamespace())
                .getCommitTimestamps(
                        request.getNumTimestamps(),
                        request.getLastKnownVersion().map(this::toIdentifiedVersion));
        return Futures.transform(
                commitTimestamps,
                response -> NamespacedGetCommitTimestampsResponse.builder()
                        .namespace(request.getNamespace())
                        .inclusiveLower(response.getInclusiveLower())
                        .inclusiveUpper(response.getInclusiveUpper())
                        .lockWatchUpdate(response.getLockWatchUpdate())
                        .build(),
                MoreExecutors.directExecutor());
    }

    private AsyncTimelockService getServiceForNamespace(String namespace) {
        return timelockServices.apply(namespace);
    }

    private <T> ListenableFuture<T> handleExceptions(Supplier<ListenableFuture<T>> supplier) {
        return exceptionHandler.handleExceptions(supplier);
    }

    private LockWatchVersion toIdentifiedVersion(ConjureIdentifiedVersion conjureIdentifiedVersion) {
        return LockWatchVersion.of(conjureIdentifiedVersion.getId(), conjureIdentifiedVersion.getVersion());
    }

    public static final class JerseyAdapter implements MultiClientConjureTimelockService {
        private final MultiClientConjureTimelockResource resource;

        private JerseyAdapter(MultiClientConjureTimelockResource resource) {
            this.resource = resource;
        }

        @Override
        public List<NamespacedLeaderTime> leaderTimes(AuthHeader authHeader, Set<String> namespaces) {
            return unwrap(resource.leaderTimes(authHeader, namespaces));
        }

        @Override
        public List<NamespacedGetCommitTimestampsResponse> getCommitTimestamps(
                AuthHeader authHeader, List<NamespacedGetCommitTimestampsRequest> requests) {
            return unwrap(resource.getCommitTimestamps(authHeader, requests));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }
}
