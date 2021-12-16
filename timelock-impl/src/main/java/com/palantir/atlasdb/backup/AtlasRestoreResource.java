/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.backup;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.backup.api.AtlasRestoreClient;
import com.palantir.atlasdb.backup.api.AtlasRestoreClientEndpoints;
import com.palantir.atlasdb.backup.api.CompleteRestoreRequest;
import com.palantir.atlasdb.backup.api.CompleteRestoreResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.api.UndertowAtlasRestoreClient;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.ConjureResourceExceptionHandler;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AtlasRestoreResource implements UndertowAtlasRestoreClient {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasRestoreResource.class);

    private final Function<String, AsyncTimelockService> timelockServices;
    private final ConjureResourceExceptionHandler exceptionHandler;

    @VisibleForTesting
    AtlasRestoreResource(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockServices = timelockServices;
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        return AtlasRestoreClientEndpoints.of(new AtlasRestoreResource(redirectRetryTargeter, timelockServices));
    }

    public static AtlasRestoreClient jersey(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        return new JerseyAtlasRestoreClientAdapter(new AtlasRestoreResource(redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<CompleteRestoreResponse> completeRestore(
            AuthHeader authHeader, CompleteRestoreRequest request) {
        return handleExceptions(() -> completeRestoreInternal(request));
    }

    private ListenableFuture<CompleteRestoreResponse> completeRestoreInternal(CompleteRestoreRequest request) {
        Map<CompletedBackup, ListenableFuture<Optional<Namespace>>> futureMap = request.getCompletedBackups().stream()
                .collect(Collectors.toMap(backup -> backup, this::completeRestoreAsync));
        ListenableFuture<Map<CompletedBackup, Namespace>> singleFuture =
                AtlasFutures.allAsMap(futureMap, MoreExecutors.newDirectExecutorService());

        return Futures.transform(
                singleFuture,
                map -> CompleteRestoreResponse.of(ImmutableSet.copyOf(map.values())),
                MoreExecutors.directExecutor());
    }

    private ListenableFuture<Optional<Namespace>> completeRestoreAsync(CompletedBackup completedBackup) {
        Namespace namespace = completedBackup.getNamespace();
        AsyncTimelockService timelock = timelock(namespace);

        // Validate that we'll actually be going forwards
        long freshTimestamp = timelock.getFreshTimestamp();
        long fastForwardTimestamp = completedBackup.getBackupEndTimestamp();
        if (freshTimestamp > fastForwardTimestamp) {
            log.error(
                    "Attempted to fast forward timestamp, but our timestamp bound is already greater than the fast"
                            + " forward timestamp",
                    SafeArg.of("namespace", namespace),
                    SafeArg.of("freshTimestamp", freshTimestamp),
                    SafeArg.of("fastForwardTimestamp", fastForwardTimestamp));
            return Futures.immediateFuture(Optional.empty());
        }

        timelock.fastForwardTimestamp(fastForwardTimestamp);
        return Futures.immediateFuture(Optional.of(namespace));
    }

    private AsyncTimelockService timelock(Namespace namespace) {
        return timelockServices.apply(namespace.get());
    }

    private <T> ListenableFuture<T> handleExceptions(Supplier<ListenableFuture<T>> supplier) {
        return exceptionHandler.handleExceptions(supplier);
    }

    private static class JerseyAtlasRestoreClientAdapter implements AtlasRestoreClient {
        private final AtlasRestoreResource resource;

        public JerseyAtlasRestoreClientAdapter(AtlasRestoreResource resource) {
            this.resource = resource;
        }

        @Override
        public CompleteRestoreResponse completeRestore(AuthHeader authHeader, CompleteRestoreRequest request) {
            return AtlasFutures.getUnchecked(resource.completeRestore(authHeader, request));
        }
    }
}
