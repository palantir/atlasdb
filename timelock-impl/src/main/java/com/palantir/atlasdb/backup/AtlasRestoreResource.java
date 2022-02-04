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
import com.palantir.conjure.java.api.errors.ErrorType;
import com.palantir.conjure.java.api.errors.ServiceException;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tokens.auth.BearerToken;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AtlasRestoreResource implements UndertowAtlasRestoreClient {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasRestoreResource.class);

    private final Function<String, AsyncTimelockService> timelockServices;
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final Supplier<BearerToken> permittedToken;

    @VisibleForTesting
    AtlasRestoreResource(
            Supplier<BearerToken> permittedToken,
            RedirectRetryTargeter redirectRetryTargeter,
            Function<String, AsyncTimelockService> timelockServices) {
        this.permittedToken = permittedToken;
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockServices = timelockServices;
    }

    public static UndertowService undertow(
            Supplier<BearerToken> permittedToken,
            RedirectRetryTargeter redirectRetryTargeter,
            Function<String, AsyncTimelockService> timelockServices) {
        return AtlasRestoreClientEndpoints.of(
                new AtlasRestoreResource(permittedToken, redirectRetryTargeter, timelockServices));
    }

    public static AtlasRestoreClient jersey(
            Supplier<BearerToken> permittedToken,
            RedirectRetryTargeter redirectRetryTargeter,
            Function<String, AsyncTimelockService> timelockServices) {
        return new JerseyAtlasRestoreClientAdapter(
                new AtlasRestoreResource(permittedToken, redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<CompleteRestoreResponse> completeRestore(
            AuthHeader authHeader, CompleteRestoreRequest request) {
        return handleExceptions(() -> completeRestoreInternal(authHeader, request));
    }

    private ListenableFuture<CompleteRestoreResponse> completeRestoreInternal(
            AuthHeader authHeader, CompleteRestoreRequest request) {
        if (permittedBackupToken.get() != null && !permittedToken.get().equals(authHeader.getBearerToken())) {
            log.error("Attempted to complete restore with an invalid auth header", SafeArg.of("request", request));
            throw new ServiceException(ErrorType.PERMISSION_DENIED);
        }

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
        timelock.fastForwardTimestamp(completedBackup.getBackupEndTimestamp());
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
