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
import com.palantir.atlasdb.backup.api.AtlasBackupClient;
import com.palantir.atlasdb.backup.api.AtlasBackupClientEndpoints;
import com.palantir.atlasdb.backup.api.CompleteBackupRequest;
import com.palantir.atlasdb.backup.api.CompleteBackupResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.api.InProgressBackupToken;
import com.palantir.atlasdb.backup.api.PrepareBackupRequest;
import com.palantir.atlasdb.backup.api.PrepareBackupResponse;
import com.palantir.atlasdb.backup.api.UndertowAtlasBackupClient;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.BackupTimeLockServiceView;
import com.palantir.atlasdb.timelock.ConjureResourceExceptionHandler;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.conjure.java.api.errors.ErrorType;
import com.palantir.conjure.java.api.errors.ServiceException;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AtlasBackupResource implements UndertowAtlasBackupClient {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasBackupResource.class);

    private final AuthHeaderValidator authHeaderValidator;
    private final Function<String, ? extends BackupTimeLockServiceView> timelockServices;
    private final ConjureResourceExceptionHandler exceptionHandler;

    @VisibleForTesting
    AtlasBackupResource(
            AuthHeaderValidator authHeaderValidator,
            RedirectRetryTargeter redirectRetryTargeter,
            Function<String, ? extends BackupTimeLockServiceView> timelockServices) {
        this.authHeaderValidator = authHeaderValidator;
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockServices = timelockServices;
    }

    public static UndertowService undertow(
            AuthHeaderValidator authHeaderValidator,
            RedirectRetryTargeter redirectRetryTargeter,
            Function<String, ? extends BackupTimeLockServiceView> timelockServices) {
        return AtlasBackupClientEndpoints.of(
                new AtlasBackupResource(authHeaderValidator, redirectRetryTargeter, timelockServices));
    }

    public static AtlasBackupClient jersey(
            AuthHeaderValidator authHeaderValidator,
            RedirectRetryTargeter redirectRetryTargeter,
            Function<String, ? extends BackupTimeLockServiceView> timelockServices) {
        return new JerseyAtlasBackupClientAdapter(
                new AtlasBackupResource(authHeaderValidator, redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<PrepareBackupResponse> prepareBackup(AuthHeader authHeader, PrepareBackupRequest request) {
        return handleExceptions(() -> Futures.immediateFuture(prepareBackupInternal(authHeader, request)));
    }

    private PrepareBackupResponse prepareBackupInternal(AuthHeader authHeader, PrepareBackupRequest request) {
        if (!authHeaderValidator.suppliedTokenIsValid(authHeader)) {
            log.error(
                    "Attempted to prepare backup with an invalid auth header. "
                            + "The provided token must match the configured permitted-backup-token.",
                    SafeArg.of("request", request));
            throw new ServiceException(ErrorType.PERMISSION_DENIED);
        }

        Set<InProgressBackupToken> preparedBackups =
                request.getNamespaces().stream().map(this::prepareBackup).collect(Collectors.toSet());
        return PrepareBackupResponse.of(preparedBackups);
    }

    private InProgressBackupToken prepareBackup(Namespace namespace) {
        BackupTimeLockServiceView timelock = timelock(namespace);
        LockImmutableTimestampResponse response = timelock.lockImmutableTimestamp(IdentifiedTimeLockRequest.create());
        long timestamp = timelock.getFreshTimestamp();

        return InProgressBackupToken.builder()
                .namespace(namespace)
                .lockToken(response.getLock())
                .immutableTimestamp(response.getImmutableTimestamp())
                .backupStartTimestamp(timestamp)
                .build();
    }

    @Override
    public ListenableFuture<CompleteBackupResponse> completeBackup(
            AuthHeader authHeader, CompleteBackupRequest request) {
        return handleExceptions(() -> completeBackupInternal(authHeader, request));
    }

    @SuppressWarnings("ConstantConditions")
    private ListenableFuture<CompleteBackupResponse> completeBackupInternal(
            AuthHeader authHeader, CompleteBackupRequest request) {
        if (!authHeaderValidator.suppliedTokenIsValid(authHeader)) {
            log.error(
                    "Attempted to complete backup with an invalid auth header. "
                            + "The provided token must match the configured permitted-backup-token.",
                    SafeArg.of("request", request));
            throw new ServiceException(ErrorType.PERMISSION_DENIED);
        }

        Map<InProgressBackupToken, ListenableFuture<Optional<CompletedBackup>>> futureMap =
                request.getBackupTokens().stream().collect(Collectors.toMap(token -> token, this::completeBackupAsync));
        ListenableFuture<Map<InProgressBackupToken, CompletedBackup>> singleFuture =
                AtlasFutures.allAsMap(futureMap, MoreExecutors.newDirectExecutorService());

        return Futures.transform(
                singleFuture,
                map -> CompleteBackupResponse.of(ImmutableSet.copyOf(map.values())),
                MoreExecutors.directExecutor());
    }

    @SuppressWarnings("ConstantConditions") // optional token is never null
    private ListenableFuture<Optional<CompletedBackup>> completeBackupAsync(InProgressBackupToken backupToken) {
        return Futures.transform(
                maybeUnlock(backupToken),
                maybeToken -> maybeToken.map(_successfulUnlock -> fetchFastForwardTimestamp(backupToken)),
                MoreExecutors.directExecutor());
    }

    @SuppressWarnings("ConstantConditions") // Set of locks is never null
    private ListenableFuture<Optional<LockToken>> maybeUnlock(InProgressBackupToken backupToken) {
        return Futures.transform(
                timelock(backupToken.getNamespace()).unlock(ImmutableSet.of(backupToken.getLockToken())),
                singletonOrEmptySet -> singletonOrEmptySet.stream().findFirst(),
                MoreExecutors.directExecutor());
    }

    private CompletedBackup fetchFastForwardTimestamp(InProgressBackupToken backupToken) {
        Namespace namespace = backupToken.getNamespace();
        long fastForwardTimestamp = timelock(namespace).getFreshTimestamp();
        return CompletedBackup.builder()
                .namespace(namespace)
                .immutableTimestamp(backupToken.getImmutableTimestamp())
                .backupStartTimestamp(backupToken.getBackupStartTimestamp())
                .backupEndTimestamp(fastForwardTimestamp)
                .build();
    }

    private BackupTimeLockServiceView timelock(Namespace namespace) {
        return timelockServices.apply(namespace.get());
    }

    private <T> ListenableFuture<T> handleExceptions(Supplier<ListenableFuture<T>> supplier) {
        return exceptionHandler.handleExceptions(supplier);
    }

    public static final class JerseyAtlasBackupClientAdapter implements AtlasBackupClient {
        private final AtlasBackupResource resource;

        public JerseyAtlasBackupClientAdapter(AtlasBackupResource resource) {
            this.resource = resource;
        }

        @Override
        public PrepareBackupResponse prepareBackup(AuthHeader authHeader, PrepareBackupRequest request) {
            return unwrap(resource.prepareBackup(authHeader, request));
        }

        @Override
        public CompleteBackupResponse completeBackup(AuthHeader authHeader, CompleteBackupRequest request) {
            return unwrap(resource.completeBackup(authHeader, request));
        }

        private static <T> T unwrap(ListenableFuture<T> future) {
            return AtlasFutures.getUnchecked(future);
        }
    }
}
