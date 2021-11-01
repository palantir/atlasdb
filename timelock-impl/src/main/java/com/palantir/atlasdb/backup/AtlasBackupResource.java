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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.backup.api.AtlasBackupService;
import com.palantir.atlasdb.backup.api.AtlasBackupServiceEndpoints;
import com.palantir.atlasdb.backup.api.UndertowAtlasBackupService;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.ConjureResourceExceptionHandler;
import com.palantir.atlasdb.timelock.api.BackupToken;
import com.palantir.atlasdb.timelock.api.CompleteBackupRequest;
import com.palantir.atlasdb.timelock.api.CompleteBackupResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.PrepareBackupRequest;
import com.palantir.atlasdb.timelock.api.PrepareBackupResponse;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tokens.auth.AuthHeader;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AtlasBackupResource implements UndertowAtlasBackupService {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasBackupResource.class);
    private final Function<String, AsyncTimelockService> timelockServices;
    private final ConjureResourceExceptionHandler exceptionHandler;

    @VisibleForTesting
    AtlasBackupResource(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        this.exceptionHandler = new ConjureResourceExceptionHandler(redirectRetryTargeter);
        this.timelockServices = timelockServices;
    }

    public static UndertowService undertow(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        return AtlasBackupServiceEndpoints.of(new AtlasBackupResource(redirectRetryTargeter, timelockServices));
    }

    public static AtlasBackupService jersey(
            RedirectRetryTargeter redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
        return new JerseyAtlasBackupServiceAdapter(new AtlasBackupResource(redirectRetryTargeter, timelockServices));
    }

    @Override
    public ListenableFuture<PrepareBackupResponse> prepareBackup(AuthHeader authHeader, PrepareBackupRequest request) {
        return handleExceptions(() -> Futures.immediateFuture(prepareBackupInternal(request)));
    }

    private PrepareBackupResponse prepareBackupInternal(PrepareBackupRequest request) {
        Set<BackupToken> preparedBackups = request.getNamespaces().stream()
                .map(this::prepareBackup)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
        return PrepareBackupResponse.of(preparedBackups);
    }

    Optional<BackupToken> prepareBackup(Namespace namespace) {
        try {
            return Optional.of(tryPrepareBackup(namespace));
        } catch (Exception ex) {
            log.info("Failed to prepare backup for namespace", SafeArg.of("namespace", namespace), ex);
            return Optional.empty();
        }
    }

    private BackupToken tryPrepareBackup(Namespace namespace) {
        AsyncTimelockService timelock = timelock(namespace);
        LockImmutableTimestampResponse response = timelock.lockImmutableTimestamp(IdentifiedTimeLockRequest.create());
        long timestamp = timelock.getFreshTimestamp();
        return BackupToken.builder()
                .namespace(namespace)
                .lockToken(response.getLock())
                .immutableTimestamp(response.getImmutableTimestamp())
                .backupStartTimestamp(timestamp)
                .build();
    }

    @Override
    public ListenableFuture<CompleteBackupResponse> completeBackup(
            AuthHeader authHeader, CompleteBackupRequest request) {
        Map<BackupToken, ListenableFuture<Optional<BackupToken>>> futureMap =
                request.getBackupTokens().stream().collect(Collectors.toMap(token -> token, this::completeBackupAsync));
        ListenableFuture<Map<BackupToken, BackupToken>> singleFuture = AtlasFutures.futuresCombiner(
                        MoreExecutors.newDirectExecutorService())
                .allAsMap(futureMap);
        return Futures.transform(
                singleFuture,
                map -> CompleteBackupResponse.of(new HashSet<>(map.values())),
                MoreExecutors.directExecutor());
    }

    private ListenableFuture<Optional<BackupToken>> completeBackupAsync(BackupToken backupToken) {
        return Futures.transform(
                unlockAsync(backupToken),
                tokens -> fetchFastForwardTimestampIfSuccessful(backupToken, tokens),
                MoreExecutors.directExecutor());
    }

    private ListenableFuture<Set<LockToken>> unlockAsync(BackupToken backupToken) {
        return timelock(backupToken.getNamespace()).unlock(Set.of(backupToken.getLockToken()));
    }

    private Optional<BackupToken> fetchFastForwardTimestampIfSuccessful(
            BackupToken backupToken, Set<LockToken> tokens) {
        return (tokens != null && tokens.contains(backupToken.getLockToken()))
                ? Optional.of(fetchFastForwardTimestamp(backupToken))
                : Optional.empty();
    }

    private BackupToken fetchFastForwardTimestamp(BackupToken backupToken) {
        Namespace namespace = backupToken.getNamespace();
        long fastForwardTimestamp = timelock(namespace).getFreshTimestamp();
        return BackupToken.builder()
                .from(backupToken)
                .backupEndTimestamp(fastForwardTimestamp)
                .build();
    }

    private AsyncTimelockService timelock(Namespace namespace) {
        return timelockServices.apply(namespace.get());
    }

    private <T> ListenableFuture<T> handleExceptions(Supplier<ListenableFuture<T>> supplier) {
        return exceptionHandler.handleExceptions(supplier);
    }

    public static final class JerseyAtlasBackupServiceAdapter implements AtlasBackupService {
        private final AtlasBackupResource resource;

        public JerseyAtlasBackupServiceAdapter(AtlasBackupResource resource) {
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
