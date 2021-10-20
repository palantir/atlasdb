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
import com.palantir.atlasdb.backup.api.AtlasBackupService;
import com.palantir.atlasdb.backup.api.AtlasBackupServiceEndpoints;
import com.palantir.atlasdb.backup.api.UndertowAtlasBackupService;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.api.BackupToken;
import com.palantir.atlasdb.timelock.api.CompleteBackupRequest;
import com.palantir.atlasdb.timelock.api.CompleteBackupResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.PrepareBackupRequest;
import com.palantir.atlasdb.timelock.api.PrepareBackupResponse;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AtlasBackupResource implements AtlasBackupService {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasBackupResource.class);
    private final Function<String, AsyncTimelockService> timelockServices;

    @VisibleForTesting
    AtlasBackupResource(Function<String, AsyncTimelockService> timelockServices) {
        this.timelockServices = timelockServices;
    }

    // TODO(gs): add handleExceptions stuff
    // public static UndertowService undertow(
    //         RedirectRetryTargeter _redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
    //     return AtlasBackupServiceEndpoints.of(new AtlasBackupResource(timelockServices));
    // }
    //
    // // TOOO(gs): jersey wrapper
    // public static AtlasBackupService jersey(
    //         RedirectRetryTargeter _redirectRetryTargeter, Function<String, AsyncTimelockService> timelockServices) {
    //     return new AtlasBackupResource(timelockServices);
    // }

    // public static AtlasBackupResource create(
    //         Refreshable<ServerListConfig> serverListConfig,
    //         DialogueClients.ReloadingFactory reloadingFactory,
    //         UserAgent userAgent,
    //         TaggedMetricRegistry taggedMetricRegistry) {
    //     AtlasDbDialogueServiceProvider serviceProvider = AtlasDbDialogueServiceProvider.create(
    //             serverListConfig, reloadingFactory, userAgent, taggedMetricRegistry);
    //     ConjureTimelockService timelockService = serviceProvider.getConjureTimelockService();
    //
    //     return new AtlasBackupResource(timelockService);
    // }

    @Override
    public PrepareBackupResponse prepareBackup(AuthHeader authHeader, PrepareBackupRequest request) {
        Set<BackupToken> preparedBackups = request.getNamespaces().stream()
                .map(this::prepareBackup)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
        return PrepareBackupResponse.of(preparedBackups);
    }

    // TODO(gs): refresh locks
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
        LockImmutableTimestampResponse response = timelock.lockImmutableTimestamp();
        long timestamp = timelock.getFreshTimestamp();
        return BackupToken.builder()
                .namespace(namespace)
                .lockToken(response.getLock())
                .immutableTimestamp(response.getImmutableTimestamp())
                .backupStartTimestamp(timestamp)
                .build();
    }

    @Override
    public CompleteBackupResponse completeBackup(AuthHeader authHeader, CompleteBackupRequest request) {
        Set<BackupToken> completedBackups = request.getBackupTokens().stream()
                .filter(this::wasCompleteBackupSuccessful)
                .map(this::fetchFastForwardTimestamp)
                .collect(Collectors.toSet());
        return CompleteBackupResponse.of(completedBackups);
    }

    private boolean wasCompleteBackupSuccessful(BackupToken backupToken) {
        LockToken lockToken = backupToken.getLockToken();
        ListenableFuture<Set<LockToken>> unlockResult =
                timelock(backupToken.getNamespace()).unlock(Set.of(lockToken));
        // TODO(gs): proper future handling
        return Futures.getUnchecked(unlockResult).contains(lockToken)
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
}
