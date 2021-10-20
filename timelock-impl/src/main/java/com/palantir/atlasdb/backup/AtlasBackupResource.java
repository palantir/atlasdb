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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.backup.api.AtlasBackupService;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.factory.AtlasDbDialogueServiceProvider;
import com.palantir.atlasdb.timelock.api.BackupToken;
import com.palantir.atlasdb.timelock.api.CheckBackupIsValidRequest;
import com.palantir.atlasdb.timelock.api.CheckBackupIsValidResponse;
import com.palantir.atlasdb.timelock.api.CompleteBackupRequest;
import com.palantir.atlasdb.timelock.api.CompleteBackupResponse;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockImmutableTimestampResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockImmutableTimestampResponse.Visitor;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.PrepareBackupRequest;
import com.palantir.atlasdb.timelock.api.PrepareBackupResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulLockImmutableTimestampResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulPrepareBackupResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockImmutableTimestampResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulPrepareBackupResponse;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.lock.v2.LockToken;
import com.palantir.refreshable.Refreshable;
import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public class AtlasBackupResource implements AtlasBackupService {
    private final ConjureTimelockService timelockService;

    @VisibleForTesting
    AtlasBackupResource(ConjureTimelockService timelockService) {
        this.timelockService = timelockService;
    }

    public static AtlasBackupResource create(
            Refreshable<ServerListConfig> serverListConfig,
            DialogueClients.ReloadingFactory reloadingFactory,
            UserAgent userAgent,
            TaggedMetricRegistry taggedMetricRegistry) {
        AtlasDbDialogueServiceProvider serviceProvider = AtlasDbDialogueServiceProvider.create(
                serverListConfig, reloadingFactory, userAgent, taggedMetricRegistry);
        ConjureTimelockService timelockService = serviceProvider.getConjureTimelockService();

        return new AtlasBackupResource(timelockService);
    }

    @Override
    public PrepareBackupResponse prepareBackup(AuthHeader authHeader, PrepareBackupRequest request) {
        Namespace namespace = getAny(request.getNamespaces());
        ConjureLockImmutableTimestampResponse response =
                timelockService.lockImmutableTimestamp(authHeader, namespace.get());
        return response.accept(new Visitor<>() {
            @Override
            public PrepareBackupResponse visitSuccessful(SuccessfulLockImmutableTimestampResponse value) {
                LockToken lockToken = LockToken.of(value.getLockToken().getRequestId());
                BackupToken backupToken = BackupToken.of(namespace, value.getImmutableTimestamp(), lockToken);
                return PrepareBackupResponse.successful(SuccessfulPrepareBackupResponse.builder()
                        .backupTokens(backupToken)
                        .build());
            }

            @Override
            public PrepareBackupResponse visitUnsuccessful(UnsuccessfulLockImmutableTimestampResponse value) {
                return PrepareBackupResponse.unsuccessful(UnsuccessfulPrepareBackupResponse.of());
            }

            @Override
            public PrepareBackupResponse visitUnknown(String unknownType) {
                return PrepareBackupResponse.unsuccessful(UnsuccessfulPrepareBackupResponse.of());
            }
        });
    }

    @NotNull
    private <T> T getAny(Set<T> input) {
        // single ns for now
        return input.stream().findAny().orElseThrow();
    }

    @Override
    public Map<Namespace, Long> getFreshTimestamps(AuthHeader authHeader, Set<Namespace> namespaces) {
        ConjureGetFreshTimestampsRequest request = ConjureGetFreshTimestampsRequest.of(1);
        Namespace namespace = getAny(namespaces);
        ConjureGetFreshTimestampsResponse response =
                timelockService.getFreshTimestamps(authHeader, namespace.get(), request);
        return ImmutableMap.of(namespace, response.getInclusiveLower());
    }

    @Override
    public CheckBackupIsValidResponse checkBackupIsValid(AuthHeader authHeader, CheckBackupIsValidRequest request) {
        BackupToken backupToken = getAny(request.getBackupTokens());
        ConjureLockToken conjureLockToken = getConjureLockToken(backupToken);
        ConjureRefreshLocksRequest conjureRequest = ConjureRefreshLocksRequest.of(Set.of(conjureLockToken));
        ConjureRefreshLocksResponse response = timelockService.refreshLocks(
                authHeader, backupToken.getNamespace().get(), conjureRequest);
        return response.getRefreshedTokens().contains(conjureLockToken)
                ? CheckBackupIsValidResponse.of(ImmutableSet.of(backupToken), ImmutableSet.of())
                : CheckBackupIsValidResponse.of(ImmutableSet.of(), ImmutableSet.of(backupToken));
    }

    @Override
    public CompleteBackupResponse completeBackup(AuthHeader authHeader, CompleteBackupRequest request) {
        BackupToken backupToken = getAny(request.getBackupTokens());
        ConjureLockToken conjureLockToken = getConjureLockToken(backupToken);
        ConjureUnlockRequest conjureRequest = ConjureUnlockRequest.of(Set.of(conjureLockToken));
        Set<ConjureLockToken> unlockedTokens = timelockService
                .unlock(authHeader, backupToken.getNamespace().get(), conjureRequest)
                .getTokens();
        Set<Namespace> oneNamespace = ImmutableSet.of(backupToken.getNamespace());
        return unlockedTokens.contains(conjureLockToken)
                ? CompleteBackupResponse.of(oneNamespace, ImmutableSet.of())
                : CompleteBackupResponse.of(ImmutableSet.of(), oneNamespace);
    }

    @NotNull
    private ConjureLockToken getConjureLockToken(BackupToken backupToken) {
        return ConjureLockToken.of(backupToken.getLockToken().getRequestId());
    }
}
