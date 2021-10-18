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
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.factory.AtlasDbDialogueServiceProvider;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockImmutableTimestampResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockImmutableTimestampResponse.Visitor;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.NamespacedLockToken;
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
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public class AtlasBackupService {
    private final ConjureTimelockService timelockService;

    @VisibleForTesting
    AtlasBackupService(ConjureTimelockService timelockService) {
        this.timelockService = timelockService;
    }

    public static AtlasBackupService create(
            Refreshable<ServerListConfig> serverListConfig,
            DialogueClients.ReloadingFactory reloadingFactory,
            UserAgent userAgent,
            TaggedMetricRegistry taggedMetricRegistry) {
        AtlasDbDialogueServiceProvider serviceProvider = AtlasDbDialogueServiceProvider.create(
                serverListConfig, reloadingFactory, userAgent, taggedMetricRegistry);
        ConjureTimelockService timelockService = serviceProvider.getConjureTimelockService();

        return new AtlasBackupService(timelockService);
    }

    // lock immutable timestamp
    public PrepareBackupResponse prepareBackup(AuthHeader authHeader, String namespace) {
        ConjureLockImmutableTimestampResponse response = timelockService.lockImmutableTimestamp(authHeader, namespace);
        return response.accept(new Visitor<>() {
            @Override
            public PrepareBackupResponse visitSuccessful(SuccessfulLockImmutableTimestampResponse value) {
                LockToken lockToken = LockToken.of(value.getLockToken().getRequestId());
                NamespacedLockToken namespacedLockToken = NamespacedLockToken.of(namespace, lockToken);
                return PrepareBackupResponse.successful(SuccessfulPrepareBackupResponse.builder()
                        .namespacedLockToken(namespacedLockToken)
                        .immutableTimestamp(value.getImmutableTimestamp())
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

    public long getBackupTimestamp(AuthHeader authHeader, String namespace) {
        ConjureGetFreshTimestampsRequest request = ConjureGetFreshTimestampsRequest.of(1);
        ConjureGetFreshTimestampsResponse response = timelockService.getFreshTimestamps(authHeader, namespace, request);
        return response.getInclusiveLower();
    }

    // check immutable ts lock
    public boolean checkBackupIsValid(AuthHeader authHeader, NamespacedLockToken namespacedLockToken) {
        ConjureLockToken conjureLockToken = getConjureLockToken(namespacedLockToken);
        ConjureRefreshLocksRequest request = ConjureRefreshLocksRequest.of(Set.of(conjureLockToken));
        ConjureRefreshLocksResponse response =
                timelockService.refreshLocks(authHeader, namespacedLockToken.getNamespace(), request);
        return response.getRefreshedTokens().contains(conjureLockToken);
    }

    // unlocks immutable timestamp
    public boolean completeBackup(AuthHeader authHeader, NamespacedLockToken namespacedLockToken) {
        ConjureLockToken conjureLockToken = getConjureLockToken(namespacedLockToken);
        ConjureUnlockRequest request = ConjureUnlockRequest.of(Set.of(conjureLockToken));
        Set<ConjureLockToken> unlockedTokens = timelockService
                .unlock(authHeader, namespacedLockToken.getNamespace(), request)
                .getTokens();
        return unlockedTokens.contains(conjureLockToken);
    }

    @NotNull
    private ConjureLockToken getConjureLockToken(NamespacedLockToken namespacedLockToken) {
        return ConjureLockToken.of(namespacedLockToken.getLockToken().getRequestId());
    }
}
