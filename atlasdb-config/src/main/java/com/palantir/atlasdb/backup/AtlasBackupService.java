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

import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.factory.AtlasDbDialogueServiceProvider;
import com.palantir.atlasdb.timelock.api.ConjureLockImmutableTimestampResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockImmutableTimestampResponse.Visitor;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.SuccessfulLockImmutableTimestampResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockImmutableTimestampResponse;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.lock.v2.LockToken;
import com.palantir.refreshable.Refreshable;
import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Optional;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

// TODO(gs): move to atlasdb-backups project?
// TODO(gs): tests
public class AtlasBackupService {
    private final ConjureTimelockService timelockService;

    public AtlasBackupService(ConjureTimelockService timelockService) {
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
    public Optional<LockToken> prepareBackup(AuthHeader authHeader, String namespace) {
        ConjureLockImmutableTimestampResponse response = timelockService.lockImmutableTimestamp(authHeader, namespace);
        return response.accept(new Visitor<Optional<LockToken>>() {
            @Override
            public Optional<LockToken> visitSuccessful(SuccessfulLockImmutableTimestampResponse value) {
                return Optional.of(LockToken.of(value.getLockToken().getRequestId()));
            }

            @Override
            public Optional<LockToken> visitUnsuccessful(UnsuccessfulLockImmutableTimestampResponse value) {
                // TODO(gs): handle
                return Optional.empty();
            }

            @Override
            public Optional<LockToken> visitUnknown(String unknownType) {
                // TODO(gs): handle
                return Optional.empty();
            }
        });
    }

    // check immutable ts lock
    public boolean checkBackupIsValid(AuthHeader authHeader, String namespace, LockToken token) {
        ConjureLockToken conjureLockToken = getConjureLockToken(token);
        ConjureRefreshLocksRequest request = ConjureRefreshLocksRequest.of(Set.of(conjureLockToken));
        ConjureRefreshLocksResponse response = timelockService.refreshLocks(authHeader, namespace, request);
        return response.getRefreshedTokens().contains(conjureLockToken);
    }

    // unlocks immutable timestamp
    public boolean completeBackup(AuthHeader authHeader, String namespace, LockToken token) {
        ConjureLockToken conjureLockToken = getConjureLockToken(token);
        ConjureUnlockRequest request = ConjureUnlockRequest.of(Set.of(conjureLockToken));
        Set<ConjureLockToken> unlockedTokens =
                timelockService.unlock(authHeader, namespace, request).getTokens();
        return unlockedTokens.contains(conjureLockToken);
    }

    @NotNull
    private ConjureLockToken getConjureLockToken(LockToken token) {
        return ConjureLockToken.of(token.getRequestId());
    }
}
