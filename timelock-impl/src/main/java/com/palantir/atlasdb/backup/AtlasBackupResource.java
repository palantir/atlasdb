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
import com.palantir.atlasdb.backup.api.AtlasBackupService;
import com.palantir.atlasdb.timelock.api.BackupToken;
import com.palantir.atlasdb.timelock.api.CheckBackupIsValidRequest;
import com.palantir.atlasdb.timelock.api.CheckBackupIsValidResponse;
import com.palantir.atlasdb.timelock.api.CompleteBackupRequest;
import com.palantir.atlasdb.timelock.api.CompleteBackupResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.PrepareBackupRequest;
import com.palantir.atlasdb.timelock.api.PrepareBackupResponse;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tokens.auth.AuthHeader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AtlasBackupResource implements AtlasBackupService {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasBackupResource.class);
    private final Function<String, TimelockService> timelockServices;

    @VisibleForTesting
    AtlasBackupResource(Function<String, TimelockService> timelockServices) {
        this.timelockServices = timelockServices;
    }

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
        // TODO(gs): map?
        Set<BackupToken> backupTokens = new HashSet<>();
        Set<Namespace> unsuccessfulNamespaces = new HashSet<>();
        for (Namespace namespace : request.getNamespaces()) {
            try {
                LockImmutableTimestampResponse response = timelock(namespace).lockImmutableTimestamp();
                BackupToken backupToken =
                        BackupToken.of(namespace, response.getImmutableTimestamp(), response.getLock());
                backupTokens.add(backupToken);
            } catch (Exception e) {
                log.info("Failed to prepare backup for namespace", SafeArg.of("namespace", namespace), e);
                unsuccessfulNamespaces.add(namespace);
            }
        }

        return PrepareBackupResponse.builder()
                .successful(backupTokens)
                .unsuccessful(unsuccessfulNamespaces)
                .build();
    }

    @Override
    public Map<Namespace, Long> getFreshTimestamps(AuthHeader authHeader, Set<Namespace> namespaces) {
        return namespaces.stream()
                .collect(Collectors.toMap(ns -> ns, ns -> timelock(ns).getFreshTimestamp()));
    }

    @Override
    public CheckBackupIsValidResponse checkBackupIsValid(AuthHeader authHeader, CheckBackupIsValidRequest request) {
        Map<Boolean, List<BackupToken>> validAndInvalidTokens =
                request.getBackupTokens().stream().collect(Collectors.partitioningBy(this::isValid));

        return CheckBackupIsValidResponse.builder()
                .validBackupTokens(validAndInvalidTokens.get(true))
                .invalidBackupTokens(validAndInvalidTokens.get(false))
                .build();
    }

    private boolean isValid(BackupToken backupToken) {
        LockToken lockToken = backupToken.getLockToken();
        return timelock(backupToken.getNamespace())
                .refreshLockLeases(Set.of(lockToken))
                .contains(lockToken);
    }

    @Override
    public CompleteBackupResponse completeBackup(AuthHeader authHeader, CompleteBackupRequest request) {
        Map<Boolean, List<BackupToken>> results =
                request.getBackupTokens().stream().collect(Collectors.partitioningBy(this::completeBackup));

        return CompleteBackupResponse.builder()
                .successfulBackups(namespaces(results.get(true)))
                .unsuccessfulBackups(namespaces(results.get(false)))
                .build();
    }

    private boolean completeBackup(BackupToken backupToken) {
        LockToken lockToken = backupToken.getLockToken();
        return timelock(backupToken.getNamespace()).unlock(Set.of(lockToken)).contains(lockToken);
    }

    private TimelockService timelock(Namespace namespace) {
        return timelockServices.apply(namespace.get());
    }

    private static Set<Namespace> namespaces(List<BackupToken> tokens) {
        return tokens.stream().map(BackupToken::getNamespace).collect(Collectors.toSet());
    }
}
