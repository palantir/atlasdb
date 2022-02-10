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
import com.palantir.atlasdb.backup.api.AtlasBackupClientBlocking;
import com.palantir.atlasdb.backup.api.CompleteBackupRequest;
import com.palantir.atlasdb.backup.api.CompleteBackupResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.api.InProgressBackupToken;
import com.palantir.atlasdb.backup.api.PrepareBackupRequest;
import com.palantir.atlasdb.backup.api.PrepareBackupResponse;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.dialogue.clients.DialogueClients.ReloadingFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tokens.auth.AuthHeader;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class AtlasBackupService {
    private final AuthHeader authHeader;
    private final AtlasBackupClientBlocking atlasBackupClientBlocking;
    private final CoordinationServiceRecorder coordinationServiceRecorder;
    private final BackupPersister backupPersister;
    private final Map<Namespace, InProgressBackupToken> inProgressBackups;

    @VisibleForTesting
    AtlasBackupService(
            AuthHeader authHeader,
            AtlasBackupClientBlocking atlasBackupClientBlocking,
            CoordinationServiceRecorder coordinationServiceRecorder,
            BackupPersister backupPersister) {
        this.authHeader = authHeader;
        this.atlasBackupClientBlocking = atlasBackupClientBlocking;
        this.coordinationServiceRecorder = coordinationServiceRecorder;
        this.backupPersister = backupPersister;
        this.inProgressBackups = new ConcurrentHashMap<>();
    }

    public static AtlasBackupService create(
            AuthHeader authHeader,
            Refreshable<ServicesConfigBlock> servicesConfigBlock,
            String serviceName,
            Function<Namespace, Path> backupFolderFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        ReloadingFactory reloadingFactory = DialogueClients.create(servicesConfigBlock)
                // TODO(gs): wire a proper user agent!
                .withUserAgent(UserAgent.of(UserAgent.Agent.of("atlas-backup", "0.539.0")));
        AtlasBackupClientBlocking atlasBackupClientBlocking =
                reloadingFactory.get(AtlasBackupClientBlocking.class, serviceName);

        BackupPersister backupPersister = new ExternalBackupPersister(backupFolderFactory);
        CoordinationServiceRecorder coordinationServiceRecorder =
                new CoordinationServiceRecorder(keyValueServiceFactory, backupPersister);

        return new AtlasBackupService(
                authHeader, atlasBackupClientBlocking, coordinationServiceRecorder, backupPersister);
    }

    public Set<Namespace> prepareBackup(Set<Namespace> namespaces) {
        PrepareBackupRequest request = PrepareBackupRequest.of(namespaces);
        PrepareBackupResponse response = atlasBackupClientBlocking.prepareBackup(authHeader, request);

        return response.getSuccessful().stream()
                .peek(this::storeBackupToken)
                .map(InProgressBackupToken::getNamespace)
                .collect(Collectors.toSet());
    }

    private void storeBackupToken(InProgressBackupToken backupToken) {
        inProgressBackups.put(backupToken.getNamespace(), backupToken);
        backupPersister.storeImmutableTimestamp(backupToken);
    }

    public Set<Namespace> completeBackup(Set<Namespace> namespaces) {
        Set<InProgressBackupToken> tokens = namespaces.stream()
                .map(inProgressBackups::remove)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        CompleteBackupRequest request = CompleteBackupRequest.of(tokens);
        CompleteBackupResponse response = atlasBackupClientBlocking.completeBackup(authHeader, request);

        return response.getSuccessfulBackups().stream()
                .peek(coordinationServiceRecorder::storeFastForwardState)
                .peek(backupPersister::storeCompletedBackup)
                .map(CompletedBackup::getNamespace)
                .collect(Collectors.toSet());
    }
}
