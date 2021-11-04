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

import com.palantir.atlasdb.backup.api.AtlasBackupServiceBlocking;
import com.palantir.atlasdb.timelock.api.BackupToken;
import com.palantir.atlasdb.timelock.api.CompleteBackupRequest;
import com.palantir.atlasdb.timelock.api.CompleteBackupResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.PrepareBackupRequest;
import com.palantir.atlasdb.timelock.api.PrepareBackupResponse;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.dialogue.clients.DialogueClients.ReloadingFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tokens.auth.AuthHeader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

// TODO(gs): add tests
public final class AtlasBackupTask {
    private final AuthHeader authHeader;
    private final AtlasBackupServiceBlocking atlasBackupServiceBlocking;
    private final Map<Namespace, BackupToken> storedTokens;

    private AtlasBackupTask(AuthHeader authHeader, AtlasBackupServiceBlocking atlasBackupServiceBlocking) {
        this.authHeader = authHeader;
        this.atlasBackupServiceBlocking = atlasBackupServiceBlocking;
        this.storedTokens = new HashMap<>();
    }

    public static AtlasBackupTask create(
            AuthHeader authHeader, Refreshable<ServicesConfigBlock> servicesConfigBlock, String serviceName) {
        ReloadingFactory reloadingFactory = DialogueClients.create(servicesConfigBlock);
        AtlasBackupServiceBlocking atlasBackupServiceBlocking =
                reloadingFactory.get(AtlasBackupServiceBlocking.class, serviceName);
        return new AtlasBackupTask(authHeader, atlasBackupServiceBlocking);
    }

    public Set<Namespace> prepareBackup(Set<Namespace> namespaces) {
        PrepareBackupRequest request = PrepareBackupRequest.of(namespaces);
        PrepareBackupResponse response = atlasBackupServiceBlocking.prepareBackup(authHeader, request);

        return response.getSuccessful().stream()
                .peek(this::storeBackupToken)
                .map(BackupToken::getNamespace)
                .collect(Collectors.toSet());
    }

    private void storeBackupToken(BackupToken backupToken) {
        storedTokens.put(backupToken.getNamespace(), backupToken);
    }

    // TODO(gs): actually persist the token using a persister passed into this class.
    //   Then we have an atlas-side implementation of the persister that conforms with the current backup story
    public Set<Namespace> completeBackup(Set<Namespace> namespaces) {
        Set<BackupToken> tokens = namespaces.stream()
                .map(storedTokens::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        CompleteBackupRequest request = CompleteBackupRequest.of(tokens);
        CompleteBackupResponse response = atlasBackupServiceBlocking.completeBackup(authHeader, request);

        return response.getSuccessfulBackups().stream()
                .map(BackupToken::getNamespace)
                .collect(Collectors.toSet());
    }
}
