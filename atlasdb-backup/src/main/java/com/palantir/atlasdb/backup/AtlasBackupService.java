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
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.timelock.api.CompleteBackupRequest;
import com.palantir.atlasdb.timelock.api.CompleteBackupResponse;
import com.palantir.atlasdb.timelock.api.CompletedBackup;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockServiceBlocking;
import com.palantir.atlasdb.timelock.api.InProgressBackupToken;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.PrepareBackupRequest;
import com.palantir.atlasdb.timelock.api.PrepareBackupResponse;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.dialogue.clients.DialogueClients.ReloadingFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tokens.auth.AuthHeader;
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
    private final Map<Namespace, InProgressBackupToken> storedTokens;

    @VisibleForTesting
    AtlasBackupService(
            AuthHeader authHeader,
            AtlasBackupClientBlocking atlasBackupClientBlocking,
            CoordinationServiceRecorder coordinationServiceRecorder) {
        this.authHeader = authHeader;
        this.atlasBackupClientBlocking = atlasBackupClientBlocking;
        this.coordinationServiceRecorder = coordinationServiceRecorder;
        this.storedTokens = new ConcurrentHashMap<>();
    }

    // TODO(gs): pass persister into this class
    public static AtlasBackupService create(
            AuthHeader authHeader,
            Refreshable<ServicesConfigBlock> servicesConfigBlock,
            String serviceName,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        ReloadingFactory reloadingFactory = DialogueClients.create(servicesConfigBlock);
        AtlasBackupClientBlocking atlasBackupClientBlocking =
                reloadingFactory.get(AtlasBackupClientBlocking.class, serviceName);

        // TODO(gs): will this work in practice? do we need a different serviceName?
        ConjureTimelockServiceBlocking conjureTimelockServiceBlocking =
                reloadingFactory.get(ConjureTimelockServiceBlocking.class, serviceName);
        CoordinationServiceRecorder coordinationServiceRecorder = new CoordinationServiceRecorder(
                keyValueServiceFactory,
                ns -> getFreshTimestamp(conjureTimelockServiceBlocking, authHeader, ns),
                new InMemorySchemaMetadataPersister());

        return new AtlasBackupService(authHeader, atlasBackupClientBlocking, coordinationServiceRecorder);
    }

    private static Long getFreshTimestamp(
            ConjureTimelockServiceBlocking conjureTimelockServiceBlocking, AuthHeader authHeader, Namespace ns) {
        ConjureGetFreshTimestampsRequest request = ConjureGetFreshTimestampsRequest.of(1);
        ConjureGetFreshTimestampsResponse response =
                conjureTimelockServiceBlocking.getFreshTimestamps(authHeader, ns.get(), request);
        return response.getInclusiveLower();
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
        storedTokens.put(backupToken.getNamespace(), backupToken);
    }

    // TODO(gs): actually persist the token using a persister passed into this class.
    //   Then we have an atlas-side implementation of the persister that conforms with the current backup story
    public Set<Namespace> completeBackup(Set<Namespace> namespaces) {
        Set<InProgressBackupToken> tokens = namespaces.stream()
                .map(storedTokens::remove)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        CompleteBackupRequest request = CompleteBackupRequest.of(tokens);
        CompleteBackupResponse response = atlasBackupClientBlocking.completeBackup(authHeader, request);

        return response.getSuccessfulBackups().stream()
                // Store coordination service state locally.
                // This should be done here because we want the local host to be responsible for keeping that
                // information
                // AtlasBackupClient is remote (on timelock), so we might hit different nodes
                .peek(coordinationServiceRecorder::storeFastForwardState)
                .map(CompletedBackup::getNamespace)
                .collect(Collectors.toSet());
    }
}
