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
import com.palantir.atlasdb.backup.api.AtlasBackupClient;
import com.palantir.atlasdb.backup.api.AtlasBackupClientBlocking;
import com.palantir.atlasdb.backup.api.CompleteBackupRequest;
import com.palantir.atlasdb.backup.api.CompleteBackupResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.api.InProgressBackupToken;
import com.palantir.atlasdb.backup.api.PrepareBackupRequest;
import com.palantir.atlasdb.backup.api.PrepareBackupResponse;
import com.palantir.atlasdb.backup.api.RefreshBackupRequest;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.dialogue.clients.DialogueClients.ReloadingFactory;
import com.palantir.lock.client.LockRefresher;
import com.palantir.lock.v2.LockLeaseRefresher;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tokens.auth.AuthHeader;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class AtlasBackupService {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasBackupService.class);

    private static final long REFRESH_INTERVAL_MILLIS = 5_000L;

    private final AuthHeader authHeader;
    private final AtlasBackupClient atlasBackupClient;
    private final CoordinationServiceRecorder coordinationServiceRecorder;
    private final BackupPersister backupPersister;
    private final Map<Namespace, InProgressBackupToken> inProgressBackups;
    private final LockRefresher lockRefresher;

    @VisibleForTesting
    AtlasBackupService(
            AuthHeader authHeader,
            AtlasBackupClient atlasBackupClient,
            CoordinationServiceRecorder coordinationServiceRecorder,
            BackupPersister backupPersister,
            LockRefresher lockRefresher) {
        this.authHeader = authHeader;
        this.atlasBackupClient = atlasBackupClient;
        this.coordinationServiceRecorder = coordinationServiceRecorder;
        this.backupPersister = backupPersister;
        this.lockRefresher = lockRefresher;
        this.inProgressBackups = new ConcurrentHashMap<>();
    }

    public static AtlasBackupService create(
            AuthHeader authHeader,
            Refreshable<ServicesConfigBlock> servicesConfigBlock,
            String serviceName,
            Function<Namespace, Path> backupFolderFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        ReloadingFactory reloadingFactory = DialogueClients.create(servicesConfigBlock)
                .withUserAgent(UserAgent.of(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT));

        AtlasBackupClient atlasBackupClient = new DialogueAdaptingAtlasBackupClient(
                reloadingFactory.get(AtlasBackupClientBlocking.class, serviceName));

        BackupPersister backupPersister = new ExternalBackupPersister(backupFolderFactory);
        KvsRunner kvsRunner = KvsRunner.create(keyValueServiceFactory);
        CoordinationServiceRecorder coordinationServiceRecorder =
                new CoordinationServiceRecorder(kvsRunner, backupPersister);
        LockRefresher lockRefresher = getLockRefresher(authHeader, atlasBackupClient);

        return new AtlasBackupService(
                authHeader, atlasBackupClient, coordinationServiceRecorder, backupPersister, lockRefresher);
    }

    public static AtlasBackupService createForTests(
            AuthHeader authHeader,
            AtlasBackupClient atlasBackupClient,
            TransactionManager transactionManager,
            Function<Namespace, Path> backupFolderFactory) {
        BackupPersister backupPersister = new ExternalBackupPersister(backupFolderFactory);
        KvsRunner kvsRunner = KvsRunner.create(transactionManager);
        CoordinationServiceRecorder coordinationServiceRecorder =
                new CoordinationServiceRecorder(kvsRunner, backupPersister);
        LockRefresher lockRefresher = getLockRefresher(authHeader, atlasBackupClient);

        return new AtlasBackupService(
                authHeader, atlasBackupClient, coordinationServiceRecorder, backupPersister, lockRefresher);
    }

    private static LockRefresher getLockRefresher(AuthHeader authHeader, AtlasBackupClient atlasBackupClient) {
        ScheduledExecutorService refreshExecutor =
                PTExecutors.newSingleThreadScheduledExecutor(new NamedThreadFactory("backupLockRefresher", true));
        LockLeaseRefresher<InProgressBackupToken> lockLeaseRefresher = tokens -> atlasBackupClient
                .refreshBackup(authHeader, RefreshBackupRequest.of(tokens))
                .getRefreshedTokens();
        return new LockRefresher(refreshExecutor, lockLeaseRefresher, REFRESH_INTERVAL_MILLIS);
    }

    public Set<Namespace> prepareBackup(Set<Namespace> namespaces) {
        PrepareBackupRequest request = PrepareBackupRequest.of(namespaces);
        PrepareBackupResponse response = atlasBackupClient.prepareBackup(authHeader, request);

        return response.getSuccessful().stream()
                .peek(this::storeBackupToken)
                .map(InProgressBackupToken::getNamespace)
                .collect(Collectors.toSet());
    }

    private void storeBackupToken(InProgressBackupToken backupToken) {
        inProgressBackups.put(backupToken.getNamespace(), backupToken);
        backupPersister.storeImmutableTimestamp(backupToken);
        lockRefresher.registerLocks(ImmutableSet.of(backupToken.getLockToken()));
    }

    public Set<Namespace> completeBackup(Set<Namespace> namespaces) {
        log.info("Attempting to complete backup", SafeArg.of("namespaces", namespaces));

        Set<InProgressBackupToken> tokens = namespaces.stream()
                .map(inProgressBackups::remove)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        Set<LockToken> tokensToUnlock =
                tokens.stream().map(InProgressBackupToken::getLockToken).collect(Collectors.toSet());
        lockRefresher.unregisterLocks(tokensToUnlock);

        log.info(
                "Found this many in progress backups",
                SafeArg.of("count", inProgressBackups.size()),
                SafeArg.of("namespaces", inProgressBackups.keySet()));

        CompleteBackupRequest request = CompleteBackupRequest.of(tokens);
        CompleteBackupResponse response = atlasBackupClient.completeBackup(authHeader, request);

        log.info("Complete backup returned this response", SafeArg.of("response", response));

        return response.getSuccessfulBackups().stream()
                .peek(coordinationServiceRecorder::storeFastForwardState)
                .peek(backupPersister::storeCompletedBackup)
                .map(CompletedBackup::getNamespace)
                .collect(Collectors.toSet());
    }
}
