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
import com.google.common.collect.Sets;
import com.palantir.atlasdb.backup.api.AtlasBackupClient;
import com.palantir.atlasdb.backup.api.AtlasBackupClientBlocking;
import com.palantir.atlasdb.backup.api.AtlasService;
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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tokens.auth.AuthHeader;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *  Service for Atlas backup tasks.
 *  While a single backup operation may encompass multiple namespaces, it is essential that each namespace in a given
 *  request corresponds to the same TimeLock service, since we support a single AtlasBackupClient (this exists on
 *  TimeLock rather than on the backup client side). If the set of AtlasServices in a given request contains
 *  duplicated namespaces (e.g. {(123, namespace), (456, namespace)}), then a SafeIllegalArgumentException will be
 *  thrown.
 */
public final class AtlasBackupService {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasBackupService.class);

    private static final long REFRESH_INTERVAL_MILLIS = 5_000L;

    private final AuthHeader authHeader;
    private final AtlasBackupClient atlasBackupClient;
    private final CoordinationServiceRecorder coordinationServiceRecorder;
    private final BackupPersister backupPersister;
    private final Map<AtlasService, InProgressBackupToken> inProgressBackups;
    private final LockRefresher<InProgressBackupToken> lockRefresher;

    @VisibleForTesting
    AtlasBackupService(
            AuthHeader authHeader,
            AtlasBackupClient atlasBackupClient,
            CoordinationServiceRecorder coordinationServiceRecorder,
            BackupPersister backupPersister,
            LockRefresher<InProgressBackupToken> lockRefresher) {
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
            Function<AtlasService, Path> backupFolderFactory,
            Function<AtlasService, KeyValueService> keyValueServiceFactory) {
        ReloadingFactory reloadingFactory = DialogueClients.create(servicesConfigBlock)
                .withUserAgent(UserAgent.of(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT));

        AtlasBackupClient atlasBackupClient = new DialogueAdaptingAtlasBackupClient(
                reloadingFactory.get(AtlasBackupClientBlocking.class, serviceName));

        BackupPersister backupPersister = new ExternalBackupPersister(backupFolderFactory);
        KvsRunner kvsRunner = KvsRunner.create(keyValueServiceFactory);
        CoordinationServiceRecorder coordinationServiceRecorder =
                new CoordinationServiceRecorder(kvsRunner, backupPersister);
        LockRefresher<InProgressBackupToken> lockRefresher = getLockRefresher(authHeader, atlasBackupClient);

        return new AtlasBackupService(
                authHeader, atlasBackupClient, coordinationServiceRecorder, backupPersister, lockRefresher);
    }

    public static AtlasBackupService createForTests(
            AuthHeader authHeader,
            AtlasBackupClient atlasBackupClient,
            TransactionManager transactionManager,
            Function<AtlasService, Path> backupFolderFactory) {
        BackupPersister backupPersister = new ExternalBackupPersister(backupFolderFactory);
        KvsRunner kvsRunner = KvsRunner.create(transactionManager);
        CoordinationServiceRecorder coordinationServiceRecorder =
                new CoordinationServiceRecorder(kvsRunner, backupPersister);
        LockRefresher<InProgressBackupToken> lockRefresher = getLockRefresher(authHeader, atlasBackupClient);

        return new AtlasBackupService(
                authHeader, atlasBackupClient, coordinationServiceRecorder, backupPersister, lockRefresher);
    }

    private static LockRefresher<InProgressBackupToken> getLockRefresher(
            AuthHeader authHeader, AtlasBackupClient atlasBackupClient) {
        ScheduledExecutorService refreshExecutor =
                PTExecutors.newSingleThreadScheduledExecutor(new NamedThreadFactory("backupLockRefresher", true));
        LockLeaseRefresher<InProgressBackupToken> lockLeaseRefresher = tokens -> atlasBackupClient
                .refreshBackup(authHeader, RefreshBackupRequest.of(tokens))
                .getRefreshedTokens();
        return new LockRefresher<>(refreshExecutor, lockLeaseRefresher, REFRESH_INTERVAL_MILLIS);
    }

    public Set<AtlasService> prepareBackup(Set<AtlasService> atlasServices) {
        validateAtlasServices(atlasServices);
        PrepareBackupRequest request = PrepareBackupRequest.of(atlasServices);
        PrepareBackupResponse response = atlasBackupClient.prepareBackup(authHeader, request);

        return response.getSuccessful().stream()
                .peek(this::storeBackupToken)
                .map(InProgressBackupToken::getAtlasService)
                .collect(Collectors.toSet());
    }

    private void storeBackupToken(InProgressBackupToken backupToken) {
        inProgressBackups.put(backupToken.getAtlasService(), backupToken);
        backupPersister.storeImmutableTimestamp(backupToken);
        lockRefresher.registerLocks(ImmutableSet.of(backupToken));
    }

    /**
     * Completes backup for the given set of atlas services.
     * This will store metadata about the completed backup via the BackupPersister.
     *
     * In order to do this, we must unlock the immutable timestamp for each service. If {@link #prepareBackup(Set)}
     * was not called, we will not have a record of the in-progress backup (and will not have been refreshing
     * its lock anyway). Thus, we attempt to complete backup only for those atlas services where we have the in-progress
     * backup stored.
     *
     * @return the atlas services whose backups were successfully completed
     */
    public Set<AtlasService> completeBackup(Set<AtlasService> atlasServices) {
        validateAtlasServices(atlasServices);
        Set<InProgressBackupToken> tokens = atlasServices.stream()
                .map(inProgressBackups::remove)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        lockRefresher.unregisterLocks(tokens);

        Set<AtlasService> atlasServicesWithInProgressBackups =
                tokens.stream().map(InProgressBackupToken::getAtlasService).collect(Collectors.toSet());
        if (tokens.size() < atlasServices.size()) {
            Set<AtlasService> atlasServicesWithNoInProgressBackup =
                    Sets.difference(atlasServices, atlasServicesWithInProgressBackups);
            log.error(
                    "In progress backups were not found for some atlasServices. We will not complete backup for these.",
                    SafeArg.of("numAtlasServicesWithBackup", tokens.size()),
                    SafeArg.of("numAtlasServicesWithoutBackup", atlasServicesWithNoInProgressBackup.size()),
                    SafeArg.of("atlasServicesWithoutBackup", atlasServicesWithNoInProgressBackup),
                    SafeArg.of("allRequestedAtlasServices", atlasServices));
        }

        CompleteBackupRequest request = CompleteBackupRequest.of(tokens);
        CompleteBackupResponse response = atlasBackupClient.completeBackup(authHeader, request);

        if (response.getSuccessfulBackups().size() < atlasServicesWithInProgressBackups.size()) {
            Set<AtlasService> successfulAtlasServices = response.getSuccessfulBackups().stream()
                    .map(CompletedBackup::getAtlasService)
                    .collect(Collectors.toSet());
            Set<AtlasService> failedAtlasServices =
                    Sets.difference(atlasServicesWithInProgressBackups, successfulAtlasServices);
            log.error(
                    "Backup did not complete successfully for all atlasServices. Check TimeLock logs to debug.",
                    SafeArg.of("failedAtlasServices", failedAtlasServices),
                    SafeArg.of("successfulAtlasServices", successfulAtlasServices),
                    SafeArg.of("atlasServicesWithoutBackup", atlasServicesWithInProgressBackups));
        }

        return response.getSuccessfulBackups().stream()
                .peek(coordinationServiceRecorder::storeFastForwardState)
                .peek(backupPersister::storeCompletedBackup)
                .map(CompletedBackup::getAtlasService)
                .collect(Collectors.toSet());
    }

    private static void validateAtlasServices(Set<AtlasService> atlasServices) {
        Map<Namespace, Long> namespacesByCount = atlasServices.stream()
                .collect(Collectors.groupingBy(AtlasService::getNamespace, Collectors.counting()));
        Set<Namespace> duplicatedNamespaces = namespacesByCount.entrySet().stream()
                .filter(m -> m.getValue() > 1)
                .map(Entry::getKey)
                .collect(Collectors.toSet());
        if (!duplicatedNamespaces.isEmpty()) {
            throw new SafeIllegalArgumentException(
                    "Duplicated namespaces found in backup request. Backup cannot safely proceed.",
                    SafeArg.of("duplicatedNamespaces", duplicatedNamespaces),
                    SafeArg.of("atlasServices", atlasServices));
        }
    }
}
