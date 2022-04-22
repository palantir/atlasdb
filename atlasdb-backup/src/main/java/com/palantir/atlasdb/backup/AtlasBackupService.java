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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
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
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.dialogue.clients.DialogueClients.ReloadingFactory;
import com.palantir.lock.client.LockRefresher;
import com.palantir.lock.v2.LockLeaseRefresher;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tokens.auth.AuthHeader;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
    private final int completeBackupNumThreads;

    @VisibleForTesting
    AtlasBackupService(
            AuthHeader authHeader,
            AtlasBackupClient atlasBackupClient,
            CoordinationServiceRecorder coordinationServiceRecorder,
            BackupPersister backupPersister,
            LockRefresher<InProgressBackupToken> lockRefresher,
            int completeBackupNumThreads) {
        this.authHeader = authHeader;
        this.atlasBackupClient = atlasBackupClient;
        this.coordinationServiceRecorder = coordinationServiceRecorder;
        this.backupPersister = backupPersister;
        this.lockRefresher = lockRefresher;
        this.inProgressBackups = new ConcurrentHashMap<>();
        this.completeBackupNumThreads = completeBackupNumThreads;
    }

    public static AtlasBackupService create(
            AuthHeader authHeader,
            Refreshable<ServicesConfigBlock> servicesConfigBlock,
            String serviceName,
            Function<AtlasService, Path> backupFolderFactory,
            Function<AtlasService, KeyValueService> keyValueServiceFactory,
            int completeBackupNumThreads) {
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
                authHeader,
                atlasBackupClient,
                coordinationServiceRecorder,
                backupPersister,
                lockRefresher,
                completeBackupNumThreads);
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
                authHeader, atlasBackupClient, coordinationServiceRecorder, backupPersister, lockRefresher, 10);
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
        Set<AtlasService> inProgressAndProposedBackups = Sets.union(atlasServices, inProgressBackups.keySet());
        AtlasServices.throwIfAtlasServicesCollide(inProgressAndProposedBackups);
        Map<Namespace, AtlasService> namespaceToServices = KeyedStream.of(atlasServices)
                .mapKeys(AtlasService::getNamespace)
                .collectToMap();
        PrepareBackupRequest request = PrepareBackupRequest.of(namespaceToServices.keySet());
        PrepareBackupResponse response = atlasBackupClient.prepareBackup(authHeader, request);

        Map<AtlasService, InProgressBackupToken> tokensPerService = KeyedStream.of(response.getSuccessful())
                .mapKeys(token -> namespaceToServices.get(token.getNamespace()))
                .collectToMap();
        KeyedStream.stream(tokensPerService).forEach(this::storeBackupToken);
        return tokensPerService.keySet();
    }

    private void storeBackupToken(AtlasService atlasService, InProgressBackupToken backupToken) {
        inProgressBackups.put(atlasService, backupToken);
        backupPersister.storeImmutableTimestamp(atlasService, backupToken);
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
        AtlasServices.throwIfAtlasServicesCollide(atlasServices);

        Map<AtlasService, InProgressBackupToken> knownBackups = KeyedStream.of(atlasServices)
                .map((Function<AtlasService, InProgressBackupToken>) inProgressBackups::remove)
                .filter(Objects::nonNull)
                .collectToMap();
        Set<InProgressBackupToken> tokens = ImmutableSet.copyOf(knownBackups.values());

        lockRefresher.unregisterLocks(tokens);

        Set<AtlasService> atlasServicesWithInProgressBackups = knownBackups.keySet();
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

        Map<Namespace, AtlasService> namespaceToServices = KeyedStream.of(atlasServices)
                .mapKeys(AtlasService::getNamespace)
                .collectToMap();
        CompleteBackupRequest request = CompleteBackupRequest.of(tokens);
        CompleteBackupResponse response = atlasBackupClient.completeBackup(authHeader, request);

        Map<AtlasService, CompletedBackup> successfulBackups = KeyedStream.of(response.getSuccessfulBackups())
                .mapKeys(token -> namespaceToServices.get(token.getNamespace()))
                .collectToMap();
        Set<AtlasService> successfullyStoredBackups = storeCompletedBackups(successfulBackups);

        if (successfullyStoredBackups.size() < atlasServicesWithInProgressBackups.size()) {
            Set<AtlasService> failedAtlasServices =
                    Sets.difference(atlasServicesWithInProgressBackups, successfullyStoredBackups);
            log.error(
                    "Backup did not complete successfully for all atlasServices. Check TimeLock logs to debug.",
                    SafeArg.of("failedAtlasServices", failedAtlasServices),
                    SafeArg.of("successfulAtlasServices", successfullyStoredBackups),
                    SafeArg.of("atlasServicesWithoutBackup", atlasServicesWithInProgressBackups));
        }

        return successfulBackups.keySet();
    }

    private Set<AtlasService> storeCompletedBackups(Map<AtlasService, CompletedBackup> successfulBackups) {
        ExecutorService executorService = PTExecutors.newFixedThreadPool(completeBackupNumThreads);
        Map<AtlasService, ListenableFuture<Optional<Boolean>>> storedBackups = KeyedStream.stream(successfulBackups)
                .map((atlasService, completeBackup) -> Futures.submit(
                        () -> Optional.of(storeCompletedBackup(atlasService, completeBackup)), executorService))
                .collectToMap();
        ListenableFuture<Map<AtlasService, Boolean>> allStorageTasks =
                AtlasFutures.allAsMap(storedBackups, MoreExecutors.directExecutor());

        // Waits for future to complete
        Map<AtlasService, Boolean> storageTaskResults = AtlasFutures.getUnchecked(allStorageTasks);
        executorService.shutdown();
        return KeyedStream.stream(storageTaskResults)
                .filter(Boolean::booleanValue)
                .keys()
                .collect(Collectors.toSet());
    }

    private Boolean storeCompletedBackup(AtlasService atlasService, CompletedBackup completedBackup) {
        try {
            coordinationServiceRecorder.storeFastForwardState(atlasService, completedBackup);
            backupPersister.storeCompletedBackup(atlasService, completedBackup);
            return true;
        } catch (Exception ex) {
            log.warn("Failed to store completed backup for AtlasService", SafeArg.of("atlasService", atlasService), ex);
            return false;
        }
    }
}
