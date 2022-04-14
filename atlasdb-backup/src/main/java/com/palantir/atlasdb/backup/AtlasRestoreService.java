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

import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.backup.api.AtlasRestoreClient;
import com.palantir.atlasdb.backup.api.AtlasRestoreClientBlocking;
import com.palantir.atlasdb.backup.api.AtlasService;
import com.palantir.atlasdb.backup.api.CompleteRestoreRequest;
import com.palantir.atlasdb.backup.api.CompleteRestoreResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.cassandra.backup.RangesForRepair;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementService;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementServiceBlocking;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import com.palantir.tokens.auth.AuthHeader;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *  Service for Atlas restore tasks.
 *  While a single restore operation may encompass multiple namespaces, it is essential that each namespace in a given
 *  request corresponds to the same TimeLock service, since we support a single AtlasRestoreClient and
 *  TimelockManagementService (these both exist on TimeLock rather than on the backup client side).
 *  If the set of AtlasServices in a given request contains duplicated namespaces
 *  (e.g. {(123, namespace), (456, namespace)}), then a SafeIllegalArgumentException will be thrown.
 */
public class AtlasRestoreService {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasRestoreService.class);

    private final AuthHeader authHeader;
    private final AtlasRestoreClient atlasRestoreClient;
    private final TimeLockManagementService timeLockManagementService;
    private final BackupPersister backupPersister;
    private final CassandraRepairHelper cassandraRepairHelper;

    @VisibleForTesting
    AtlasRestoreService(
            AuthHeader authHeader,
            AtlasRestoreClient atlasRestoreClient,
            TimeLockManagementService timeLockManagementService,
            BackupPersister backupPersister,
            CassandraRepairHelper cassandraRepairHelper) {
        this.authHeader = authHeader;
        this.atlasRestoreClient = atlasRestoreClient;
        this.timeLockManagementService = timeLockManagementService;
        this.backupPersister = backupPersister;
        this.cassandraRepairHelper = cassandraRepairHelper;
    }

    public static AtlasRestoreService create(
            AuthHeader authHeader,
            Refreshable<ServicesConfigBlock> servicesConfigBlock,
            String serviceName,
            BackupPersister backupPersister,
            Function<AtlasService, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<AtlasService, KeyValueService> keyValueServiceFactory) {
        DialogueClients.ReloadingFactory reloadingFactory = DialogueClients.create(servicesConfigBlock)
                .withUserAgent(UserAgent.of(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT));
        AtlasRestoreClient atlasRestoreClient = new DialogueAdaptingAtlasRestoreClient(
                reloadingFactory.get(AtlasRestoreClientBlocking.class, serviceName));
        TimeLockManagementService timeLockManagementService = new DialogueAdaptingTimeLockManagementService(
                reloadingFactory.get(TimeLockManagementServiceBlocking.class, serviceName));
        CassandraRepairHelper cassandraRepairHelper =
                new CassandraRepairHelper(KvsRunner.create(keyValueServiceFactory), keyValueServiceConfigFactory);

        return new AtlasRestoreService(
                authHeader, atlasRestoreClient, timeLockManagementService, backupPersister, cassandraRepairHelper);
    }

    public static AtlasRestoreService createForTests(
            AuthHeader authHeader,
            AtlasRestoreClient atlasRestoreClient,
            TimeLockManagementService timeLockManagementService,
            BackupPersister backupPersister,
            TransactionManager transactionManager,
            Function<AtlasService, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory) {
        CassandraRepairHelper cassandraRepairHelper =
                new CassandraRepairHelper(KvsRunner.create(transactionManager), keyValueServiceConfigFactory);

        return new AtlasRestoreService(
                authHeader, atlasRestoreClient, timeLockManagementService, backupPersister, cassandraRepairHelper);
    }

    /**
     * Disables TimeLock on all nodes for the given atlasServices.
     * This will fail if any atlasService is already disabled, unless it was disabled with the provided backupId.
     * AtlasServices for which we don't have a recorded backup will be ignored.
     *
     * @param restoreRequests the requests to prepare.
     * @param backupId a unique identifier for this request (uniquely identifies the backup to which we're restoring)
     *
     * @return the atlasServices successfully disabled.
     */
    public Set<AtlasService> prepareRestore(Set<RestoreRequest> restoreRequests, String backupId) {
        validateRestoreRequests(restoreRequests);
        Map<RestoreRequest, CompletedBackup> completedBackups = getCompletedBackups(restoreRequests);
        Set<AtlasService> atlasServicesToRestore = getAtlasServicesToRestore(completedBackups);
        Preconditions.checkArgument(
                atlasServicesToRestore.size() == completedBackups.size(),
                "Attempting to restore multiple atlasServices into the same atlasService! "
                        + "This will cause severe data corruption.",
                SafeArg.of("restoreRequests", restoreRequests));

        Set<Namespace> namespacesToRestore =
                atlasServicesToRestore.stream().map(AtlasService::getNamespace).collect(Collectors.toSet());
        DisableNamespacesRequest request = DisableNamespacesRequest.of(namespacesToRestore, backupId);
        DisableNamespacesResponse response = timeLockManagementService.disableTimelock(authHeader, request);
        return response.accept(new DisableNamespacesResponse.Visitor<>() {
            @Override
            public Set<AtlasService> visitSuccessful(SuccessfulDisableNamespacesResponse value) {
                return atlasServicesToRestore;
            }

            @Override
            public Set<AtlasService> visitUnsuccessful(UnsuccessfulDisableNamespacesResponse value) {
                log.error(
                        "Failed to disable namespaces prior to restore",
                        SafeArg.of("requests", restoreRequests),
                        SafeArg.of("response", value));
                return ImmutableSet.of();
            }

            @Override
            public Set<AtlasService> visitUnknown(String unknownType) {
                throw new SafeIllegalStateException(
                        "Unknown DisableNamespacesResponse", SafeArg.of("unknownType", unknownType));
            }
        });
    }

    /**
     *  Returns the set of namespaces for which we successfully repaired internal tables.
     *  Only namespaces for which a known backup exists will be repaired.
     *  Namespaces are repaired serially. If repairTable throws an exception, then this will propagate back to the
     *  caller. In such cases, some namespaces may not have been repaired.
     *
     * @param restoreRequests the repair requests.
     * @param repairTable supplied function which is expected to repair the given ranges.
     *
     * @return the set of namespaces for which we issued a repair command via the provided Consumer.
     */
    public Set<AtlasService> repairInternalTables(
            Set<RestoreRequest> restoreRequests, BiConsumer<String, RangesForRepair> repairTable) {
        validateRestoreRequests(restoreRequests);
        Map<RestoreRequest, CompletedBackup> completedBackups = getCompletedBackups(restoreRequests);
        Set<AtlasService> namespacesToRepair = getAtlasServicesToRestore(completedBackups);
        repairTables(repairTable, completedBackups, namespacesToRepair);
        return namespacesToRepair;
    }

    /**
     * Completes the restore process for the requested atlasServices.
     * This includes fast-forwarding the timestamp, and then re-enabling the TimeLock namespaces.
     *
     * @param restoreRequests the requests to complete.
     * @param backupId the backup identifier, which must match the one given to {@link #prepareRestore(Set, String)}
     * @return the set of atlasServices that were successfully fast-forwarded and re-enabled.
     */
    public Set<AtlasService> completeRestore(Set<RestoreRequest> restoreRequests, String backupId) {
        validateRestoreRequests(restoreRequests);
        Map<RestoreRequest, CompletedBackup> completedBackups = getCompletedBackups(restoreRequests);
        Set<AtlasService> atlasServicesToRestore = getAtlasServicesToRestore(completedBackups);

        if (completedBackups.isEmpty()) {
            log.info(
                    "Attempted to complete restore, but no completed backups were found",
                    SafeArg.of("restoreRequests", restoreRequests));
            return ImmutableSet.of();
        } else if (completedBackups.size() < restoreRequests.size()) {
            Set<AtlasService> atlasServicesWithBackup = completedBackups.keySet().stream()
                    .map(RestoreRequest::oldAtlasService)
                    .collect(Collectors.toSet());
            Set<AtlasService> allOldAtlasServices = restoreRequests.stream()
                    .map(RestoreRequest::oldAtlasService)
                    .collect(Collectors.toSet());
            Set<AtlasService> atlasServicesWithoutBackup =
                    Sets.difference(allOldAtlasServices, atlasServicesWithBackup);
            log.warn(
                    "Completed backups were not found for some atlasServices",
                    SafeArg.of("atlasServicesWithBackup", atlasServicesWithBackup),
                    SafeArg.of("atlasServicesWithoutBackup", atlasServicesWithoutBackup));
        }

        // Fast forward timestamps
        Map<Namespace, CompletedBackup> completeRequest = KeyedStream.stream(completedBackups)
                .mapKeys(RestoreRequest::newAtlasService)
                .mapKeys(AtlasService::getNamespace)
                .collectToMap();
        Map<Namespace, AtlasService> namespaceToServices = KeyedStream.of(atlasServicesToRestore)
                .mapKeys(AtlasService::getNamespace)
                .collectToMap();

        CompleteRestoreResponse response =
                atlasRestoreClient.completeRestore(authHeader, CompleteRestoreRequest.of(completeRequest));
        Set<AtlasService> successfulAtlasServices = response.getSuccessfulNamespaces().stream()
                .map(namespaceToServices::get)
                .collect(Collectors.toSet());
        Set<AtlasService> failedAtlasServices = Sets.difference(atlasServicesToRestore, successfulAtlasServices);
        if (!failedAtlasServices.isEmpty()) {
            log.error(
                    "Failed to fast-forward timestamp for some atlasServices. These will not be re-enabled.",
                    SafeArg.of("failedAtlasServices", failedAtlasServices),
                    SafeArg.of("fastForwardedAtlasServices", successfulAtlasServices));
        }

        // Re-enable timelock
        timeLockManagementService.reenableTimelock(
                authHeader, ReenableNamespacesRequest.of(response.getSuccessfulNamespaces(), backupId));
        if (successfulAtlasServices.containsAll(atlasServicesToRestore)) {
            log.info(
                    "Successfully completed restore for all atlasServices",
                    SafeArg.of("atlasServices", successfulAtlasServices));
        }

        return successfulAtlasServices;
    }

    private void repairTables(
            BiConsumer<String, RangesForRepair> repairTable,
            Map<RestoreRequest, CompletedBackup> completedBackups,
            Set<AtlasService> atlasServicesToRepair) {
        // ConsistentCasTablesTask
        atlasServicesToRepair.forEach(
                atlasService -> cassandraRepairHelper.repairInternalTables(atlasService, repairTable));

        // RepairTransactionsTablesTask
        KeyedStream.stream(completedBackups)
                .forEach((restoreRequest, completedBackup) ->
                        repairTransactionsTables(restoreRequest, completedBackup, repairTable));
    }

    private void repairTransactionsTables(
            RestoreRequest restoreRequest,
            CompletedBackup completedBackup,
            BiConsumer<String, RangesForRepair> repairTable) {
        Map<FullyBoundedTimestampRange, Integer> coordinationMap =
                getCoordinationMap(restoreRequest.oldAtlasService(), completedBackup);
        List<TransactionsTableInteraction> transactionsTableInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(
                        coordinationMap, DefaultRetryPolicy.INSTANCE);
        AtlasService atlasService = restoreRequest.newAtlasService();
        cassandraRepairHelper.repairTransactionsTables(atlasService, transactionsTableInteractions, repairTable);
        cassandraRepairHelper.cleanTransactionsTables(
                atlasService, completedBackup.getBackupStartTimestamp(), transactionsTableInteractions);
    }

    private Map<FullyBoundedTimestampRange, Integer> getCoordinationMap(
            AtlasService atlasService, CompletedBackup completedBackup) {
        Optional<InternalSchemaMetadataState> schemaMetadataState = backupPersister.getSchemaMetadata(atlasService);

        long fastForwardTs = completedBackup.getBackupEndTimestamp();
        long immutableTs = completedBackup.getBackupStartTimestamp();

        return CoordinationServiceUtilities.getCoordinationMapOnRestore(
                schemaMetadataState, fastForwardTs, immutableTs);
    }

    private Map<RestoreRequest, CompletedBackup> getCompletedBackups(Set<RestoreRequest> restoreRequests) {
        return KeyedStream.of(restoreRequests)
                .map(RestoreRequest::oldAtlasService)
                .map(backupPersister::getCompletedBackup)
                .flatMap(Optional::stream)
                .collectToMap();
    }

    private Set<AtlasService> getAtlasServicesToRestore(Map<RestoreRequest, CompletedBackup> completedBackups) {
        return completedBackups.keySet().stream()
                .map(RestoreRequest::newAtlasService)
                .collect(Collectors.toSet());
    }

    private static void validateRestoreRequests(Set<RestoreRequest> restoreRequests) {
        Set<AtlasService> newAtlasServices =
                restoreRequests.stream().map(RestoreRequest::newAtlasService).collect(Collectors.toSet());
        AtlasServices.throwIfAtlasServicesCollide(newAtlasServices);
    }
}
