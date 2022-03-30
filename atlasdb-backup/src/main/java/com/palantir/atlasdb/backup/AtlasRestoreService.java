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
import com.palantir.atlasdb.backup.api.CompleteRestoreRequest;
import com.palantir.atlasdb.backup.api.CompleteRestoreResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.cassandra.MergedCassandraKeyValueServiceConfig;
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
            Function<Namespace, MergedCassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
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
            Function<Namespace, MergedCassandraKeyValueServiceConfig> keyValueServiceConfigFactory) {
        CassandraRepairHelper cassandraRepairHelper =
                new CassandraRepairHelper(KvsRunner.create(transactionManager), keyValueServiceConfigFactory);

        return new AtlasRestoreService(
                authHeader, atlasRestoreClient, timeLockManagementService, backupPersister, cassandraRepairHelper);
    }

    /**
     * Disables TimeLock on all nodes for the given namespaces.
     * This will fail if any namespace is already disabled, unless it was disabled with the provided backupId.
     * Namespaces for which we don't have a recorded backup will be ignored.
     *
     * @param restoreRequests the requests to prepare.
     * @param backupId a unique identifier for this request (uniquely identifies the backup to which we're restoring)
     *
     * @return the namespaces successfully disabled.
     */
    public Set<Namespace> prepareRestore(Set<RestoreRequest> restoreRequests, String backupId) {
        Map<RestoreRequest, CompletedBackup> completedBackups = getCompletedBackups(restoreRequests);
        Set<Namespace> namespacesToRestore = getNamespacesToRestore(completedBackups);
        Preconditions.checkArgument(
                namespacesToRestore.size() == completedBackups.size(),
                "Attempting to restore multiple namespaces into the same namespace! "
                        + "This will cause severe data corruption.",
                SafeArg.of("restoreRequests", restoreRequests));

        DisableNamespacesRequest request = DisableNamespacesRequest.of(namespacesToRestore, backupId);
        DisableNamespacesResponse response = timeLockManagementService.disableTimelock(authHeader, request);
        return response.accept(new DisableNamespacesResponse.Visitor<>() {
            @Override
            public Set<Namespace> visitSuccessful(SuccessfulDisableNamespacesResponse value) {
                return namespacesToRestore;
            }

            @Override
            public Set<Namespace> visitUnsuccessful(UnsuccessfulDisableNamespacesResponse value) {
                log.error(
                        "Failed to disable namespaces prior to restore",
                        SafeArg.of("requests", restoreRequests),
                        SafeArg.of("response", value));
                return ImmutableSet.of();
            }

            @Override
            public Set<Namespace> visitUnknown(String unknownType) {
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
    public Set<Namespace> repairInternalTables(
            Set<RestoreRequest> restoreRequests, BiConsumer<String, RangesForRepair> repairTable) {
        Map<RestoreRequest, CompletedBackup> completedBackups = getCompletedBackups(restoreRequests);
        Set<Namespace> namespacesToRepair = getNamespacesToRestore(completedBackups);
        repairTables(repairTable, completedBackups, namespacesToRepair);
        return namespacesToRepair;
    }

    /**
     * Completes the restore process for the requested namespaces.
     * This includes fast-forwarding the timestamp, and then re-enabling the TimeLock namespaces.
     *
     * @param restoreRequests the requests to complete.
     * @param backupId the backup identifier, which must match the one given to {@link #prepareRestore(Set, String)}
     * @return the set of namespaces that were successfully fast-forwarded and re-enabled.
     */
    public Set<Namespace> completeRestore(Set<RestoreRequest> restoreRequests, String backupId) {
        Map<RestoreRequest, CompletedBackup> completedBackups = getCompletedBackups(restoreRequests);
        Set<Namespace> namespacesToRestore = getNamespacesToRestore(completedBackups);

        if (completedBackups.isEmpty()) {
            log.info(
                    "Attempted to complete restore, but no completed backups were found",
                    SafeArg.of("restoreRequests", restoreRequests));
            return ImmutableSet.of();
        } else if (completedBackups.size() < restoreRequests.size()) {
            Set<Namespace> namespacesWithBackup = completedBackups.values().stream()
                    .map(CompletedBackup::getNamespace)
                    .collect(Collectors.toSet());
            Set<Namespace> allOldNamespaces =
                    restoreRequests.stream().map(RestoreRequest::oldNamespace).collect(Collectors.toSet());
            Set<Namespace> namespacesWithoutBackup = Sets.difference(allOldNamespaces, namespacesWithBackup);
            log.warn(
                    "Completed backups were not found for some namespaces",
                    SafeArg.of("namespacesWithBackup", namespacesWithBackup),
                    SafeArg.of("namespacesWithoutBackup", namespacesWithoutBackup));
        }

        // Fast forward timestamps
        Map<Namespace, CompletedBackup> completeRequest = KeyedStream.stream(completedBackups)
                .mapKeys(RestoreRequest::newNamespace)
                .collectToMap();
        CompleteRestoreResponse response =
                atlasRestoreClient.completeRestore(authHeader, CompleteRestoreRequest.of(completeRequest));
        Set<Namespace> successfulNamespaces = response.getSuccessfulNamespaces();
        Set<Namespace> failedNamespaces = Sets.difference(namespacesToRestore, successfulNamespaces);
        if (!failedNamespaces.isEmpty()) {
            log.error(
                    "Failed to fast-forward timestamp for some namespaces. These will not be re-enabled.",
                    SafeArg.of("failedNamespaces", failedNamespaces),
                    SafeArg.of("fastForwardedNamespaces", successfulNamespaces));
        }

        // Re-enable timelock
        Set<Namespace> newNamespaces =
                restoreRequests.stream().map(RestoreRequest::newNamespace).collect(Collectors.toSet());
        timeLockManagementService.reenableTimelock(
                authHeader, ReenableNamespacesRequest.of(successfulNamespaces, backupId));
        if (successfulNamespaces.containsAll(newNamespaces)) {
            log.info(
                    "Successfully completed restore for all namespaces",
                    SafeArg.of("namespaces", successfulNamespaces));
        }

        return successfulNamespaces;
    }

    private void repairTables(
            BiConsumer<String, RangesForRepair> repairTable,
            Map<RestoreRequest, CompletedBackup> completedBackups,
            Set<Namespace> namespacesToRepair) {
        // ConsistentCasTablesTask
        namespacesToRepair.forEach(namespace -> cassandraRepairHelper.repairInternalTables(namespace, repairTable));

        // RepairTransactionsTablesTask
        KeyedStream.stream(completedBackups)
                .forEach((restoreRequest, completedBackup) ->
                        repairTransactionsTables(restoreRequest, completedBackup, repairTable));
    }

    private void repairTransactionsTables(
            RestoreRequest restoreRequest,
            CompletedBackup completedBackup,
            BiConsumer<String, RangesForRepair> repairTable) {
        Map<FullyBoundedTimestampRange, Integer> coordinationMap = getCoordinationMap(completedBackup);
        List<TransactionsTableInteraction> transactionsTableInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(
                        coordinationMap, DefaultRetryPolicy.INSTANCE);
        Namespace namespace = restoreRequest.newNamespace();
        cassandraRepairHelper.repairTransactionsTables(namespace, transactionsTableInteractions, repairTable);
        cassandraRepairHelper.cleanTransactionsTables(
                namespace, completedBackup.getBackupStartTimestamp(), transactionsTableInteractions);
    }

    private Map<FullyBoundedTimestampRange, Integer> getCoordinationMap(CompletedBackup completedBackup) {
        Optional<InternalSchemaMetadataState> schemaMetadataState =
                backupPersister.getSchemaMetadata(completedBackup.getNamespace());

        long fastForwardTs = completedBackup.getBackupEndTimestamp();
        long immutableTs = completedBackup.getBackupStartTimestamp();

        return CoordinationServiceUtilities.getCoordinationMapOnRestore(
                schemaMetadataState, fastForwardTs, immutableTs);
    }

    private Map<RestoreRequest, CompletedBackup> getCompletedBackups(Set<RestoreRequest> restoreRequests) {
        return KeyedStream.of(restoreRequests)
                .map(RestoreRequest::oldNamespace)
                .map(backupPersister::getCompletedBackup)
                .flatMap(Optional::stream)
                .collectToMap();
    }

    private Set<Namespace> getNamespacesToRestore(Map<RestoreRequest, CompletedBackup> completedBackups) {
        return completedBackups.keySet().stream()
                .map(RestoreRequest::newNamespace)
                .collect(Collectors.toSet());
    }
}
