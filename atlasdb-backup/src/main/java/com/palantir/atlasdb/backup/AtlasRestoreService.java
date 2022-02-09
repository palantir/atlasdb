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
import com.palantir.atlasdb.backup.api.AtlasRestoreClientBlocking;
import com.palantir.atlasdb.backup.api.CompleteRestoreRequest;
import com.palantir.atlasdb.backup.api.CompleteRestoreResponse;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.cassandra.backup.RangesForRepair;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementServiceBlocking;
import com.palantir.common.annotation.NonIdempotent;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.dialogue.clients.DialogueClients;
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
    private final AtlasRestoreClientBlocking atlasRestoreClientBlocking;
    private final TimeLockManagementServiceBlocking timeLockManagementService;
    private final BackupPersister backupPersister;
    private final CassandraRepairHelper cassandraRepairHelper;

    @VisibleForTesting
    AtlasRestoreService(
            AuthHeader authHeader,
            AtlasRestoreClientBlocking atlasRestoreClientBlocking,
            TimeLockManagementServiceBlocking timeLockManagementService,
            BackupPersister backupPersister,
            CassandraRepairHelper cassandraRepairHelper) {
        this.authHeader = authHeader;
        this.atlasRestoreClientBlocking = atlasRestoreClientBlocking;
        this.timeLockManagementService = timeLockManagementService;
        this.backupPersister = backupPersister;
        this.cassandraRepairHelper = cassandraRepairHelper;
    }

    public static AtlasRestoreService create(
            AuthHeader authHeader,
            Refreshable<ServicesConfigBlock> servicesConfigBlock,
            String serviceName,
            BackupPersister backupPersister,
            Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        DialogueClients.ReloadingFactory reloadingFactory = DialogueClients.create(servicesConfigBlock);
        AtlasRestoreClientBlocking atlasRestoreClientBlocking =
                reloadingFactory.get(AtlasRestoreClientBlocking.class, serviceName);
        TimeLockManagementServiceBlocking timeLockManagementService =
                reloadingFactory.get(TimeLockManagementServiceBlocking.class, serviceName);

        CassandraRepairHelper cassandraRepairHelper =
                new CassandraRepairHelper(keyValueServiceConfigFactory, keyValueServiceFactory);
        return new AtlasRestoreService(
                authHeader,
                atlasRestoreClientBlocking,
                timeLockManagementService,
                backupPersister,
                cassandraRepairHelper);
    }

    /**
     * Disables TimeLock on all nodes for the given namespaces.
     * Should be called exactly once prior to a restore operation. Calling this on multiple nodes will cause conflicts.
     *
     * @param namespaces the namespaces to disable
     *
     * @return the namespaces successfully disabled.
     */
    @NonIdempotent // TODO(gs): disable twice with same ID should be acceptable
    public Set<Namespace> prepareRestore(Set<Namespace> namespaces, String backupId) {
        Map<Namespace, CompletedBackup> completedBackups = getCompletedBackups(namespaces);
        Set<Namespace> namespacesToRestore = completedBackups.keySet();

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
                        SafeArg.of("namespaces", namespaces),
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
     * @param namespaces the namespaces to repair.
     * @param repairTable supplied function which is expected to repair the given ranges.
     *
     * @return the set of namespaces for which we issued a repair command via the provided Consumer.
     */
    public Set<Namespace> repairInternalTables(
            Set<Namespace> namespaces, BiConsumer<String, RangesForRepair> repairTable) {
        Map<Namespace, CompletedBackup> completedBackups = getCompletedBackups(namespaces);
        Set<Namespace> namespacesToRepair = completedBackups.keySet();
        repairTables(repairTable, completedBackups, namespacesToRepair);
        return namespacesToRepair;
    }

    /**
     * Completes the restore process for the requested namespaces.
     * This includes fast-forwarding the timestamp, and then re-enabling the TimeLock namespaces.
     *
     * @param request the request object, which must include the lock ID given to {@link #prepareRestore(Set, String)}
     * @return the set of namespaces that were successfully fast-forwarded and re-enabled.
     */
    @NonIdempotent
    public Set<Namespace> completeRestore(ReenableNamespacesRequest request) {
        Set<CompletedBackup> completedBackups = request.getNamespaces().stream()
                .map(backupPersister::getCompletedBackup)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());

        if (completedBackups.isEmpty()) {
            log.info(
                    "Attempted to complete restore, but no completed backups were found",
                    SafeArg.of("namespaces", request.getNamespaces()));
            return ImmutableSet.of();
        }

        // Fast forward timestamps
        CompleteRestoreResponse response =
                atlasRestoreClientBlocking.completeRestore(authHeader, CompleteRestoreRequest.of(completedBackups));
        Set<Namespace> successfulNamespaces = response.getSuccessfulNamespaces();
        Set<Namespace> failedNamespaces = Sets.difference(request.getNamespaces(), successfulNamespaces);
        if (!failedNamespaces.isEmpty()) {
            log.error(
                    "Failed to fast-forward timestamp for some namespaces. These will not be re-enabled.",
                    SafeArg.of("failedNamespaces", failedNamespaces),
                    SafeArg.of("fastForwardedNamespaces", successfulNamespaces));
        }

        // Re-enable timelock
        timeLockManagementService.reenableTimelock(
                authHeader, ReenableNamespacesRequest.of(successfulNamespaces, request.getLockId()));
        if (successfulNamespaces.containsAll(request.getNamespaces())) {
            log.info(
                    "Successfully completed restore for all namespaces",
                    SafeArg.of("namespaces", successfulNamespaces));
        }

        return successfulNamespaces;
    }

    private void repairTables(
            BiConsumer<String, RangesForRepair> repairTable,
            Map<Namespace, CompletedBackup> completedBackups,
            Set<Namespace> namespacesToRepair) {
        // ConsistentCasTablesTask
        namespacesToRepair.forEach(namespace -> cassandraRepairHelper.repairInternalTables(namespace, repairTable));

        // RepairTransactionsTablesTask
        KeyedStream.stream(completedBackups)
                .forEach((namespace, completedBackup) ->
                        repairTransactionsTables(namespace, completedBackup, repairTable));
    }

    private void repairTransactionsTables(
            Namespace namespace, CompletedBackup completedBackup, BiConsumer<String, RangesForRepair> repairTable) {
        Map<FullyBoundedTimestampRange, Integer> coordinationMap = getCoordinationMap(namespace, completedBackup);
        List<TransactionsTableInteraction> transactionsTableInteractions =
                TransactionsTableInteraction.getTransactionTableInteractions(
                        coordinationMap, DefaultRetryPolicy.INSTANCE);
        cassandraRepairHelper.repairTransactionsTables(namespace, transactionsTableInteractions, repairTable);
        cassandraRepairHelper.cleanTransactionsTables(
                namespace, completedBackup.getBackupStartTimestamp(), transactionsTableInteractions);
    }

    private Map<FullyBoundedTimestampRange, Integer> getCoordinationMap(
            Namespace namespace, CompletedBackup completedBackup) {
        Optional<InternalSchemaMetadataState> schemaMetadataState = backupPersister.getSchemaMetadata(namespace);

        long fastForwardTs = completedBackup.getBackupEndTimestamp();
        long immutableTs = completedBackup.getBackupStartTimestamp();

        return CoordinationServiceUtilities.getCoordinationMapOnRestore(
                schemaMetadataState, fastForwardTs, immutableTs);
    }

    private Map<Namespace, CompletedBackup> getCompletedBackups(Set<Namespace> namespaces) {
        return KeyedStream.of(namespaces)
                .map(backupPersister::getCompletedBackup)
                .flatMap(Optional::stream)
                .collectToMap();
    }
}
