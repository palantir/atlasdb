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
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.cassandra.backup.RangesForRepair;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class AtlasRestoreService {
    private final BackupPersister backupPersister;
    private final CassandraRepairHelper cassandraRepairHelper;

    @VisibleForTesting
    AtlasRestoreService(BackupPersister backupPersister, CassandraRepairHelper cassandraRepairHelper) {
        this.backupPersister = backupPersister;
        this.cassandraRepairHelper = cassandraRepairHelper;
    }

    public static AtlasRestoreService create(
            BackupPersister backupPersister,
            Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        CassandraRepairHelper cassandraRepairHelper =
                new CassandraRepairHelper(keyValueServiceConfigFactory, keyValueServiceFactory);
        return new AtlasRestoreService(backupPersister, cassandraRepairHelper);
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

        // ConsistentCasTablesTask
        namespacesToRepair.forEach(namespace -> cassandraRepairHelper.repairInternalTables(namespace, repairTable));

        // RepairTransactionsTablesTask
        KeyedStream.stream(completedBackups)
                .forEach((namespace, completedBackup) ->
                        repairTransactionsTables(namespace, completedBackup, repairTable));

        return namespacesToRepair;
    }

    private void repairTransactionsTables(
            Namespace namespace, CompletedBackup completedBackup, BiConsumer<String, RangesForRepair> repairTable) {
        Map<FullyBoundedTimestampRange, Integer> coordinationMap = getCoordinationMap(namespace, completedBackup);
        cassandraRepairHelper.repairTransactionsTables(namespace, coordinationMap, repairTable);
    }

    private Map<FullyBoundedTimestampRange, Integer> getCoordinationMap(
            Namespace namespace, CompletedBackup completedBackup) {
        Optional<InternalSchemaMetadataState> schemaMetadataState = backupPersister.getSchemaMetadata(namespace);

        long fastForwardTs = completedBackup.getBackupEndTimestamp();

        // Should this be the immutableTs from the completed backup?
        long immutableTs = completedBackup.getBackupStartTimestamp();

        return CoordinationServiceUtilities.getCoordinationMapOnRestore(
                schemaMetadataState, fastForwardTs, immutableTs);
    }

    private Map<Namespace, CompletedBackup> getCompletedBackups(Set<Namespace> namespaces) {
        return KeyedStream.of(namespaces)
                .map(backupPersister::getCompletedBackup)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collectToMap();
    }
}
