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
import com.google.common.collect.Range;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.CassandraRepairHelper;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadataState;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AtlasRestoreService {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasRestoreService.class);

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
            Set<Namespace> namespaces, Consumer<Map<InetSocketAddress, Set<Range<LightweightOppToken>>>> repairTable) {
        Set<Namespace> namespacesToRepair =
                namespaces.stream().filter(this::backupExists).collect(Collectors.toSet());

        // ConsistentCasTablesTask
        namespacesToRepair.forEach(namespace -> cassandraRepairHelper.repairInternalTables(namespace, repairTable));

        // RepairTransactionsTablesTask
        namespacesToRepair.forEach(namespace -> repairTransactionsTables(namespace, repairTable));

        return namespacesToRepair;
    }

    private void repairTransactionsTables(
            Namespace namespace, Consumer<Map<InetSocketAddress, Set<Range<LightweightOppToken>>>> repairTable) {
        // 1. get schema metadata
        InternalSchemaMetadataState schemaMetadataState =
                backupPersister.getSchemaMetadata(namespace).orElseThrow();

        // 2. get txn tables interactions

        // 3. get partition tokens
        // 4. get repair ranges
        // 5. repair ranges (can use repairTable method here?)
    }

    private boolean backupExists(Namespace namespace) {
        Optional<CompletedBackup> maybeCompletedBackup = backupPersister.getCompletedBackup(namespace);
        Optional<InternalSchemaMetadataState> maybeInternalMetadata = backupPersister.getSchemaMetadata(namespace);

        if (maybeCompletedBackup.isEmpty() || maybeInternalMetadata.isEmpty()) {
            log.error("Could not restore namespace, as no backup is stored", SafeArg.of("namespace", namespace));
            return false;
        }

        return true;
    }
}
