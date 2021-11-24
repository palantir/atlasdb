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

import com.datastax.driver.core.TokenRange;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.cassandra.CqlCluster;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.TargetedSweepTables;
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
    private final Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory;
    private final Function<Namespace, KeyValueService> keyValueServiceFactory;

    public AtlasRestoreService(
            BackupPersister backupPersister,
            Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        this.backupPersister = backupPersister;
        this.keyValueServiceConfigFactory = keyValueServiceConfigFactory;
        this.keyValueServiceFactory = keyValueServiceFactory;
    }

    private static final String COORDINATION = AtlasDbConstants.COORDINATION_TABLE.getTableName();
    private static final Set<String> TABLES_TO_REPAIR =
            Sets.union(ImmutableSet.of(COORDINATION), TargetedSweepTables.REPAIR_ON_RESTORE);

    // Returns the set of namespaces for which we successfully repaired internal tables
    public Set<Namespace> repairInternalTables(
            Set<Namespace> namespaces, Consumer<Map<InetSocketAddress, Set<TokenRange>>> repairTable) {
        return namespaces.stream()
                .filter(this::backupExists)
                .peek(namespace -> repairInternalTables(namespace, repairTable))
                .collect(Collectors.toSet());
    }

    private boolean backupExists(Namespace namespace) {
        Optional<CompletedBackup> maybeCompletedBackup = backupPersister.getCompletedBackup(namespace);

        if (maybeCompletedBackup.isEmpty()) {
            log.error("Could not restore namespace, as no backup is stored", SafeArg.of("namespace", namespace));
            return false;
        }

        return true;
    }

    private void repairInternalTables(
            Namespace namespace, Consumer<Map<InetSocketAddress, Set<TokenRange>>> repairTable) {
        KeyValueService kvs = keyValueServiceFactory.apply(namespace);
        Set<TableReference> allTableNames = kvs.getAllTableNames();
        allTableNames.stream()
                .map(TableReference::getTableName)
                .filter(TABLES_TO_REPAIR::contains)
                .map(tableName -> getRangesToRepair(namespace, tableName))
                .forEach(repairTable);
    }

    private Map<InetSocketAddress, Set<TokenRange>> getRangesToRepair(Namespace namespace, String tableName) {
        CassandraKeyValueServiceConfig config = keyValueServiceConfigFactory.apply(namespace);
        CqlCluster cqlCluster = CqlCluster.create(config);
        return cqlCluster.getTokenRanges(tableName);
    }
}
