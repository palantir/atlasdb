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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.cassandra.ClusterMetadataUtils;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.DefaultConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.Visitor;
import com.palantir.atlasdb.containers.SocksProxyNettyOptions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AtlasRestoreService {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasRestoreService.class);

    private final BackupPersister backupPersister;
    private final Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory;
    private final Function<Namespace, KeyValueService> keyValueServiceFactory;
    private final MetricsManager metricsManager;

    // TODO(gs): factor out Cassandra utils
    // this is the partition key column name for the coord/TS tables
    private static final String KEY_NAME = "key";
    private static final int LONG_READ_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(2);
    // reduce this from default because we run RepairTableTask across N keyspaces at the same time
    private static final int SELECT_FETCH_SIZE = 1_000;

    // TODO(gs): factory method / helper classes like AtlasBackupService?
    public AtlasRestoreService(
            BackupPersister backupPersister,
            // TODO(gs): wrapper class for config, runtimeConfig, KVS?
            Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory,
            MetricsManager metricsManager) {
        this.backupPersister = backupPersister;
        this.keyValueServiceConfigFactory = keyValueServiceConfigFactory;
        this.keyValueServiceFactory = keyValueServiceFactory;
        this.metricsManager = metricsManager;
    }

    // TODO(gs): get from where these table names are defined?
    private static final String COORDINATION = "_coordination";
    private static final String TARGETED_SWEEP_PROGRESS = "sweep__sweepProgressPerShard";
    private static final String TARGETED_SWEEP_ID_TO_NAME = "sweep__sweepIdToName";
    private static final String TARGETED_SWEEP_NAME_TO_ID = "sweep__sweepNameToId";
    private static final String TARGETED_SWEEP_TABLE_CLEARS = "sweep__tableClears";

    private static final Set<String> TABLES_TO_REPAIR = ImmutableSet.of(
            COORDINATION,
            TARGETED_SWEEP_PROGRESS,
            TARGETED_SWEEP_ID_TO_NAME,
            TARGETED_SWEEP_NAME_TO_ID,
            TARGETED_SWEEP_TABLE_CLEARS);

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

        // TODO(gs): error handling/retry
        Set<InetSocketAddress> hosts = config.servers().accept(new Visitor<>() {
            @Override
            public Set<InetSocketAddress> visit(DefaultConfig defaultConfig) {
                // TODO(gs): fail instead here? can we assume CQL support?
                return defaultConfig.thriftHosts();
            }

            @Override
            public Set<InetSocketAddress> visit(CqlCapableConfig cqlCapableConfig) {
                return cqlCapableConfig.cqlHosts();
            }
        });
        InetSocketAddress firstHost = hosts.stream().findAny().orElseThrow();
        Cluster cluster = new ClusterFactory(
                        () -> Cluster.builder().withNettyOptions(new SocksProxyNettyOptions(firstHost)))
                .constructCluster(hosts, config);

        try (Session session = cluster.connect()) {
            Metadata metadata = session.getCluster().getMetadata();
            String keyspaceName = config.getKeyspaceOrThrow();
            KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
            TableMetadata tableMetadata = keyspace.getTable(tableName);
            Set<Token> partitionTokens = getPartitionTokens(session, tableMetadata);
            Map<InetSocketAddress, Set<TokenRange>> tokenRangesByNode =
                    ClusterMetadataUtils.getTokenMapping(hosts, metadata, keyspaceName, partitionTokens);

            if (partitionTokens.isEmpty()) {
                log.trace(
                        "No token ranges identified requiring repair",
                        SafeArg.of("keyspace", keyspace),
                        SafeArg.of("table", tableName));
            } else {
                log.trace(
                        "Identified token ranges requiring repair",
                        SafeArg.of("keyspace", keyspace),
                        SafeArg.of("table", tableName),
                        SafeArg.of("numPartitionKeys", partitionTokens.size()),
                        SafeArg.of("tokenRanges", tokenRangesByNode));

                int numTokenRanges =
                        tokenRangesByNode.values().stream().mapToInt(Set::size).sum();

                log.debug(
                        "Identified token ranges requiring repair",
                        SafeArg.of("keyspace", keyspace),
                        SafeArg.of("table", tableName),
                        SafeArg.of("numPartitionKeys", partitionTokens.size()),
                        SafeArg.of("numTokenRanges", numTokenRanges));
            }

            return tokenRangesByNode;
        }
    }

    // TODO(gs): copied from IBRP
    private Set<Token> getPartitionTokens(Session session, TableMetadata tableMetadata) {
        Statement fullTableScan = createSelectStatement(tableMetadata);
        Iterator<Row> rows = session.execute(fullTableScan).iterator();
        return Streams.stream(rows).map(row -> row.getToken(KEY_NAME)).collect(Collectors.toSet());
    }

    private Statement createSelectStatement(TableMetadata table) {
        return QueryBuilder.select(KEY_NAME)
                // only returns the column that we need instead of all of them, otherwise we get a timeout
                .distinct()
                .from(table)
                .setConsistencyLevel(ConsistencyLevel.ALL)
                .setFetchSize(SELECT_FETCH_SIZE)
                .setReadTimeoutMillis(LONG_READ_TIMEOUT_MS);
    }
}
