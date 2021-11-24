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
import com.google.common.collect.Streams;
import com.palantir.atlasdb.backup.api.CompletedBackup;
import com.palantir.atlasdb.backup.cassandra.ClusterMetadataUtils;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.containers.SocksProxyNettyOptions;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolImpl;
import com.palantir.atlasdb.keyvalue.cassandra.async.CqlClient;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.DefaultCqlClientFactory;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AtlasRestoreService {
    private static final SafeLogger log = SafeLoggerFactory.get(AtlasRestoreService.class);

    private final BackupPersister backupPersister;
    private final Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory;
    private final Function<Namespace, CassandraKeyValueServiceRuntimeConfig> runtimeConfigFactory;
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
            Function<Namespace, CassandraKeyValueServiceRuntimeConfig> runtimeConfigFactory,
            MetricsManager metricsManager) {
        this.backupPersister = backupPersister;
        this.keyValueServiceConfigFactory = keyValueServiceConfigFactory;
        this.runtimeConfigFactory = runtimeConfigFactory;
        this.metricsManager = metricsManager;
    }

    // ConsistentCasTablesTask
    public boolean repairInternalTables(
            Namespace namespace, Consumer<Map<InetSocketAddress, Set<TokenRange>>> repairer) {
        Optional<CompletedBackup> maybeCompletedBackup = backupPersister.getCompletedBackup(namespace);

        if (maybeCompletedBackup.isEmpty()) {
            log.error("Could not restore namespace, as no backup is stored", SafeArg.of("namespace", namespace));
            return false;
        }

        // Fixed table name for now - TODO(gs): list + iteration
        String tableName = "_coordination";
        Map<InetSocketAddress, Set<TokenRange>> rangesToRepair = getRangesToRepair(namespace, tableName);
        repairer.accept(rangesToRepair);
        return true;
    }

    // TODO(gs): return type
    private Map<InetSocketAddress, Set<TokenRange>> getRangesToRepair(Namespace namespace, String tableName) {
        CassandraKeyValueServiceConfig config = keyValueServiceConfigFactory.apply(namespace);
        Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfig = () -> runtimeConfigFactory.apply(namespace);
        CassandraClientPool clientPool = CassandraClientPoolImpl.create(metricsManager, config, runtimeConfig, true);

        // TODO(gs): error handling/retry
        Collection<InetSocketAddress> addresses = config.addressTranslation().values();
        InetSocketAddress inetSocketAddress = addresses.stream().findAny().orElseThrow();
        DefaultCqlClientFactory cqlClientFactory = new DefaultCqlClientFactory(
                () -> Cluster.builder().withNettyOptions(new SocksProxyNettyOptions(inetSocketAddress)));
        CqlClient cqlClient = cqlClientFactory
                .constructClient(metricsManager.getTaggedRegistry(), config, false)
                .orElseThrow();
        Session session = cqlClient.getSession();
        Metadata metadata = session.getCluster().getMetadata();
        String keyspaceName = config.getKeyspaceOrThrow();
        KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
        TableMetadata tableMetadata = keyspace.getTable(tableName);
        Set<Token> partitionTokens = getPartitionTokens(session, tableMetadata);
        Map<InetSocketAddress, Set<TokenRange>> tokenRangesByNode =
                ClusterMetadataUtils.getTokenMapping(addresses, metadata, keyspaceName, partitionTokens);

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
