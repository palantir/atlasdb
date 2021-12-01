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

package com.palantir.atlasdb.cassandra.backup;

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
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.DefaultConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.Visitor;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("CompileTimeConstant") // Temporary
public final class CqlCluster {
    private static final SafeLogger log = SafeLoggerFactory.get(CqlCluster.class);

    // this is the partition key column name for the coord/TS tables
    private static final String KEY_NAME = "key";
    private static final int LONG_READ_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(2);
    // reduce this from default because we run RepairTableTask across N keyspaces at the same time
    private static final int SELECT_FETCH_SIZE = 1_000;

    private final Cluster cluster;
    private final CassandraKeyValueServiceConfig config;

    private CqlCluster(Cluster cluster, CassandraKeyValueServiceConfig config) {
        this.cluster = cluster;
        this.config = config;
    }

    public static CqlCluster create(CassandraKeyValueServiceConfig config) {
        // TODO(gs): error handling/retry
        Set<InetSocketAddress> hosts = getHosts(config);
        // InetSocketAddress firstHost = hosts.stream().findAny().orElseThrow();
        Cluster cluster = new ClusterFactory(Cluster::builder)
                // Cluster cluster = new ClusterFactory(() -> Cluster.builder().withNettyOptions(new
                // SocksProxyNettyOptions(firstHost))
                .constructCluster(hosts, config);
        return new CqlCluster(cluster, config);
    }

    private static Set<InetSocketAddress> getHosts(CassandraKeyValueServiceConfig config) {
        return config.servers().accept(new Visitor<>() {
            @Override
            public Set<InetSocketAddress> visit(DefaultConfig defaultConfig) {
                throw new SafeIllegalStateException("Attempting to set up CqlCluster with thrift config!");
            }

            @Override
            public Set<InetSocketAddress> visit(CqlCapableConfig cqlCapableConfig) {
                return cqlCapableConfig.cqlHosts();
            }
        });
    }

    public Map<InetSocketAddress, Set<TokenRange>> getTokenRanges(String tableName) {
        try (Session session = cluster.connect()) {
            Metadata metadata = session.getCluster().getMetadata();
            String keyspaceName = config.getKeyspaceOrThrow();
            log.debug("Keyspace " + keyspaceName);
            KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
            log.debug("KeyspaceMetadata: " + keyspace.exportAsString());
            TableMetadata tableMetadata = keyspace.getTable(tableName);
            log.debug("TableMetadata for " + tableName + ": "
                    + (tableMetadata != null ? tableMetadata.exportAsString() : "null"));
            Set<Token> partitionTokens = getPartitionTokens(session, tableMetadata);
            Map<InetSocketAddress, Set<TokenRange>> tokenRangesByNode =
                    ClusterMetadataUtils.getTokenMapping(getHosts(config), metadata, keyspaceName, partitionTokens);

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

                if (log.isDebugEnabled()) {
                    int numTokenRanges = tokenRangesByNode.values().stream()
                            .mapToInt(Set::size)
                            .sum();

                    log.debug(
                            "Identified token ranges requiring repair",
                            SafeArg.of("keyspace", keyspace),
                            SafeArg.of("table", tableName),
                            SafeArg.of("numPartitionKeys", partitionTokens.size()),
                            SafeArg.of("numTokenRanges", numTokenRanges));
                }
            }

            return tokenRangesByNode;
        }
    }

    private static Set<Token> getPartitionTokens(Session session, TableMetadata tableMetadata) {
        Statement fullTableScan = createSelectStatement(tableMetadata);
        Iterator<Row> rows = session.execute(fullTableScan).iterator();
        return Streams.stream(rows).map(row -> row.getToken(KEY_NAME)).collect(Collectors.toSet());
    }

    private static Statement createSelectStatement(TableMetadata table) {
        return QueryBuilder.select(KEY_NAME)
                // only returns the column that we need instead of all of them, otherwise we get a timeout
                .distinct()
                .from(table)
                .setConsistencyLevel(ConsistencyLevel.ALL)
                .setFetchSize(SELECT_FETCH_SIZE)
                .setReadTimeoutMillis(LONG_READ_TIMEOUT_MS);
    }
}
