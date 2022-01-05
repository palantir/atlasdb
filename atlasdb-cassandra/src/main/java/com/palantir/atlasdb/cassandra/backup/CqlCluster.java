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
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public final class CqlCluster implements Closeable {
    private static final SafeLogger log = SafeLoggerFactory.get(CqlCluster.class);

    private static final int LONG_READ_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(2);
    // reduce this from default because we run RepairTableTask across N keyspaces at the same time
    private static final int SELECT_FETCH_SIZE = 1_000;

    private final Cluster cluster;
    private final CassandraKeyValueServiceConfig config;

    // VisibleForTesting
    public CqlCluster(Cluster cluster, CassandraKeyValueServiceConfig config) {
        this.cluster = cluster;
        this.config = config;
    }

    public static CqlCluster create(CassandraKeyValueServiceConfig config) {
        Cluster cluster = new ClusterFactory(Cluster::builder).constructCluster(config);
        return new CqlCluster(cluster, config);
    }

    @Override
    public void close() throws IOException {
        cluster.close();
    }

    private static Set<InetSocketAddress> getHosts(CassandraKeyValueServiceConfig config) {
        return CassandraServersConfigs.getCqlCapableConfigIfValid(config)
                .map(CqlCapableConfig::cqlHosts)
                .orElseThrow(() -> new SafeIllegalStateException("Attempting to get token ranges with thrift config!"));
    }

    public Map<InetSocketAddress, RangeSet<LightweightOppToken>> getTokenRanges(String tableName) {
        try (CqlSession session = new CqlSession(cluster.connect())) {
            CqlMetadata metadata = session.getMetadata();
            String keyspaceName = config.getKeyspaceOrThrow();
            KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
            TableMetadata tableMetadata = keyspace.getTable(tableName);
            Set<LightweightOppToken> partitionTokens = getPartitionTokens(session, tableMetadata);
            Map<InetSocketAddress, RangeSet<LightweightOppToken>> tokenRangesByNode =
                    ClusterMetadataUtils.getTokenMapping(getHosts(config), metadata, keyspaceName, partitionTokens);

            if (!partitionTokens.isEmpty() && log.isDebugEnabled()) {
                int numTokenRanges = tokenRangesByNode.values().stream()
                        .mapToInt(ranges -> ranges.asRanges().size())
                        .sum();

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

    public Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> getTransactionsTableRangesForRepair(
            List<TransactionsTableInteraction> transactionsTableInteractions) {
        try (CqlSession session = new CqlSession(cluster.connect())) {
            String keyspaceName = config.getKeyspaceOrThrow();
            CqlMetadata metadata = session.getMetadata();

            Map<String, Set<LightweightOppToken>> partitionKeysByTable =
                    getPartitionTokensByTable(session, transactionsTableInteractions, keyspaceName, metadata);

            maybeLogTokenRanges(transactionsTableInteractions, partitionKeysByTable);

            Set<InetSocketAddress> hosts = getHosts(config);
            return KeyedStream.stream(partitionKeysByTable)
                    .map(ranges -> ClusterMetadataUtils.getTokenMapping(hosts, metadata, keyspaceName, ranges))
                    .collectToMap();
        }
    }

    private static Map<String, Set<LightweightOppToken>> getPartitionTokensByTable(
            CqlSession cqlSession,
            List<TransactionsTableInteraction> transactionsTableInteractions,
            String keyspaceName,
            CqlMetadata metadata) {
        Multimap<String, TransactionsTableInteraction> interactionsByTable =
                Multimaps.index(transactionsTableInteractions, TransactionsTableInteraction::getTransactionsTableName);
        return KeyedStream.stream(interactionsByTable.asMap())
                .map(interactionsForTable -> getPartitionsTokenForSingleTransactionsTable(
                        cqlSession, keyspaceName, metadata, interactionsForTable))
                .collectToMap();
    }

    private static Set<LightweightOppToken> getPartitionsTokenForSingleTransactionsTable(
            CqlSession cqlSession,
            String keyspaceName,
            CqlMetadata metadata,
            Collection<TransactionsTableInteraction> interactions) {
        return interactions.stream()
                .map(interaction ->
                        getPartitionTokensForTransactionsTable(cqlSession, keyspaceName, metadata, interaction))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private static Set<LightweightOppToken> getPartitionTokensForTransactionsTable(
            CqlSession cqlSession,
            String keyspaceName,
            CqlMetadata metadata,
            TransactionsTableInteraction interaction) {
        TableMetadata transactionsTableMetadata =
                ClusterMetadataUtils.getTableMetadata(metadata, keyspaceName, interaction.getTransactionsTableName());
        List<Statement> selectStatements = interaction.createSelectStatements(transactionsTableMetadata);
        return cqlSession.executeAtConsistencyAll(selectStatements);
    }

    private static void maybeLogTokenRanges(
            List<TransactionsTableInteraction> transactionsTableInteractions,
            Map<String, Set<LightweightOppToken>> partitionKeysByTable) {
        if (log.isDebugEnabled()) {
            Multimap<String, TransactionsTableInteraction> indexedInteractions = Multimaps.index(
                    transactionsTableInteractions, TransactionsTableInteraction::getTransactionsTableName);
            Multimap<String, FullyBoundedTimestampRange> loggableTableRanges =
                    Multimaps.transformValues(indexedInteractions, TransactionsTableInteraction::getTimestampRange);
            Map<String, Integer> numPartitionKeysByTable =
                    KeyedStream.stream(partitionKeysByTable).map(Set::size).collectToMap();
            log.debug(
                    "Identified token ranges requiring repair in the following transactions tables",
                    SafeArg.of("transactionsTablesWithRanges", loggableTableRanges),
                    SafeArg.of("numPartitionKeysByTable", numPartitionKeysByTable));
        }
    }

    private static Set<LightweightOppToken> getPartitionTokens(CqlSession session, TableMetadata tableMetadata) {
        return session.executeAtConsistencyAll(createSelectStatements(tableMetadata));
    }

    private static List<Statement> createSelectStatements(TableMetadata table) {
        return ImmutableList.of(createSelectStatement(table));
    }

    private static Statement createSelectStatement(TableMetadata table) {
        return QueryBuilder.select(CassandraConstants.ROW)
                // only returns the column that we need instead of all of them, otherwise we get a timeout
                .distinct()
                .from(table)
                .setConsistencyLevel(ConsistencyLevel.ALL)
                .setFetchSize(SELECT_FETCH_SIZE)
                .setReadTimeoutMillis(LONG_READ_TIMEOUT_MS);
    }
}
