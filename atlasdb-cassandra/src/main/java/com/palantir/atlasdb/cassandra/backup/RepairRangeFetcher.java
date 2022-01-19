/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class RepairRangeFetcher {
    private static final SafeLogger log = SafeLoggerFactory.get(RepairRangeFetcher.class);

    private final CqlSession cqlSession;
    private final CqlMetadata cqlMetadata;
    private final CassandraKeyValueServiceConfig config;

    public RepairRangeFetcher(CqlSession cqlSession, CassandraKeyValueServiceConfig config) {
        this.cqlSession = cqlSession;
        this.cqlMetadata = cqlSession.getMetadata();
        this.config = config;
    }

    public Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> getTransactionTableRangesForRepair(
            List<TransactionsTableInteraction> transactionsTableInteractions) {
        String keyspaceName = config.getKeyspaceOrThrow();

        Map<String, Set<LightweightOppToken>> partitionKeysByTable =
                getPartitionTokensByTable(transactionsTableInteractions, keyspaceName);

        maybeLogTokenRanges(transactionsTableInteractions, partitionKeysByTable);

        Set<InetSocketAddress> hosts = CassandraServersConfigs.getCqlHosts(config);
        return KeyedStream.stream(partitionKeysByTable)
                .map(ranges -> ClusterMetadataUtils.getTokenMapping(hosts, cqlMetadata, keyspaceName, ranges))
                .collectToMap();
    }

    private Map<String, Set<LightweightOppToken>> getPartitionTokensByTable(
            List<TransactionsTableInteraction> transactionsTableInteractions, String keyspaceName) {
        Multimap<String, TransactionsTableInteraction> interactionsByTable =
                Multimaps.index(transactionsTableInteractions, TransactionsTableInteraction::getTransactionsTableName);
        return KeyedStream.stream(interactionsByTable.asMap())
                .map(interactionsForTable ->
                        getPartitionsTokenForSingleTransactionsTable(keyspaceName, interactionsForTable))
                .collectToMap();
    }

    private Set<LightweightOppToken> getPartitionsTokenForSingleTransactionsTable(
            String keyspaceName, Collection<TransactionsTableInteraction> interactions) {
        return interactions.stream()
                .map(interaction -> getPartitionTokensForTransactionsTable(keyspaceName, interaction))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private Set<LightweightOppToken> getPartitionTokensForTransactionsTable(
            String keyspaceName, TransactionsTableInteraction interaction) {
        TableMetadata transactionsTableMetadata = ClusterMetadataUtils.getTableMetadata(
                cqlMetadata, keyspaceName, interaction.getTransactionsTableName());
        List<Statement> selectStatements =
                interaction.createSelectStatementsForScanningFullTimestampRange(transactionsTableMetadata);
        return cqlSession.retrieveRowKeysAtConsistencyAll(selectStatements);
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
}
