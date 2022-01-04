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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionTableEntries;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionTableEntry;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory;
import com.palantir.atlasdb.pue.PutUnlessExistsValue;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class CqlCluster implements Closeable {
    private static final SafeLogger log = SafeLoggerFactory.get(CqlCluster.class);

    private static final int LONG_READ_TIMEOUT_MS = (int) TimeUnit.MINUTES.toMillis(2);
    // reduce this from default because we run RepairTableTask across N keyspaces at the same time
    private static final int SELECT_FETCH_SIZE = 1_000;
    private static final int RETRY_COUNT = 3;

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

    public Map<InetSocketAddress, Set<TokenRange>> getTokenRanges(String tableName) {
        try (Session session = cluster.connect()) {
            Metadata metadata = session.getCluster().getMetadata();
            String keyspaceName = config.getKeyspaceOrThrow();
            KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
            TableMetadata tableMetadata = keyspace.getTable(tableName);
            Set<Token> partitionTokens = getPartitionTokens(session, tableMetadata);
            Map<InetSocketAddress, Set<TokenRange>> tokenRangesByNode =
                    ClusterMetadataUtils.getTokenMapping(getHosts(config), metadata, keyspaceName, partitionTokens);

            if (!partitionTokens.isEmpty() && log.isDebugEnabled()) {
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

    public Map<String, Map<InetSocketAddress, Set<TokenRange>>> getTransactionsTableRangesForRepair(
            List<TransactionsTableInteraction> transactionsTableInteractions) {
        try (Session session = cluster.connect()) {
            String keyspaceName = config.getKeyspaceOrThrow();
            Metadata metadata = session.getCluster().getMetadata();
            Map<String, Set<Token>> partitionKeysByTable =
                    getPartitionTokensByTable(transactionsTableInteractions, session, keyspaceName, metadata);

            maybeLogTokenRanges(transactionsTableInteractions, partitionKeysByTable);

            Set<InetSocketAddress> hosts = getHosts(config);
            return KeyedStream.stream(partitionKeysByTable)
                    .map(ranges -> ClusterMetadataUtils.getTokenMapping(hosts, metadata, keyspaceName, ranges))
                    .collectToMap();
        }
    }

    public void abortTransactions(long timestamp, List<TransactionsTableInteraction> transactionsTableInteractions) {
        try (Session session = cluster.connect()) {
            Metadata clusterMetadata = cluster.getMetadata();
            String keyspaceName = config.getKeyspaceOrThrow();
            transactionsTableInteractions.forEach(txnInteraction -> {
                log.info(
                        "Aborting transactions after backup timestamp",
                        SafeArg.of("backupTimestamp", timestamp),
                        SafeArg.of("keyspace", keyspaceName),
                        SafeArg.of("table", txnInteraction.getTransactionsTableName()));

                TableMetadata transactionsTable = ClusterMetadataUtils.getTableMetadata(
                        clusterMetadata, keyspaceName, txnInteraction.getTransactionsTableName());
                PreparedStatement preparedAbortStatement =
                        txnInteraction.prepareAbortStatement(transactionsTable, session);
                PreparedStatement preparedCheckStatement =
                        txnInteraction.prepareCheckStatement(transactionsTable, session);
                Stream<TransactionTableEntry> keysToAbort =
                        getTransactionsToAbort(session, txnInteraction, transactionsTable, timestamp);
                executeTransactionAborts(
                        session,
                        keyspaceName,
                        txnInteraction,
                        preparedAbortStatement,
                        preparedCheckStatement,
                        keysToAbort);
            });
        }
    }

    private Stream<TransactionTableEntry> getTransactionsToAbort(
            Session session,
            TransactionsTableInteraction txnInteraction,
            TableMetadata transactionsTable,
            long timestamp) {
        List<Statement> selectStatement = txnInteraction.createSelectStatements(transactionsTable);

        Stream<Row> rowResults = selectStatement.stream()
                .map(select -> session.execute(select).iterator())
                .flatMap(Streams::stream);

        return KeyedStream.of(rowResults)
                .map(txnInteraction::extractTimestamps)
                .filter(entry -> isInRange(txnInteraction, entry, timestamp))
                .values();
    }

    private boolean isInRange(
            TransactionsTableInteraction txnInteraction, TransactionTableEntry entry, long timestamp) {
        Optional<Long> maybeCommitTimestamp = getCommitTimestamp(entry);
        if (maybeCommitTimestamp.isEmpty()) {
            return false;
        }

        long startTimestamp = TransactionTableEntries.getStartTimestamp(entry);
        long commitTimestamp = maybeCommitTimestamp.get();
        boolean isInRange = txnInteraction.getTimestampRange().contains(startTimestamp);
        if (commitTimestamp <= timestamp || !isInRange) {
            return false;
        }

        log.debug(
                "Found transaction to abort",
                SafeArg.of("startTimestamp", startTimestamp),
                SafeArg.of("commitTimestamp", commitTimestamp),
                SafeArg.of("keyspace", config.getKeyspaceOrThrow()),
                SafeArg.of("table", txnInteraction.getTransactionsTableName()));

        return true;
    }

    private void executeTransactionAborts(
            Session session,
            String keyspace,
            TransactionsTableInteraction txnInteraction,
            PreparedStatement preparedAbortStatement,
            PreparedStatement preparedCheckStatement,
            Stream<TransactionTableEntry> entries) {
        entries.forEach(entry -> {
            Statement abortStatement = txnInteraction.bindAbortStatement(preparedAbortStatement, entry);
            Statement checkStatement = txnInteraction.bindCheckStatement(preparedCheckStatement, entry);
            executeWithRetry(keyspace, session, txnInteraction, abortStatement, checkStatement, entry);
        });
    }

    private void executeWithRetry(
            String keyspace,
            Session session,
            TransactionsTableInteraction txnInteraction,
            Statement abortStatement,
            Statement checkStatement,
            TransactionTableEntry entry) {
        long startTs = TransactionTableEntries.getStartTimestamp(entry);
        long commitTs = getCommitTimestamp(entry).orElseThrow();

        Preconditions.checkArgument(
                abortStatement.getSerialConsistencyLevel() == ConsistencyLevel.SERIAL,
                "Abort statement was not at expected consistency level",
                SafeArg.of("consistencyLevel", abortStatement.getSerialConsistencyLevel()),
                SafeArg.of("expectedConsistencyLevel", ConsistencyLevel.SERIAL));
        Preconditions.checkArgument(
                checkStatement.getSerialConsistencyLevel() == ConsistencyLevel.SERIAL,
                "Check statement was not at expected consistency level",
                SafeArg.of("consistencyLevel", checkStatement.getSerialConsistencyLevel()),
                SafeArg.of("expectedConsistencyLevel", ConsistencyLevel.SERIAL));

        // TODO(gs): use proper retry logic
        for (int retryCount = 0; retryCount < RETRY_COUNT; retryCount++) {
            log.info(
                    "Aborting transaction",
                    SafeArg.of("startTs", startTs),
                    SafeArg.of("commitTs", commitTs),
                    SafeArg.of("retryCount", retryCount),
                    SafeArg.of("keyspace", keyspace));
            ResultSet abortResultSet = session.execute(abortStatement);
            if (abortResultSet.wasApplied()) {
                return;
            }

            ResultSet checkResultSet = session.execute(checkStatement);
            log.debug(
                    "Executing check statement",
                    SafeArg.of("startTs", startTs),
                    SafeArg.of("commitTs", commitTs),
                    SafeArg.of("retryCount", retryCount),
                    SafeArg.of("keyspace", keyspace));
            Row result = Iterators.getOnlyElement(checkResultSet.all().iterator());

            TransactionTableEntry transactionTableEntry = txnInteraction.extractTimestamps(result);
            if (isAborted(transactionTableEntry)) {
                return;
            }

            log.warn(
                    "Retrying abort statement",
                    SafeArg.of("startTs", startTs),
                    SafeArg.of("commitTs", commitTs),
                    SafeArg.of("retryCount", retryCount + 1),
                    SafeArg.of("keyspace", keyspace));
        }

        log.error(
                "Unable to abort transactions even with retry",
                SafeArg.of("startTs", startTs),
                SafeArg.of("commitTs", commitTs),
                SafeArg.of("retryCount", RETRY_COUNT),
                SafeArg.of("keyspace", keyspace));
        throw new SafeIllegalStateException("Unable to verify abort statements even with retry");
    }

    private boolean isAborted(TransactionTableEntry transactionTableEntry) {
        return TransactionTableEntries.caseOf(transactionTableEntry)
                .explicitlyAborted(_startTs -> true)
                .otherwise(() -> false);
    }

    private Optional<Long> getCommitTimestamp(TransactionTableEntry entry) {
        return TransactionTableEntries.getCommitTimestamp(entry).or(() -> getCommitValue(entry));
    }

    private Optional<Long> getCommitValue(TransactionTableEntry entry) {
        return TransactionTableEntries.getCommitValue(entry).map(PutUnlessExistsValue::value);
    }

    private Map<String, Set<Token>> getPartitionTokensByTable(
            List<TransactionsTableInteraction> transactionsTableInteractions,
            Session session,
            String keyspaceName,
            Metadata metadata) {
        Multimap<String, TransactionsTableInteraction> interactionsByTable =
                Multimaps.index(transactionsTableInteractions, TransactionsTableInteraction::getTransactionsTableName);
        return KeyedStream.stream(interactionsByTable.asMap())
                .map(interactionsForTable -> getPartitionsTokenForSingleTransactionsTable(
                        session, keyspaceName, metadata, interactionsForTable))
                .collectToMap();
    }

    private Set<Token> getPartitionsTokenForSingleTransactionsTable(
            Session session,
            String keyspaceName,
            Metadata metadata,
            Collection<TransactionsTableInteraction> interactions) {
        return interactions.stream()
                .map(interaction ->
                        getPartitionTokensForTransactionsTable(session, keyspaceName, metadata, interaction))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private Set<Token> getPartitionTokensForTransactionsTable(
            Session session, String keyspaceName, Metadata metadata, TransactionsTableInteraction interaction) {
        TableMetadata transactionsTableMetadata =
                ClusterMetadataUtils.getTableMetadata(metadata, keyspaceName, interaction.getTransactionsTableName());
        List<Statement> selectStatements = interaction.createSelectStatements(transactionsTableMetadata);
        return executeAtConsistencyAll(session, selectStatements);
    }

    private void maybeLogTokenRanges(
            List<TransactionsTableInteraction> transactionsTableInteractions,
            Map<String, Set<Token>> partitionKeysByTable) {
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

    private static Set<Token> getPartitionTokens(Session session, TableMetadata tableMetadata) {
        return executeAtConsistencyAll(session, createSelectStatements(tableMetadata));
    }

    private static Set<Token> executeAtConsistencyAll(Session session, List<Statement> selectStatements) {
        return selectStatements.stream()
                .map(statement -> statement.setConsistencyLevel(ConsistencyLevel.ALL))
                .flatMap(select -> StreamSupport.stream(session.execute(select).spliterator(), false))
                .map(row -> row.getToken(CassandraConstants.ROW))
                .collect(Collectors.toSet());
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
