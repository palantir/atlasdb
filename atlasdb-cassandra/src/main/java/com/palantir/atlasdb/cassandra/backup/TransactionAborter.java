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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionTableEntries;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionTableEntry;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.pue.PutUnlessExistsValue;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

class TransactionAborter {
    private static final SafeLogger log = SafeLoggerFactory.get(TransactionAborter.class);

    private static final int RETRY_COUNT = 3;

    private final Session session;
    private final CqlSession cqlSession;
    private final CassandraKeyValueServiceConfig config;

    public TransactionAborter(Session session, CassandraKeyValueServiceConfig config) {
        this.session = session;
        this.cqlSession = new CqlSession(session);
        this.config = config;
    }

    public void abortTransactions(long timestamp, List<TransactionsTableInteraction> transactionsTableInteractions) {
        CqlMetadata clusterMetadata = cqlSession.getMetadata();
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
                    txnInteraction.prepareAbortStatement(transactionsTable, cqlSession);
            PreparedStatement preparedCheckStatement =
                    txnInteraction.prepareCheckStatement(transactionsTable, cqlSession);
            Stream<TransactionTableEntry> keysToAbort =
                    getTransactionsToAbort(keyspaceName, txnInteraction, transactionsTable, timestamp);
            executeTransactionAborts(
                    keyspaceName, txnInteraction, preparedAbortStatement, preparedCheckStatement, keysToAbort);
        });
    }

    private Stream<TransactionTableEntry> getTransactionsToAbort(
            String keyspaceName,
            TransactionsTableInteraction txnInteraction,
            TableMetadata transactionsTable,
            long timestamp) {
        List<Statement> selectStatement =
                txnInteraction.createSelectStatementsForScanningFullTimestampRange(transactionsTable);

        Stream<Row> rowResults = selectStatement.stream()
                .map(select -> cqlSession.execute(select).iterator())
                .flatMap(Streams::stream);

        return KeyedStream.of(rowResults)
                .map(txnInteraction::extractTimestamps)
                .filter(entry -> isInRange(keyspaceName, txnInteraction, entry, timestamp))
                .values();
    }

    private static boolean isInRange(
            String keyspaceName,
            TransactionsTableInteraction txnInteraction,
            TransactionTableEntry entry,
            long timestamp) {
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
                SafeArg.of("keyspace", keyspaceName),
                SafeArg.of("table", txnInteraction.getTransactionsTableName()));

        return true;
    }

    private void executeTransactionAborts(
            String keyspace,
            TransactionsTableInteraction txnInteraction,
            PreparedStatement preparedAbortStatement,
            PreparedStatement preparedCheckStatement,
            Stream<TransactionTableEntry> entries) {
        entries.forEach(entry -> {
            Statement abortStatement = txnInteraction.bindAbortStatement(preparedAbortStatement, entry);
            Statement checkStatement = txnInteraction.bindCheckStatement(preparedCheckStatement, entry);
            executeWithRetry(keyspace, txnInteraction, abortStatement, checkStatement, entry);
        });
    }

    private void executeWithRetry(
            String keyspace,
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
            ResultSet abortResultSet = cqlSession.execute(abortStatement);
            if (abortResultSet.wasApplied()) {
                return;
            }

            ResultSet checkResultSet = cqlSession.execute(checkStatement);
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

    private static boolean isAborted(TransactionTableEntry transactionTableEntry) {
        return TransactionTableEntries.caseOf(transactionTableEntry)
                .explicitlyAborted(_startTs -> true)
                .otherwise(() -> false);
    }

    private static Optional<Long> getCommitTimestamp(TransactionTableEntry entry) {
        return TransactionTableEntries.getCommitTimestamp(entry).or(() -> getCommitValue(entry));
    }

    private static Optional<Long> getCommitValue(TransactionTableEntry entry) {
        return TransactionTableEntries.getCommitValue(entry).map(PutUnlessExistsValue::value);
    }
}
