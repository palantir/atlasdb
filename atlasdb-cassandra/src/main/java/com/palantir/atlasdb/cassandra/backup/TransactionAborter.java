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

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

final class TransactionAborter {
    private static final SafeLogger log = SafeLoggerFactory.get(TransactionAborter.class);

    private static final int RETRY_COUNT = 3;

    private final CqlSession cqlSession;
    private final CassandraKeyValueServiceConfig config;
    private final Retryer<Boolean> abortRetryer;

    public TransactionAborter(CqlSession cqlSession, CassandraKeyValueServiceConfig config) {
        this.cqlSession = cqlSession;
        this.config = config;

        this.abortRetryer = new Retryer<>(
                StopStrategies.stopAfterAttempt(RETRY_COUNT),
                WaitStrategies.fixedWait(1L, TimeUnit.SECONDS),
                attempt -> !attempt.hasResult() || !attempt.getResult());
    }

    public void abortTransactions(long timestamp, List<TransactionsTableInteraction> transactionsTableInteractions) {
        CqlMetadata clusterMetadata = cqlSession.getMetadata();
        String keyspaceName = config.getKeyspaceOrThrow();
        transactionsTableInteractions.forEach(
                txnInteraction -> abortTransactions(clusterMetadata, keyspaceName, timestamp, txnInteraction));
    }

    private void abortTransactions(
            CqlMetadata clusterMetadata,
            String keyspaceName,
            long timestamp,
            TransactionsTableInteraction txnInteraction) {
        log.info(
                "Aborting transactions after backup timestamp",
                SafeArg.of("backupTimestamp", timestamp),
                SafeArg.of("keyspace", keyspaceName),
                SafeArg.of("table", txnInteraction.getTransactionsTableName()));

        TableMetadata transactionsTable = ClusterMetadataUtils.getTableMetadata(
                clusterMetadata, keyspaceName, txnInteraction.getTransactionsTableName());

        PreparedStatement preparedAbortStatement = txnInteraction.prepareAbortStatement(transactionsTable, cqlSession);
        PreparedStatement preparedCheckStatement = txnInteraction.prepareCheckStatement(transactionsTable, cqlSession);
        Stream<TransactionTableEntry> keysToAbort =
                getTransactionsToAbort(keyspaceName, txnInteraction, transactionsTable, timestamp);
        executeTransactionAborts(
                keyspaceName, txnInteraction, preparedAbortStatement, preparedCheckStatement, keysToAbort);
    }

    @VisibleForTesting
    Stream<TransactionTableEntry> getTransactionsToAbort(
            String keyspaceName,
            TransactionsTableInteraction txnInteraction,
            TableMetadata transactionsTable,
            long timestamp) {
        Stream<Row> rowResults =
                txnInteraction.createSelectStatementsForScanningFullTimestampRange(transactionsTable).stream()
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

    @VisibleForTesting
    void executeTransactionAborts(
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

        try {
            abortRetryer.call(() ->
                    tryAbortTransactions(keyspace, txnInteraction, abortStatement, checkStatement, startTs, commitTs));
        } catch (ExecutionException e) {
            throw new SafeIllegalStateException(
                    "Failed to execute transaction abort",
                    e,
                    SafeArg.of("startTs", startTs),
                    SafeArg.of("commitTs", commitTs),
                    SafeArg.of("retryCount", RETRY_COUNT),
                    SafeArg.of("keyspace", keyspace));

        } catch (RetryException e) {
            throw new SafeIllegalStateException(
                    "Unable to abort transactions even with retry",
                    e,
                    SafeArg.of("startTs", startTs),
                    SafeArg.of("commitTs", commitTs),
                    SafeArg.of("retryCount", RETRY_COUNT),
                    SafeArg.of("keyspace", keyspace));
        }
    }

    private boolean tryAbortTransactions(
            String keyspace,
            TransactionsTableInteraction txnInteraction,
            Statement abortStatement,
            Statement checkStatement,
            long startTs,
            long commitTs) {
        log.info(
                "Aborting transaction",
                SafeArg.of("startTs", startTs),
                SafeArg.of("commitTs", commitTs),
                SafeArg.of("keyspace", keyspace));
        ResultSet abortResultSet = cqlSession.execute(abortStatement);
        if (abortResultSet.wasApplied()) {
            return true;
        }

        log.debug(
                "Executing check statement",
                SafeArg.of("startTs", startTs),
                SafeArg.of("commitTs", commitTs),
                SafeArg.of("keyspace", keyspace));
        ResultSet checkResultSet = cqlSession.execute(checkStatement);
        Row result = Iterators.getOnlyElement(checkResultSet.all().iterator());

        TransactionTableEntry transactionTableEntry = txnInteraction.extractTimestamps(result);
        if (isAborted(transactionTableEntry)) {
            return true;
        }

        log.warn(
                "Retrying abort statement",
                SafeArg.of("startTs", startTs),
                SafeArg.of("commitTs", commitTs),
                SafeArg.of("keyspace", keyspace));
        return false;
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
