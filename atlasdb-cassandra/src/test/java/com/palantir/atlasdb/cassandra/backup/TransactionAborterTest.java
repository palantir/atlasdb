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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionTableEntries;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionTableEntry;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.pue.PutUnlessExistsValue;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TransactionAborterTest {
    private static final long BACKUP_TIMESTAMP = 123;

    private static final FullyBoundedTimestampRange TIMESTAMP_RANGE =
            FullyBoundedTimestampRange.of(Range.closed(AtlasDbConstants.STARTING_TS, 200L));

    private static final String TXN_TABLE_NAME = "txn_table";
    private static final String KEYSPACE = "keyspace";

    @Mock
    private CqlSession cqlSession;

    @Mock
    private CassandraKeyValueServiceConfig config;

    @Mock
    private TransactionsTableInteraction transactionInteraction;

    @Mock
    private PreparedStatement preparedAbortStatement;

    @Mock
    private Statement abortStatement;

    @Mock
    private PreparedStatement preparedCheckStatement;

    @Mock
    private Statement checkStatement;

    @Mock
    private TableMetadata tableMetadata;

    private TransactionAborter transactionAborter;

    @Before
    public void before() {
        when(transactionInteraction.getTransactionsTableName()).thenReturn(TXN_TABLE_NAME);

        when(abortStatement.getSerialConsistencyLevel()).thenReturn(ConsistencyLevel.SERIAL);
        when(transactionInteraction.bindAbortStatement(eq(preparedAbortStatement), any()))
                .thenReturn(abortStatement);

        when(checkStatement.getSerialConsistencyLevel()).thenReturn(ConsistencyLevel.SERIAL);
        when(transactionInteraction.bindCheckStatement(eq(preparedCheckStatement), any()))
                .thenReturn(checkStatement);

        when(config.getKeyspaceOrThrow()).thenReturn(KEYSPACE);

        when(tableMetadata.getName()).thenReturn(TXN_TABLE_NAME);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getTables()).thenReturn(ImmutableList.of(tableMetadata));
        CqlMetadata cqlMetadata = mock(CqlMetadata.class);
        when(cqlMetadata.getKeyspaceMetadata(KEYSPACE)).thenReturn(keyspaceMetadata);
        when(cqlSession.getMetadata()).thenReturn(cqlMetadata);

        Statement mockStatement = mock(Statement.class);
        doReturn(ImmutableList.of(mockStatement))
                .when(transactionInteraction)
                .createSelectStatementsForScanningFullTimestampRange(any());
        ResultSet selectResponse = createSelectResponse(ImmutableList.of());
        when(cqlSession.execute(mockStatement)).thenReturn(selectResponse);

        doReturn(mock(PreparedStatement.class)).when(transactionInteraction).prepareAbortStatement(any(), any());
        doReturn(mock(PreparedStatement.class)).when(transactionInteraction).prepareCheckStatement(any(), any());

        transactionAborter = new TransactionAborter(cqlSession, config);
    }

    @Test
    public void willNotRunAbortWhenNothingToAbort() {
        transactionAborter.abortTransactions(BACKUP_TIMESTAMP, List.of(transactionInteraction));

        verify(cqlSession, times(1)).execute(any(Statement.class));
    }

    @Test
    public void willNotAbortTimestampsLowerThanInputTimestamp() {
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(10L, 20L),
                TransactionTableEntries.committedTwoPhase(20L, PutUnlessExistsValue.committed(30L)),
                TransactionTableEntries.committedLegacy(30L, BACKUP_TIMESTAMP - 1));
        setupAbortTimestampTask(rows, TIMESTAMP_RANGE);

        Stream<TransactionTableEntry> transactionsToAbort = transactionAborter.getTransactionsToAbort(
                KEYSPACE, transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isZero();
    }

    @Test
    public void willNotAbortTimestampsOutsideRange() {
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(BACKUP_TIMESTAMP + 10, BACKUP_TIMESTAMP + 12),
                TransactionTableEntries.committedTwoPhase(
                        BACKUP_TIMESTAMP + 10, PutUnlessExistsValue.committed(BACKUP_TIMESTAMP + 13)),
                TransactionTableEntries.committedLegacy(BACKUP_TIMESTAMP + 10, BACKUP_TIMESTAMP + 14));
        setupAbortTimestampTask(rows, FullyBoundedTimestampRange.of(Range.closed(BACKUP_TIMESTAMP + 15, 1000L)));

        Stream<TransactionTableEntry> transactionsToAbort = transactionAborter.getTransactionsToAbort(
                KEYSPACE, transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isZero();
    }

    @Test
    public void willNotAbortTimestampsAlreadyAborted() {
        ImmutableList<TransactionTableEntry> rows =
                ImmutableList.of(TransactionTableEntries.explicitlyAborted(BACKUP_TIMESTAMP + 1));
        setupAbortTimestampTask(rows, TIMESTAMP_RANGE);

        Stream<TransactionTableEntry> transactionsToAbort = transactionAborter.getTransactionsToAbort(
                KEYSPACE, transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isZero();
    }

    @Test
    public void willAbortTimestampInRange() {
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(BACKUP_TIMESTAMP + 1, BACKUP_TIMESTAMP + 2),
                TransactionTableEntries.committedTwoPhase(
                        BACKUP_TIMESTAMP + 3, PutUnlessExistsValue.committed(BACKUP_TIMESTAMP + 4)));
        setupAbortTimestampTask(rows, FullyBoundedTimestampRange.of(Range.closed(25L, 1000L)));

        Stream<TransactionTableEntry> transactionsToAbort = transactionAborter.getTransactionsToAbort(
                KEYSPACE, transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isEqualTo(2L);
    }

    @Test
    public void willAbortTimestampsHigherThanInputTimestamp() {
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(BACKUP_TIMESTAMP + 1, BACKUP_TIMESTAMP + 2),
                TransactionTableEntries.committedTwoPhase(
                        BACKUP_TIMESTAMP + 3, PutUnlessExistsValue.committed(BACKUP_TIMESTAMP + 4)));
        setupAbortTimestampTask(rows, TIMESTAMP_RANGE);

        Stream<TransactionTableEntry> transactionsToAbort = transactionAborter.getTransactionsToAbort(
                KEYSPACE, transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isEqualTo(2L);
    }

    @Test
    public void willAbortTimestampsThatStartBeforeButEndAfterInputTimestamp() {
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(BACKUP_TIMESTAMP - 2, BACKUP_TIMESTAMP + 1),
                TransactionTableEntries.committedTwoPhase(
                        BACKUP_TIMESTAMP - 1, PutUnlessExistsValue.committed(BACKUP_TIMESTAMP + 2)));
        setupAbortTimestampTask(rows, TIMESTAMP_RANGE);

        Stream<TransactionTableEntry> transactionsToAbort = transactionAborter.getTransactionsToAbort(
                KEYSPACE, transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isEqualTo(2L);
    }

    @Test
    public void executeWithRetryTriesSingleTimeIfAbortSucceeds() {
        ResultSet abortResponse = createAbortResponse(true);
        when(cqlSession.execute(abortStatement)).thenReturn(abortResponse);

        Stream<TransactionTableEntry> entries = Stream.of(TransactionTableEntries.committedLegacy(100L, 101L));
        transactionAborter.executeTransactionAborts(
                KEYSPACE, transactionInteraction, preparedAbortStatement, preparedCheckStatement, entries);

        verify(cqlSession).execute(abortStatement);
    }

    @Test
    public void executeWithRetryChecksIfAbortWasNotAppliedAndRetriesIfNoMatch() {
        ResultSet abortResponse1 = createAbortResponse(false);
        ResultSet abortResponse2 = createAbortResponse(true);

        when(cqlSession.execute(abortStatement)).thenReturn(abortResponse1).thenReturn(abortResponse2);

        Row row = mock(Row.class);
        ResultSet checkResponse = createSelectResponse(ImmutableList.of(row));
        when(cqlSession.execute(checkStatement)).thenReturn(checkResponse);

        Stream<TransactionTableEntry> entries = Stream.of(TransactionTableEntries.committedLegacy(100L, 101L));
        transactionAborter.executeTransactionAborts(
                KEYSPACE, transactionInteraction, preparedAbortStatement, preparedCheckStatement, entries);

        verify(cqlSession, times(2)).execute(abortStatement);
        verify(cqlSession).execute(checkStatement);
    }

    @Test
    public void executeWithRetryChecksIfAbortWasNotAppliedAndDoesNotRetryIfEqualsAbortTimestamp() {
        ResultSet abortResponse = createAbortResponse(false);
        when(cqlSession.execute(abortStatement)).thenReturn(abortResponse);

        Row row = mock(Row.class);
        ResultSet checkResponse = createSelectResponse(ImmutableList.of(row));

        TransactionTableEntry entry = TransactionTableEntries.explicitlyAborted(100L);
        when(cqlSession.execute(checkStatement)).thenReturn(checkResponse);
        when(transactionInteraction.extractTimestamps(row)).thenReturn(entry);

        TransactionTableEntry notYetAborted = TransactionTableEntries.committedLegacy(100L, 101L);
        Stream<TransactionTableEntry> entries = Stream.of(notYetAborted);
        transactionAborter.executeTransactionAborts(
                KEYSPACE, transactionInteraction, preparedAbortStatement, preparedCheckStatement, entries);

        verify(cqlSession, times(1)).execute(abortStatement);
        verify(cqlSession, times(1)).execute(checkStatement);
    }

    @Test
    public void executeWithRetryChecksEventuallyFails() {
        ResultSet abortResponse = createAbortResponse(false);
        when(cqlSession.execute(abortStatement)).thenReturn(abortResponse);

        Row row = mock(Row.class);
        ResultSet checkResponse = createSelectResponse(ImmutableList.of(row));
        when(cqlSession.execute(checkStatement)).thenReturn(checkResponse);

        Stream<TransactionTableEntry> entries = Stream.of(TransactionTableEntries.committedLegacy(100L, 101L));

        assertThatThrownBy(() -> transactionAborter.executeTransactionAborts(
                        KEYSPACE, transactionInteraction, preparedAbortStatement, preparedCheckStatement, entries))
                .isInstanceOf(IllegalStateException.class);

        verify(cqlSession, times(3)).execute(abortStatement);
        verify(cqlSession, times(3)).execute(checkStatement);
    }

    private void setupAbortTimestampTask(
            ImmutableList<TransactionTableEntry> entries, FullyBoundedTimestampRange range) {
        when(transactionInteraction.getTimestampRange()).thenReturn(range);
        List<Row> rows = entries.stream()
                .map(entry -> {
                    Row row = mock(Row.class);
                    when(transactionInteraction.extractTimestamps(row)).thenReturn(entry);
                    return row;
                })
                .collect(Collectors.toList());
        ResultSet selectResponse = createSelectResponse(rows);
        when(transactionInteraction.createSelectStatementsForScanningFullTimestampRange(any()))
                .thenReturn(ImmutableList.of(mock(Statement.class)));
        when(cqlSession.execute(any(Statement.class))).thenReturn(selectResponse);
    }

    private static ResultSet createAbortResponse(boolean wasApplied) {
        ResultSet response = mock(ResultSet.class);
        when(response.wasApplied()).thenReturn(wasApplied);
        return response;
    }

    private static ResultSet createSelectResponse(List<Row> transactions) {
        ResultSet response = mock(ResultSet.class);
        when(response.iterator()).thenReturn(transactions.iterator());
        when(response.all()).thenReturn(transactions);
        return response;
    }
}
