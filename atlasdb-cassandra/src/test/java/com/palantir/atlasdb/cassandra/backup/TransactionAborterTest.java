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
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
import com.palantir.atlasdb.atomic.AtomicValue;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionTableEntries;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionTableEntry;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionStatusUtils;
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
            FullyBoundedTimestampRange.of(Range.closed(TransactionConstants.LOWEST_POSSIBLE_START_TS, 200L));

    private static final String TXN_TABLE_NAME = "txn_table";
    private static final Namespace NAMESPACE = Namespace.of("keyspace");

    @Mock
    private CqlSession cqlSession;

    @Mock
    private TransactionsTableInteraction transactionInteraction;

    @Mock
    private Statement selectStatement;

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

        when(tableMetadata.getName()).thenReturn(TXN_TABLE_NAME);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getTables()).thenReturn(ImmutableList.of(tableMetadata));
        CqlMetadata cqlMetadata = mock(CqlMetadata.class);
        when(cqlMetadata.getKeyspaceMetadata(NAMESPACE)).thenReturn(keyspaceMetadata);
        when(cqlSession.getMetadata()).thenReturn(cqlMetadata);

        doReturn(ImmutableList.of(selectStatement))
                .when(transactionInteraction)
                .createSelectStatementsForScanningFullTimestampRange(any());
        ResultSet selectResponse = createSelectResponse(ImmutableList.of());
        when(cqlSession.execute(selectStatement)).thenReturn(selectResponse);

        doReturn(mock(PreparedStatement.class)).when(transactionInteraction).prepareAbortStatement(any(), any());
        doReturn(mock(PreparedStatement.class)).when(transactionInteraction).prepareCheckStatement(any(), any());

        transactionAborter = new TransactionAborter(cqlSession, NAMESPACE);
    }

    @Test
    public void willNotRunAbortWhenNothingToAbort() {
        transactionAborter.abortTransactions(BACKUP_TIMESTAMP, List.of(transactionInteraction));

        verify(cqlSession, times(1)).execute(selectStatement);
        verify(cqlSession, times(1)).getMetadata();
        verifyNoMoreInteractions(cqlSession);
    }

    @Test
    public void willNotAbortTimestampsLowerThanBackupTimestamp() {
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(10L, 20L),
                TransactionTableEntries.committedTwoPhase(
                        20L, AtomicValue.committed(TransactionStatusUtils.fromTimestamp(30L))),
                TransactionTableEntries.committedLegacy(30L, BACKUP_TIMESTAMP - 1));
        setupAbortTimestampTask(rows, TIMESTAMP_RANGE);

        Stream<TransactionTableEntry> transactionsToAbort =
                transactionAborter.getTransactionsToAbort(transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isZero();
    }

    @Test
    public void willNotAbortTimestampsOutsideRange() {
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(BACKUP_TIMESTAMP + 10, BACKUP_TIMESTAMP + 12),
                TransactionTableEntries.committedTwoPhase(
                        BACKUP_TIMESTAMP + 12,
                        AtomicValue.committed(TransactionStatusUtils.fromTimestamp(BACKUP_TIMESTAMP + 13))),
                TransactionTableEntries.committedLegacy(BACKUP_TIMESTAMP + 14, BACKUP_TIMESTAMP + 15));
        setupAbortTimestampTask(rows, FullyBoundedTimestampRange.of(Range.closed(BACKUP_TIMESTAMP + 16, 1000L)));

        Stream<TransactionTableEntry> transactionsToAbort =
                transactionAborter.getTransactionsToAbort(transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isZero();
    }

    @Test
    public void willNotAbortTimestampsAlreadyAborted() {
        ImmutableList<TransactionTableEntry> rows =
                ImmutableList.of(TransactionTableEntries.explicitlyAborted(BACKUP_TIMESTAMP + 1));
        setupAbortTimestampTask(rows, TIMESTAMP_RANGE);

        Stream<TransactionTableEntry> transactionsToAbort =
                transactionAborter.getTransactionsToAbort(transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isZero();
    }

    @Test
    public void willAbortTimestampInRange() {
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(BACKUP_TIMESTAMP + 1, BACKUP_TIMESTAMP + 2),
                TransactionTableEntries.committedTwoPhase(
                        BACKUP_TIMESTAMP + 3,
                        AtomicValue.committed(TransactionStatusUtils.fromTimestamp(BACKUP_TIMESTAMP + 4))));
        setupAbortTimestampTask(rows, FullyBoundedTimestampRange.of(Range.closed(25L, 1000L)));

        Stream<TransactionTableEntry> transactionsToAbort =
                transactionAborter.getTransactionsToAbort(transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isEqualTo(2L);
    }

    @Test
    public void willAbortTimestampsHigherThanBackupTimestamp() {
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(BACKUP_TIMESTAMP + 1, BACKUP_TIMESTAMP + 2),
                TransactionTableEntries.committedTwoPhase(
                        BACKUP_TIMESTAMP + 3,
                        AtomicValue.committed(TransactionStatusUtils.fromTimestamp(BACKUP_TIMESTAMP + 4))));
        setupAbortTimestampTask(rows, TIMESTAMP_RANGE);

        Stream<TransactionTableEntry> transactionsToAbort =
                transactionAborter.getTransactionsToAbort(transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isEqualTo(2L);
    }

    @Test
    public void willAbortTimestampsThatStartBeforeButEndAfterBackupTimestamp() {
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(BACKUP_TIMESTAMP - 2, BACKUP_TIMESTAMP + 1),
                TransactionTableEntries.committedTwoPhase(
                        BACKUP_TIMESTAMP - 1,
                        AtomicValue.committed(TransactionStatusUtils.fromTimestamp(BACKUP_TIMESTAMP + 2))));
        setupAbortTimestampTask(rows, TIMESTAMP_RANGE);

        Stream<TransactionTableEntry> transactionsToAbort =
                transactionAborter.getTransactionsToAbort(transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isEqualTo(2L);
    }

    @Test
    public void willAbortTimestampsThatStartAfterBackupTimestampAndEndOutsideRange() {
        long endOfRange = TIMESTAMP_RANGE.inclusiveUpperBound();
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(BACKUP_TIMESTAMP + 1, endOfRange + 1),
                TransactionTableEntries.committedTwoPhase(
                        BACKUP_TIMESTAMP + 2,
                        AtomicValue.committed(TransactionStatusUtils.fromTimestamp(endOfRange + 2))));
        setupAbortTimestampTask(rows, TIMESTAMP_RANGE);

        Stream<TransactionTableEntry> transactionsToAbort =
                transactionAborter.getTransactionsToAbort(transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isEqualTo(2L);
    }

    @Test
    public void willNotAbortTimestampsThatStartAfterRange() {
        long endOfRange = TIMESTAMP_RANGE.inclusiveUpperBound();
        ImmutableList<TransactionTableEntry> rows = ImmutableList.of(
                TransactionTableEntries.committedLegacy(endOfRange + 1, endOfRange + 2),
                TransactionTableEntries.committedTwoPhase(
                        endOfRange + 3, AtomicValue.committed(TransactionStatusUtils.fromTimestamp(endOfRange + 4))));
        setupAbortTimestampTask(rows, TIMESTAMP_RANGE);

        Stream<TransactionTableEntry> transactionsToAbort =
                transactionAborter.getTransactionsToAbort(transactionInteraction, tableMetadata, BACKUP_TIMESTAMP);
        assertThat(transactionsToAbort.count()).isEqualTo(0L);
    }

    @Test
    public void executeWithRetryTriesSingleTimeIfAbortSucceeds() {
        ResultSet abortResponse = createAbortResponse(true);
        when(cqlSession.execute(abortStatement)).thenReturn(abortResponse);

        Stream<TransactionTableEntry> entries = Stream.of(TransactionTableEntries.committedLegacy(100L, 101L));
        transactionAborter.executeTransactionAborts(
                transactionInteraction, preparedAbortStatement, preparedCheckStatement, entries);

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
                transactionInteraction, preparedAbortStatement, preparedCheckStatement, entries);

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
                transactionInteraction, preparedAbortStatement, preparedCheckStatement, entries);

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
                        transactionInteraction, preparedAbortStatement, preparedCheckStatement, entries))
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
