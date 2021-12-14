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

package com.palantir.atlasdb.backup.transaction;

import static com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy.PARTITIONING_QUANTUM;
import static com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy.ROWS_PER_QUANTUM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.collect.Range;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.pue.PutUnlessExistsValue;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.binary.Hex;
import org.junit.Before;
import org.junit.Test;

public class Transactions3TableInteractionTest {
    private static final FullyBoundedTimestampRange RANGE = FullyBoundedTimestampRange.of(Range.closed(5L, 500L));
    private static final String KEYSPACE = "keyspace";

    private final RetryPolicy mockPolicy = mock(RetryPolicy.class);
    private final TransactionsTableInteraction interaction = new Transactions3TableInteraction(RANGE, mockPolicy);
    private final TableMetadata tableMetadata = mock(TableMetadata.class, RETURNS_DEEP_STUBS);

    @Before
    public void setupMock() {
        when(tableMetadata.getKeyspace().getName()).thenReturn(KEYSPACE);
        when(tableMetadata.getName()).thenReturn(TransactionConstants.TRANSACTIONS2_TABLE.getTableName());
    }

    @Test
    public void extractCommittedTimestampTest() {
        TransactionTableEntry entry = interaction.extractTimestamps(createRow(150L, 200L));
        TransactionTableEntryAssertions.assertTwoPhase(entry, (startTs, commitValue) -> {
            assertThat(startTs).isEqualTo(150L);
            assertThat(commitValue).isEqualTo(PutUnlessExistsValue.committed(200L));
        });
    }

    @Test
    public void extractStagingCommitTimestampTest() {
        TransactionTableEntry entry =
                interaction.extractTimestamps(createRow(150L, PutUnlessExistsValue.staging(200L)));
        TransactionTableEntryAssertions.assertTwoPhase(entry, (startTs, commitValue) -> {
            assertThat(startTs).isEqualTo(150L);
            assertThat(commitValue).isEqualTo(PutUnlessExistsValue.staging(200L));
        });
    }

    @Test
    public void extractAbortedTimestampTest() {
        TransactionTableEntry entry = interaction.extractTimestamps(createAbortedRow(150L));
        TransactionTableEntryAssertions.assertAborted(
                entry, startTimestamp -> assertThat(startTimestamp).isEqualTo(150L));
    }

    @Test
    public void extractStagingAbortedTimestampTest() {
        TransactionTableEntry entry = interaction.extractTimestamps(
                createRow(150L, PutUnlessExistsValue.staging(TransactionConstants.FAILED_COMMIT_TS)));
        TransactionTableEntryAssertions.assertAborted(
                entry, startTimestamp -> assertThat(startTimestamp).isEqualTo(150L));
    }

    @Test
    public void getAllRowsInPartition() {
        Range<Long> rangeWithinOnePartition = Range.closed(100L, 1000L);
        Transactions3TableInteraction txnInteraction =
                new Transactions3TableInteraction(FullyBoundedTimestampRange.of(rangeWithinOnePartition), mockPolicy);
        List<Statement> selects = txnInteraction.createSelectStatements(tableMetadata);
        List<String> correctSelects = createSelectStatement(0L, ROWS_PER_QUANTUM - 1);
        assertThat(selects)
                .extracting(statement -> statement.toString().trim().toLowerCase())
                .containsExactlyInAnyOrderElementsOf(correctSelects);
    }

    @Test
    public void getsRowsInAllSpannedPartitions() {
        Range<Long> rangeWithinOnePartition = Range.closed(100L, PARTITIONING_QUANTUM + 1000000);
        Transactions3TableInteraction txnInteraction =
                new Transactions3TableInteraction(FullyBoundedTimestampRange.of(rangeWithinOnePartition), mockPolicy);
        List<Statement> selects = txnInteraction.createSelectStatements(tableMetadata);
        List<String> correctSelects = createSelectStatement(0, 2 * ROWS_PER_QUANTUM - 1);
        assertThat(selects)
                .extracting(statement -> statement.toString().trim().toLowerCase())
                .containsExactlyInAnyOrderElementsOf(correctSelects);
    }

    @Test
    public void doesntGetNextPartitionIfOpenBounded() {
        Range<Long> rangeWithinOnePartition = Range.closedOpen(100L, 25000000L);
        Transactions3TableInteraction txnInteraction =
                new Transactions3TableInteraction(FullyBoundedTimestampRange.of(rangeWithinOnePartition), mockPolicy);
        List<Statement> selects = txnInteraction.createSelectStatements(tableMetadata);
        List<String> correctSelects = createSelectStatement(0L, 15L);
        assertThat(selects)
                .extracting(statement -> statement.toString().trim().toLowerCase())
                .containsExactlyInAnyOrderElementsOf(correctSelects);
    }

    private static List<String> createSelectStatement(long startInclusive, long endInclusive) {
        List<String> statements = new ArrayList<>();
        for (long row = startInclusive; row <= endInclusive; row++) {
            statements.add(String.format(
                    "select * from \"%s\".\"%s\" where key=0x%s;",
                    KEYSPACE,
                    TransactionConstants.TRANSACTIONS2_TABLE.getTableName(),
                    Hex.encodeHexString(encodeRowKeyFromRowValue(row))));
        }
        return statements;
    }

    private static Row createRow(long start, long commit) {
        return createRow(start, PutUnlessExistsValue.committed(commit));
    }

    private static Row createRow(long start, PutUnlessExistsValue<Long> commit) {
        Row row = mock(Row.class);
        Cell cell = TicketsEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(start);
        when(row.getBytes(CassandraConstants.ROW)).thenReturn(ByteBuffer.wrap(cell.getRowName()));
        when(row.getBytes(CassandraConstants.COLUMN)).thenReturn(ByteBuffer.wrap(cell.getColumnName()));
        when(row.getBytes(CassandraConstants.VALUE))
                .thenReturn(
                        ByteBuffer.wrap(TwoPhaseEncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(start, commit)));
        return row;
    }

    private static Row createAbortedRow(long start) {
        Row row = mock(Row.class);
        Cell cell = TicketsEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(start);
        when(row.getBytes(CassandraConstants.ROW)).thenReturn(ByteBuffer.wrap(cell.getRowName()));
        when(row.getBytes(CassandraConstants.COLUMN)).thenReturn(ByteBuffer.wrap(cell.getColumnName()));
        when(row.getBytes(CassandraConstants.VALUE))
                .thenReturn(ByteBuffer.wrap(TwoPhaseEncodingStrategy.ABORTED_TRANSACTION_COMMITTED_VALUE));
        return row;
    }

    // c/p of duplicated logic from internal backup tool
    private static byte[] encodeRowKeyFromRowValue(long rowValue) {
        return Longs.toByteArray(Long.reverse(rowValue));
    }
}
