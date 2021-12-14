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

package com.palantir.atlasdb.cassandra.backup.transaction;

import static com.palantir.atlasdb.cassandra.backup.transaction.Transactions1TableInteraction.COLUMN_NAME_BB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

public class Transactions1TableInteractionTest {
    private static final FullyBoundedTimestampRange TXN1_RANGE = FullyBoundedTimestampRange.of(Range.closed(5L, 500L));

    private final RetryPolicy mockPolicy = mock(RetryPolicy.class);
    private final TransactionsTableInteraction interaction = new Transactions1TableInteraction(TXN1_RANGE, mockPolicy);

    @Test
    public void cellEncodingTest() {
        Cell actualCell = V1EncodingStrategy.INSTANCE.encodeStartTimestampAsCell(111L);

        assertThat(Transactions1TableInteraction.encodeStartTimestamp(111L))
                .isEqualTo(ByteBuffer.wrap(actualCell.getRowName()));
        assertThat(COLUMN_NAME_BB).isEqualTo(ByteBuffer.wrap(actualCell.getColumnName()));
    }

    @Test
    public void valueEncodingTest() {
        byte[] actualValue = V1EncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(111L, 222L);
        byte[] abort =
                V1EncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(1L, TransactionConstants.FAILED_COMMIT_TS);

        assertThat(Transactions1TableInteraction.encodeCommitTimestamp(222L)).isEqualTo(ByteBuffer.wrap(actualValue));
        assertThat(Transactions1TableInteraction.encodeCommitTimestamp(TransactionConstants.FAILED_COMMIT_TS))
                .isEqualTo(ByteBuffer.wrap(abort));
    }

    @Test
    public void extractCommittedTimestampTest() {
        TransactionTableEntry entry = interaction.extractTimestamps(createRow(150L, 200L));
        TransactionTableEntryAssertions.assertLegacy(entry, (startTimestamp, commitTimestamp) -> {
            assertThat(startTimestamp).isEqualTo(150L);
            assertThat(commitTimestamp).isEqualTo(200L);
        });
    }

    @Test
    public void extractAbortedTimestampTest() {
        TransactionTableEntry entry = interaction.extractTimestamps(createAbortedRow(150L));
        TransactionTableEntryAssertions.assertAborted(
                entry, startTimestamp -> assertThat(startTimestamp).isEqualTo(150L));
    }

    @Test
    public void selectStatementSelectsRange() {
        TableMetadata tableMetadata = mock(TableMetadata.class, RETURNS_DEEP_STUBS);
        String keyspace = "keyspace";
        when(tableMetadata.getKeyspace().getName()).thenReturn(keyspace);
        when(tableMetadata.getName()).thenReturn(TransactionConstants.TRANSACTION_TABLE.getTableName());

        Transactions1TableInteraction txnInteraction =
                new Transactions1TableInteraction(FullyBoundedTimestampRange.of(Range.closedOpen(1L, 50L)), mockPolicy);

        Statement statement = Iterables.getOnlyElement(
                txnInteraction.createSelectStatements(tableMetadata).collect(Collectors.toSet()));

        // We encode it first to get a fixed length of the string--0x1 vs 0x01, for example
        assertThat(statement.toString().trim())
                .containsIgnoringCase(String.format(
                        "select * from \"%s\".\"%s\"", keyspace, txnInteraction.getTransactionsTableName()))
                .containsIgnoringCase(String.format(
                        "token(key)>=token(0x%s)",
                        Hex.encodeHexString(V1EncodingStrategy.INSTANCE
                                .encodeStartTimestampAsCell(1L)
                                .getRowName())))
                .containsIgnoringCase(String.format(
                        "token(key)<=token(0x%s)",
                        Hex.encodeHexString(V1EncodingStrategy.INSTANCE
                                .encodeStartTimestampAsCell(49L)
                                .getRowName())));
    }

    private static Row createRow(long start, long commit) {
        Row row = mock(Row.class);
        when(row.getBytes(CassandraConstants.ROW))
                .thenReturn(Transactions1TableInteraction.encodeStartTimestamp(start));
        when(row.getBytes(CassandraConstants.VALUE))
                .thenReturn(Transactions1TableInteraction.encodeCommitTimestamp(commit));
        return row;
    }

    private static Row createAbortedRow(long start) {
        Row row = mock(Row.class);
        when(row.getBytes(CassandraConstants.ROW))
                .thenReturn(Transactions1TableInteraction.encodeStartTimestamp(start));
        when(row.getBytes(CassandraConstants.VALUE))
                .thenReturn(Transactions1TableInteraction.encodeCommitTimestamp(TransactionConstants.FAILED_COMMIT_TS));
        return row;
    }
}
