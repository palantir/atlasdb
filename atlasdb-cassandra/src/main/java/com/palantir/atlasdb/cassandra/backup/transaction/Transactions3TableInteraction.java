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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.Bytes;
import com.palantir.atlasdb.atomic.AtomicValue;
import com.palantir.atlasdb.cassandra.backup.CqlSession;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.keyvalue.cassandra.CellValuePutter;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Transactions3TableInteraction implements TransactionsTableInteraction {
    private final FullyBoundedTimestampRange timestampRange;

    public Transactions3TableInteraction(FullyBoundedTimestampRange timestampRange) {
        this.timestampRange = timestampRange;
    }

    @Override
    public FullyBoundedTimestampRange getTimestampRange() {
        return timestampRange;
    }

    @Override
    public String getTransactionsTableName() {
        return TransactionConstants.TRANSACTIONS2_TABLE.getTableName();
    }

    @Override
    public PreparedStatement prepareAbortStatement(TableMetadata transactionsTable, CqlSession session) {
        // we are declaring bankruptcy if this fails anyway
        ByteBuffer abortCommitTsBb = ByteBuffer.wrap(TwoPhaseEncodingStrategy.ABORTED_TRANSACTION_COMMITTED_VALUE);

        Statement abortStatement = QueryBuilder.update(transactionsTable)
                .with(QueryBuilder.set(CassandraConstants.VALUE, abortCommitTsBb))
                .where(QueryBuilder.eq(CassandraConstants.ROW, QueryBuilder.bindMarker()))
                .and(QueryBuilder.eq(CassandraConstants.COLUMN, QueryBuilder.bindMarker()))
                .and(QueryBuilder.eq(CassandraConstants.TIMESTAMP, CassandraConstants.ENCODED_CAS_TABLE_TIMESTAMP))
                .using(QueryBuilder.timestamp(CellValuePutter.SET_TIMESTAMP + 1));
        // abortRetryPolicy must match the type of operation of the abort statement
        // so if the statement is changed TO a CAS, callers will need to change their policy
        return session.prepare(abortStatement);
    }

    @Override
    public PreparedStatement prepareCheckStatement(TableMetadata transactionsTable, CqlSession session) {
        Statement checkStatement = QueryBuilder.select()
                .from(transactionsTable)
                .where(QueryBuilder.eq(CassandraConstants.ROW, QueryBuilder.bindMarker()))
                .and(QueryBuilder.eq(CassandraConstants.COLUMN, QueryBuilder.bindMarker()))
                .and(QueryBuilder.eq(CassandraConstants.TIMESTAMP, CassandraConstants.ENCODED_CAS_TABLE_TIMESTAMP));
        return session.prepare(checkStatement);
    }

    @Override
    public TransactionTableEntry extractTimestamps(Row row) {
        long startTimestamp = TwoPhaseEncodingStrategy.INSTANCE.decodeCellAsStartTimestamp(Cell.create(
                Bytes.getArray(row.getBytes(CassandraConstants.ROW)),
                Bytes.getArray(row.getBytes(CassandraConstants.COLUMN))));
        AtomicValue<Long> commitValue = TwoPhaseEncodingStrategy.INSTANCE.decodeValueAsCommitTimestamp(
                startTimestamp, Bytes.getArray(row.getBytes(CassandraConstants.VALUE)));
        if (commitValue.value() == TransactionConstants.FAILED_COMMIT_TS) {
            return TransactionTableEntries.explicitlyAborted(startTimestamp);
        }

        return TransactionTableEntries.committedTwoPhase(startTimestamp, commitValue);
    }

    @Override
    public Statement bindCheckStatement(PreparedStatement preparedCheckStatement, TransactionTableEntry entry) {
        long startTs = TransactionTableEntries.getStartTimestamp(entry);
        Cell cell = TwoPhaseEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(startTs);
        ByteBuffer rowKeyBb = ByteBuffer.wrap(cell.getRowName());
        ByteBuffer columnNameBb = ByteBuffer.wrap(cell.getColumnName());
        BoundStatement bound = preparedCheckStatement.bind(rowKeyBb, columnNameBb);
        return bound.setConsistencyLevel(ConsistencyLevel.QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
                .setReadTimeoutMillis(LONG_READ_TIMEOUT_MS)
                .setRetryPolicy(DefaultRetryPolicy.INSTANCE);
    }

    @Override
    public Statement bindAbortStatement(PreparedStatement preparedAbortStatement, TransactionTableEntry entry) {
        long startTs = TransactionTableEntries.getStartTimestamp(entry);
        Cell cell = TwoPhaseEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(startTs);
        ByteBuffer rowKeyBb = ByteBuffer.wrap(cell.getRowName());
        ByteBuffer columnNameBb = ByteBuffer.wrap(cell.getColumnName());
        BoundStatement bound = preparedAbortStatement.bind(rowKeyBb, columnNameBb);
        return bound.setConsistencyLevel(ConsistencyLevel.QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
                .setReadTimeoutMillis(LONG_READ_TIMEOUT_MS)
                .setRetryPolicy(DefaultRetryPolicy.INSTANCE);
    }

    @Override
    public List<Statement> createSelectStatementsForScanningFullTimestampRange(TableMetadata transactionsTable) {
        Set<ByteBuffer> encodedRowKeys = TwoPhaseEncodingStrategy.INSTANCE
                .encodeRangeOfStartTimestampsAsRows(
                        timestampRange.inclusiveLowerBound(), timestampRange.inclusiveUpperBound())
                .map(ByteBuffer::wrap)
                .collect(Collectors.toSet());
        return encodedRowKeys.stream()
                .map(rowKey -> QueryBuilder.select()
                        .all()
                        .from(transactionsTable)
                        .where(QueryBuilder.eq(CassandraConstants.ROW, rowKey))
                        .setConsistencyLevel(ConsistencyLevel.QUORUM)
                        .setFetchSize(SELECT_TRANSACTIONS_FETCH_SIZE)
                        .setReadTimeoutMillis(LONG_READ_TIMEOUT_MS))
                .collect(Collectors.toList());
    }
}
