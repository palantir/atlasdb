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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cassandra.backup.CqlSession;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.timestamp.FullyBoundedTimestampRange;
import java.nio.ByteBuffer;
import java.time.Duration;

public class Transactions1TableInteraction implements TransactionsTableInteraction {
    private final FullyBoundedTimestampRange timestampRange;
    private final RetryPolicy abortRetryPolicy;

    public Transactions1TableInteraction(FullyBoundedTimestampRange timestampRange, RetryPolicy abortRetryPolicy) {
        this.timestampRange = timestampRange;
        this.abortRetryPolicy = abortRetryPolicy;
    }

    private static final byte[] DUMMY = new byte[] {1};
    private static final byte[] ABORT_COMMIT_TS_ENCODED =
            V1EncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(0, TransactionConstants.FAILED_COMMIT_TS);
    static final ByteBuffer COLUMN_NAME_BB = ByteBuffer.wrap(TransactionConstants.COMMIT_TS_COLUMN);

    @Override
    public FullyBoundedTimestampRange getTimestampRange() {
        return timestampRange;
    }

    @Override
    public String getTransactionsTableName() {
        return TransactionConstants.TRANSACTION_TABLE.getTableName();
    }

    @Override
    // TODO(gs): find replacement for QueryBuilder
    public PreparedStatement prepareAbortStatement(TableMetadata transactionsTable, CqlSession session) {
        Statement abortStatement = QueryBuilder.update(transactionsTable)
                .with(QueryBuilder.set(CassandraConstants.VALUE, ByteBuffer.wrap(ABORT_COMMIT_TS_ENCODED)))
                .where(QueryBuilder.eq(CassandraConstants.ROW, QueryBuilder.bindMarker()))
                .and(QueryBuilder.eq(CassandraConstants.COLUMN, COLUMN_NAME_BB))
                .and(QueryBuilder.eq(CassandraConstants.TIMESTAMP, CassandraConstants.ENCODED_CAS_TABLE_TIMESTAMP))
                .onlyIf(QueryBuilder.eq(CassandraConstants.VALUE, QueryBuilder.bindMarker()));
        // if you change this from CAS then you must update RetryPolicy
        return session.prepare(abortStatement);
    }

    @Override
    public PreparedStatement prepareCheckStatement(TableMetadata transactionsTable, CqlSession session) {
        Statement checkStatement = QueryBuilder.select()
                .from(transactionsTable)
                .where(QueryBuilder.eq(CassandraConstants.ROW, QueryBuilder.bindMarker()))
                .and(QueryBuilder.eq(CassandraConstants.COLUMN, COLUMN_NAME_BB))
                .and(QueryBuilder.eq(CassandraConstants.TIMESTAMP, CassandraConstants.ENCODED_CAS_TABLE_TIMESTAMP));
        return session.prepare(checkStatement);
    }

    @Override
    public TransactionTableEntry extractTimestamps(Row row) {
        long startTimestamp = decodeStartTs(Bytes.getArray(row.getByteBuffer(CassandraConstants.ROW)));
        long commitTimestamp = decodeCommitTs(Bytes.getArray(row.getByteBuffer(CassandraConstants.VALUE)));
        return commitTimestamp == TransactionConstants.FAILED_COMMIT_TS
                ? TransactionTableEntries.explicitlyAborted(startTimestamp)
                : TransactionTableEntries.committedLegacy(startTimestamp, commitTimestamp);
    }

    @Override
    public Statement bindCheckStatement(PreparedStatement preparedCheckStatement, TransactionTableEntry entry) {
        long startTs = TransactionTableEntries.getStartTimestamp(entry);
        ByteBuffer startTimestampBb = encodeStartTimestamp(startTs);
        BoundStatement bound = preparedCheckStatement.bind(startTimestampBb);
        return bound.setConsistencyLevel(ConsistencyLevel.QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
                .setTimeout(Duration.ofMillis(LONG_READ_TIMEOUT_MS))
                .setRetryPolicy(DefaultRetryPolicy.RETRYING_ON_ERROR); // ?
    }

    @Override
    public Statement bindAbortStatement(PreparedStatement preparedAbortStatement, TransactionTableEntry entry) {
        long startTs = TransactionTableEntries.getStartTimestamp(entry);
        long commitTs = TransactionTableEntries.getCommitTimestamp(entry).orElseThrow(() -> illegalEntry(entry));
        ByteBuffer startTimestampBb = encodeStartTimestamp(startTs);
        ByteBuffer commitTimestampBb = encodeCommitTimestamp(commitTs);
        BoundStatement bound = preparedAbortStatement.bind(startTimestampBb, commitTimestampBb);
        return bound.setConsistencyLevel(ConsistencyLevel.QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
                .setDefaultTimestamp(CassandraConstants.CAS_TABLE_TIMESTAMP)
                .setReadTimeoutMillis(LONG_READ_TIMEOUT_MS)
                .setIdempotent(true) // by default CAS operations are not idempotent in case of multiple clients
                .setRetryPolicy(abortRetryPolicy);
    }

    @Override
    public List<Statement> createSelectStatementsForScanningFullTimestampRange(TableMetadata transactionsTable) {
        Statement select = QueryBuilder.select()
                .all()
                .from(transactionsTable)
                .where(QueryBuilder.lte(
                        QueryBuilder.token(CassandraConstants.ROW),
                        QueryBuilder.token(encodeStartTimestamp(timestampRange.inclusiveUpperBound()))))
                .and(QueryBuilder.gte(
                        QueryBuilder.token(CassandraConstants.ROW),
                        QueryBuilder.token(encodeStartTimestamp(timestampRange.inclusiveLowerBound()))))
                .setConsistencyLevel(ConsistencyLevel.QUORUM)
                .setFetchSize(SELECT_TRANSACTIONS_FETCH_SIZE)
                .setReadTimeoutMillis(LONG_READ_TIMEOUT_MS);

        return ImmutableList.of(select);
    }

    static ByteBuffer encodeStartTimestamp(long timestamp) {
        return ByteBuffer.wrap(V1EncodingStrategy.INSTANCE
                .encodeStartTimestampAsCell(timestamp)
                .getRowName());
    }

    private long decodeStartTs(byte[] startTsRow) {
        return V1EncodingStrategy.INSTANCE.decodeCellAsStartTimestamp(Cell.create(startTsRow, DUMMY));
    }

    static ByteBuffer encodeCommitTimestamp(long timestamp) {
        return ByteBuffer.wrap(V1EncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(0, timestamp));
    }

    private long decodeCommitTs(byte[] value) {
        return V1EncodingStrategy.INSTANCE.decodeValueAsCommitTimestamp(0, value);
    }
}
