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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class Transactions1TableInteraction implements TransactionsTableInteraction {
    private final FullyBoundedTimestampRange timestampRange;
    private final RetryPolicy abortRetryPolicy;
    private final int longReadTimeoutMs;

    public Transactions1TableInteraction(
            FullyBoundedTimestampRange timestampRange, RetryPolicy abortRetryPolicy, int longReadTimeoutMs) {
        this.timestampRange = timestampRange;
        this.abortRetryPolicy = abortRetryPolicy;
        this.longReadTimeoutMs = longReadTimeoutMs;
    }

    @VisibleForTesting
    static final byte[] ABORT_COMMIT_TS_ENCODED =
            V1EncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(0, TransactionConstants.FAILED_COMMIT_TS);

    @VisibleForTesting
    static final byte[] COLUMN_NAME = TransactionConstants.COMMIT_TS_COLUMN;

    @Override
    public FullyBoundedTimestampRange getTimestampRange() {
        return timestampRange;
    }

    @Override
    public String getTransactionsTableName() {
        return TransactionConstants.TRANSACTION_TABLE.getTableName();
    }

    @Override
    public PreparedStatement prepareAbortStatement(TableMetadata transactionsTable, Session session) {
        ByteBuffer abortCommitTsBb = ByteBuffer.wrap(ABORT_COMMIT_TS_ENCODED);
        ByteBuffer columnNameBb = ByteBuffer.wrap(COLUMN_NAME);

        Statement abortStatement = QueryBuilder.update(transactionsTable)
                .with(QueryBuilder.set(CassandraConstants.VALUE, abortCommitTsBb))
                .where(QueryBuilder.eq(CassandraConstants.ROW, QueryBuilder.bindMarker())) // startTimestampBb
                .and(QueryBuilder.eq(CassandraConstants.COLUMN, columnNameBb))
                .and(QueryBuilder.eq(CassandraConstants.TIMESTAMP, ENCODED_TRANSACTION_TIMESTAMP))
                .onlyIf(QueryBuilder.eq(CassandraConstants.VALUE, QueryBuilder.bindMarker())); // commitTimestampBb
        // if you change this from CAS then you must update RetryPolicy
        return session.prepare(abortStatement.toString());
    }

    @Override
    public PreparedStatement prepareCheckStatement(TableMetadata transactionsTable, Session session) {
        ByteBuffer columnNameBb = ByteBuffer.wrap(COLUMN_NAME);
        Statement checkStatement = QueryBuilder.select()
                .from(transactionsTable)
                .where(QueryBuilder.eq(CassandraConstants.ROW, QueryBuilder.bindMarker())) // startTimestampBb
                .and(QueryBuilder.eq(CassandraConstants.COLUMN, columnNameBb))
                .and(QueryBuilder.eq(CassandraConstants.TIMESTAMP, ENCODED_TRANSACTION_TIMESTAMP));
        return session.prepare(checkStatement.toString());
    }

    @Override
    public TransactionTableEntry extractTimestamps(Row row) {
        long startTimestamp = decodeStartTs(Bytes.getArray(row.getBytes(CassandraConstants.ROW)));
        if (isRowAbortedTransaction(row)) {
            return new TransactionTableEntry(startTimestamp, Optional.empty());
        }
        long commitTimestamp = decodeCommitTs(Bytes.getArray(row.getBytes(CassandraConstants.VALUE)));
        return new TransactionTableEntry(startTimestamp, Optional.of(commitTimestamp));
    }

    @Override
    public Statement bindCheckStatement(PreparedStatement preparedCheckStatement, long startTs, long _commitTs) {
        ByteBuffer startTimestampBb = encodeStartTimestamp(startTs);
        BoundStatement bound = preparedCheckStatement.bind(startTimestampBb);
        return bound.setConsistencyLevel(ConsistencyLevel.QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
                .setReadTimeoutMillis(longReadTimeoutMs)
                .setRetryPolicy(DefaultRetryPolicy.INSTANCE);
    }

    @Override
    public Statement bindAbortStatement(PreparedStatement preparedAbortStatement, long startTs, long commitTs) {
        ByteBuffer startTimestampBb = encodeStartTimestamp(startTs);
        ByteBuffer commitTimestampBb = encodeCommitTimestamp(commitTs);
        BoundStatement bound = preparedAbortStatement.bind(startTimestampBb, commitTimestampBb);
        return bound.setConsistencyLevel(ConsistencyLevel.QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
                .setDefaultTimestamp(CassandraConstants.CAS_TABLE_TIMESTAMP)
                .setReadTimeoutMillis(longReadTimeoutMs)
                .setIdempotent(true) // by default CAS operations are not idempotent in case of multiple clients
                .setRetryPolicy(abortRetryPolicy);
    }

    @Override
    public boolean isRowAbortedTransaction(Row row) {
        byte[] value = Bytes.getArray(row.getBytes(CassandraConstants.VALUE));
        return Arrays.equals(value, ABORT_COMMIT_TS_ENCODED);
    }

    @Override
    public List<Statement> createSelectStatements(TableMetadata transactionsTable) {
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
                .setReadTimeoutMillis(longReadTimeoutMs);

        return ImmutableList.of(select);
    }

    private ByteBuffer encodeStartTimestamp(long timestamp) {
        return ByteBuffer.wrap(V1EncodingStrategy.INSTANCE
                .encodeStartTimestampAsCell(timestamp)
                .getRowName());
    }

    private ByteBuffer encodeCommitTimestamp(long timestamp) {
        return ByteBuffer.wrap(V1EncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(0, timestamp));
    }

    private long decodeStartTs(byte[] startTsRow) {
        return V1EncodingStrategy.INSTANCE.decodeCellAsStartTimestamp(
                Cell.create(startTsRow, PtBytes.EMPTY_BYTE_ARRAY));
    }

    private long decodeCommitTs(byte[] value) {
        return V1EncodingStrategy.INSTANCE.decodeValueAsCommitTimestamp(0, value);
    }
}
