/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cell.api.TransactionKeyValueService;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;
import org.apache.commons.lang3.NotImplementedException;

public class CqlTransactionKeyValueService implements TransactionKeyValueService {

    private static final String ATLAS_ROW = "key";
    private static final String ATLAS_COLUMN = "column1";
    private static final String ATLAS_TIMESTAMP = "column2";
    private static final String ATLAS_VALUE = "value";
    private static final String[] INSERT_COLUMN_NAMES = {ATLAS_ROW, ATLAS_COLUMN, ATLAS_TIMESTAMP, ATLAS_VALUE};

    private final CassandraKeyValueServiceConfig config;
    private final Session session;
    private final Executor executor;

    public CqlTransactionKeyValueService(CassandraKeyValueServiceConfig config, Session session, Executor executor) {
        this.config = config;
        this.session = session;
        this.executor = executor;
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        return StreamEx.of(rangeRequests.iterator())
                .mapToEntry(Function.identity(), rangeRequest -> {
                    Where query = QueryBuilder.select("*")
                            .from(config.getKeyspaceOrThrow(), tableRef.getTableName())
                            .where(QueryBuilder.in(ATLAS_COLUMN, rangeRequest.getColumnNames()))
                            .and(QueryBuilder.gte(ATLAS_ROW, rangeRequest.getStartInclusive()))
                            .and(QueryBuilder.lt(ATLAS_ROW, rangeRequest.getEndExclusive()))
                            .and(QueryBuilder.lt(ATLAS_TIMESTAMP, timestamp));
                    if (rangeRequest.getBatchHint() != null) {
                        query.setFetchSize(rangeRequest.getBatchHint());
                    }
                    ResultSet resultSet = session.execute(query);
                    return (TokenBackedBasicResultsPage<RowResult<Value>, byte[]>) SimpleTokenBackedResultsPage.create(
                            rangeRequest.getStartInclusive(),
                            StreamEx.of(resultSet.iterator()).map(CqlTransactionKeyValueService::rowResultFromRow));
                })
                .toMap();
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        if (columnSelection.noColumnsSelected()) {
            return Map.of();
        }

        return StreamEx.of(rows)
                .flatMap(row -> StreamEx.of(session.execute(getRowsQuery(tableRef, row, timestamp, columnSelection))
                        .iterator()))
                .toMap(CqlTransactionKeyValueService::cellFromRow, CqlTransactionKeyValueService::valueFromRow);
    }

    private BuiltStatement getRowsQuery(
            TableReference tableRef, Iterable<byte[]> row, long timestamp, ColumnSelection columnSelection) {
        Where query = QueryBuilder.select("*")
                .from(config.getKeyspaceOrThrow(), tableRef.getTableName())
                .where(QueryBuilder.eq(ATLAS_ROW, row))
                .and(QueryBuilder.lt(ATLAS_TIMESTAMP, timestamp));
        if (!columnSelection.allColumnsSelected()) {
            query.and(QueryBuilder.in(ATLAS_COLUMN, columnSelection.getSelectedColumns()));
        }
        return query;
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {

        return StreamEx.of(rows.iterator())
                .mapToEntry(
                        Function.identity(),
                        row -> (RowColumnRangeIterator) StreamEx.of(session.execute(getRowsColumnRangeQuery(
                                                tableRef,
                                                row,
                                                batchColumnRangeSelection.getColumnRangeSelection(),
                                                batchColumnRangeSelection.getBatchHint(),
                                                timestamp))
                                        .iterator())
                                .map(row2 -> Map.entry(cellFromRow(row2), valueFromRow(row2)))
                                .iterator())
                .toMap();
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {

        return (RowColumnRangeIterator) StreamEx.of(rows.iterator())
                .flatMap(row -> StreamEx.of(session.execute(
                                getRowsColumnRangeQuery(tableRef, row, columnRangeSelection, cellBatchHint, timestamp))
                        .iterator()))
                .map(row -> Map.entry(cellFromRow(row), valueFromRow(row)))
                .iterator();
    }

    private Statement getRowsColumnRangeQuery(
            TableReference tableRef,
            byte[] row,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        return QueryBuilder.select("*")
                .from(config.getKeyspaceOrThrow(), tableRef.getTableName())
                .where(QueryBuilder.eq(ATLAS_ROW, row))
                .and(QueryBuilder.lt(ATLAS_TIMESTAMP, timestamp))
                .and(QueryBuilder.gt(ATLAS_COLUMN, columnRangeSelection.getStartCol()))
                .and(QueryBuilder.lt(ATLAS_COLUMN, columnRangeSelection.getEndCol()))
                .setFetchSize(cellBatchHint);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        String keyspace = config.getKeyspaceOrThrow();
        String tableName = tableRef.getTableName();

        ResultSet resultSet = session.execute(QueryBuilder.select("*")
                .from(keyspace, tableName)
                .where(QueryBuilder.gt(QueryBuilder.token(ATLAS_ROW), rangeRequest.getStartInclusive()))
                .and(QueryBuilder.lt(QueryBuilder.token(ATLAS_ROW), rangeRequest.getEndExclusive()))
                .and(QueryBuilder.lt(ATLAS_TIMESTAMP, timestamp)));

        return ClosableIterators.wrapWithEmptyClose(StreamEx.of(resultSet.iterator())
                .map(CqlTransactionKeyValueService::rowResultFromRow)
                .iterator());
    }

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        String keyspace = config.getKeyspaceOrThrow();
        String tableName = tableRef.getTableName();

        return Futures.transform(
                Futures.allAsList(EntryStream.of(timestampByCell)
                        .mapKeyValue((cell, timestamp) -> session.executeAsync(QueryBuilder.select("*")
                                .from(keyspace, tableName)
                                .where(QueryBuilder.eq(ATLAS_ROW, cell.getRowName()))
                                .and(QueryBuilder.lt(ATLAS_TIMESTAMP, timestamp))))),
                resultsList -> StreamEx.of(resultsList)
                        .toMap(resultSet -> cellFromRow(resultSet.one()), resultSet -> valueFromRow(resultSet.one())),
                executor);
    }

    private static Cell cellFromRow(Row row) {
        return Cell.create(
                row.getBytes(ATLAS_ROW).array(), row.getBytes(ATLAS_COLUMN).array());
    }

    private static Value valueFromRow(Row row) {
        return Value.create(row.getBytes(ATLAS_VALUE).array(), row.getLong(ATLAS_TIMESTAMP));
    }

    private static RowResult<Value> rowResultFromRow(Row row) {
        return RowResult.of(cellFromRow(row), valueFromRow(row));
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        throw new NotImplementedException();
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        valuesByTable.forEach((tableRef, values) -> put(tableRef, values, timestamp));
    }

    private void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp)
            throws KeyAlreadyExistsException {
        String keyspace = config.getKeyspaceOrThrow();
        String tableName = tableRef.getTableName();
        values.forEach((cell, value) -> {
            Insert query = QueryBuilder.insertInto(keyspace, tableName)
                    .values(
                            INSERT_COLUMN_NAMES,
                            getValuesToInsert(cell.getRowName(), cell.getColumnName(), timestamp, value));
            query.using(QueryBuilder.timestamp(timestamp));
            session.execute(query);
        });
    }

    private static Object[] getValuesToInsert(byte[] atlasRow, byte[] atlasColumn, long timestamp, byte[] atlasValue) {
        return new Object[] {atlasRow, atlasColumn, timestamp, atlasValue};
    }

    @Override
    public boolean isValid(long timestamp) {
        return true;
    }
}
