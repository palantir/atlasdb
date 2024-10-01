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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
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
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Function;
import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;

public class CqlTransactionKeyValueService implements TransactionKeyValueService {

    private static final String ATLAS_ROW = "key";
    private static final String ATLAS_COLUMN = "column1";
    private static final String ATLAS_TIMESTAMP = "column2";
    private static final String ATLAS_VALUE = "value";
    private static final String[] INSERT_COLUMN_NAMES = {ATLAS_ROW, ATLAS_COLUMN, ATLAS_TIMESTAMP, ATLAS_VALUE};

    private static final BuildableQuery GET_FIRST_BATCH_FOR_RANGES = QueryBuilder.selectFrom("catalog", "files3")
            .all()
            .where(
                    Relation.column(ATLAS_COLUMN).in(QueryBuilder.bindMarker("columnNames")),
                    Relation.column(ATLAS_ROW).isGreaterThanOrEqualTo(QueryBuilder.bindMarker("startInclusive")),
                    Relation.column(ATLAS_ROW).isLessThan(QueryBuilder.bindMarker("endExclusive")),
                    Relation.column(ATLAS_TIMESTAMP).isLessThan(QueryBuilder.bindMarker("timestamp")));

    private final CassandraKeyValueServiceConfig config;
    private final CqlSession session;
    private final Executor executor;

    public CqlTransactionKeyValueService(CassandraKeyValueServiceConfig config, CqlSession session, Executor executor) {
        this.config = config;
        this.session = session;
        this.executor = executor;
    }

    private static final String GET_FIRST_BATCH_FOR_RANGES_TEMPLATE =
            """
            SELECT * FROM %s.%s
            WHERE column1 IN :columnNames
                AND key >= :startInclusive
                AND key < :endExclusive
                AND column2 < :timestamp
            """;

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        String query = withTableName(GET_FIRST_BATCH_FOR_RANGES_TEMPLATE, tableRef);
        return StreamEx.of(rangeRequests.iterator())
                .mapToEntry(Function.identity(), rangeRequest -> {
                    SimpleStatement simpleStatement = SimpleStatement.newInstance(query);
                    if (rangeRequest.getBatchHint() != null) {
                        simpleStatement = simpleStatement.setPageSize(rangeRequest.getBatchHint());
                    }
                    ResultSet resultSet = session.execute(simpleStatement.setNamedValues(Map.of(
                            "columnNames",
                            rangeRequest.getColumnNames(),
                            "startInclusive",
                            rangeRequest.getStartInclusive(),
                            "endExclusive",
                            rangeRequest.getEndExclusive(),
                            "timestamp",
                            timestamp)));
                    return (TokenBackedBasicResultsPage<RowResult<Value>, byte[]>) SimpleTokenBackedResultsPage.create(
                            rangeRequest.getStartInclusive(),
                            StreamEx.of(resultSet.iterator()).map(CqlTransactionKeyValueService::rowResultFromRow));
                })
                .toMap();
    }

    private static final String GET_ROWS_QUERY_TEMPLATE =
            """
            SELECT * FROM %s.%s
            WHERE key IN (:rows)
                AND column1 IN :selectedColumns
                AND column2 < :timestamp
            """;
    private static final String GET_ROWS_ALL_COLUMNS_QUERY_TEMPLATE =
            """
            SELECT * FROM %s.%s
            WHERE key IN (:row)
                AND column2 < :timestamp
            """;

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        if (columnSelection.noColumnsSelected()) {
            return Map.of();
        }

        String query = withTableName(
                columnSelection.allColumnsSelected() ? GET_ROWS_ALL_COLUMNS_QUERY_TEMPLATE : GET_ROWS_QUERY_TEMPLATE,
                tableRef);

        return StreamEx.of(rows)
                .flatMap(row -> StreamEx.of(session.execute(query, Map.of("row", row, "timestamp", timestamp))
                        .iterator()))
                .toMap(CqlTransactionKeyValueService::cellFromRow, CqlTransactionKeyValueService::valueFromRow);
    }

    private static final String GET_ROWS_COLUMN_RANGE_QUERY_TEMPLATE =
            """
            SELECT * FROM %s.%s
            WHERE key IN (:rows)
                AND column1 >= :startCol
                AND column1 < :endCol
                AND column2 < :timestamp
            """;

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

    private static final String GET_RANGE_QUERY_TEMPLATE =
            """
            SELECT * FROM %s.%s
            WHERE key >= :startRow
                AND key < :endRow
                AND column1 >= :startCol
                AND column1 < :endCol
                AND column2 < :timestamp
            """;

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {}

    private static final String GET_ASYNC_QUERY_TEMPLATE =
            """
            SELECT * FROM %s.%s
            WHERE key == :row
                AND column1 == :column
                AND column2 < :timestamp
            LIMIT 1
            """;

    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        String keyspace = config.getKeyspaceOrThrow();
        String tableName = tableRef.getTableName();

        return Futures.transform(
                Futures.allAsList(EntryStream.of(timestampByCell)
                        .mapKeyValue((cell, timestamp) -> session.executeAsync(
                                String.format(GET_ASYNC_QUERY_TEMPLATE, keyspace, tableName),
                                Map.of(
                                        "rows", cell.getRowName(),
                                        "column", cell.getColumnName(),
                                        "timestamp", timestamp)))
                resultsList -> StreamEx.of(resultsList)
                        .toMap(resultSet -> cellFromRow(resultSet.one()), resultSet -> valueFromRow(resultSet.one())),
                executor);
    }

    private static Cell cellFromRow(Row row) {
        return Cell.create(
                row.getByteBuffer(ATLAS_ROW).array(),
                row.getByteBuffer(ATLAS_COLUMN).array());
    }

    private static Value valueFromRow(Row row) {
        return Value.create(row.getByteBuffer(ATLAS_VALUE).array(), row.getLong(ATLAS_TIMESTAMP));
    }

    private static RowResult<Value> rowResultFromRow(Row row) {
        return RowResult.of(cellFromRow(row), valueFromRow(row));
    }

    private static final String GET_LATEST_TIMESTAMPS_QUERY_TEMPLATE =
            """
            SELECT * FROM %s.%s
            WHERE key == :row
                AND column1 == :col
                AND column2 < :timestamp
                LIMIT 1
            """;

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        String keyspace = config.getKeyspaceOrThrow();
        String tableName = tableRef.getTableName();
        return EntryStream.of(timestampByCell)
                .mapKeyValue((cell, timestamp) -> session.execute(
                        String.format(GET_LATEST_TIMESTAMPS_QUERY_TEMPLATE, keyspace, tableName),
                        Map.of(
                                ATLAS_ROW, cell.getRowName(),
                                ATLAS_COLUMN, cell.getColumnName(),
                                ATLAS_TIMESTAMP, timestamp)))
                .map(ResultSet::one)
                .filter(result -> !Objects.isNull(result))
                .toMap(CqlTransactionKeyValueService::cellFromRow, result -> result.getLong(ATLAS_TIMESTAMP));
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        valuesByTable.forEach((tableRef, values) -> put(tableRef, values, timestamp));
    }

    private static final String PUT_QUERY_TEMPLATE =
            """
            INSERT INTO %s.%s (row, column1, column2, value)
                VALUES (?, ?, ?, ?)
                USING TIMESTAMP ?
            """;

    private void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp)
            throws KeyAlreadyExistsException {
        String keyspace = config.getKeyspaceOrThrow();
        String tableName = tableRef.getTableName();
        String query = String.format(PUT_QUERY_TEMPLATE, keyspace, tableName);
        values.forEach((cell, value) ->
                session.execute(query, getValuesToInsert(cell.getRowName(), cell.getColumnName(), timestamp, value)));
    }

    private static Object[] getValuesToInsert(byte[] atlasRow, byte[] atlasColumn, long timestamp, byte[] atlasValue) {
        return new Object[] {atlasRow, atlasColumn, timestamp, atlasValue, timestamp};
    }

    private String withTableName(String template, TableReference tableReference) {
        return String.format(template, config.getKeyspaceOrThrow(), tableReference.getTableName());
    }

    @Override
    public boolean isValid(long timestamp) {
        return true;
    }
}
