/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbPerformanceConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.PrefixedTableNames;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.PrimaryKeyConstraintNames;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.RowResults;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.collect.IterableView;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.BasicSQLUtils;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.util.AssertUtils;
import com.palantir.util.Pair;
import com.palantir.util.jmx.OperationTimer;
import com.palantir.util.jmx.OperationTimer.TimingState;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import com.palantir.util.timer.LoggingOperationTimer;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbKvsGetRanges {
    private static final Logger log = LoggerFactory.getLogger(DbKvsGetRanges.class);
    private static final OperationTimer logTimer = LoggingOperationTimer.create(log);
    private static final byte[] SMALLEST_NAME =
            Cells.createSmallestCellForRow(new byte[] {0}).getColumnName();
    private static final byte[] LARGEST_NAME =
            Cells.createLargestCellForRow(new byte[] {0}).getColumnName();

    private final DbKvs kvs;
    private final DBType dbType;
    private final Supplier<SqlConnection> connectionSupplier;
    private PrefixedTableNames prefixedTableNames;

    public DbKvsGetRanges(
            DbKvs kvs,
            DBType dbType,
            Supplier<SqlConnection> connectionSupplier,
            PrefixedTableNames prefixedTableNames) {
        this.kvs = kvs;
        this.dbType = dbType;
        this.connectionSupplier = connectionSupplier;
        this.prefixedTableNames = prefixedTableNames;
    }

    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> results = new HashMap<>();
        for (List<RangeRequest> batch : Iterables.partition(rangeRequests, 500)) {
            results.putAll(getFirstPages(tableRef, batch, timestamp));
        }
        return results;
    }

    private Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstPages(
            TableReference tableRef, List<RangeRequest> requests, long timestamp) {
        List<String> subQueries = new ArrayList<>();
        List<Object> argsList = new ArrayList<>();
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            Pair<String, List<Object>> queryAndArgs = getRangeQueryAndArgs(
                    tableRef,
                    request.getStartInclusive(),
                    request.getEndExclusive(),
                    request.isReverse(),
                    request.getBatchHint() == null ? 1 : request.getBatchHint(),
                    i);
            subQueries.add(queryAndArgs.lhSide);
            argsList.addAll(queryAndArgs.rhSide);
        }
        String query = Joiner.on(") UNION ALL (")
                .appendTo(new StringBuilder("("), subQueries)
                .append(")")
                .toString();
        Object[] args = argsList.toArray();

        TimingState timer = logTimer.begin("Table: " + tableRef.getQualifiedName() + " get_page");
        try {
            return getFirstPagesFromDb(tableRef, requests, timestamp, query, args);
        } finally {
            timer.end();
        }
    }

    private Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstPagesFromDb(
            TableReference tableRef, List<RangeRequest> requests, long timestamp, String query, Object[] args) {
        SortedSetMultimap<Integer, byte[]> rowsForBatches = getRowsForBatches(connectionSupplier, query, args);
        Map<Cell, Value> cells = kvs.getRows(tableRef, rowsForBatches.values(), ColumnSelection.all(), timestamp);
        NavigableMap<byte[], NavigableMap<byte[], Value>> cellsByRow = Cells.breakCellsUpByRow(cells);
        log.debug("getRange actualRowsReturned: {}", cellsByRow.size());
        return breakUpByBatch(requests, rowsForBatches, cellsByRow);
    }

    private static SortedSetMultimap<Integer, byte[]> getRowsForBatches(
            Supplier<SqlConnection> connectionSupplier, String query, Object[] args) {
        SqlConnection connection = connectionSupplier.get();
        try {
            AgnosticResultSet results = connection.selectResultSetUnregisteredQuery(query, args);
            SortedSetMultimap<Integer, byte[]> ret =
                    TreeMultimap.create(Ordering.natural(), UnsignedBytes.lexicographicalComparator());
            for (AgnosticResultRow row : results.rows()) {
                @SuppressWarnings("deprecation")
                byte[] rowName = row.getBytes("row_name");
                int batchNum = row.getInteger("batch_num");
                if (rowName != null) {
                    ret.put(batchNum, rowName);
                }
            }
            return ret;
        } finally {
            closeSql(connection);
        }
    }

    private Pair<String, List<Object>> getRangeQueryAndArgs(
            TableReference tableRef, byte[] startRow, byte[] endRow, boolean reverse, int numRowsToGet, int queryNum) {
        String extraWhere;
        List<Object> args = new ArrayList<>();
        args.add(queryNum);
        if (reverse) {
            extraWhere = " t.row_name <= ? ";
        } else {
            extraWhere = " t.row_name >= ? ";
        }
        if (startRow.length > 0) {
            args.add(startRow);
        } else {
            args.add(reverse ? LARGEST_NAME : SMALLEST_NAME);
        }

        if (endRow.length > 0) {
            if (reverse) {
                extraWhere += " AND t.row_name > ? ";
            } else {
                extraWhere += " AND t.row_name < ? ";
            }
            args.add(endRow);
        }

        String order = reverse ? "DESC" : "ASC";
        try (ConnectionSupplier conns = new ConnectionSupplier(connectionSupplier)) {
            if (numRowsToGet == 1) {
                String minMax = reverse ? "max" : "min";
                // QA-69854 Special case 1 row reads because oracle is terrible at optimizing queries
                String query = dbType == DBType.ORACLE
                        ? getSimpleRowSelectOneQueryOracle(tableRef, minMax, extraWhere, conns)
                        : getSimpleRowSelectOneQueryPostgres(tableRef, extraWhere, order, conns);
                return Pair.create(query, args);
            } else {
                String query = String.format(
                        SIMPLE_ROW_SELECT_TEMPLATE,
                        DbKvs.internalTableName(tableRef),
                        getPrimaryKeyConstraintName(tableRef, conns),
                        getPrefixedTableName(tableRef, conns),
                        extraWhere,
                        order);
                String limitQuery = BasicSQLUtils.limitQuery(query, numRowsToGet, args, dbType);
                return Pair.create(limitQuery, args);
            }
        }
    }

    /**
     * This tablehod expects the input to be sorted by rowname ASC for both rowsForBatches and
     * cellsByRow.
     */
    @SuppressWarnings("BadAssert") // performance sensitive asserts
    private Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> breakUpByBatch(
            List<RangeRequest> requests,
            SortedSetMultimap<Integer, byte[]> rowsForBatches,
            NavigableMap<byte[], NavigableMap<byte[], Value>> cellsByRow) {
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ret = new HashMap<>();
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            if (ret.containsKey(request)) {
                continue;
            }
            SortedSet<byte[]> rowNames = rowsForBatches.get(i);
            NavigableMap<byte[], NavigableMap<byte[], Value>> cellsForBatch = Maps.filterKeys(
                    request.isReverse() ? cellsByRow.descendingMap() : cellsByRow, Predicates.in(rowNames));

            validateRowNames(
                    cellsForBatch.keySet(),
                    request.getStartInclusive(),
                    request.getEndExclusive(),
                    request.isReverse());

            IterableView<RowResult<Value>> rows = RowResults.viewOfMap(cellsForBatch);
            if (!request.getColumnNames().isEmpty()) {
                rows = filterColumnSelection(rows, request);
            }
            if (rowNames.isEmpty()) {
                assert rows.isEmpty();
                ret.put(request, SimpleTokenBackedResultsPage.create(request.getEndExclusive(), rows, false));
            } else {
                byte[] last = rowNames.last();
                if (request.isReverse()) {
                    last = rowNames.first();
                }
                if (RangeRequests.isTerminalRow(request.isReverse(), last)) {
                    ret.put(request, SimpleTokenBackedResultsPage.create(last, rows, false));
                } else {
                    // If rowNames isn't a whole batch we know we don't have any more results.
                    boolean hasMore = request.getBatchHint() == null || request.getBatchHint() <= rowNames.size();
                    byte[] nextStartRow = RangeRequests.getNextStartRow(request.isReverse(), last);
                    ret.put(request, SimpleTokenBackedResultsPage.create(nextStartRow, rows, hasMore));
                }
            }
        }
        return ret;
    }

    private void validateRowNames(Iterable<byte[]> rows, byte[] startInclusive, byte[] endExclusive, boolean reverse) {
        for (byte[] row : rows) {
            if (reverse) {
                AssertUtils.assertAndLog(
                        log,
                        startInclusive.length == 0
                                || UnsignedBytes.lexicographicalComparator().compare(startInclusive, row) >= 0,
                        "row was out of range");
                AssertUtils.assertAndLog(
                        log,
                        endExclusive.length == 0
                                || UnsignedBytes.lexicographicalComparator().compare(row, endExclusive) > 0,
                        "row was out of range");
            } else {
                AssertUtils.assertAndLog(
                        log,
                        startInclusive.length == 0
                                || UnsignedBytes.lexicographicalComparator().compare(startInclusive, row) <= 0,
                        "row was out of range");
                AssertUtils.assertAndLog(
                        log,
                        endExclusive.length == 0
                                || UnsignedBytes.lexicographicalComparator().compare(row, endExclusive) < 0,
                        "row was out of range");
            }
        }
    }

    private IterableView<RowResult<Value>> filterColumnSelection(
            IterableView<RowResult<Value>> rows, RangeRequest request) {
        return rows.transform(RowResults.createFilterColumns(request::containsColumn))
                .filter(Predicates.not(RowResults.createIsEmptyPredicate()));
    }

    private static void closeSql(SqlConnection conn) {
        Connection underlyingConnection = conn.getUnderlyingConnection();
        if (underlyingConnection != null) {
            try {
                underlyingConnection.close();
            } catch (Exception e) {
                log.debug("Error occurred trying to close the sql connection", e);
            }
        }
    }

    private String getSimpleRowSelectOneQueryPostgres(
            TableReference tableRef, String extraWhere, String order, ConnectionSupplier conns) {
        return String.format(
                SIMPLE_ROW_SELECT_ONE_POSTGRES_TEMPLATE,
                DbKvs.internalTableName(tableRef),
                getPrefixedTableName(tableRef, conns),
                extraWhere,
                order);
    }

    private String getSimpleRowSelectOneQueryOracle(
            TableReference tableRef, String minMax, String extraWhere, ConnectionSupplier conns) {
        return String.format(
                SIMPLE_ROW_SELECT_ONE_ORACLE_TEMPLATE,
                DbKvs.internalTableName(tableRef),
                getPrimaryKeyConstraintName(tableRef, conns),
                minMax,
                getPrefixedTableName(tableRef, conns),
                extraWhere);
    }

    private String getPrimaryKeyConstraintName(TableReference tableRef, ConnectionSupplier conns) {
        return PrimaryKeyConstraintNames.get(getPrefixedTableName(tableRef, conns));
    }

    private String getPrefixedTableName(TableReference tableRef, ConnectionSupplier conns) {
        return prefixedTableNames.get(tableRef, conns);
    }

    public static int getMaxCellsPerPage(
            TableReference tableRef,
            RangeRequest rangeRequest,
            int maxRowsPerPage,
            SqlConnectionSupplier connectionPool,
            TableMetadataCache tableMetadataCache) {

        int approxCellsPerRow;
        if (!rangeRequest.getColumnNames().isEmpty()) {
            approxCellsPerRow = rangeRequest.getColumnNames().size();
        } else {
            TableMetadata metadata = getTableMetadataUsingNewConnection(tableRef, connectionPool, tableMetadataCache);
            if (metadata.getColumns().hasDynamicColumns()) {
                approxCellsPerRow = 100;
            } else {
                approxCellsPerRow =
                        Math.max(1, metadata.getColumns().getNamedColumns().size());
            }
        }

        return Math.min(AtlasDbPerformanceConstants.MAX_BATCH_SIZE, maxRowsPerPage * approxCellsPerRow) + 1;
    }

    private static TableMetadata getTableMetadataUsingNewConnection(
            TableReference tableRef, SqlConnectionSupplier connectionPool, TableMetadataCache tableMetadataCache) {
        try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool)) {
            return tableMetadataCache.getTableMetadata(tableRef, conns);
        }
    }

    private static final String SIMPLE_ROW_SELECT_TEMPLATE = " /* SIMPLE_ROW_SELECT_TEMPLATE (%s) */ "
            + " SELECT /*+ INDEX(t %s) */ "
            + "   DISTINCT row_name, ? as batch_num "
            + " FROM %s t "
            + " WHERE %s "
            + " ORDER BY row_name %s ";

    private static final String SIMPLE_ROW_SELECT_ONE_POSTGRES_TEMPLATE =
            " /* SIMPLE_ROW_SELECT_ONE_TEMPLATE_PSQL (%s) */ "
                    + " SELECT"
                    + "   DISTINCT row_name, ? as batch_num "
                    + " FROM %s t "
                    + " WHERE %s "
                    + " ORDER BY row_name %s LIMIT 1";

    private static final String SIMPLE_ROW_SELECT_ONE_ORACLE_TEMPLATE =
            " /* SIMPLE_ROW_SELECT_ONE_TEMPLATE_ORA (%s) */ "
                    + " SELECT /*+ INDEX(t %s) */ "
                    + "   %s(row_name) as row_name, ? as batch_num "
                    + " FROM %s t "
                    + " WHERE %s";
}
