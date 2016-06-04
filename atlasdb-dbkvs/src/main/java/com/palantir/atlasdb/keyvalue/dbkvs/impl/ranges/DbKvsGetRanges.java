/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.RowResults;
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

public class DbKvsGetRanges {
    private static final Logger log = LoggerFactory.getLogger(DbKvsGetRanges.class);
    private static final OperationTimer logTimer = LoggingOperationTimer.create(log);
    private static final byte[] SMALLEST_NAME = Cells.createSmallestCellForRow(new byte[] {0}).getColumnName();
    private static final byte[] LARGEST_NAME = Cells.createLargestCellForRow(new byte[] {0}).getColumnName();
    private final DbKvs kvs;
    private final DBType dbType;
    private final Supplier<SqlConnection> connectionSupplier;

    public DbKvsGetRanges(DbKvs kvs,
                          DBType dbType,
                          Supplier<SqlConnection> connectionSupplier) {
        this.kvs = kvs;
        this.dbType = dbType;
        this.connectionSupplier = connectionSupplier;
    }

    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(final TableReference tableRef,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           final long timestamp) {
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> results = Maps.newHashMap();
        for (List<RangeRequest> batch : Iterables.partition(rangeRequests, 500)) {
            results.putAll(getFirstPages(tableRef, batch, timestamp));
        }
        return results;
    }

    private Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstPages(TableReference tableRef,
                                                                                                   List<RangeRequest> requests,
                                                                                                   long timestamp) {
        List<String> subQueries = Lists.newArrayList();
        List<Object> argsList = Lists.newArrayList();
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            Pair<String, List<Object>> queryAndArgs = getRangeQueryAndArgs(
                    tableRef.getQualifiedName(),
                    request.getStartInclusive(),
                    request.getEndExclusive(),
                    request.isReverse(),
                    request.getBatchHint() == null ? 1 : request.getBatchHint(),
                    i);
            subQueries.add(queryAndArgs.lhSide);
            argsList.addAll(queryAndArgs.rhSide);
        }
        String query = Joiner.on(") UNION ALL (").appendTo(new StringBuilder("("), subQueries).append(")").toString();
        Object[] args = argsList.toArray();

        TimingState timer = logTimer.begin("Table: " + tableRef.getQualifiedName() + " get_page");
        final SqlConnection conn = connectionSupplier.get();
        try {
            return getFirstPagesFromDb(tableRef, requests, timestamp, conn, query, args);
        } finally {
            closeSql(conn);
            timer.end();
        }
    }

    private Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstPagesFromDb(TableReference tableRef,
                                                                                                         List<RangeRequest> requests,
                                                                                                         long timestamp,
                                                                                                         final SqlConnection conn,
                                                                                                         String query,
                                                                                                         Object[] args) {
        TreeMultimap<Integer, byte[]> rowsForBatches = getRowsForBatches(conn, query, args);
        Map<Cell, Value> cells = kvs.getRows(tableRef, rowsForBatches.values(),
                ColumnSelection.all(), timestamp);
        NavigableMap<byte[], SortedMap<byte[], Value>> cellsByRow = Cells.breakCellsUpByRow(cells);
        log.info("getRange actualRowsReturned: {}", cellsByRow.size());
        return breakUpByBatch(requests, rowsForBatches, cellsByRow);
    }

    private static TreeMultimap<Integer, byte[]> getRowsForBatches(SqlConnection c, String query, Object[] args) {
        AgnosticResultSet results = c.selectResultSetUnregisteredQuery(query, args);
        TreeMultimap<Integer, byte[]> ret = TreeMultimap.create(
                Ordering.natural(),
                UnsignedBytes.lexicographicalComparator());
        for (AgnosticResultRow row : results.rows()) {
            @SuppressWarnings("deprecation")
            byte[] rowName = row.getBytes("row_name");
            int batchNum = row.getInteger("batch_num");
            if (rowName != null) {
                ret.put(batchNum, rowName);
            }
        }
        return ret;
    }

    private Pair<String, List<Object>> getRangeQueryAndArgs(String tableName,
                                                            byte[] startRow,
                                                            byte[] endRow,
                                                            boolean reverse,
                                                            int numRowsToGet,
                                                            int queryNum) {
        if (startRow.length == 0) {
            if (reverse) {
                startRow = LARGEST_NAME;
            } else {
                startRow = SMALLEST_NAME;
            }
        }

        String extraWhere;
        List<Object> args = Lists.newArrayList();
        args.add(queryNum);
        if (reverse) {
            extraWhere = " t.row_name <= ? ";
        } else {
            extraWhere = " t.row_name >= ? ";
        }
        args.add(startRow);

        if (endRow.length > 0) {
            if (reverse) {
                extraWhere += " AND t.row_name > ? ";
            } else {
                extraWhere += " AND t.row_name < ? ";
            }
            args.add(endRow);
        }

        String order = reverse ? "DESC" : "ASC";
        if (numRowsToGet == 1) {
            String minMax = reverse ? "max" : "min";
            // QA-69854 Special case 1 row reads because oracle is terrible at optimizing queries
            String query = dbType == DBType.ORACLE
                    ? getSimpleRowSelectOneQueryOracle(tableName, minMax, extraWhere, order)
                    : getSimpleRowSelectOneQueryPostgres(tableName, minMax, extraWhere, order);
            return Pair.create(query, args);
        } else {
            String query = String.format(
                    SIMPLE_ROW_SELECT_TEMPLATE,
                    tableName,
                    prefixTableName(tableName),
                    prefixTableName(tableName),
                    extraWhere,
                    order);
            String limitQuery = BasicSQLUtils.limitQuery(query, numRowsToGet, args, dbType);
            return Pair.create(limitQuery, args);
        }
    }
    /**
     * This tablehod expects the input to be sorted by rowname ASC for both rowsForBatches and
     * cellsByRow.
     */
    private Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> breakUpByBatch(List<RangeRequest> requests,
                                                                                                    TreeMultimap<Integer, byte[]> rowsForBatches,
                                                                                                    NavigableMap<byte[], SortedMap<byte[], Value>>   cellsByRow) {
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> ret = Maps.newHashMap();
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            if (ret.containsKey(request)) {
                continue;
            }
            SortedSet<byte[]> rowNames = rowsForBatches.get(i);
            SortedMap<byte[], SortedMap<byte[], Value>> cellsForBatch = Maps.filterKeys(
                    request.isReverse() ? cellsByRow.descendingMap() : cellsByRow,
                    Predicates.in(rowNames));
            validateRowNames(cellsForBatch.keySet(), request.getStartInclusive(), request.getEndExclusive(), request.isReverse());
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
                AssertUtils.assertAndLog(startInclusive.length == 0
                        || UnsignedBytes.lexicographicalComparator().compare(startInclusive, row) >= 0, "row was out of range");
                AssertUtils.assertAndLog(endExclusive.length == 0
                        || UnsignedBytes.lexicographicalComparator().compare(row, endExclusive) > 0, "row was out of range");
            } else {
                AssertUtils.assertAndLog(startInclusive.length == 0
                        || UnsignedBytes.lexicographicalComparator().compare(startInclusive, row) <= 0, "row was out of range");
                AssertUtils.assertAndLog(endExclusive.length == 0
                        || UnsignedBytes.lexicographicalComparator().compare(row, endExclusive) < 0, "row was out of range");
            }
        }
    }

    private IterableView<RowResult<Value>> filterColumnSelection(IterableView<RowResult<Value>> rows,
                                                                 final RangeRequest request) {
        return rows.transform(RowResults.<Value>createFilterColumns(new Predicate<byte[]>() {
            @Override
            public boolean apply(byte[] col) {
                return request.containsColumn(col);
            }
        })).filter(Predicates.not(RowResults.<Value>createIsEmptyPredicate()));
    }

    private static void closeSql(SqlConnection conn) {
        Connection underlyingConnection = conn.getUnderlyingConnection();
        if (underlyingConnection != null) {
            try {
                underlyingConnection.close();
            } catch (Exception e) {
                log.debug(e.getMessage(), e);
            }
        }
    }

    private String getSimpleRowSelectOneQueryPostgres(String tableName, String minMax, String extraWhere, String order) {
        return String.format(
                SIMPLE_ROW_SELECT_ONE_POSTGRES_TEMPLATE,
                tableName,
                prefixTableName(tableName),
                prefixTableName(tableName),
                extraWhere,
                order);
    }

    private String getSimpleRowSelectOneQueryOracle(String tableName,
                                                    String minMax,
                                                    String extraWhere,
                                                    String order) {
        return String.format(
                SIMPLE_ROW_SELECT_ONE_ORACLE_TEMPLATE,
                tableName,
                prefixTableName(tableName),
                minMax,
                prefixTableName(tableName),
                extraWhere);
    }

    private String prefixTableName(String tableName) {
        return kvs.getConfig().shared().tablePrefix() + tableName;
    }

    private static final String SIMPLE_ROW_SELECT_TEMPLATE =
            " /* SQL_MET_SIMPLE_ROW_SELECT_TEMPLATE (%s) */ " +
                    " SELECT /*+ INDEX(t pk_%s) */ " +
                    "   DISTINCT row_name, ? as batch_num " +
                    " FROM %s t " +
                    " WHERE %s " +
                    " ORDER BY row_name %s ";

    private static final String SIMPLE_ROW_SELECT_ONE_POSTGRES_TEMPLATE =
            " /* SQL_MET_SIMPLE_ROW_SELECT_ONE_TEMPLATE_PSQL (%s) */ " +
                    " SELECT /*+ INDEX(t pk_%s) */ " +
                    "   DISTINCT row_name, ? as batch_num " +
                    " FROM %s t " +
                    " WHERE %s " +
                    " ORDER BY row_name %s LIMIT 1";

    private static final String SIMPLE_ROW_SELECT_ONE_ORACLE_TEMPLATE =
            " /* SQL_MET_SIMPLE_ROW_SELECT_ONE_TEMPLATE_ORA (%s) */ " +
                    " SELECT /*+ INDEX(t pk_%s) */ " +
                    "   %s(row_name) as row_name, ? as batch_num " +
                    " FROM %s t " +
                    " WHERE %s";
}
