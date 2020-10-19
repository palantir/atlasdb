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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowValueLoader;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyle;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCache;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.DbKvsGetRange;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.DbKvsGetRanges;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangeHelpers;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges.RangePredicateHelper;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * 1) When in comes to implementing paging, Oracle is the exact opposite of Postgres:
 *
 *      - It can page at atlas row boundaries very efficiently using the DENSE_RANK()
 *        window function. For a query like
 *              SELECT * FROM (
 *                  SELECT ..., DENSE_RANK() OVER (ORDER BY row_name) AS rn FROM my_table
 *                  ...
 *                  ORDER BY row_name, col_name
 *              ) WHERE rn <= 1000
 *        the Oracle optimizer is smart enough to realize that 'rn' can only increase,
 *        and therefore, 'WHERE rn <= 1000' can be used as a result limiting clause
 *        efficiently. Even though Postgres supports the same syntax, it doesn't have this
 *        optimization, so it fetches the whole result set for the inner query (which
 *        could the whole table, depending on the range request) and treats 'WHERE rn <= 1000'
 *        as an ordinary filter.
 *
 *      - Oracle doesn't support the row value syntax (i.e. 'WHERE (row_name, col_name) >= (?, ?)')
 *        so there is no efficient way to start a range scan from a given row and column pair.
 *        One can write something like
 *             WHERE (row_name >= :1) AND (row_name > :1 OR col_name >= :2)
 *        or similar, but this results in an inefficient query plan: only 'row_name >= :1'
 *        is used as an access predicate, while the rest of the 'WHERE' clause makes up
 *        filter predicates. In other words, Oracle will scan from the beginning of
 *        the Atlas row and filter out the leading columns. This can be a performance
 *        problem with wide rows, or rows with lots of accumulated historical values.
 *
 *        See also http://use-the-index-luke.com/sql/partial-results/fetch-next-page
 *
 *    Hence, on Oracle we page at Atlas row boundaries, so that we can simply use 'row_name >= ?'.
 *
 *
 * 2) There are different approaches to loading the values that correspond to the maximum
 *    timestamp for each cell. First, there is the traditional approach with a 'GROUP BY'
 *    wrapped in an extra self-join to load the values:
 *
 *        SELECT wrap.* FROM my_table wrap JOIN (
 *            SELECT row_name, col_name, MAX(ts) AS ts FROM my_table
 *            WHERE ...
 *            GROUP BY row_name, col_name
 *        ) sub ON wrap.row_name = sub.row_name AND wrap.col_name = sub.row_name AND wrap.ts = sub.ts
 *
 *    Second, there is the obscure 'KEEP' syntax to do everything with a single 'GROUP BY':
 *
 *       SELECT row_name, col_name,
 *            MAX(ts) AS ts,
 *            MAX(val) KEEP (DENSE_RANK LAST ORDER BY ts)
 *       FROM my_table
 *       WHERE ...
 *       GROUP BY row_name, col_name
 *
 *    The latter query seems to slightly outperform the former one on 'clean' tables
 *    (i.e., tables with few or none unswept historical values) but is significantly slower
 *    on 'dirty' tables. For example, I observed a ~2x difference in a benchmark
 *    that performs range scans of 1000 elements on a table where each cell has 101
 *    different timestamps.
 *
 *    So, we stick to the 'traditional' query here. The other option would be to use window
 *    functions but it is likely to exhibit performance similar to the 'KEEP'-style query.
 *
 *
 * 3) The execution plan we are shooting for:
 *
 *      SELECT STATEMENT
 *      `- NESTED LOOPS
 *         |- VIEW [ Filter pred: sub.rn <= ... ]
 *         |  `- WINDOW (NOSORT STOPKEY) [Filter pred: DENSE_RANK() OVER (...) <= ... ]
 *         |     `- SORT (GROUP BY NOSORT)
 *         |        `- INDEX (RANGE SCAN)
 *         `- INDEX (UNIQUE SCAN)
 *
 *    Note 'STOPKEY' and 'NOSORT': this is what makes the query plan fast.
 */
public class OracleGetRange implements DbKvsGetRange {
    private final SqlConnectionSupplier connectionPool;
    private final OverflowValueLoader overflowValueLoader;
    private final OracleTableNameGetter tableNameGetter;
    private final TableValueStyleCache valueStyleCache;
    private final TableMetadataCache tableMetadataCache;
    private final OracleDdlConfig config;

    public OracleGetRange(SqlConnectionSupplier connectionPool,
                          OverflowValueLoader overflowValueLoader,
                          OracleTableNameGetter tableNameGetter,
                          TableValueStyleCache valueStyleCache,
                          TableMetadataCache tableMetadataCache,
                          OracleDdlConfig config) {
        this.connectionPool = connectionPool;
        this.overflowValueLoader = overflowValueLoader;
        this.tableNameGetter = tableNameGetter;
        this.valueStyleCache = valueStyleCache;
        this.tableMetadataCache = tableMetadataCache;
        this.config = config;
    }

    @Override
    public Iterator<RowResult<Value>> getRange(TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        boolean haveOverflow = checkIfTableHasOverflowUsingNewConnection(tableRef);
        int maxRowsPerPage = RangeHelpers.getMaxRowsPerPage(rangeRequest);
        int maxCellsPerPage = DbKvsGetRanges.getMaxCellsPerPage(
                tableRef, rangeRequest, maxRowsPerPage, connectionPool, tableMetadataCache);

        return Iterators.concat(new PageIterator(
                rangeRequest.getStartInclusive(),
                rangeRequest.getEndExclusive(),
                rangeRequest.getColumnNames(),
                rangeRequest.isReverse(),
                tableRef,
                haveOverflow,
                maxRowsPerPage,
                maxCellsPerPage,
                timestamp));
    }

    private boolean checkIfTableHasOverflowUsingNewConnection(TableReference tableRef) {
        try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool)) {
            return valueStyleCache.getTableType(conns, tableRef, config.metadataTable()) == TableValueStyle.OVERFLOW;
        }
    }

    private class PageIterator extends AbstractIterator<Iterator<RowResult<Value>>> {
        private byte[] startInclusive;
        private boolean endOfResults = false;
        private final byte[] endExclusive;
        private final Set<byte[]> columnSelection;
        private final boolean reverse;
        private final TableReference tableRef;
        private final boolean haveOverflowValues;
        private final int maxRowsPerPage;
        private final int maxCellsPerPage;
        private final long timestamp;

        PageIterator(
                byte[] startInclusive, byte[] endExclusive, Set<byte[]> columnSelection,
                boolean reverse, TableReference tableRef, boolean haveOverflowValues,
                int maxRowsPerPage, int maxCellsPerPage, long timestamp) {
            this.startInclusive = startInclusive;
            this.endExclusive = endExclusive;
            this.columnSelection = columnSelection;
            this.reverse = reverse;
            this.tableRef = tableRef;
            this.haveOverflowValues = haveOverflowValues;
            this.maxRowsPerPage = maxRowsPerPage;
            this.maxCellsPerPage = maxCellsPerPage;
            this.timestamp = timestamp;
        }

        @Override
        protected Iterator<RowResult<Value>> computeNext() {
            if (endOfResults) {
                return endOfData();
            } else {
                try (ConnectionSupplier conns = new ConnectionSupplier(connectionPool)) {
                    List<RawSqlRow> sqlRows = loadSqlRows(conns);
                    Map<Long, byte[]> overflowValues = overflowValueLoader.loadOverflowValues(
                            conns,
                            tableRef,
                            sqlRows.stream()
                                    .map(r -> r.overflowId)
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList()));
                    List<RowResult<Value>> rowResults = createRowResults(sqlRows, overflowValues, maxRowsPerPage);
                    if (rowResults.isEmpty()) {
                        endOfResults = true;
                    } else {
                        byte[] lastRowName = rowResults.get(rowResults.size() - 1).getRowName();
                        startInclusive = RangeRequests.getNextStartRowUnlessTerminal(reverse, lastRowName);
                        endOfResults = (rowResults.size() < maxRowsPerPage) || startInclusive == null;
                    }
                    return rowResults.iterator();
                }
            }
        }

        @SuppressWarnings("deprecation")
        private List<RawSqlRow> loadSqlRows(ConnectionSupplier conns) {
            List<RawSqlRow> sqlRows = new ArrayList<>();
            try (ClosableIterator<AgnosticLightResultRow> rangeResults = selectNextPage(conns)) {
                while (rangeResults.hasNext()) {
                    AgnosticLightResultRow row = rangeResults.next();
                    Cell cell = Cell.create(row.getBytes("row_name"), row.getBytes("col_name"));
                    long ts = row.getLong("ts");
                    byte[] val = row.getBytes("val");
                    Long overflowId = haveOverflowValues ? row.getLongObject("overflow") : null;
                    sqlRows.add(new RawSqlRow(cell, ts, val, overflowId));
                }
            }
            return sqlRows;
        }

        private ClosableIterator<AgnosticLightResultRow> selectNextPage(ConnectionSupplier conns) {
            FullQuery query = getRangeQuery(conns);
            AgnosticLightResultSet resultSet = conns.get().selectLightResultSetUnregisteredQueryWithFetchSize(
                    query.getQuery(), maxCellsPerPage, query.getArgs());
            return ClosableIterators.wrap(resultSet.iterator(), resultSet);
        }

        private FullQuery getRangeQuery(ConnectionSupplier conns) {
            String direction = reverse ? "DESC" : "ASC";
            String shortTableName = getInternalShortTableName(conns);
            String pkIndex = PrimaryKeyConstraintNames.get(shortTableName);
            FullQuery.Builder queryBuilder = FullQuery.builder()
                    .append("/* GET_RANGE(" + shortTableName + ") */")
                    .append("  SELECT /*+ USE_NL(sub v)")
                    .append("             LEADING(sub) ")
                    .append("             INDEX(v ").append(pkIndex).append(")")
                    .append("             NO_INDEX_SS(v ").append(pkIndex).append(")")
                    .append("             NO_INDEX_FFS(v ").append(pkIndex).append(") */")
                    .append("  sub.row_name, sub.col_name, sub.ts")
                    .append(OracleQueryHelpers.getValueSubselect(haveOverflowValues, "v", true))
                    .append("  FROM (")
                    .append("    SELECT /*+ INDEX_").append(direction).append("(m ").append(pkIndex).append(")")
                    .append("               NO_INDEX_SS(m ").append(pkIndex).append(")")
                    .append("               NO_INDEX_FFS(m ").append(pkIndex).append(") */")
                    .append("      m.row_name, m.col_name, MAX(m.ts) AS ts,")
                    .append("        DENSE_RANK() OVER (ORDER BY m.row_name ").append(direction).append(") AS rn")
                    .append("    FROM ").append(shortTableName).append(" m ")
                    .append("    WHERE m.ts < ? ", timestamp);
            RangePredicateHelper.create(reverse, DBType.ORACLE, queryBuilder)
                    .startRowInclusive(startInclusive)
                    .endRowExclusive(endExclusive)
                    .columnSelection(columnSelection);
            return queryBuilder
                    .append("    GROUP BY m.row_name, m.col_name")
                    .append("    ORDER BY m.row_name ").append(direction).append(", m.col_name ").append(direction)
                    .append("  ) sub")
                    .append("  JOIN ").append(shortTableName).append(" v ON ")
                    .append("    sub.row_name = v.row_name and sub.col_name = v.col_name and sub.ts = v.ts")
                    .append("  WHERE sub.rn <= ").append(maxRowsPerPage)
                    .append("  ORDER BY sub.row_name ").append(direction).append(", sub.col_name ").append(direction)
                    .build();
        }

        private String getInternalShortTableName(ConnectionSupplier conns) {
            try {
                return tableNameGetter.getInternalShortTableName(conns, tableRef);
            } catch (TableMappingNotFoundException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static List<RowResult<Value>> createRowResults(List<RawSqlRow> sqlRows,
            Map<Long, byte[]> overflowValues,
            int expectedNumRows) {
        List<RowResult<Value>> rowResults = new ArrayList<>(expectedNumRows);
        ImmutableSortedMap.Builder<byte[], Value> currentRowCells = null;
        byte[] currentRowName = null;
        for (RawSqlRow sqlRow : sqlRows) {
            if (currentRowName == null || !Arrays.equals(sqlRow.cell.getRowName(), currentRowName)) {
                if (currentRowName != null) {
                    rowResults.add(RowResult.create(currentRowName, currentRowCells.build()));
                }
                currentRowCells = ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
                currentRowName = sqlRow.cell.getRowName();
            }
            byte[] value = getValue(sqlRow, overflowValues);
            currentRowCells.put(sqlRow.cell.getColumnName(), Value.create(value, sqlRow.ts));
        }
        if (currentRowName != null) {
            rowResults.add(RowResult.create(currentRowName, currentRowCells.build()));
        }
        return rowResults;
    }

    private static byte[] getValue(RawSqlRow sqlRow, Map<Long, byte[]> overflowValues) {
        if (sqlRow.overflowId != null) {
            return Preconditions.checkNotNull(overflowValues.get(sqlRow.overflowId),
                    "Failed to load overflow data: cell=%s, overflowId=%s", sqlRow.cell, sqlRow.overflowId);
        } else {
            return sqlRow.val;
        }
    }

    private static class RawSqlRow {
        public final Cell cell;
        public final long ts;
        public final byte[] val;
        public final Long overflowId;

        RawSqlRow(Cell cell, long ts, byte[] val, Long overflowId) {
            this.cell = cell;
            this.ts = ts;
            this.val = val;
            this.overflowId = overflowId;
        }
    }
}
