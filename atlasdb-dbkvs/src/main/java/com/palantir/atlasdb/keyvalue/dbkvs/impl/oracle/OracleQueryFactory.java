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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.db.oracle.JdbcHandler.ArrayHandler;

public abstract class OracleQueryFactory implements DbQueryFactory {
    protected final String tableName;
    protected final OracleDdlConfig config;

    public OracleQueryFactory(String tableName, OracleDdlConfig config) {
        this.tableName = tableName;
        this.config = config;
    }

    @Override
    public FullQuery getLatestRowQuery(byte[] row,
                                       long ts,
                                       ColumnSelection columns,
                                       boolean includeValue) {
        String query = " /* GET_LATEST_ONE_ROW_INNER (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + prefixedTableName() + " m "
                + " WHERE m.row_name = ? "
                + "   AND m.ts < ? "
                + (columns.allColumnsSelected() ? "" :
                    " AND EXISTS ("
                            + "SELECT "
                            + "  /*+ NL_SJ */"
                            + "  1"
                            + " FROM TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE))"
                            + " WHERE row_name = m.col_name)")
                + " GROUP BY m.row_name, m.col_name";
        query = wrapQueryWithIncludeValue("GET_LATEST_ONE_ROW", query, includeValue);
        FullQuery fullQuery = new FullQuery(query).withArgs(row, ts);
        return columns.allColumnsSelected()
                ? fullQuery
                : fullQuery.withArg(rowsToOracleArray(columns.getSelectedColumns()));
    }

    @Override
    public FullQuery getLatestRowsQuery(Iterable<byte[]> rows,
                                        long ts,
                                        ColumnSelection columns,
                                        boolean includeValue) {
        String query = " /* GET_LATEST_ROWS_SINGLE_BOUND_INNER (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + prefixedTableName() + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.ts < ? "
                + (columns.allColumnsSelected() ? "" :
                    " AND EXISTS ("
                            + "SELECT"
                            + "  /*+ NL_SJ */"
                            + "  1"
                            + " FROM TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE))"
                            + " WHERE row_name = m.col_name) ")
                + " GROUP BY m.row_name, m.col_name";
        query = wrapQueryWithIncludeValue("GET_LATEST_ROWS_SINGLE_BOUND", query, includeValue);
        FullQuery fullQuery = new FullQuery(query).withArgs(rowsToOracleArray(rows), ts);
        return columns.allColumnsSelected()
                ? fullQuery
                : fullQuery.withArg(rowsToOracleArray(columns.getSelectedColumns()));
    }

    @Override
    public FullQuery getLatestRowsQuery(Collection<Map.Entry<byte[], Long>> rows,
                                        ColumnSelection columns,
                                        boolean includeValue) {
        String query = " /* GET_LATEST_ROWS_MANY_BOUNDS_INNER (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + prefixedTableName() + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.ts < t.max_ts "
                + (columns.allColumnsSelected() ? "" :
                    " AND EXISTS ("
                            + "SELECT"
                            + "  /*+ NL_SJ */"
                            + "  1"
                            + " FROM TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE))"
                            + " WHERE row_name = m.col_name) ")
                + " GROUP BY m.row_name, m.col_name";
        query = wrapQueryWithIncludeValue("GET_LATEST_ROWS_MANY_BOUNDS", query, includeValue);
        FullQuery fullQuery = new FullQuery(query).withArg(rowsAndTimestampsToOracleArray(rows));
        return columns.allColumnsSelected()
                ? fullQuery
                : fullQuery.withArg(rowsToOracleArray(columns.getSelectedColumns()));
    }

    @Override
    public FullQuery getAllRowQuery(byte[] row,
                                    long ts,
                                    ColumnSelection columns,
                                    boolean includeValue) {
        String query = " /* GET_ALL_ONE_ROW (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + prefixedTableName() + " m "
                + " WHERE m.row_name = ? "
                + "   AND m.ts < ? "
                + (columns.allColumnsSelected() ? "" :
                    " AND EXISTS ("
                            + "SELECT"
                            + "  /*+ NL_SJ */"
                            + "  1"
                            + " FROM TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE))"
                            + " WHERE row_name = m.col_name) ");
        FullQuery fullQuery = new FullQuery(query).withArgs(row, ts);
        return columns.allColumnsSelected()
                ? fullQuery
                : fullQuery.withArg(rowsToOracleArray(columns.getSelectedColumns()));
    }

    @Override
    public FullQuery getAllRowsQuery(Iterable<byte[]> rows,
                                     long ts,
                                     ColumnSelection columns,
                                     boolean includeValue) {
        String query = " /* GET_ALL_ROWS_SINGLE_BOUND (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + prefixedTableName() + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.ts < ? "
                + (columns.allColumnsSelected() ? "" :
                    " AND EXISTS ("
                            + "SELECT"
                            + "  /*+ NL_SJ */"
                            + "  1"
                            + " FROM TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE))"
                            + " WHERE row_name = m.col_name) ");
        FullQuery fullQuery = new FullQuery(query).withArgs(rowsToOracleArray(rows), ts);
        return columns.allColumnsSelected()
                ? fullQuery
                : fullQuery.withArg(rowsToOracleArray(columns.getSelectedColumns()));
    }

    @Override
    public FullQuery getAllRowsQuery(Collection<Map.Entry<byte[], Long>> rows,
                                     ColumnSelection columns,
                                     boolean includeValue) {
        String query = " /* GET_ALL_ROWS_MANY_BOUNDS (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + prefixedTableName() + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.ts < t.max_ts "
                + (columns.allColumnsSelected() ? "" :
                    " AND EXISTS ("
                            + "SELECT"
                            + "  /*+ NL_SJ */"
                            + "  1"
                            + " FROM TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE))"
                            + " WHERE row_name = m.col_name) ");
        FullQuery fullQuery = new FullQuery(query).withArg(rowsAndTimestampsToOracleArray(rows));
        return columns.allColumnsSelected()
                ? fullQuery
                : fullQuery.withArg(rowsToOracleArray(columns.getSelectedColumns()));
    }

    @Override
    public FullQuery getLatestCellQuery(Cell cell, long ts, boolean includeValue) {
        String query = " /* GET_LATEST_ONE_CELLS_INNER (" + tableName + ") */ "
                + " SELECT "
                + "   /*+ INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + prefixedTableName() + " m "
                + " WHERE m.row_name = ? "
                + "   AND m.col_name = ? "
                + "   AND m.ts < ? "
                + " GROUP BY m.row_name, m.col_name";
        query = wrapQueryWithIncludeValue("GET_LATEST_ONE_CELL", query, includeValue);
        return new FullQuery(query).withArgs(cell.getRowName(), cell.getColumnName(), ts);
    }

    @Override
    public FullQuery getLatestCellsQuery(Iterable<Cell> cells, long ts, boolean includeValue) {
        String query = " /* GET_LATEST_CELLS_SINGLE_BOUND_INNER (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + prefixedTableName() + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.col_name = t.col_name "
                + "   AND m.ts < ? "
                + " GROUP BY m.row_name, m.col_name";
        query = wrapQueryWithIncludeValue("GET_LATEST_CELLS_SINGLE_BOUND", query, includeValue);
        return new FullQuery(query).withArgs(cellsToOracleArray(cells), ts);
    }

    @Override
    public FullQuery getLatestCellsQuery(Collection<Map.Entry<Cell, Long>> cells, boolean includeValue) {
        String query = " /* GET_LATEST_CELLS_MANY_BOUNDS_INNER (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + prefixedTableName() + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.col_name = t.col_name "
                + "   AND m.ts < t.max_ts "
                + " GROUP BY m.row_name, m.col_name";
        query = wrapQueryWithIncludeValue("GET_LATEST_CELLS_MANY_BOUNDS", query, includeValue);
        return new FullQuery(query).withArg(cellsAndTimestampsToOracleArray(cells));
    }

    @Override
    public FullQuery getAllCellQuery(Cell cell, long ts, boolean includeValue) {
        String query = " /* GET_ALL_ONE_CELL (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + prefixedTableName() + " m "
                + " WHERE m.row_name = ? "
                + "   AND m.col_name = ? "
                + "   AND m.ts < ? ";
        return new FullQuery(query).withArgs(cell.getRowName(), cell.getColumnName(), ts);
    }

    @Override
    public FullQuery getAllCellsQuery(Iterable<Cell> cells, long ts, boolean includeValue) {
        String query = " /* GET_ALL_CELLS_SINGLE_BOUND (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + prefixedTableName() + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.col_name = t.col_name "
                + "   AND m.ts < ? ";
        return new FullQuery(query).withArgs(cellsToOracleArray(cells), ts);
    }

    @Override
    public FullQuery getAllCellsQuery(Collection<Map.Entry<Cell, Long>> cells, boolean includeValue) {
        String query = " /* GET_ALL_CELLS_MANY_BOUNDS (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m pk_" + prefixedTableName() + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + prefixedTableName() + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.col_name = t.col_name "
                + "   AND m.ts < t.max_ts ";
        return new FullQuery(query).withArg(cellsAndTimestampsToOracleArray(cells));
    }

    @Override
    public FullQuery getRangeQuery(RangeRequest range, long ts, int maxRows) {
        List<String> bounds = Lists.newArrayListWithCapacity(2);
        List<Object> args = Lists.newArrayListWithCapacity(2);
        byte[] start = range.getStartInclusive();
        byte[] end = range.getEndExclusive();
        if (start.length > 0) {
            bounds.add(range.isReverse() ? "m.row_name <= ?" : "m.row_name >= ?");
            args.add(start);
        }
        if (end.length > 0) {
            bounds.add(range.isReverse() ? "m.row_name > ?" : "m.row_name < ?");
            args.add(end);
        }
        if (maxRows == 1) {
            String query = " /* GET_RANGE_ONE_ROW (" + tableName + ") */ "
                    + " SELECT /*+ INDEX(m pk_" + prefixedTableName() + ") */ "
                    + (range.isReverse() ? "max" : "min") + "(m.row_name) as row_name "
                    + " FROM " + prefixedTableName() + " m "
                    + (bounds.isEmpty() ? "" : " WHERE  " + Joiner.on(" AND ").join(bounds));
            return new FullQuery(query).withArgs(args);
        }

        String query = " /* GET_RANGE_ROWS (" + tableName + ") */ "
                + " SELECT inner.row_name FROM "
                + "   ( SELECT /*+ INDEX(m pk_" + prefixedTableName() + ") */ "
                + "       DISTINCT m.row_name "
                + "     FROM " + prefixedTableName() + " m "
                + (bounds.isEmpty() ? "" : " WHERE  " + Joiner.on(" AND ").join(bounds))
                + "     ORDER BY m.row_name " + (range.isReverse() ? "DESC" : "ASC")
                + "   ) inner WHERE rownum <= " + maxRows;
        return new FullQuery(query).withArgs(args);
    }

    @Override
    public FullQuery getRowsColumnRangeCountsQuery(
            Iterable<byte[]> rows,
            long ts,
            ColumnRangeSelection columnRangeSelection) {
        String query = " /* GET_ROWS_COLUMN_RANGE_COUNT(" + tableName + ") */"
                + " SELECT m.row_name, COUNT(m.col_name) AS column_count "
                + "   FROM " + prefixedTableName() + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + "  WHERE m.row_name = t.row_name "
                + "    AND m.ts < ? "
                + (columnRangeSelection.getStartCol().length > 0 ? " AND m.col_name >= ?" : "")
                + (columnRangeSelection.getEndCol().length > 0 ? " AND m.col_name < ?" : "")
                + " GROUP BY m.row_name";
        FullQuery fullQuery = new FullQuery(query).withArgs(rowsToOracleArray(rows), ts);
        if (columnRangeSelection.getStartCol().length > 0) {
            fullQuery = fullQuery.withArg(columnRangeSelection.getStartCol());
        }
        if (columnRangeSelection.getEndCol().length > 0) {
            fullQuery = fullQuery.withArg(columnRangeSelection.getEndCol());
        }
        return fullQuery;
    }

    @Override
    public FullQuery getRowsColumnRangeQuery(
            Map<byte[], BatchColumnRangeSelection> columnRangeSelectionsByRow,
            long ts) {
        List<String> subQueries = new ArrayList<>(columnRangeSelectionsByRow.size());
        int totalArgs = 0;
        for (BatchColumnRangeSelection columnRangeSelection : columnRangeSelectionsByRow.values()) {
            totalArgs += 2
                    + ((columnRangeSelection.getStartCol().length > 0) ? 1 : 0)
                    + ((columnRangeSelection.getEndCol().length > 0) ? 1 : 0);
        }
        List<Object> args = new ArrayList<>(totalArgs);
        for (Map.Entry<byte[], BatchColumnRangeSelection> entry : columnRangeSelectionsByRow.entrySet()) {
            FullQuery query = getRowsColumnRangeSubQuery(entry.getKey(), ts, entry.getValue());
            subQueries.add(query.getQuery());
            for (Object arg : query.getArgs()) {
                args.add(arg);
            }
        }
        String query = Joiner.on(") UNION ALL (").appendTo(new StringBuilder("("), subQueries).append(")")
                .append(" ORDER BY row_name ASC, col_name ASC").toString();
        return new FullQuery(query).withArgs(args);
    }

    private FullQuery getRowsColumnRangeSubQuery(byte[] row, long ts, BatchColumnRangeSelection columnRangeSelection) {
        String query = " /* GET_ROWS_COLUMN_RANGE (" + tableName + ") */ "
                + "SELECT * FROM ( SELECT m.row_name, m.col_name, max(m.ts) as ts"
                + "   FROM " + prefixedTableName() + " m"
                + "  WHERE m.row_name = ?"
                + "    AND m.ts < ? "
                + (columnRangeSelection.getStartCol().length > 0 ? " AND m.col_name >= ?" : "")
                + (columnRangeSelection.getEndCol().length > 0 ? " AND m.col_name < ?" : "")
                + " GROUP BY m.row_name, m.col_name"
                + " ORDER BY m.col_name ASC ) WHERE rownum <= " + columnRangeSelection.getBatchHint();
        FullQuery fullQuery = new FullQuery(wrapQueryWithIncludeValue("GET_ROWS_COLUMN_RANGE", query, true))
                .withArg(row)
                .withArg(ts);
        if (columnRangeSelection.getStartCol().length > 0) {
            fullQuery = fullQuery.withArg(columnRangeSelection.getStartCol());
        }
        if (columnRangeSelection.getEndCol().length > 0) {
            fullQuery = fullQuery.withArg(columnRangeSelection.getEndCol());
        }
        return fullQuery;
    }

    private String wrapQueryWithIncludeValue(String wrappedName, String query, boolean includeValue) {
        if (!includeValue) {
            return query;
        }
        return " /* " + wrappedName + " (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(i wrap) LEADING(i wrap) NO_MERGE(i) NO_PUSH_PRED(i)"
                + "       INDEX(wrap pk_" + prefixedTableName() + ") */ "
                + "   wrap.row_name, wrap.col_name, wrap.ts" + getValueSubselect("wrap", includeValue)
                + " FROM " + prefixedTableName() + " wrap, ( " + query + " ) i "
                + " WHERE wrap.row_name = i.row_name "
                + "   AND wrap.col_name = i.col_name "
                + "   AND wrap.ts = i.ts ";
    }

    abstract String getValueSubselect(String tableAlias, boolean includeValue);

    private ArrayHandler rowsToOracleArray(Iterable<byte[]> rows) {
        List<Object[]> oraRows = Lists.newArrayListWithCapacity(Iterables.size(rows));
        for (byte[] row : rows) {
            oraRows.add(new Object[] { row, null, null });
        }
        return config.jdbcHandler().createStructArray(
                structArrayPrefix() + "CELL_TS", "" + structArrayPrefix() + "CELL_TS_TABLE", oraRows);
    }

    private ArrayHandler cellsToOracleArray(Iterable<Cell> cells) {
        List<Object[]> oraRows = Lists.newArrayListWithCapacity(Iterables.size(cells));
        for (Cell cell : cells) {
            oraRows.add(new Object[] { cell.getRowName(), cell.getColumnName(), null });
        }
        return config.jdbcHandler().createStructArray(
                structArrayPrefix() + "CELL_TS", "" + structArrayPrefix() + "CELL_TS_TABLE", oraRows);
    }

    private ArrayHandler rowsAndTimestampsToOracleArray(Collection<Map.Entry<byte[], Long>> rows) {
        List<Object[]> oraRows = Lists.newArrayListWithCapacity(rows.size());
        for (Entry<byte[], Long> entry : rows) {
            oraRows.add(new Object[] { entry.getKey(), null, entry.getValue() });
        }
        return config.jdbcHandler().createStructArray(
                structArrayPrefix() + "CELL_TS", "" + structArrayPrefix() + "CELL_TS_TABLE", oraRows);
    }

    private ArrayHandler cellsAndTimestampsToOracleArray(Collection<Map.Entry<Cell, Long>> cells) {
        List<Object[]> oraRows = Lists.newArrayListWithCapacity(cells.size());
        for (Entry<Cell, Long> entry : cells) {
            Cell cell = entry.getKey();
            oraRows.add(new Object[] { cell.getRowName(), cell.getColumnName(), entry.getValue() });
        }
        return config.jdbcHandler().createStructArray(
                structArrayPrefix() + "CELL_TS", "" + structArrayPrefix() + "CELL_TS_TABLE", oraRows);
    }

    private String prefixedTableName() {
        return config.tableNameMapper().hashTableNameToFitOracleTableNameLimits(
                config.tablePrefix(),
                tableName);
    }

    private String structArrayPrefix() {
        return config.tablePrefix().toUpperCase();
    }
}
