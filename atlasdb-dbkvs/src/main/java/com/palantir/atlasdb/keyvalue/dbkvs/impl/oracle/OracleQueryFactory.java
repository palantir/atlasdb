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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.AbstractDbQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.db.oracle.JdbcHandler.ArrayHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class OracleQueryFactory extends AbstractDbQueryFactory {
    private final OracleDdlConfig config;
    private final String tableName;
    private final boolean hasOverflowValues;

    public OracleQueryFactory(OracleDdlConfig config, String tableName, boolean hasOverflowValues) {
        this.config = config;
        this.tableName = tableName;
        this.hasOverflowValues = hasOverflowValues;
    }

    @Override
    public FullQuery getLatestRowQuery(byte[] row, long ts, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_LATEST_ONE_ROW_INNER (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) LEADING(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + tableName + " m "
                + " WHERE m.row_name = ? "
                + "   AND m.ts < ? "
                + (columns.allColumnsSelected()
                        ? ""
                        : " AND EXISTS ("
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
    public FullQuery getLatestRowsQuery(Iterable<byte[]> rows, long ts, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_LATEST_ROWS_SINGLE_BOUND_INNER (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) LEADING(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.ts < ? "
                + (columns.allColumnsSelected()
                        ? ""
                        : " AND EXISTS ("
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
    public FullQuery getLatestRowsQuery(
            Collection<Map.Entry<byte[], Long>> rows, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_LATEST_ROWS_MANY_BOUNDS_INNER (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) LEADING(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.ts < t.max_ts "
                + (columns.allColumnsSelected()
                        ? ""
                        : " AND EXISTS ("
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
    public FullQuery getAllRowQuery(byte[] row, long ts, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_ALL_ONE_ROW (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ INDEX(m " + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + tableName + " m "
                + " WHERE m.row_name = ? "
                + "   AND m.ts < ? "
                + (columns.allColumnsSelected()
                        ? ""
                        : " AND EXISTS ("
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
    public FullQuery getAllRowsQuery(Iterable<byte[]> rows, long ts, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_ALL_ROWS_SINGLE_BOUND (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) LEADING(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.ts < ? "
                + (columns.allColumnsSelected()
                        ? ""
                        : " AND EXISTS ("
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
    public FullQuery getAllRowsQuery(
            Collection<Map.Entry<byte[], Long>> rows, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_ALL_ROWS_MANY_BOUNDS (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) LEADING(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.ts < t.max_ts "
                + (columns.allColumnsSelected()
                        ? ""
                        : " AND EXISTS ("
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
                + "   /*+ INDEX(m " + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + tableName + " m "
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
                + "   /*+ USE_NL(t m) LEADING(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
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
                + "   /*+ USE_NL(t m) LEADING(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
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
                + "   /*+ INDEX(m " + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + tableName + " m "
                + " WHERE m.row_name = ? "
                + "   AND m.col_name = ? "
                + "   AND m.ts < ? ";
        return new FullQuery(query).withArgs(cell.getRowName(), cell.getColumnName(), ts);
    }

    @Override
    public FullQuery getAllCellsQuery(Iterable<Cell> cells, long ts, boolean includeValue) {
        String query = " /* GET_ALL_CELLS_SINGLE_BOUND (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) LEADING(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.col_name = t.col_name "
                + "   AND m.ts < ? ";
        return new FullQuery(query).withArgs(cellsToOracleArray(cells), ts);
    }

    @Override
    public FullQuery getAllCellsQuery(Collection<Map.Entry<Cell, Long>> cells, boolean includeValue) {
        String query = " /* GET_ALL_CELLS_MANY_BOUNDS (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) LEADING(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE m.row_name = t.row_name "
                + "   AND m.col_name = t.col_name "
                + "   AND m.ts < t.max_ts ";
        return new FullQuery(query).withArg(cellsAndTimestampsToOracleArray(cells));
    }

    @Override
    public FullQuery getRangeQuery(RangeRequest range, long ts, int maxRows) {
        List<String> bounds = new ArrayList<>(2);
        List<Object> args = new ArrayList<>(2);
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
                    + " SELECT /*+ INDEX(m " + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                    + (range.isReverse() ? "max" : "min") + "(m.row_name) as row_name "
                    + " FROM " + tableName + " m "
                    + (bounds.isEmpty() ? "" : " WHERE  " + Joiner.on(" AND ").join(bounds));
            return new FullQuery(query).withArgs(args);
        }

        String query = " /* GET_RANGE_ROWS (" + tableName + ") */ "
                + " SELECT inner.row_name FROM "
                + "   ( SELECT /*+ INDEX(m " + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "       DISTINCT m.row_name "
                + "     FROM " + tableName + " m "
                + (bounds.isEmpty() ? "" : " WHERE  " + Joiner.on(" AND ").join(bounds))
                + "     ORDER BY m.row_name " + (range.isReverse() ? "DESC" : "ASC")
                + "   ) inner WHERE rownum <= " + maxRows;
        return new FullQuery(query).withArgs(args);
    }

    @Override
    public boolean hasOverflowValues() {
        return hasOverflowValues;
    }

    @Override
    public FullQuery getRowsColumnRangeCountsQuery(
            Iterable<byte[]> rows, long ts, ColumnRangeSelection columnRangeSelection) {
        String query = " /* GET_ROWS_COLUMN_RANGE_COUNT(" + tableName + ") */"
                + " SELECT m.row_name, COUNT(m.col_name) AS column_count "
                + "   FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
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
    protected FullQuery getRowsColumnRangeSubQuery(
            byte[] row, long ts, BatchColumnRangeSelection columnRangeSelection) {
        String query = " /* GET_ROWS_COLUMN_RANGE (" + tableName + ") */ "
                + "SELECT s.row_name, s.col_name, s.ts" + getValueSubselect("s", true)
                + " FROM ( SELECT m.row_name, m.col_name, max(m.ts) as ts"
                + getValueSubselectForGroupBy("m")
                + "   FROM " + tableName + " m"
                + "  WHERE m.row_name = ?"
                + "    AND m.ts < ? "
                + (columnRangeSelection.getStartCol().length > 0 ? " AND m.col_name >= ?" : "")
                + (columnRangeSelection.getEndCol().length > 0 ? " AND m.col_name < ?" : "")
                + " GROUP BY m.row_name, m.col_name"
                + " ORDER BY m.row_name ASC, m.col_name ASC ) s WHERE rownum <= " + columnRangeSelection.getBatchHint();
        FullQuery fullQuery = new FullQuery(query).withArg(row).withArg(ts);
        if (columnRangeSelection.getStartCol().length > 0) {
            fullQuery = fullQuery.withArg(columnRangeSelection.getStartCol());
        }
        if (columnRangeSelection.getEndCol().length > 0) {
            fullQuery = fullQuery.withArg(columnRangeSelection.getEndCol());
        }
        return fullQuery;
    }

    @Override
    protected FullQuery getRowsColumnRangeFullyLoadedRowsSubQuery(
            List<byte[]> rows, long ts, ColumnRangeSelection columnRangeSelection) {
        String query = " /* GET_ROWS_COLUMN_RANGE_FULLY_LOADED_ROWS (" + tableName + ") */ "
                + "SELECT * FROM ( SELECT m.row_name, m.col_name, max(m.ts) as ts"
                + "   FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + "  WHERE m.row_name = t.row_name "
                + "    AND m.ts < ? "
                + (columnRangeSelection.getStartCol().length > 0 ? " AND m.col_name >= ?" : "")
                + (columnRangeSelection.getEndCol().length > 0 ? " AND m.col_name < ?" : "")
                + " GROUP BY m.row_name, m.col_name"
                + " ORDER BY m.row_name ASC, m.col_name ASC )";
        String wrappedQuery = wrapQueryWithIncludeValue("GET_ROWS_COLUMN_RANGE_FULLY_LOADED_ROWS", query, true);
        FullQuery fullQuery = new FullQuery(wrappedQuery).withArgs(rowsToOracleArray(rows), ts);
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
                + "       INDEX(wrap " + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   wrap.row_name, wrap.col_name, wrap.ts" + getValueSubselect("wrap", includeValue)
                + " FROM " + tableName + " wrap, ( " + query + " ) i "
                + " WHERE wrap.row_name = i.row_name "
                + "   AND wrap.col_name = i.col_name "
                + "   AND wrap.ts = i.ts ";
    }

    private String getValueSubselect(String tableAlias, boolean includeValue) {
        return OracleQueryHelpers.getValueSubselect(hasOverflowValues, tableAlias, includeValue);
    }

    private String getValueSubselectForGroupBy(String tableAlias) {
        return OracleQueryHelpers.getValueSubselectForGroupBy(hasOverflowValues, tableAlias);
    }

    private ArrayHandler rowsToOracleArray(Iterable<byte[]> rows) {
        List<Object[]> oraRows = new ArrayList<>(Iterables.size(rows));
        for (byte[] row : rows) {
            oraRows.add(new Object[] {row, null, null});
        }
        return config.jdbcHandler()
                .createStructArray(
                        structArrayPrefix() + "CELL_TS", "" + structArrayPrefix() + "CELL_TS_TABLE", oraRows);
    }

    private ArrayHandler cellsToOracleArray(Iterable<Cell> cells) {
        List<Object[]> oraRows = new ArrayList<>(Iterables.size(cells));
        for (Cell cell : cells) {
            oraRows.add(new Object[] {cell.getRowName(), cell.getColumnName(), null});
        }
        return config.jdbcHandler()
                .createStructArray(
                        structArrayPrefix() + "CELL_TS", "" + structArrayPrefix() + "CELL_TS_TABLE", oraRows);
    }

    private ArrayHandler rowsAndTimestampsToOracleArray(Collection<Map.Entry<byte[], Long>> rows) {
        List<Object[]> oraRows = new ArrayList<>(rows.size());
        for (Map.Entry<byte[], Long> entry : rows) {
            oraRows.add(new Object[] {entry.getKey(), null, entry.getValue()});
        }
        return config.jdbcHandler()
                .createStructArray(
                        structArrayPrefix() + "CELL_TS", "" + structArrayPrefix() + "CELL_TS_TABLE", oraRows);
    }

    private ArrayHandler cellsAndTimestampsToOracleArray(Collection<Map.Entry<Cell, Long>> cells) {
        List<Object[]> oraRows = new ArrayList<>(cells.size());
        for (Map.Entry<Cell, Long> entry : cells) {
            Cell cell = entry.getKey();
            oraRows.add(new Object[] {cell.getRowName(), cell.getColumnName(), entry.getValue()});
        }
        return config.jdbcHandler()
                .createStructArray(
                        structArrayPrefix() + "CELL_TS", "" + structArrayPrefix() + "CELL_TS_TABLE", oraRows);
    }

    private String structArrayPrefix() {
        return config.tablePrefix().toUpperCase();
    }
}
