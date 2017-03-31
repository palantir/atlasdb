/*
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.AbstractDbQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.db.oracle.JdbcHandler.ArrayHandler;

public abstract class OracleQueryFactory extends AbstractDbQueryFactory {
    protected final OracleDdlConfig config;
    protected final String tableName;

    public OracleQueryFactory(OracleDdlConfig config, String tableName) {
        this.config = config;
        this.tableName = tableName;
    }

    @Override
    public FullQuery getLatestRowQuery(byte[] row,
                                       long ts,
                                       ColumnSelection columns,
                                       boolean includeValue) {
        String query = " /* GET_LATEST_ONE_ROW_INNER (" + tableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + tableName + " m "
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
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
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
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, max(m.ts) as ts "
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
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
                + "   /*+ INDEX(m " + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + tableName + " m "
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
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
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
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
                + PrimaryKeyConstraintNames.get(tableName) + ") */ "
                + "   m.row_name, m.col_name, m.ts" + getValueSubselect("m", includeValue)
                + " FROM " + tableName + " m, TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
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
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
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
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
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
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
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
                + "   /*+ USE_NL(t m) CARDINALITY(t 1) CARDINALITY(m 10) INDEX(m "
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
    public FullQuery getRowsColumnRangeCountsQuery(
            Iterable<byte[]> rows,
            long ts,
            ColumnRangeSelection columnRangeSelection) {
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
            byte[] row,
            long ts,
            BatchColumnRangeSelection columnRangeSelection) {
        String query = " /* GET_ROWS_COLUMN_RANGE (" + tableName + ") */ "
                + "SELECT s.row_name, s.col_name, s.ts" + getValueSubselect("s", true)
                + " FROM ( SELECT m.row_name, m.col_name, max(m.ts) as ts"
                +         getValueSubselectForGroupBy("m")
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
            List<byte[]> rows,
            long ts,
            ColumnRangeSelection columnRangeSelection) {
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
        if (includeValue) {
            List<String> colNames = getValueColumnNames();
            StringBuilder ret = new StringBuilder(10 * colNames.size());
            for (String colName : colNames) {
                // e.g., ", m.val"
                ret.append(", ").append(tableAlias).append('.').append(colName);
            }
            return ret.toString();
        } else {
            return "";
        }
    }

    private String getValueSubselectForGroupBy(String tableAlias) {
        List<String> colNames = getValueColumnNames();
        StringBuilder ret = new StringBuilder(70 * colNames.size());
        for (String colName : colNames) {
            // E.g., ", MAX(m.val) KEEP (DENSE_RANK LAST ORDER BY m.ts ASC) AS val".
            // How this works, assuming we have a "GROUP BY row_name, col_name" clause:
            //  1) Among each group of rows with the same (row_name, col_name) pair,
            //     "KEEP (DENSE_RANK LAST ORDER BY m.ts ASC)" will select the subset
            //     of rows with the biggest 'ts' value. Since (row_name, col_name, ts)
            //     is the primary key, that subset will always contain exactly one
            //     element in our case.
            //  2) For that subset of rows, we take an aggregate function "MAX(m.val)".
            //     It doesn't make a difference which function we use since our subset
            //     is a singleton, so MAX will simply return the only element in that set.
            //  To sum it up: for each (row_name, col_name) group, this will select
            //  the value of the row that has the largest 'ts' value within that group.
            ret.append(", MAX(").append(tableAlias).append('.').append(colName)
                    .append(") KEEP (DENSE_RANK LAST ORDER BY ")
                    .append(tableAlias).append(".ts ASC) AS ").append(colName);
        }
        return ret.toString();
    }

    private List<String> getValueColumnNames() {
        if (hasOverflowValues()) {
            return VAL_AND_OVERFLOW;
        } else {
            return VAL_ONLY;
        }
    }

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

    private String structArrayPrefix() {
        return config.tablePrefix().toUpperCase();
    }

    private static final List<String> VAL_ONLY = ImmutableList.of("val");
    private static final List<String> VAL_AND_OVERFLOW = ImmutableList.of("val", "overflow");
}
