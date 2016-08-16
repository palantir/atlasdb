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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.dbkvs.PostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowValue;

public class PostgresQueryFactory implements DbQueryFactory {
    private final String tableName;
    private final PostgresDdlConfig config;

    public PostgresQueryFactory(String tableName, PostgresDdlConfig config) {
        this.tableName = tableName;
        this.config = config;
    }

    @Override
    public FullQuery getLatestRowQuery(byte[] row,
                                       long ts,
                                       ColumnSelection columns,
                                       boolean includeValue) {
        if (columns.allColumnsSelected() || columns.getSelectedColumns().size() > 50) {
            return getLatestRowQueryWithAllOrManyColumns(row, ts, columns, includeValue);
        } else {

            String query = "SELECT cells.row_name, cells.col_name, data.ts, data.val " +
                    "FROM (SELECT t1.row_name AS row_name, t2.col_name AS col_name " +
                    "      FROM (SELECT unnest(array[?]) AS row_name) t1, (SELECT unnest(array[" + nParamsWithoutBrackets(Iterables.size(columns.getSelectedColumns())) + "]) AS col_name) t2) cells " +
                    "INNER JOIN LATERAL (SELECT ts, val FROM " + prefixedTableName() + " tbl " +
                    "WHERE tbl.row_name = cells.row_name " +
                    "    AND tbl.col_name = cells.col_name " +
                    "    AND tbl.ts < ? " +
                    "    ORDER BY tbl.ts " +
                    "    DESC LIMIT 1) data " +
                    "ON true";

            return new FullQuery(query).withArg(row).withArgs(columns.getSelectedColumns()).withArg(ts);
        }
    }

    private FullQuery getLatestRowQueryWithAllOrManyColumns(byte[] row, long ts, ColumnSelection columns, boolean includeValue) {
        String query =
                " /* GET_LATEST_ROW_INNER (" + tableName + ") */ " +
                        " SELECT m.row_name, m.col_name, max(m.ts) as ts " +
                        "   FROM " + prefixedTableName() + " m " +
                        "  WHERE m.row_name = ? " +
                        "    AND m.ts < ? " +
                        (columns.allColumnsSelected() ? "" :
                                "    AND m.col_name IN " + nParams(Iterables.size(columns.getSelectedColumns()))) +
                        " GROUP BY m.row_name, m.col_name";
        query = wrapQueryWithIncludeValue("GET_LATEST_ROW", query, includeValue);
        FullQuery fullQuery = new FullQuery(query).withArgs(row, ts);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getLatestRowsQuery(Iterable<byte[]> rows,
                                        long ts,
                                        ColumnSelection columns,
                                        boolean includeValue) {
        String query =
                " /* GET_LATEST_ROWS_INNER (" + tableName + ") */ " +
                " SELECT m.row_name, m.col_name, max(m.ts) as ts " +
                "   FROM " + prefixedTableName() + " m " +
                "  WHERE m.row_name IN " + nParams(Iterables.size(rows)) +
                "    AND m.ts < ? " +
                (columns.allColumnsSelected() ? "" :
                    "    AND m.col_name IN " + nParams(Iterables.size(columns.getSelectedColumns()))) +
                " GROUP BY m.row_name, m.col_name ";
        query = wrapQueryWithIncludeValue("GET_LATEST_ROW", query, includeValue);
        FullQuery fullQuery = new FullQuery(query).withArgs(rows).withArg(ts);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getLatestRowsQuery(Collection<Entry<byte[], Long>> rows,
                                        ColumnSelection columns,
                                        boolean includeValue) {
        String query =
                " /* GET_LATEST_ROWS_INNER (" + tableName + ") */ " +
                " SELECT m.row_name, m.col_name, max(m.ts) as ts " +
                "   FROM " + prefixedTableName() + " m, (VALUES " + nkParams(2, rows.size()) + ") t(row_name, ts) " +
                "  WHERE m.row_name = t.row_name " +
                "    AND m.ts < t.ts " +
                (columns.allColumnsSelected() ? "" :
                    "    AND m.col_name IN " + nParams(Iterables.size(columns.getSelectedColumns()))) +
                " GROUP BY m.row_name, m.col_name ";
        query = wrapQueryWithIncludeValue("GET_LATEST_ROW", query, includeValue);
        FullQuery fullQuery = addRowTsArgs(new FullQuery(query), rows);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getAllRowQuery(byte[] row,
                                    long ts,
                                    ColumnSelection columns,
                                    boolean includeValue) {
        String query =
                " /* GET_ALL_ROW (" + tableName + ") */ " +
                " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ") +
                "   FROM " + prefixedTableName() + " m " +
                "  WHERE m.row_name = ? " +
                "    AND m.ts < ? " +
                (columns.allColumnsSelected() ? "" :
                    "    AND m.col_name IN " + nParams(Iterables.size(columns.getSelectedColumns())));
        FullQuery fullQuery = new FullQuery(query).withArgs(row, ts);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getAllRowsQuery(Iterable<byte[]> rows,
                                     long ts,
                                     ColumnSelection columns,
                                     boolean includeValue) {
        String query =
                " /* GET_ALL_ROWS (" + tableName + ") */ " +
                " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ") +
                "   FROM " + prefixedTableName() + " m " +
                "  WHERE m.row_name IN " + nParams(Iterables.size(rows)) +
                "    AND m.ts < ? " +
                (columns.allColumnsSelected() ? "" :
                    "    AND m.col_name IN " + nParams(Iterables.size(columns.getSelectedColumns())));
        FullQuery fullQuery = new FullQuery(query).withArgs(rows).withArg(ts);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getAllRowsQuery(Collection<Entry<byte[], Long>> rows,
                                     ColumnSelection columns,
                                     boolean includeValue) {
        String query =
                " /* GET_ALL_ROWS (" + tableName + ") */ " +
                " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ") +
                "   FROM " + prefixedTableName() + " m, (VALUES " + nkParams(2, rows.size()) + ") t(row_name, ts) " +
                "  WHERE m.row_name = t.row_name " +
                "    AND m.ts < t.ts " +
                (columns.allColumnsSelected() ? "" :
                    "    AND m.col_name IN " + nParams(Iterables.size(columns.getSelectedColumns())));
        FullQuery fullQuery = addRowTsArgs(new FullQuery(query), rows);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getLatestCellQuery(Cell cell, long ts, boolean includeValue) {
        String query =
                " /* GET_LATEST_CELL_INNER (" + tableName + ") */ " +
                " SELECT m.row_name, m.col_name, max(m.ts) as ts " +
                "   FROM " + prefixedTableName() + " m " +
                "  WHERE m.row_name = ? " +
                "    AND m.col_name = ? " +
                "    AND m.ts < ? " +
                " GROUP BY m.row_name, m.col_name " +
                " LIMIT 1";
        query = wrapQueryWithIncludeValue("GET_LATEST_CELL", query, includeValue);
        return new FullQuery(query).withArgs(cell.getRowName(), cell.getColumnName(), ts);
    }

    @Override
    public FullQuery getLatestCellsQuery(Iterable<Cell> cells, long ts, boolean includeValue) {
        String query =
                " /* GET_LATEST_CELLS_INNER (" + tableName + ") */ " +
                " SELECT m.row_name, m.col_name, max(m.ts) as ts " +
                "   FROM " + prefixedTableName() + " m, (VALUES " + nkParams(2, Iterables.size(cells)) + ") t(row_name, col_name) " +
                "  WHERE m.row_name = t.row_name " +
                "    AND m.col_name = t.col_name " +
                "    AND m.ts < ? " +
                " GROUP BY m.row_name, m.col_name ";
        query = wrapQueryWithIncludeValue("GET_LATEST_CELLS", query, includeValue);
        return addCellArgs(new FullQuery(query), cells).withArg(ts);
    }

    @Override
    public FullQuery getLatestCellsQuery(Collection<Entry<Cell, Long>> cells, boolean includeValue) {
        String query =
                " /* GET_LATEST_CELLS_INNER (" + tableName + ") */ " +
                " SELECT m.row_name, m.col_name, max(m.ts) as ts " +
                "   FROM " + prefixedTableName() + " m, (VALUES " + nkParams(3, Iterables.size(cells)) + ") t(row_name, col_name, ts) " +
                "  WHERE m.row_name = t.row_name " +
                "    AND m.col_name = t.col_name " +
                "    AND m.ts < t.ts " +
                " GROUP BY m.row_name, m.col_name ";
        query = wrapQueryWithIncludeValue("GET_LATEST_CELLS", query, includeValue);
        return addCellTsArgs(new FullQuery(query), cells);
    }

    @Override
    public FullQuery getAllCellQuery(Cell cell, long ts, boolean includeValue) {
        String query =
                " /* GET_ALL_CELL (" + tableName + ") */ " +
                " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ") +
                "   FROM " + prefixedTableName() + " m " +
                "  WHERE m.row_name = ? " +
                "    AND m.col_name = ? " +
                "    AND m.ts < ? ";
        return new FullQuery(query).withArgs(cell.getRowName(), cell.getColumnName(), ts);
    }

    @Override
    public FullQuery getAllCellsQuery(Iterable<Cell> cells, long ts, boolean includeValue) {
        String query =
                " /* GET_ALL_CELLS (" + tableName + ") */ " +
                " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ") +
                "   FROM " + prefixedTableName() + " m, (VALUES " + nkParams(2, Iterables.size(cells)) + ") t(row_name, col_name) " +
                "  WHERE m.row_name = t.row_name " +
                "    AND m.col_name = t.col_name " +
                "    AND m.ts < ? ";
        return addCellArgs(new FullQuery(query), cells).withArg(ts);
    }

    @Override
    public FullQuery getAllCellsQuery(Collection<Entry<Cell, Long>> cells, boolean includeValue) {
        String query =
                " /* GET_ALL_CELLS (" + tableName + ") */ " +
                " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ") +
                "   FROM " + prefixedTableName() + " m, (VALUES " + nkParams(3, Iterables.size(cells)) + ") t(row_name, col_name, ts) " +
                "  WHERE m.row_name = t.row_name " +
                "    AND m.col_name = t.col_name " +
                "    AND m.ts < t.ts ";
        return addCellTsArgs(new FullQuery(query), cells);
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
        String query =
                " /* GET_RANGE_ROWS (" + tableName + ") */ " +
                " SELECT DISTINCT m.row_name " +
                " FROM " + prefixedTableName() + " m " +
                (bounds.isEmpty() ? "" : " WHERE  " + Joiner.on(" AND ").join(bounds)) +
                " ORDER BY m.row_name " + (range.isReverse() ? "DESC" : "ASC") + " LIMIT " + maxRows;
        return new FullQuery(query).withArgs(args);
    }

    @Override
    public boolean hasOverflowValues() {
        return false;
    }

    @Override
    public Collection<FullQuery> getOverflowQueries(Collection<OverflowValue> overflowIds) {
        throw new IllegalStateException("postgres tables don't have overflow fields");
    }

    private String nParamsWithoutBrackets(int numParams) {
        StringBuilder builder = new StringBuilder(2*numParams - 1);
        Joiner.on(',').appendTo(builder, Iterables.limit(Iterables.cycle('?'), numParams));
        return builder.toString();
    }

    private String nParams(int numParams) {
        StringBuilder builder = new StringBuilder(2*numParams + 1).append('(');
        Joiner.on(',').appendTo(builder, Iterables.limit(Iterables.cycle('?'), numParams));
        return builder.append(')').toString();
    }

    private String nkParams(int numParams, int numEntries) {
        String params = nParams(numParams);
        return Joiner.on(',').join(Iterables.limit(Iterables.cycle(params.toString()), numEntries));
    }

    private String wrapQueryWithIncludeValue(String wrappedName, String query, boolean includeValue) {
        if (!includeValue) {
            return query;
        }
        return " /* " + wrappedName + " (" + tableName + ") */ " +
                " SELECT wrap.row_name, wrap.col_name, wrap.ts, wrap.val " +
                " FROM " + prefixedTableName() + " wrap, ( " + query + " ) i " +
                " WHERE wrap.row_name = i.row_name " +
                "   AND wrap.col_name = i.col_name " +
                "   AND wrap.ts = i.ts ";
    }

    private FullQuery addRowTsArgs(FullQuery fullQuery, Iterable<Entry<byte[], Long>> rows) {
        for (Entry<byte[], Long> entry : rows) {
            fullQuery.withArgs(entry.getKey(), entry.getValue());
        }
        return fullQuery;
    }

    private FullQuery addCellArgs(FullQuery fullQuery, Iterable<Cell> cells) {
        for (Cell cell : cells) {
            fullQuery.withArgs(cell.getRowName(), cell.getColumnName());
        }
        return fullQuery;
    }

    private FullQuery addCellTsArgs(FullQuery fullQuery, Collection<Entry<Cell, Long>> cells) {
        for (Entry<Cell, Long> entry : cells) {
            Cell cell = entry.getKey();
            fullQuery.withArgs(cell.getRowName(), cell.getColumnName(), entry.getValue());
        }
        return fullQuery;
    }

    private String prefixedTableName() {
        return config.tablePrefix() + tableName;
    }

    @Override
    public FullQuery getRowsColumnRangeQuery(List<byte[]> rows, long ts, ColumnRangeSelection columnRangeSelection) {
        List<String> subQueries = Lists.newArrayListWithCapacity(rows.size());
        int argsPerRow = 2 + ((columnRangeSelection.getStartCol().length > 0) ? 1 : 0) +
                ((columnRangeSelection.getEndCol().length > 0) ? 1 : 0);
        List<Object> args = Lists.newArrayListWithCapacity(rows.size() * argsPerRow);
        for (byte[] row : rows) {
            FullQuery query = getRowsColumnRangeSubQuery(row, ts, columnRangeSelection);
            subQueries.add(query.getQuery());
            for (Object arg : query.getArgs()) {
                args.add(arg);
            }
        }
        String query = Joiner.on(") UNION ALL (").appendTo(new StringBuilder("("), subQueries).append(")")
                .append(" ORDER BY wrap.row_name ASC, wrap.col_name ASC").toString();
        return new FullQuery(query).withArgs(args);
    }

    private FullQuery getRowsColumnRangeSubQuery(byte[] row, long ts, ColumnRangeSelection columnRangeSelection) {
        String query =
                " /* GET_ROWS_COLUMN_RANGE (" + tableName + ") */ " +
                        " SELECT m.row_name, m.col_name, max(m.ts) as ts" +
                        "   FROM " + prefixedTableName() + " m " +
                        "  WHERE m.row_name = ? " +
                        "    AND m.ts < ? " +
                        (columnRangeSelection.getStartCol().length > 0 ? " AND m.col_name >= ?" : "") +
                        (columnRangeSelection.getEndCol().length > 0 ? " AND m.col_name < ?" : "") +
                        " GROUP BY m.row_name, m.col_name" +
                        " ORDER BY m.col_name ASC LIMIT " + columnRangeSelection.getBatchHint();
        FullQuery fullQuery = new FullQuery(wrapQueryWithIncludeValue("GET_ROWS_COLUMN_RANGE", query, true)).withArg(row).withArg(ts);
        if (columnRangeSelection.getStartCol().length > 0) {
            fullQuery = fullQuery.withArg(columnRangeSelection.getStartCol());
        }
        if (columnRangeSelection.getEndCol().length > 0) {
            fullQuery = fullQuery.withArg(columnRangeSelection.getEndCol());
        }
        return fullQuery;
    }
}
