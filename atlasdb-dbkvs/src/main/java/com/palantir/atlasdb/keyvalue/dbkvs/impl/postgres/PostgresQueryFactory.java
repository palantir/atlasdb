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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.dbkvs.PostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.AbstractDbQueryFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PostgresQueryFactory extends AbstractDbQueryFactory {
    private final String tableName;
    private final PostgresDdlConfig config;

    public PostgresQueryFactory(String tableName, PostgresDdlConfig config) {
        this.tableName = tableName;
        this.config = config;
    }

    @Override
    public FullQuery getLatestRowQuery(byte[] row, long ts, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_LATEST_ROW_INNER (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, max(m.ts) as ts "
                + "   FROM " + prefixedTableName() + " m "
                + "  WHERE m.row_name = ? "
                + "    AND m.ts < ? "
                + (columns.allColumnsSelected()
                        ? ""
                        : "    AND m.col_name IN " + numParams(Iterables.size(columns.getSelectedColumns())))
                + " GROUP BY m.row_name, m.col_name";
        query = wrapQueryWithIncludeValue("GET_LATEST_ROW", query, includeValue);
        FullQuery fullQuery = new FullQuery(query).withArgs(row, ts);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getLatestRowsQuery(Iterable<byte[]> rows, long ts, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_LATEST_ROWS_INNER (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, max(m.ts) as ts "
                + "   FROM " + prefixedTableName() + " m "
                + "  WHERE m.row_name IN " + numParams(Iterables.size(rows))
                + "    AND m.ts < ? "
                + (columns.allColumnsSelected()
                        ? ""
                        : "    AND m.col_name IN " + numParams(Iterables.size(columns.getSelectedColumns())))
                + " GROUP BY m.row_name, m.col_name ";
        query = wrapQueryWithIncludeValue("GET_LATEST_ROW", query, includeValue);
        FullQuery fullQuery = new FullQuery(query).withArgs(rows).withArg(ts);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getLatestRowsQuery(
            Collection<Map.Entry<byte[], Long>> rows, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_LATEST_ROWS_INNER (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, max(m.ts) as ts "
                + "   FROM " + prefixedTableName() + " m,"
                + "     (VALUES " + groupOfNumParams(2, rows.size()) + ") t(row_name, ts) "
                + "  WHERE m.row_name = t.row_name "
                + "    AND m.ts < t.ts "
                + (columns.allColumnsSelected()
                        ? ""
                        : "    AND m.col_name IN " + numParams(Iterables.size(columns.getSelectedColumns())))
                + " GROUP BY m.row_name, m.col_name ";
        query = wrapQueryWithIncludeValue("GET_LATEST_ROW", query, includeValue);
        FullQuery fullQuery = addRowTsArgs(new FullQuery(query), rows);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getAllRowQuery(byte[] row, long ts, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_ALL_ROW (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ")
                + "   FROM " + prefixedTableName() + " m "
                + "  WHERE m.row_name = ? "
                + "    AND m.ts < ? "
                + (columns.allColumnsSelected()
                        ? ""
                        : "    AND m.col_name IN " + numParams(Iterables.size(columns.getSelectedColumns())));
        FullQuery fullQuery = new FullQuery(query).withArgs(row, ts);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getAllRowsQuery(Iterable<byte[]> rows, long ts, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_ALL_ROWS (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ")
                + "   FROM " + prefixedTableName() + " m "
                + "  WHERE m.row_name IN " + numParams(Iterables.size(rows))
                + "    AND m.ts < ? "
                + (columns.allColumnsSelected()
                        ? ""
                        : "    AND m.col_name IN " + numParams(Iterables.size(columns.getSelectedColumns())));
        FullQuery fullQuery = new FullQuery(query).withArgs(rows).withArg(ts);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getAllRowsQuery(
            Collection<Map.Entry<byte[], Long>> rows, ColumnSelection columns, boolean includeValue) {
        String query = " /* GET_ALL_ROWS (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ")
                + "   FROM " + prefixedTableName() + " m,"
                + "     (VALUES " + groupOfNumParams(2, rows.size()) + ") t(row_name, ts) "
                + "  WHERE m.row_name = t.row_name "
                + "    AND m.ts < t.ts "
                + (columns.allColumnsSelected()
                        ? ""
                        : "    AND m.col_name IN " + numParams(Iterables.size(columns.getSelectedColumns())));
        FullQuery fullQuery = addRowTsArgs(new FullQuery(query), rows);
        return columns.allColumnsSelected() ? fullQuery : fullQuery.withArgs(columns.getSelectedColumns());
    }

    @Override
    public FullQuery getLatestCellQuery(Cell cell, long ts, boolean includeValue) {
        String query = " /* GET_LATEST_CELL_INNER (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, max(m.ts) as ts "
                + "   FROM " + prefixedTableName() + " m "
                + "  WHERE m.row_name = ? "
                + "    AND m.col_name = ? "
                + "    AND m.ts < ? "
                + " GROUP BY m.row_name, m.col_name "
                + " LIMIT 1";
        query = wrapQueryWithIncludeValue("GET_LATEST_CELL", query, includeValue);
        return new FullQuery(query).withArgs(cell.getRowName(), cell.getColumnName(), ts);
    }

    @Override
    public FullQuery getLatestCellsQuery(Iterable<Cell> cells, long ts, boolean includeValue) {
        String query = " /* GET_LATEST_CELLS_INNER (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, max(m.ts) as ts "
                + "   FROM " + prefixedTableName() + " m,"
                + "    (VALUES " + groupOfNumParams(2, Iterables.size(cells)) + ") t(row_name, col_name) "
                + "  WHERE m.row_name = t.row_name "
                + "    AND m.col_name = t.col_name "
                + "    AND m.ts < ? "
                + " GROUP BY m.row_name, m.col_name ";
        query = wrapQueryWithIncludeValue("GET_LATEST_CELLS", query, includeValue);
        return addCellArgs(new FullQuery(query), cells).withArg(ts);
    }

    @Override
    public FullQuery getLatestCellsQuery(Collection<Map.Entry<Cell, Long>> cells, boolean includeValue) {
        String query = " /* GET_LATEST_CELLS_INNER (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, max(m.ts) as ts "
                + "   FROM " + prefixedTableName() + " m,"
                + "     (VALUES " + groupOfNumParams(3, Iterables.size(cells)) + ") t(row_name, col_name, ts) "
                + "  WHERE m.row_name = t.row_name "
                + "    AND m.col_name = t.col_name "
                + "    AND m.ts < t.ts "
                + " GROUP BY m.row_name, m.col_name ";
        query = wrapQueryWithIncludeValue("GET_LATEST_CELLS", query, includeValue);
        return addCellTsArgs(new FullQuery(query), cells);
    }

    @Override
    public FullQuery getAllCellQuery(Cell cell, long ts, boolean includeValue) {
        String query = " /* GET_ALL_CELL (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ")
                + "   FROM " + prefixedTableName() + " m "
                + "  WHERE m.row_name = ? "
                + "    AND m.col_name = ? "
                + "    AND m.ts < ? ";
        return new FullQuery(query).withArgs(cell.getRowName(), cell.getColumnName(), ts);
    }

    @Override
    public FullQuery getAllCellsQuery(Iterable<Cell> cells, long ts, boolean includeValue) {
        String query = " /* GET_ALL_CELLS (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ")
                + "   FROM " + prefixedTableName() + " m,"
                + "     (VALUES " + groupOfNumParams(2, Iterables.size(cells)) + ") t(row_name, col_name) "
                + "  WHERE m.row_name = t.row_name "
                + "    AND m.col_name = t.col_name "
                + "    AND m.ts < ? ";
        return addCellArgs(new FullQuery(query), cells).withArg(ts);
    }

    @Override
    public FullQuery getAllCellsQuery(Collection<Map.Entry<Cell, Long>> cells, boolean includeValue) {
        String query = " /* GET_ALL_CELLS (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, m.ts" + (includeValue ? ", m.val " : " ")
                + "   FROM " + prefixedTableName() + " m,"
                + "     (VALUES " + groupOfNumParams(3, Iterables.size(cells)) + ") t(row_name, col_name, ts) "
                + "  WHERE m.row_name = t.row_name "
                + "    AND m.col_name = t.col_name "
                + "    AND m.ts < t.ts ";
        return addCellTsArgs(new FullQuery(query), cells);
    }

    @Override
    public FullQuery getRangeQuery(RangeRequest range, long _ts, int maxRows) {
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
        String query = " /* GET_RANGE_ROWS (" + tableName + ") */ "
                + " SELECT DISTINCT m.row_name "
                + " FROM " + prefixedTableName() + " m "
                + (bounds.isEmpty() ? "" : " WHERE  " + Joiner.on(" AND ").join(bounds))
                + " ORDER BY m.row_name " + (range.isReverse() ? "DESC" : "ASC")
                + " LIMIT " + maxRows;
        return new FullQuery(query).withArgs(args);
    }

    @Override
    public boolean hasOverflowValues() {
        return false;
    }

    private String numParams(int numParams) {
        StringBuilder builder = new StringBuilder(2 * numParams + 1).append('(');
        Joiner.on(',').appendTo(builder, Iterables.limit(Iterables.cycle('?'), numParams));
        return builder.append(')').toString();
    }

    private String groupOfNumParams(int numParams, int numEntries) {
        String params = numParams(numParams);
        return Joiner.on(',').join(Iterables.limit(Iterables.cycle(params), numEntries));
    }

    private String wrapQueryWithIncludeValue(String wrappedName, String query, boolean includeValue) {
        if (!includeValue) {
            return query;
        }
        return " /* " + wrappedName + " (" + tableName + ") */ "
                + " SELECT wrap.row_name, wrap.col_name, wrap.ts, wrap.val "
                + " FROM " + prefixedTableName() + " wrap, ( " + query + " ) i "
                + " WHERE wrap.row_name = i.row_name "
                + "   AND wrap.col_name = i.col_name "
                + "   AND wrap.ts = i.ts ";
    }

    private FullQuery addRowTsArgs(FullQuery fullQuery, Iterable<Map.Entry<byte[], Long>> rows) {
        for (Map.Entry<byte[], Long> entry : rows) {
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

    private FullQuery addCellTsArgs(FullQuery fullQuery, Collection<Map.Entry<Cell, Long>> cells) {
        for (Map.Entry<Cell, Long> entry : cells) {
            Cell cell = entry.getKey();
            fullQuery.withArgs(cell.getRowName(), cell.getColumnName(), entry.getValue());
        }
        return fullQuery;
    }

    private String prefixedTableName() {
        return config.tablePrefix() + tableName;
    }

    @Override
    public FullQuery getRowsColumnRangeCountsQuery(
            Iterable<byte[]> rows, long ts, ColumnRangeSelection columnRangeSelection) {
        String query = " /* GET_ROWS_COLUMN_RANGE_COUNT(" + tableName + ") */"
                + " SELECT m.row_name, COUNT(m.col_name) AS column_count "
                + "   FROM " + prefixedTableName() + " m "
                + "  WHERE m.row_name IN " + numParams(Iterables.size(rows))
                + "    AND m.ts < ? "
                + (columnRangeSelection.getStartCol().length > 0 ? " AND m.col_name >= ?" : "")
                + (columnRangeSelection.getEndCol().length > 0 ? " AND m.col_name < ?" : "")
                + " GROUP BY m.row_name";
        FullQuery fullQuery = new FullQuery(query).withArgs(rows).withArg(ts);
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
                + " SELECT m.row_name, m.col_name, max(m.ts) as ts"
                + "   FROM " + prefixedTableName() + " m "
                + "  WHERE m.row_name = ? "
                + "    AND m.ts < ? "
                + (columnRangeSelection.getStartCol().length > 0 ? " AND m.col_name >= ?" : "")
                + (columnRangeSelection.getEndCol().length > 0 ? " AND m.col_name < ?" : "")
                + " GROUP BY m.row_name, m.col_name"
                + " ORDER BY m.col_name ASC LIMIT " + columnRangeSelection.getBatchHint();
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

    @Override
    protected FullQuery getRowsColumnRangeFullyLoadedRowsSubQuery(
            List<byte[]> rows, long ts, ColumnRangeSelection columnRangeSelection) {
        String query = " /* GET_ROWS_COLUMN_RANGE_FULLY_LOADED_ROW (" + tableName + ") */ "
                + " SELECT m.row_name, m.col_name, max(m.ts) as ts"
                + "   FROM " + prefixedTableName() + " m "
                + "  WHERE m.row_name IN " + numParams(Iterables.size(rows))
                + "    AND m.ts < ? "
                + (columnRangeSelection.getStartCol().length > 0 ? " AND m.col_name >= ?" : "")
                + (columnRangeSelection.getEndCol().length > 0 ? " AND m.col_name < ?" : "")
                + " GROUP BY m.row_name, m.col_name"
                + " ORDER BY m.row_name ASC, m.col_name ASC";
        String wrappedQuery = wrapQueryWithIncludeValue("GET_ROWS_COLUMN_RANGE_FULLY_LOADED_ROW", query, true);
        FullQuery fullQuery = new FullQuery(wrappedQuery).withArgs(rows).withArg(ts);
        if (columnRangeSelection.getStartCol().length > 0) {
            fullQuery = fullQuery.withArg(columnRangeSelection.getStartCol());
        }
        if (columnRangeSelection.getEndCol().length > 0) {
            fullQuery = fullQuery.withArg(columnRangeSelection.getEndCol());
        }
        return fullQuery;
    }
}
