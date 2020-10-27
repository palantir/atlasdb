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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DbReadTable {
    private static final int MAX_ROW_COLUMN_RANGES_FETCH_SIZE = 1000;

    private final ConnectionSupplier conns;
    private final DbQueryFactory queryFactory;

    public DbReadTable(ConnectionSupplier conns, DbQueryFactory queryFactory) {
        this.conns = conns;
        this.queryFactory = queryFactory;
    }

    public ConnectionSupplier getConnectionSupplier() {
        return conns;
    }

    public ClosableIterator<AgnosticLightResultRow> getLatestRows(
            Iterable<byte[]> rows, ColumnSelection columns, long ts, boolean includeValues) {
        if (columns.noColumnsSelected()) {
            return ClosableIterators.emptyImmutableClosableIterator();
        } else if (isSingleton(rows)) {
            byte[] row = Iterables.getOnlyElement(rows);
            return run(queryFactory.getLatestRowQuery(row, ts, columns, includeValues));
        } else {
            return run(queryFactory.getLatestRowsQuery(rows, ts, columns, includeValues));
        }
    }

    public ClosableIterator<AgnosticLightResultRow> getAllRows(
            Iterable<byte[]> rows, ColumnSelection columns, long ts, boolean includeValues) {
        return getAllRows(rows, columns, ts, includeValues, Order.UNDEFINED);
    }

    public ClosableIterator<AgnosticLightResultRow> getAllRows(
            Iterable<byte[]> rows, ColumnSelection columns, long ts, boolean includeValues, Order order) {
        if (columns.noColumnsSelected()) {
            return ClosableIterators.emptyImmutableClosableIterator();
        }
        FullQuery query;
        if (isSingleton(rows)) {
            byte[] row = Iterables.getOnlyElement(rows);
            query = queryFactory.getAllRowQuery(row, ts, columns, includeValues);
        } else {
            query = queryFactory.getAllRowsQuery(rows, ts, columns, includeValues);
        }
        return run(addOrdering(order, query));
    }

    private FullQuery addOrdering(Order order, FullQuery query) {
        return new FullQuery(query.getQuery() + order).withArgs(Arrays.asList(query.getArgs()));
    }

    public ClosableIterator<AgnosticLightResultRow> getLatestCells(Map<Cell, Long> cells, boolean includeValue) {
        if (cells.size() == 1) {
            Map.Entry<Cell, Long> onlyEntry = Iterables.getOnlyElement(cells.entrySet());
            return run(queryFactory.getLatestCellQuery(onlyEntry.getKey(), onlyEntry.getValue(), includeValue));
        } else {
            return run(queryFactory.getLatestCellsQuery(cells.entrySet(), includeValue));
        }
    }

    public ClosableIterator<AgnosticLightResultRow> getAllCells(Iterable<Cell> cells, long ts, boolean includeValue) {
        if (isSingleton(cells)) {
            Cell cell = Iterables.getOnlyElement(cells);
            return run(queryFactory.getAllCellQuery(cell, ts, includeValue));
        } else {
            return run(queryFactory.getAllCellsQuery(cells, ts, includeValue));
        }
    }

    public ClosableIterator<AgnosticLightResultRow> getRange(RangeRequest range, long ts, int maxRows) {
        FullQuery query = queryFactory.getRangeQuery(range, ts, maxRows);
        AgnosticLightResultSet results =
                conns.get().selectLightResultSetUnregisteredQuery(query.getQuery(), query.getArgs());
        results.setFetchSize(maxRows);
        return ClosableIterators.wrap(results.iterator(), results);
    }

    public ClosableIterator<AgnosticLightResultRow> getRowsColumnRangeCounts(
            List<byte[]> rows, long ts, ColumnRangeSelection columnRangeSelection) {
        if (rows.isEmpty()) {
            return ClosableIterators.emptyImmutableClosableIterator();
        } else {
            FullQuery query = queryFactory.getRowsColumnRangeCountsQuery(rows, ts, columnRangeSelection);
            AgnosticLightResultSet results =
                    conns.get().selectLightResultSetUnregisteredQuery(query.getQuery(), query.getArgs());
            results.setFetchSize(Math.min(rows.size(), MAX_ROW_COLUMN_RANGES_FETCH_SIZE));
            return ClosableIterators.wrap(results.iterator(), results);
        }
    }

    public ClosableIterator<AgnosticLightResultRow> getRowsColumnRange(
            Map<byte[], BatchColumnRangeSelection> columnRangeSelectionsByRow, long ts) {
        if (columnRangeSelectionsByRow.isEmpty()) {
            return ClosableIterators.emptyImmutableClosableIterator();
        } else {
            FullQuery query = queryFactory.getRowsColumnRangeQuery(columnRangeSelectionsByRow, ts);
            AgnosticLightResultSet results =
                    conns.get().selectLightResultSetUnregisteredQuery(query.getQuery(), query.getArgs());
            int totalSize = columnRangeSelectionsByRow.values().stream()
                    .mapToInt(BatchColumnRangeSelection::getBatchHint)
                    .sum();
            results.setFetchSize(Math.min(totalSize, MAX_ROW_COLUMN_RANGES_FETCH_SIZE));
            return ClosableIterators.wrap(results.iterator(), results);
        }
    }

    public ClosableIterator<AgnosticLightResultRow> getRowsColumnRange(
            RowsColumnRangeBatchRequest rowsColumnRangeBatch, long ts) {
        FullQuery query = queryFactory.getRowsColumnRangeQuery(rowsColumnRangeBatch, ts);
        AgnosticLightResultSet results =
                conns.get().selectLightResultSetUnregisteredQuery(query.getQuery(), query.getArgs());
        results.setFetchSize(MAX_ROW_COLUMN_RANGES_FETCH_SIZE);
        return ClosableIterators.wrap(results.iterator(), results);
    }

    public boolean hasOverflowValues() {
        return queryFactory.hasOverflowValues();
    }

    private boolean isSingleton(Iterable<?> iterable) {
        Iterator<?> iter = iterable.iterator();
        if (!iter.hasNext()) {
            return false;
        }
        iter.next();
        return !iter.hasNext();
    }

    private ClosableIterator<AgnosticLightResultRow> run(FullQuery query) {
        AgnosticLightResultSet results =
                conns.get().selectLightResultSetUnregisteredQuery(query.getQuery(), query.getArgs());
        return ClosableIterators.wrap(results.iterator(), results);
    }

    public enum Order {
        UNDEFINED(""),
        ASCENDING(" ORDER BY m.row_name ASC, m.col_name, m.ts"),
        DESCENDING(" ORDER BY m.row_name DESC, m.col_name, m.ts");

        private final String stringValue;

        Order(String value) {
            stringValue = value;
        }

        static Order fromBoolean(boolean isReverse) {
            return isReverse ? DESCENDING : ASCENDING;
        }

        @Override
        public String toString() {
            return stringValue;
        }
    }
}
