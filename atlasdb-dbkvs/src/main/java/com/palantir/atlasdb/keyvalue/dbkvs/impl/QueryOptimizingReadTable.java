/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;


import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.cache.Cache;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.common.base.ClosableIterator;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;

public class QueryOptimizingReadTable implements DbReadTable {
    private static final int TRACE_FREQUENCY = 101;
    private final DbReadTable delegate;
    private final String tableName;
    private final Cache<String, Boolean> hasManyOverwrites;
    private final AtomicInteger queryCount = new AtomicInteger();

    public QueryOptimizingReadTable(DbReadTable delegate,
            String tableName,
            Cache<String, Boolean> hasManyOverwrites) {
        this.delegate = delegate;
        this.tableName = tableName;
        this.hasManyOverwrites = hasManyOverwrites;
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestRows(Iterable<byte[]> rows,
            ColumnSelection columns,
            long ts,
            boolean includeValue) {
        return hasManyOverwrites()
                ? delegate.getLatestRows(rows, columns, ts, includeValue)
                : trace(delegate.getAllRows(rows, columns, ts, includeValue));
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestRows(Map<byte[], Long> rows,
            ColumnSelection columns,
            boolean includeValue) {
        return hasManyOverwrites()
                ? delegate.getLatestRows(rows, columns, includeValue)
                : trace(delegate.getAllRows(rows, columns, includeValue));
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllRows(Iterable<byte[]> rows,
            ColumnSelection columns,
            long ts,
            boolean includeValue) {
        return delegate.getAllRows(rows, columns, ts, includeValue);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllRows(Map<byte[], Long> rows,
            ColumnSelection columns,
            boolean includeValue) {
        return delegate.getAllRows(rows, columns, includeValue);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestCells(Iterable<Cell> cells,
            long ts,
            boolean includeValue) {
        return hasManyOverwrites()
                ? delegate.getLatestCells(cells, ts, includeValue)
                : trace(delegate.getAllCells(cells, ts, includeValue));
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getLatestCells(Map<Cell, Long> cells,
            boolean includeValue) {
        return hasManyOverwrites()
                ? delegate.getLatestCells(cells, includeValue)
                : trace(delegate.getAllCells(cells, includeValue));
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllCells(Iterable<Cell> cells,
            long ts,
            boolean includeValue) {
        return delegate.getAllCells(cells, ts, includeValue);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getAllCells(Map<Cell, Long> cells,
            boolean includeValue) {
        return delegate.getAllCells(cells, includeValue);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getRange(RangeRequest range,
            long ts,
            int maxRows) {
        return delegate.getRange(range, ts, maxRows);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getRowsColumnRangeCounts(List<byte[]> rows, long ts,
            ColumnRangeSelection columnRangeSelection) {
        return delegate.getRowsColumnRangeCounts(rows, ts, columnRangeSelection);
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getRowsColumnRange(
            Map<byte[], BatchColumnRangeSelection> columnRangeSelectionsByRow, long ts) {
        return delegate.getRowsColumnRange(columnRangeSelectionsByRow, ts);
    }

    @Override
    public boolean hasOverflowValues() {
        return delegate.hasOverflowValues();
    }

    @Override
    public ClosableIterator<AgnosticLightResultRow> getOverflow(Collection<OverflowValue> overflowIds) {
        return delegate.getOverflow(overflowIds);
    }

    private boolean hasManyOverwrites() {
        Boolean manyOverwrites = hasManyOverwrites.getIfPresent(tableName);
        return manyOverwrites != null && manyOverwrites;
    }

    private ClosableIterator<AgnosticLightResultRow> trace(final ClosableIterator<AgnosticLightResultRow> rows) {
        if (queryCount.incrementAndGet() % TRACE_FREQUENCY != 0) {
            return rows;
        }
        return new ClosableIterator<AgnosticLightResultRow>() {
            private int count;
            private Set<byte[]> uniqueRows = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());

            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }

            @Override
            @SuppressWarnings("deprecation")
            public AgnosticLightResultRow next() {
                AgnosticLightResultRow row = rows.next();
                uniqueRows.add(row.getBytes("row_name"));
                count++;
                return row;
            }

            @Override
            public void remove() {
                rows.remove();
            }

            @Override
            public void close() {
                rows.close();
                if (count > 4 * uniqueRows.size()) {
                    hasManyOverwrites.put(tableName, true);
                }
            }
        };
    }
}
