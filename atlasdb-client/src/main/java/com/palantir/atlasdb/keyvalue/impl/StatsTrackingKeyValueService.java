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
package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ForwardingClosableIterator;
import com.palantir.common.collect.MapEntries;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
@SuppressWarnings("checkstyle:all") // too many warnings to fix
public class StatsTrackingKeyValueService extends ForwardingKeyValueService {
    public static class TableStats {
        final AtomicLong totalGetValueBytes = new AtomicLong(0L);
        final AtomicLong totalPutValueBytes = new AtomicLong(0L);
        final AtomicLong totalGetCellBytes = new AtomicLong(0L);
        final AtomicLong totalPutCellBytes = new AtomicLong(0L);
        final AtomicLong totalGetCells = new AtomicLong(0L);
        final AtomicLong totalPutCells = new AtomicLong(0L);
        final AtomicLong totalGetMillis = new AtomicLong(0L);
        final AtomicLong totalPutMillis = new AtomicLong(0L);
        final AtomicLong totalGetCalls = new AtomicLong(0L);
        final AtomicLong totalPutCalls = new AtomicLong(0L);

        public long getTotalGetValueBytes() {
            return totalGetValueBytes.get();
        }

        public long getTotalPutValueBytes() {
            return totalPutValueBytes.get();
        }

        public long getTotalGetCellBytes() {
            return totalGetCellBytes.get();
        }

        public long getTotalPutCellBytes() {
            return totalPutCellBytes.get();
        }

        public long getTotalGetCells() {
            return totalGetCells.get();
        }

        public long getTotalPutCells() {
            return totalPutCells.get();
        }

        public long getTotalGetMillis() {
            return totalGetMillis.get();
        }

        public long getTotalPutMillis() {
            return totalPutMillis.get();
        }

        public long getTotalGetBytes() {
            return getTotalGetCellBytes() + getTotalGetValueBytes();
        }

        public long getTotalPutBytes() {
            return getTotalPutCellBytes() + getTotalPutValueBytes();
        }

        public long getTotalGetCalls() {
            return totalGetCalls.get();
        }

        public long getTotalPutCalls() {
            return totalPutCalls.get();
        }

        public void add(TableStats other) {
            totalGetValueBytes.addAndGet(other.totalGetValueBytes.get());
            totalPutValueBytes.addAndGet(other.totalPutValueBytes.get());
            totalGetCellBytes.addAndGet(other.totalGetCellBytes.get());
            totalPutCellBytes.addAndGet(other.totalPutCellBytes.get());
            totalGetCells.addAndGet(other.totalGetCells.get());
            totalPutCells.addAndGet(other.totalPutCells.get());
            totalGetMillis.addAndGet(other.totalGetMillis.get());
            totalPutMillis.addAndGet(other.totalPutMillis.get());
            totalGetCalls.addAndGet(other.totalGetCalls.get());
            totalPutCalls.addAndGet(other.totalPutCalls.get());
        }
    }

    private final ConcurrentMap<TableReference, TableStats> statsByTableName = new ConcurrentHashMap<>();

    private final KeyValueService delegate;

    public Map<TableReference, TableStats> getTableStats() {
        return Collections.unmodifiableMap(statsByTableName);
    }

    public TableStats getAggregateTableStats() {
        TableStats r = new TableStats();
        for (TableStats s : statsByTableName.values()) {
            r.add(s);
        }
        return r;
    }

    public StatsTrackingKeyValueService(KeyValueService delegate) {
        this.delegate = delegate;
    }

    public void reset() {
        statsByTableName.clear();
    }

    @Override
    public KeyValueService delegate() {
        return delegate;
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        long start = System.currentTimeMillis();
        Map<Cell, Value> r = super.get(tableRef, timestampByCell);
        long finish = System.currentTimeMillis();

        // Update stats only after successful get.
        TableStats s = getTableStats(tableRef);
        long cellBytes = 0;
        for (Cell cell : timestampByCell.keySet()) {
            cellBytes += cell.getRowName().length;
            cellBytes += cell.getColumnName().length;
        }
        s.totalGetCellBytes.addAndGet(cellBytes);
        s.totalGetMillis.addAndGet(finish - start);
        s.totalGetCalls.incrementAndGet();
        updateGetStats(s, r);

        return r;
    }

    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection, long timestamp) {
        long start = System.currentTimeMillis();
        Map<Cell, Value> r = super.getRows(tableRef, rows, columnSelection, timestamp);
        long finish = System.currentTimeMillis();

        // Update stats only after successful get.
        TableStats s = getTableStats(tableRef);
        for (byte[] row : rows) {
            s.totalGetCellBytes.addAndGet(row.length);
        }
        s.totalGetMillis.addAndGet(finish - start);
        s.totalGetCalls.incrementAndGet();
        updateGetStats(s, r);

        return r;
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            final TableReference tableRef, RangeRequest range, long timestamp) {
        final TableStats s = getTableStats(tableRef);

        long start = System.currentTimeMillis();
        final ClosableIterator<RowResult<Value>> it = super.getRange(tableRef, range, timestamp);
        long finish = System.currentTimeMillis();
        s.totalGetMillis.addAndGet(finish - start);
        s.totalGetCalls.incrementAndGet();

        return new ForwardingClosableIterator<RowResult<Value>>() {
            @Override
            protected ClosableIterator<RowResult<Value>> delegate() {
                return it;
            }

            @Override
            public RowResult<Value> next() {
                long begin = System.currentTimeMillis();
                RowResult<Value> ret = super.next();
                long end = System.currentTimeMillis();
                s.totalGetMillis.addAndGet(end - begin);
                updateGetStats(s, MapEntries.toMap(ret.getCells()));
                return ret;
            }
        };
    }

    private void updateGetStats(TableStats s, Map<Cell, Value> r) {
        s.totalGetCells.addAndGet(r.size());
        long totalSize = 0;
        for (Map.Entry<Cell, Value> e : r.entrySet()) {
            totalSize += e.getValue().getContents().length;
        }
        s.totalGetValueBytes.addAndGet(totalSize);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        TableStats s = getTableStats(tableRef);

        long start = System.currentTimeMillis();
        super.put(tableRef, values, timestamp);
        long finish = System.currentTimeMillis();
        s.totalPutMillis.addAndGet(finish - start);
        s.totalPutCalls.incrementAndGet();

        // Only update stats after put was successful.
        s.totalPutCells.addAndGet(values.size());
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            incrementPutBytes(s, e.getKey(), e.getValue());
        }
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        long start = System.currentTimeMillis();
        super.multiPut(valuesByTable, timestamp);
        long finish = System.currentTimeMillis();
        for (Map.Entry<TableReference, ? extends Map<Cell, byte[]>> entry : valuesByTable.entrySet()) {
            TableReference tableRef = entry.getKey();
            Map<Cell, byte[]> values = entry.getValue();
            TableStats s = getTableStats(tableRef);
            s.totalPutMillis.addAndGet(finish - start);
            s.totalPutCalls.incrementAndGet();

            // Only update stats after put was successful.
            s.totalPutCells.addAndGet(values.size());
            for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
                incrementPutBytes(s, e.getKey(), e.getValue());
            }
        }
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        TableStats s = getTableStats(tableRef);

        long start = System.currentTimeMillis();
        super.putWithTimestamps(tableRef, values);
        long finish = System.currentTimeMillis();
        s.totalPutMillis.addAndGet(finish - start);
        s.totalPutCalls.incrementAndGet();

        // Only update stats after put was successful.
        s.totalPutCells.addAndGet(values.size());
        for (Map.Entry<Cell, Value> e : values.entries()) {
            incrementPutBytes(s, e.getKey(), e.getValue().getContents());
        }
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        TableStats s = timeOperation(tableRef, () -> super.putUnlessExists(tableRef, values));

        // Only update stats after put was successful.
        s.totalPutCells.addAndGet(values.size());
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            incrementPutBytes(s, e.getKey(), e.getValue());
        }
    }

    @Override
    public void putToCasTable(TableReference tableRef, Map<Cell, byte[]> values) {
        TableStats s = timeOperation(tableRef, () -> super.putToCasTable(tableRef, values));

        // Only update stats after put was successful.
        s.totalPutCells.addAndGet(values.size());
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            incrementPutBytes(s, e.getKey(), e.getValue());
        }
    }

    @Override
    public void checkAndSet(CheckAndSetRequest request) {
        TableStats s = timeOperation(request.table(), () -> super.checkAndSet(request));

        // Only update stats after put was successful.
        s.totalPutCells.incrementAndGet(); // can only CAS one value
        incrementPutBytes(s, request.cell(), request.newValue());
    }

    private TableStats getTableStats(TableReference tableRef) {
        TableStats s = statsByTableName.get(tableRef);
        if (s == null) {
            statsByTableName.putIfAbsent(tableRef, new TableStats());
            s = statsByTableName.get(tableRef);
        }
        return s;
    }

    private TableStats timeOperation(TableReference tableRef, Runnable operation) {
        TableStats s = getTableStats(tableRef);

        long start = System.currentTimeMillis();
        operation.run();
        long finish = System.currentTimeMillis();
        s.totalPutMillis.addAndGet(finish - start);
        s.totalPutCalls.incrementAndGet();
        return s;
    }

    private void incrementPutBytes(TableStats s, Cell cell, byte[] value) {
        s.totalPutCellBytes.addAndGet(cell.getRowName().length);
        s.totalPutCellBytes.addAndGet(cell.getColumnName().length);
        s.totalPutValueBytes.addAndGet(value.length);
    }

    public void dumpStats(PrintWriter writer) {
        Map<TableReference, TableStats> sortedStats = ImmutableSortedMap.copyOf(statsByTableName);
        String headerFmt = "|| %-20s || %10s || %10s || %10s || %10s || %10s || %10s ||%n";
        String rowFmt = "|  %-20s |  %10s |  %10s |  %10s |  %10s |  %10s |  %10s |%n";

        writer.printf(
                headerFmt, "table", "get_millis", "put_millis", "get_bytes", "put_bytes", "get_calls", "put_calls");

        for (Map.Entry<TableReference, TableStats> statsEntry : sortedStats.entrySet()) {
            TableStats s = statsEntry.getValue();
            writer.printf(
                    rowFmt,
                    statsEntry.getKey().getQualifiedName(),
                    s.getTotalGetMillis(),
                    s.getTotalPutMillis(),
                    s.getTotalGetBytes(),
                    s.getTotalPutBytes(),
                    s.getTotalGetCalls(),
                    s.getTotalPutCalls());
        }

        TableStats s = getAggregateTableStats();
        writer.printf(
                rowFmt,
                "(total)",
                s.getTotalGetMillis(),
                s.getTotalPutMillis(),
                s.getTotalGetBytes(),
                s.getTotalPutBytes(),
                s.getTotalGetCalls(),
                s.getTotalPutCalls());
    }
}
