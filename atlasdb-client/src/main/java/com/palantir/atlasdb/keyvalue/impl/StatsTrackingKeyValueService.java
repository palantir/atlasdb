/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.impl;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.api.Write;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ForwardingClosableIterator;
import com.palantir.common.collect.MapEntries;

@ThreadSafe
@SuppressWarnings("checkstyle:all") // too many warnings to fix
public class StatsTrackingKeyValueService extends ForwardingKeyValueService {
    public static class TableStats {
        final LongAdder totalGetValueBytes = new LongAdder();
        final LongAdder totalPutValueBytes = new LongAdder();
        final LongAdder totalGetCellBytes = new LongAdder();
        final LongAdder totalPutCellBytes = new LongAdder();
        final LongAdder totalGetCells = new LongAdder();
        final LongAdder totalPutCells = new LongAdder();
        final LongAdder totalGetMillis = new LongAdder();
        final LongAdder totalPutMillis = new LongAdder();
        final LongAdder totalGetCalls = new LongAdder();
        final LongAdder totalPutCalls = new LongAdder();

        public long getTotalGetValueBytes() { return totalGetValueBytes.sum(); }
        public long getTotalPutValueBytes() { return totalPutValueBytes.sum(); }
        public long getTotalGetCellBytes() { return totalGetCellBytes.sum(); }
        public long getTotalPutCellBytes() { return totalPutCellBytes.sum(); }
        public long getTotalGetCells() { return totalGetCells.sum(); }
        public long getTotalPutCells() { return totalPutCells.sum(); }
        public long getTotalGetMillis() { return totalGetMillis.sum(); }
        public long getTotalPutMillis() { return totalPutMillis.sum(); }
        public long getTotalGetBytes() { return getTotalGetCellBytes() + getTotalGetValueBytes(); }
        public long getTotalPutBytes() { return getTotalPutCellBytes() + getTotalPutValueBytes(); }
        public long getTotalGetCalls() { return totalGetCalls.sum(); }
        public long getTotalPutCalls() { return totalPutCalls.sum(); }

        public void add(TableStats other) {
            totalGetValueBytes.add(other.totalGetValueBytes.sum());
            totalPutValueBytes.add(other.totalPutValueBytes.sum());
            totalGetCellBytes.add(other.totalGetCellBytes.sum());
            totalPutCellBytes.add(other.totalPutCellBytes.sum());
            totalGetCells.add(other.totalGetCells.sum());
            totalPutCells.add(other.totalPutCells.sum());
            totalGetMillis.add(other.totalGetMillis.sum());
            totalPutMillis.add(other.totalPutMillis.sum());
            totalGetCalls.add(other.totalGetCalls.sum());
            totalPutCalls.add(other.totalPutCalls.sum());
        }
    }

    private final ConcurrentMap<TableReference, TableStats> statsByTableName = Maps.newConcurrentMap();

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
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef,
                                Map<Cell, Long> timestampByCell) {
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
        s.totalGetCellBytes.add(cellBytes);
        s.totalGetMillis.add(finish - start);
        s.totalGetCalls.increment();
        updateGetStats(s, r);

        return r;
    }

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef,
                                    Iterable<byte[]> rows,
                                    ColumnSelection columnSelection,
                                    long timestamp) {
        long start = System.currentTimeMillis();
        Map<Cell, Value> r = super.getRows(tableRef, rows, columnSelection, timestamp);
        long finish = System.currentTimeMillis();

        // Update stats only after successful get.
        TableStats s = getTableStats(tableRef);
        for (byte[] row : rows) {
            s.totalGetCellBytes.add(row.length);
        }
        s.totalGetMillis.add(finish - start);
        s.totalGetCalls.increment();
        updateGetStats(s, r);

        return r;
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(final TableReference tableRef, RangeRequest range,
                                                       long timestamp) {
        final TableStats s = getTableStats(tableRef);

        long start = System.currentTimeMillis();
        final ClosableIterator<RowResult<Value>> it = super.getRange(tableRef, range, timestamp);
        long finish = System.currentTimeMillis();
        s.totalGetMillis.add(finish - start);
        s.totalGetCalls.increment();

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
                s.totalGetMillis.add(end - begin);
                updateGetStats(s, MapEntries.toMap(ret.getCells()));
                return ret;
            }
        };
    }

    private void updateGetStats(TableStats s, Map<Cell, Value> r) {
        s.totalGetCells.add(r.size());
        long totalSize = 0;
        for (Map.Entry<Cell, Value> e : r.entrySet()) {
            totalSize += e.getValue().getContents().length;
        }
        s.totalGetValueBytes.add(totalSize);
    }

    @Override
    public void put(Stream<Write> writes) {
        Multiset<TableReference> tables = HashMultiset.create();
        Multiset<TableReference> cellBytes = HashMultiset.create();
        Multiset<TableReference> valueBytes = HashMultiset.create();
        long start = System.currentTimeMillis();
        super.put(writes.peek(write -> {
            tables.add(write.table());
            cellBytes.add(write.table(), write.cell().getColumnName().length + write.cell().getRowName().length);
            valueBytes.add(write.table(), write.value().length);
        }));
        long finish = System.currentTimeMillis();
        tables.forEach(table -> {
            int count = tables.count(table);
            TableStats s = getTableStats(table);
            s.totalPutMillis.add(finish - start);
            s.totalPutCalls.increment();

            s.totalPutCells.add(count);
            s.totalPutCellBytes.add(cellBytes.count(table));
            s.totalPutValueBytes.add(valueBytes.count(table));
        });
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        TableStats s = timeOperation(tableRef, () -> super.putUnlessExists(tableRef, values));

        // Only update stats after put was successful.
        s.totalPutCells.add(values.size());
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            incrementPutBytes(s, e.getKey(), e.getValue());
        }
    }

    @Override
    public void checkAndSet(CheckAndSetRequest request) {
        TableStats s = timeOperation(request.table(), () -> super.checkAndSet(request));

        // Only update stats after put was successful.
        s.totalPutCells.increment(); // can only CAS one value
        incrementPutBytes(s, request.cell(), request.newValue());
    }

    private TableStats getTableStats(TableReference tableRef) {
        return statsByTableName.computeIfAbsent(tableRef, unused -> new TableStats());
    }

    private TableStats timeOperation(TableReference tableRef, Runnable operation) {
        TableStats s = getTableStats(tableRef);

        long start = System.currentTimeMillis();
        operation.run();
        long finish = System.currentTimeMillis();
        s.totalPutMillis.add(finish - start);
        s.totalPutCalls.increment();
        return s;
    }

    private void incrementPutBytes(TableStats s, Cell cell, byte[] value) {
        s.totalPutCellBytes.add(cell.getRowName().length);
        s.totalPutCellBytes.add(cell.getColumnName().length);
        s.totalPutValueBytes.add(value.length);
    }

    public void dumpStats(PrintWriter writer) {
        Map<TableReference, TableStats> sortedStats = ImmutableSortedMap.copyOf(statsByTableName);
        String headerFmt = "|| %-20s || %10s || %10s || %10s || %10s || %10s || %10s ||%n";
        String rowFmt =    "|  %-20s |  %10s |  %10s |  %10s |  %10s |  %10s |  %10s |%n";

        writer.printf(
                headerFmt,
                "table",
                "get_millis",
                "put_millis",
                "get_bytes",
                "put_bytes",
                "get_calls",
                "put_calls");

        for (Entry<TableReference, TableStats> statsEntry : sortedStats.entrySet()) {
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
