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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.ArrayUtils;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.remoting2.tracing.Tracers;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * In-memory (non-durable) key-value store implementation.
 * <p>
 * Intended for testing use only.
 */
@ThreadSafe
public class InMemoryKeyValueService extends AbstractKeyValueService {
    private final ConcurrentMap<TableReference, Table> tables = Maps.newConcurrentMap();
    private final ConcurrentMap<TableReference, byte[]> tableMetadata = Maps.newConcurrentMap();
    private volatile boolean createTablesAutomatically;

    public InMemoryKeyValueService(boolean createTablesAutomatically) {
        this(createTablesAutomatically,
                Tracers.wrap(PTExecutors.newFixedThreadPool(16, PTExecutors.newNamedThreadFactory(true))));
    }

    public InMemoryKeyValueService(boolean createTablesAutomatically,
                                   ExecutorService executor) {
        super(executor);
        this.createTablesAutomatically = createTablesAutomatically;
    }

    @Override
    public Map<Cell, Value> getRows(TableReference tableRef, Iterable<byte[]> rows,
                                    ColumnSelection columnSelection, long timestamp) {
        Map<Cell, Value> result = Maps.newHashMap();
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableRef).entries;

        for (byte[] row : rows) {
            Cell rowBegin = Cells.createSmallestCellForRow(row);
            Cell rowEnd = Cells.createLargestCellForRow(row);
            PeekingIterator<Entry<Key, byte[]>> entries = Iterators.peekingIterator(table.subMap(
                    new Key(rowBegin, Long.MIN_VALUE), new Key(rowEnd, timestamp)).entrySet().iterator());
            while (entries.hasNext()) {
                Entry<Key, byte[]> entry = entries.peek();
                Key key = entry.getKey();
                Iterator<Entry<Key, byte[]>> cellIter = takeCell(entries, key);
                if (columnSelection.contains(key.col)) {
                    getLatestVersionOfCell(row, key, cellIter, timestamp, result);
                }
                Iterators.size(cellIter);
            }
        }

        return result;
    }

    private void getLatestVersionOfCell(byte[] row, Key key, Iterator<Entry<Key, byte[]>> cellIter, long timestamp,
                                        @Output Map<Cell, Value> result) {
        Entry<Key, byte[]> lastEntry = null;
        while (cellIter.hasNext()) {
            Entry<Key, byte[]> curEntry = cellIter.next();
            if (curEntry.getKey().ts >= timestamp) {
                break;
            }
            lastEntry = curEntry;
        }
        if (lastEntry != null) {
            long ts = lastEntry.getKey().ts;
            byte[] value = lastEntry.getValue();
            result.put(Cell.create(row, key.col), Value.createWithCopyOfData(value, ts));
        }
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableRef).entries;
        Map<Cell, Value> result = Maps.newHashMap();
        for (Map.Entry<Cell, Long> e : timestampByCell.entrySet()) {
            Cell cell = e.getKey();
            Entry<Key, byte[]> lastEntry = table.lowerEntry(new Key(cell, e.getValue()));
            if (lastEntry != null) {
                Key key = lastEntry.getKey();
                if (key.matchesCell(cell)) {
                    long ts = lastEntry.getKey().ts;
                    result.put(cell, Value.createWithCopyOfData(lastEntry.getValue(), ts));
                }
            }
        }
        return result;
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(this, tableRef, rangeRequests, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef,
            final RangeRequest range,
            final long timestamp) {
        return getRangeInternal(tableRef, range, new ResultProducer<Value>() {
            @Override
            public Value apply(Iterator<Entry<Key, byte[]>> entries) {
                Entry<Key, byte[]> lastEntry = null;
                while (entries.hasNext()) {
                    Entry<Key, byte[]> entry = entries.next();
                    if (entry.getKey().ts >= timestamp) {
                        break;
                    }
                    lastEntry = entry;
                }
                if (lastEntry != null) {
                    long ts = lastEntry.getKey().ts;
                    return Value.createWithCopyOfData(lastEntry.getValue(), ts);
                } else {
                    return null;
                }
            }
        });
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef,
            final RangeRequest range,
            final long timestamp) {
        return getRangeInternal(tableRef, range, new ResultProducer<Set<Long>>() {
            @Override
            public Set<Long> apply(Iterator<Entry<Key, byte[]>> entries) {
                Set<Long> timestamps = Sets.newTreeSet();
                while (entries.hasNext()) {
                    Entry<Key, byte[]> entry = entries.next();
                    Key key = entry.getKey();
                    if (key.ts >= timestamp) {
                        break;
                    }
                    timestamps.add(key.ts);
                }
                if (!timestamps.isEmpty()) {
                    return timestamps;
                } else {
                    return null;
                }
            }
        });
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        return new GetCandidateCellsForSweepingShim(this).getCandidateCellsForSweeping(tableRef, request);
    }

    private <T> ClosableIterator<RowResult<T>> getRangeInternal(TableReference tableRef,
                                                                final RangeRequest range,
                                                                final ResultProducer<T> resultProducer) {
        ConcurrentNavigableMap<Key, byte[]> tableMap = getTableMap(tableRef).entries;
        if (range.isReverse()) {
            tableMap = tableMap.descendingMap();
        }
        if (range.getStartInclusive().length != 0) {
            if (range.isReverse()) {
                Cell startCell = Cells.createLargestCellForRow(range.getStartInclusive());
                tableMap = tableMap.tailMap(new Key(startCell, Long.MIN_VALUE));
            } else {
                Cell startCell = Cells.createSmallestCellForRow(range.getStartInclusive());
                tableMap = tableMap.tailMap(new Key(startCell, Long.MIN_VALUE));
            }
        }
        if (range.getEndExclusive().length != 0) {
            if (range.isReverse()) {
                Cell endCell = Cells.createLargestCellForRow(range.getEndExclusive());
                tableMap = tableMap.headMap(new Key(endCell, Long.MAX_VALUE));
            } else {
                Cell endCell = Cells.createSmallestCellForRow(range.getEndExclusive());
                tableMap = tableMap.headMap(new Key(endCell, Long.MAX_VALUE));
            }
        }
        final PeekingIterator<Entry<Key, byte[]>> it = Iterators.peekingIterator(tableMap.entrySet().iterator());
        return ClosableIterators.wrap(new AbstractIterator<RowResult<T>>() {
            @Override
            protected RowResult<T> computeNext() {
                while (true) {
                    if (!it.hasNext()) {
                        return endOfData();
                    }
                    ImmutableSortedMap.Builder<byte[], T> result = ImmutableSortedMap.orderedBy(
                            UnsignedBytes.lexicographicalComparator());
                    Key key = it.peek().getKey();
                    byte[] row = key.row;
                    Iterator<Entry<Key, byte[]>> cellIter = takeCell(it, key);
                    collectValueForTimestamp(key.col, cellIter, result, range, resultProducer);

                    while (it.hasNext()) {
                        if (!it.peek().getKey().matchesRow(row)) {
                            break;
                        }
                        key = it.peek().getKey();
                        cellIter = takeCell(it, key);
                        collectValueForTimestamp(key.col, cellIter, result, range, resultProducer);
                    }
                    SortedMap<byte[], T> columns = result.build();
                    if (!columns.isEmpty()) {
                        return RowResult.create(row, columns);
                    }
                }
            }

        });
    }

    private static Iterator<Entry<Key, byte[]>> takeCell(final PeekingIterator<Entry<Key, byte[]>> it, final Key key) {
        return new AbstractIterator<Entry<Key, byte[]>>() {
            @Override
            protected Entry<Key, byte[]> computeNext() {
                if (!it.hasNext()) {
                    return endOfData();
                }
                Entry<Key, byte[]> next = it.peek();
                Key nextKey = next.getKey();
                if (nextKey.matchesCell(key)) {
                    return it.next();
                }
                return endOfData();
            }
        };
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef,
                                                                  Iterable<byte[]> rows,
                                                                  BatchColumnRangeSelection batchColumnRangeSelection,
                                                                  long timestamp) {
        Map<byte[], RowColumnRangeIterator> result = Maps.newHashMap();
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableRef).entries;

        ColumnRangeSelection columnRangeSelection = new ColumnRangeSelection(
                batchColumnRangeSelection.getStartCol(),
                batchColumnRangeSelection.getEndCol());
        for (byte[] row : rows) {
            result.put(row, getColumnRangeForSingleRow(table, row, columnRangeSelection, timestamp));
        }

        return result;
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
                                                     Iterable<byte[]> rows,
                                                     ColumnRangeSelection columnRangeSelection,
                                                     int cellBatchHint,
                                                     long timestamp) {
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableRef).entries;
        Iterator<RowColumnRangeIterator> rowColumnRanges =
                Iterators.transform(rows.iterator(),
                        row -> getColumnRangeForSingleRow(table, row, columnRangeSelection, timestamp));
        return new LocalRowColumnRangeIterator(Iterators.concat(rowColumnRanges));
    }

    private RowColumnRangeIterator getColumnRangeForSingleRow(ConcurrentSkipListMap<Key, byte[]> table,
                                                              byte[] row,
                                                              ColumnRangeSelection columnRangeSelection,
                                                              long timestamp) {
        Cell rowBegin;
        if (columnRangeSelection.getStartCol().length > 0) {
            rowBegin = Cell.create(row, columnRangeSelection.getStartCol());
        } else {
            rowBegin = Cells.createSmallestCellForRow(row);
        }
        // Inclusive last cell.
        Cell rowEnd;
        if (columnRangeSelection.getEndCol().length > 0) {
            rowEnd = Cell.create(row, RangeRequests.previousLexicographicName(columnRangeSelection.getEndCol()));
        } else {
            rowEnd = Cells.createLargestCellForRow(row);
        }
        PeekingIterator<Entry<Key, byte[]>> entries = Iterators.peekingIterator(table.subMap(
                new Key(rowBegin, Long.MIN_VALUE), new Key(rowEnd, timestamp)).entrySet().iterator());
        Map<Cell, Value> rowResults = new LinkedHashMap<>();
        while (entries.hasNext()) {
            Entry<Key, byte[]> entry = entries.peek();
            Key key = entry.getKey();
            Iterator<Entry<Key, byte[]>> cellIter = takeCell(entries, key);
            getLatestVersionOfCell(row, key, cellIter, timestamp, rowResults);
        }
        return new LocalRowColumnRangeIterator(rowResults.entrySet().iterator());
    }

    private interface ResultProducer<T> {
        @Nullable T apply(Iterator<Entry<Key, byte[]>> timestampValues);
    }

    private static <T> void collectValueForTimestamp(byte[] col,
                                                     Iterator<Entry<Key, byte[]>> timestampValues,
                                                     @Output ImmutableSortedMap.Builder<byte[], T> results,
                                                     RangeRequest range,
                                                     ResultProducer<T> resultProducer) {
        T result = null;
        if (range.containsColumn(col)) {
            result = resultProducer.apply(timestampValues);
        }

        // exhaust remaining entries
        Iterators.size(timestampValues);
        if (result != null) {
            results.put(col, result);
        }
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        putInternal(tableRef, KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp), false);
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        putInternal(tableRef, values.entries(), false);
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        putInternal(tableRef,
                KeyValueServices.toConstantTimestampValues(values.entrySet(), AtlasDbConstants.TRANSACTION_TS),
                true);
    }

    private void putInternal(
            TableReference tableRef,
            Collection<Map.Entry<Cell, Value>> values,
            boolean doNotOverwriteWithSameValue) {
        Table table = getTableMap(tableRef);
        for (Map.Entry<Cell, Value> e : values) {
            byte[] contents = e.getValue().getContents();
            long timestamp = e.getValue().getTimestamp();

            Key key = getKey(table, e.getKey(), timestamp);
            byte[] oldContents = putIfAbsent(table, key, contents);
            if (oldContents != null && (doNotOverwriteWithSameValue || !Arrays.equals(oldContents, contents))) {
                throw new KeyAlreadyExistsException("We already have a value for this timestamp");
            }
        }
    }

    @Override
    public void checkAndSet(CheckAndSetRequest request) throws CheckAndSetException {
        TableReference tableRef = request.table();
        Table table = getTableMap(tableRef);
        Cell cell = request.cell();
        Optional<byte[]> oldValue = request.oldValue();
        byte[] contents = request.newValue();

        Key key = getKey(table, cell, AtlasDbConstants.TRANSACTION_TS);
        if (oldValue.isPresent()) {
            byte[] storedValue = table.entries.get(key);
            boolean succeeded = Arrays.equals(storedValue, oldValue.get())
                    && table.entries.replace(key, storedValue, copyOf(contents));
            if (!succeeded) {
                byte[] actual = table.entries.get(key); // Re-fetch, something may have happened between get and replace
                throwCheckAndSetException(cell, tableRef, oldValue.get(), actual);
            }
        } else {
            byte[] oldContents = putIfAbsent(table, key, contents);
            if (oldContents != null) {
                throwCheckAndSetException(cell, tableRef, null, oldContents);
            }
        }
    }

    // Returns the existing contents, if any, and null otherwise
    private byte[] putIfAbsent(Table table, Key key, final byte[] contents) {
        return table.entries.putIfAbsent(key, copyOf(contents));
    }

    private Key getKey(Table table, Cell cell, long timestamp) {
        byte[] row = cell.getRowName();
        byte[] col = cell.getColumnName();

        Key nextKey = table.entries.ceilingKey(new Key(row, ArrayUtils.EMPTY_BYTE_ARRAY, Long.MIN_VALUE));
        if (nextKey != null && nextKey.matchesRow(row)) {
            // Save memory by sharing rows.
            row = nextKey.row;
        }
        return new Key(row, col, timestamp);
    }

    private void throwCheckAndSetException(Cell cell, TableReference tableRef, byte[] expected, byte[] actual) {
        ImmutableList<byte[]> actuals = actual == null ? ImmutableList.of() : ImmutableList.of(actual);
        throw new CheckAndSetException(cell, tableRef, expected, actuals);
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableRef).entries;
        for (Map.Entry<Cell, Long> e : keys.entries()) {
            table.remove(new Key(e.getKey(), e.getValue()));
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long ts) {
        Multimap<Cell, Long> multimap = HashMultimap.create();
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableRef).entries;
        for (Cell key : cells) {
            for (Key entry : table.subMap(new Key(key, Long.MIN_VALUE), new Key(key, ts)).keySet()) {
                multimap.put(key, entry.ts);
            }
        }
        return multimap;
    }

    @Override
    public void dropTable(TableReference tableRef) {
        tables.remove(tableRef);
        tableMetadata.remove(tableRef);
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        Table table = tables.get(tableRef);
        if (table != null) {
            table.entries.clear();
        } else {
            throw new IllegalStateException(
                    String.format("Truncate called on a table (%s) that did not exist", tableRef));
        }
    }

    @Override
    public void createTable(TableReference tableRef, byte[] metadata) {
        tables.putIfAbsent(tableRef, new Table());
        putMetadataForTable(tableRef, metadata);
    }

    @Override
    public void putMetadataForTable(TableReference tableRef, byte[] metadata) {
        if (!tables.containsKey(tableRef)) {
            throw new IllegalArgumentException("No such table " + tableRef);
        }
        tableMetadata.put(tableRef, metadata);
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        if (!tables.containsKey(tableRef)) {
            return AtlasDbConstants.EMPTY_TABLE_METADATA;
        }
        byte[] ret = tableMetadata.get(tableRef);
        return ret == null ? ArrayUtils.EMPTY_BYTE_ARRAY : ret;
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return ImmutableSet.copyOf(tables.keySet());
    }

    private byte[] copyOf(byte[] contents) {
        return Arrays.copyOf(contents, contents.length);
    }

    static class Table {
        final ConcurrentSkipListMap<Key, byte[]> entries;

        Table() {
            this.entries = new ConcurrentSkipListMap<>();
        }
    }

    private Table getTableMap(TableReference tableRef) {
        if (createTablesAutomatically && !tables.containsKey(tableRef)) {
            createTable(tableRef, AtlasDbConstants.EMPTY_TABLE_METADATA);
        }
        Table table = tables.get(tableRef);
        if (table == null) {
            throw new IllegalArgumentException("table " + tableRef.getQualifiedName() + " does not exist");
        }
        return table;
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableRef).entries;
        for (Cell cell : cells) {
            table.put(new Key(cell, Value.INVALID_VALUE_TIMESTAMP), ArrayUtils.EMPTY_BYTE_ARRAY);
        }
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        // nothing to do
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        return ClusterAvailabilityStatus.ALL_AVAILABLE;
    }

    private static class Key implements Comparable<Key> {
        private final byte[] row;
        private final byte[] col;
        private final long ts;

        Key(Cell cell, long ts) {
            this(cell.getRowName(), cell.getColumnName(), ts);
        }

        Key(byte[] row, byte[] col, long ts) {
            this.row = row;
            this.col = col;
            this.ts = ts;
        }

        boolean matchesRow(byte[] otherRow) {
            return UnsignedBytes.lexicographicalComparator().compare(row, otherRow) == 0;
        }

        boolean matchesCell(Cell cell) {
            return UnsignedBytes.lexicographicalComparator().compare(row, cell.getRowName()) == 0
                    && UnsignedBytes.lexicographicalComparator().compare(col, cell.getColumnName()) == 0;
        }

        boolean matchesCell(Key key) {
            return UnsignedBytes.lexicographicalComparator().compare(row, key.row) == 0
                    && UnsignedBytes.lexicographicalComparator().compare(col, key.col) == 0;
        }

        @SuppressWarnings("CheckStyle")
        @Override
        public int compareTo(Key o) {
            int comparison = UnsignedBytes.lexicographicalComparator().compare(row, o.row);
            if (comparison != 0) {
                return comparison;
            }
            comparison = UnsignedBytes.lexicographicalComparator().compare(col, o.col);
            if (comparison != 0) {
                return comparison;
            }
            return Longs.compare(ts, o.ts);
        }


        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(col);
            result = prime * result + Arrays.hashCode(row);
            result = prime * result + (int) (ts ^ (ts >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Key other = (Key) obj;
            if (!Arrays.equals(col, other.col)) {
                return false;
            }
            if (!Arrays.equals(row, other.row)) {
                return false;
            }
            if (ts != other.ts) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "Key [row=" + BaseEncoding.base16().lowerCase().encode(row)
                    + ", col=" + BaseEncoding.base16().lowerCase().encode(col)
                    + ", ts=" + ts + "]";
        }
    }
}
