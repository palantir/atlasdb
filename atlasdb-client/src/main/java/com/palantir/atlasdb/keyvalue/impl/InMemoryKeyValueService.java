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
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.ArrayUtils;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.HashMultimap;
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
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * In-memory (non-durable) key-value store implementation.
 * <p>
 * Intended for testing use only.
 */
@ThreadSafe
public class InMemoryKeyValueService extends AbstractKeyValueService {
    private final ConcurrentMap<String, Table> tables = Maps.newConcurrentMap();
    private final ConcurrentMap<String, byte[]> tableMetadata = Maps.newConcurrentMap();
    private volatile boolean createTablesAutomatically;

    public InMemoryKeyValueService(boolean createTablesAutomatically) {
        this(createTablesAutomatically,
                PTExecutors.newFixedThreadPool(16, PTExecutors.newNamedThreadFactory(true)));
    }

    public InMemoryKeyValueService(boolean createTablesAutomatically,
                                   ExecutorService executor) {
        super(executor);
        this.createTablesAutomatically = createTablesAutomatically;
    }

    @Override
    public void initializeFromFreshInstance() {
        // All initialization is done in the constructor and initializers above
    }

    @Override
    public Map<Cell, Value> getRows(String tableName, Iterable<byte[]> rows,
                                    ColumnSelection columnSelection, long timestamp) {
        Map<Cell, Value> result = Maps.newHashMap();
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableName).entries;

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
                        result.put(Cell.create(row, key.col), Value.create(value, ts));
                    }
                }
                Iterators.size(cellIter);
            }
        }

        return result;
    }

    @Override
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableName).entries;
        Map<Cell, Value> result = Maps.newHashMap();
        for (Map.Entry<Cell, Long> e : timestampByCell.entrySet()) {
            Cell cell = e.getKey();
            Entry<Key, byte[]> lastEntry = table.lowerEntry(new Key(cell, e.getValue()));
            if (lastEntry != null) {
                Key key = lastEntry.getKey();
                if (key.matchesCell(cell)) {
                    long ts = lastEntry.getKey().ts;
                    byte[] value = lastEntry.getValue();
                    result.put(cell, Value.create(value, ts));
                }
            }
        }
        return result;
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                    Iterable<RangeRequest> rangeRequests,
                                                                    long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRange(this, tableName, rangeRequests, timestamp);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(String tableName, final RangeRequest range, final long timestamp) {
        return getRangeInternal(tableName, range, new ResultProducer<Value>() {
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
                    byte[] value = lastEntry.getValue();
                    return Value.create(value, ts);
                } else {
                    return null;
                }
            }
        });
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName, final RangeRequest range, final long timestamp) {
        return getRangeInternal(tableName, range, new ResultProducer<Set<Long>>() {
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
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName, final RangeRequest range, final long timestamp) {
        return getRangeInternal(tableName, range, new ResultProducer<Set<Value>>() {
            @Override
            public Set<Value> apply(Iterator<Entry<Key, byte[]>> entries) {
                Set<Value> values = Sets.newHashSet();
                while (entries.hasNext()) {
                    Entry<Key, byte[]> entry = entries.next();
                    Key key = entry.getKey();
                    if (key.ts >= timestamp) {
                        break;
                    }
                    values.add(Value.create(entry.getValue(), key.ts));
                }
                if (!values.isEmpty()) {
                    return values;
                } else {
                    return null;
                }
            }
        });
    }

    private <T> ClosableIterator<RowResult<T>> getRangeInternal(String tableName,
                                                                final RangeRequest range,
                                                                final ResultProducer<T> resultProducer) {
        ConcurrentNavigableMap<Key, byte[]> tableMap = getTableMap(tableName).entries;
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
                    ImmutableSortedMap.Builder<byte[], T> result = ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
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

    private static Iterator<Entry<Key, byte[]>> takeCell(final PeekingIterator<Entry<Key, byte[]>> iter, final Key key) {
        return new AbstractIterator<Entry<Key, byte[]>>() {
            @Override
            protected Entry<Key, byte[]> computeNext() {
                if (!iter.hasNext()) {
                    return endOfData();
                }
                Entry<Key, byte[]> next = iter.peek();
                Key nextKey = next.getKey();
                if (nextKey.matchesCell(key)) {
                    return iter.next();
                }
                return endOfData();
            }
        };
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
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        putInternal(tableName, KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp));
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        putInternal(tableName, values.entries());
    }

    private void putInternal(String tableName, Collection<Map.Entry<Cell, Value>> values) {
        Table table = getTableMap(tableName);
        for (Map.Entry<Cell, Value> e : values) {
            Cell cell = e.getKey();
            byte[] row = cell.getRowName();
            byte[] col = cell.getColumnName();
            byte[] contents = e.getValue().getContents();
            long timestamp = e.getValue().getTimestamp();

            Key nextKey = table.entries.ceilingKey(new Key(row, ArrayUtils.EMPTY_BYTE_ARRAY, Long.MIN_VALUE));
            if (nextKey != null && nextKey.matchesRow(row)) {
                // Save memory by sharing rows.
                row = nextKey.row;
            }
            byte[] oldContents = table.entries.putIfAbsent(new Key(row, col, timestamp), contents);
            if (oldContents != null) {
                throw new KeyAlreadyExistsException("We already have a value for this timestamp");
            }
        }
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        put(tableName, values, 0);
    }

    @Override
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableName).entries;
        for (Map.Entry<Cell, Long> e : keys.entries()) {
            table.remove(new Key(e.getKey(), e.getValue()));
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long ts) {
        Multimap<Cell, Long> multimap = HashMultimap.create();
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableName).entries;
        for (Cell key : cells) {
            for (Key entry : table.subMap(new Key(key, Long.MIN_VALUE), new Key(key, ts)).keySet()) {
                multimap.put(key, entry.ts);
            }
        }
        return multimap;
    }

    @Override
    public void dropTable(String tableName) {
        tables.remove(tableName);
        tableMetadata.remove(tableName);
    }

    @Override
    public void truncateTable(String tableName) {
        tables.get(tableName).entries.clear();
    }

    @Override
    public void createTable(String tableName, byte[] tableMetadata) {
        tables.putIfAbsent(tableName, new Table());
        putMetadataForTable(tableName, tableMetadata);
    }

    @Override
    public void putMetadataForTable(String tableName, byte[] metadata) {
        if (!tables.containsKey(tableName)) {
            throw new IllegalArgumentException("No such table " + tableName);
        }
        tableMetadata.put(tableName, metadata);
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new IllegalArgumentException("No such table");
        }
        byte[] ret = tableMetadata.get(tableName);
        return ret == null ? ArrayUtils.EMPTY_BYTE_ARRAY : ret;
    }

    @Override
    public Set<String> getAllTableNames() {
        return ImmutableSet.copyOf(tables.keySet());
    }

    static class Table {
        final ConcurrentSkipListMap<Key, byte[]> entries;

        public Table() {
            this.entries = new ConcurrentSkipListMap<Key, byte[]>();
        }
    }

    private Table getTableMap(String tableName) {
        if (createTablesAutomatically && !tables.containsKey(tableName)) {
            createTable(tableName, AtlasDbConstants.EMPTY_TABLE_METADATA);
        }
        Table table = tables.get(tableName);
        if (table == null) {
            throw new IllegalArgumentException("table " + tableName + " does not exist");
        }
        return table;
    }

    @Override
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        ConcurrentSkipListMap<Key, byte[]> table = getTableMap(tableName).entries;
        for (Cell cell : cells) {
            table.put(new Key(cell, Value.INVALID_VALUE_TIMESTAMP), ArrayUtils.EMPTY_BYTE_ARRAY);
        }
    }

    @Override
    public void compactInternally(String tableName) {
        // nothing to do
    }

    private static class Key implements Comparable<Key> {
        private final byte[] row;
        private final byte[] col;
        private final long ts;

        public Key(Cell cell, long ts) {
            this(cell.getRowName(), cell.getColumnName(), ts);
        }

        public Key(byte[] row, byte[] col, long ts) {
            this.row = row;
            this.col = col;
            this.ts = ts;
        }

        public boolean matchesRow(byte[] otherRow) {
            return UnsignedBytes.lexicographicalComparator().compare(row, otherRow) == 0;
        }

        public boolean matchesCell(Cell cell) {
            return UnsignedBytes.lexicographicalComparator().compare(row, cell.getRowName()) == 0 &&
                    UnsignedBytes.lexicographicalComparator().compare(col, cell.getColumnName()) == 0;
        }

        public boolean matchesCell(Key key) {
            return UnsignedBytes.lexicographicalComparator().compare(row, key.row) == 0 &&
                    UnsignedBytes.lexicographicalComparator().compare(col, key.col) == 0;
        }

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
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Key other = (Key) obj;
            if (!Arrays.equals(col, other.col))
                return false;
            if (!Arrays.equals(row, other.row))
                return false;
            if (ts != other.ts)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "Key [row=" + BaseEncoding.base16().lowerCase().encode(row) + ", col=" + BaseEncoding.base16().lowerCase().encode(col) + ", ts="
                    + ts + "]";
        }
    }
}
