/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.transaction.impl;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.transaction.api.Transaction;

public class CachingTransaction extends ForwardingTransaction {
    final Transaction delegate;

    private final LoadingCache<String, ConcurrentMap<Cell, byte[]>> columnTableCache = CacheBuilder.newBuilder().softValues().build(new CacheLoader<String, ConcurrentMap<Cell, byte[]>>() {
        @Override
        public ConcurrentMap<Cell, byte[]> load(String key) throws Exception {
            return Maps.newConcurrentMap();
        }
    });

    public CachingTransaction(Transaction delegate) {
        this.delegate = delegate;
    }

    @Override
    public Transaction delegate() {
        return delegate;
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(String tableName, Iterable<byte[]> rows,
                                                        ColumnSelection columnSelection) {
        ConcurrentMap<Cell, byte[]> colCache = getColCacheForTable(tableName);
        if (columnSelection.allColumnsSelected()) {
            SortedMap<byte[], RowResult<byte[]>> loaded = super.getRows(tableName, rows, columnSelection);
            cacheLoadedRows(colCache, loaded.values());
            return loaded;
        } else {
            Set<byte[]> toLoad = Sets.newHashSet();
            ImmutableSortedMap.Builder<byte[], RowResult<byte[]>> inCache =
                    ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
            for (byte[] row : rows) {
                ImmutableSortedMap.Builder<byte[], byte[]> matches =
                        ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
                boolean nonEmpty = false;
                boolean shouldLoad = false;
                for (byte[] col : columnSelection.getSelectedColumns()) {
                    byte[] val = colCache.get(Cell.create(row, col));
                    if (val == null) {
                        shouldLoad = true;
                        break;
                    } else if (val.length != 0) {
                        matches.put(col, val);
                        nonEmpty = true;
                    }
                }
                if (shouldLoad) {
                    toLoad.add(row);
                } else if (nonEmpty) {
                    inCache.put(row, RowResult.create(row, matches.build()));
                }
            }
            SortedMap<byte[], RowResult<byte[]>> results = super.getRows(tableName, toLoad, columnSelection);
            cacheLoadedRows(colCache, results.values());
            inCache.putAll(results);
            return inCache.build();
        }
    }

    private void cacheLoadedRows(ConcurrentMap<Cell, byte[]> colCache,
                                 Iterable<RowResult<byte[]>> rowView) {
        for (RowResult<byte[]> loadedRow : rowView) {
            for (Map.Entry<Cell, byte[]> e : loadedRow.getCells()) {
                cacheLoadedColumns(colCache, ImmutableSet.of(e.getKey()), ImmutableMap.of(e.getKey(), e.getValue()));
            }
        }
    }

    private void cacheLoadedColumns(ConcurrentMap<Cell, byte[]> colCache,
                                    Set<Cell> toLoad,
                                    Map<Cell, byte[]> toCache) {
        for (Cell key : toLoad) {
            byte[] value = toCache.get(key);
            if (value == null) {
                value = PtBytes.EMPTY_BYTE_ARRAY;
            }
            colCache.putIfAbsent(key, value);
        }
    }

    @Override
    public Map<Cell, byte[]> get(String tableName, Set<Cell> cells) {
        ConcurrentMap<Cell, byte[]> cache = getColCacheForTable(tableName);
        Set<Cell> toLoad = Sets.newHashSet();
        Map<Cell, byte[]> cacheHit = Maps.newHashMap();
        for (Cell cell : cells) {
            if (cache.containsKey(cell)) {
                byte[] val = cache.get(cell);
                if (val != null && val.length > 0) {
                    cacheHit.put(cell, val);
                }
            } else {
                toLoad.add(cell);
            }
        }

        final Map<Cell, byte[]> loaded = super.get(tableName, toLoad);

        cacheLoadedColumns(cache, toLoad, loaded);
        cacheHit.putAll(loaded);
        return cacheHit;
    }

    @Override
    final public void delete(String tableName, Set<Cell> cells) {
        put(tableName, Cells.constantValueMap(cells, PtBytes.EMPTY_BYTE_ARRAY));
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values) {
        super.put(tableName, values);
        Map<Cell, byte[]> colCache = getColCacheForTable(tableName);
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            byte[] value = e.getValue();
            if (value == null) {
                value = PtBytes.EMPTY_BYTE_ARRAY;
            }
            colCache.put(e.getKey(), value);
        }
    }

    private ConcurrentMap<Cell, byte[]> getColCacheForTable(String tableName) {
        return columnTableCache.getUnchecked(tableName);
    }
}
