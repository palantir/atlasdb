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
package com.palantir.atlasdb.transaction.impl;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.Throwables;

public class CachingTransaction extends ForwardingTransaction {
    private static final Logger log = LoggerFactory.getLogger(CachingTransaction.class);

    private static final int DEFAULT_MAX_CACHE_SIZE = 100;
    private static final int DEFAULT_MAX_CACHE_WEIGHT_PER_TABLE = 10_000_000;

    final Transaction delegate;
    private final int maxCacheSize;
    private final int maxCacheWeightPerTable;

    // The keys here are Strings for historic reasons, as TableReferences treat empty namespace and the met namespace the same.
    private final Cache<String, LoadingCache<Cell, byte[]>> columnTableCache;

    public CachingTransaction(Transaction delegate, int maxCacheSize, int maxCacheWeightPerTable) {
        this.delegate = delegate;
        this.maxCacheSize = maxCacheSize;
        this.maxCacheWeightPerTable = maxCacheWeightPerTable;
        this.columnTableCache = CacheBuilder.newBuilder()
                .maximumSize(maxCacheSize)
                .build();
    }

    public CachingTransaction(Transaction delegate) {
        this(delegate, DEFAULT_MAX_CACHE_SIZE, DEFAULT_MAX_CACHE_WEIGHT_PER_TABLE);
    }

    @Override
    public Transaction delegate() {
        return delegate;
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(TableReference tableRef, Iterable<byte[]> rows,
            ColumnSelection columnSelection) {
        if (Iterables.isEmpty(rows)) {
            log.info("Attempted getRows on '{}' table and {} with empty rows argument", tableRef, columnSelection);
            return AbstractTransaction.EMPTY_SORTED_ROWS;
        }

        LoadingCache<Cell, byte[]> colCache = getColCacheForTable(tableRef);
        if (columnSelection.allColumnsSelected()) {
            SortedMap<byte[], RowResult<byte[]>> loaded = super.getRows(tableRef, rows, columnSelection);
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
                    byte[] val = colCache.getIfPresent(Cell.create(row, col));
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
            SortedMap<byte[], RowResult<byte[]>> results = super.getRows(tableRef, toLoad, columnSelection);
            cacheLoadedRows(colCache, results.values());
            inCache.putAll(results);
            return inCache.build();
        }
    }

    private void cacheLoadedRows(Cache<Cell, byte[]> colCache,
            Iterable<RowResult<byte[]>> rowView) {
        for (RowResult<byte[]> loadedRow : rowView) {
            for (Map.Entry<Cell, byte[]> e : loadedRow.getCells()) {
                cacheLoadedColumns(colCache, ImmutableSet.of(e.getKey()), ImmutableMap.of(e.getKey(), e.getValue()));
            }
        }
    }

    private void cacheLoadedColumns(Cache<Cell, byte[]> colCache,
            Set<Cell> toLoad,
            Map<Cell, byte[]> toCache) {
        for (Cell key : toLoad) {
            byte[] value = toCache.get(key);
            if (value == null) {
                value = PtBytes.EMPTY_BYTE_ARRAY;
            }
            colCache.put(key, value);
        }
    }

    // Cache semantics: Zero byte values represent a deleted cell, which the delegate will return to us as null, but
    // we will put in the cache as a zero-length array to differentiate "we saw a deleted cell" from "this cell isn't
    // in the cache".
    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        if (cells.isEmpty()) {
            log.info("Attempted get on '{}' table with empty cells argument", tableRef);
            return ImmutableMap.of();
        }

        try {
            return getColCacheForTable(tableRef).getAll(cells).entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().length > 0)
                    .collect(Collectors.toConcurrentMap(entry -> entry.getKey(), entry -> entry.getValue()));
        } catch (ExecutionException | UncheckedExecutionException e) {
            throw Throwables.throwUncheckedException(e.getCause());
        }
    }

    @Override
    public final void delete(TableReference tableRef, Set<Cell> cells) {
        super.delete(tableRef, cells);
        addToColCache(tableRef, Cells.constantValueMap(cells, PtBytes.EMPTY_BYTE_ARRAY));
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        super.put(tableRef, values);
        addToColCache(tableRef, values);
    }

    private void addToColCache(TableReference tableRef, Map<Cell, byte[]> values) {
        LoadingCache<Cell, byte[]> colCache = getColCacheForTable(tableRef);
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            byte[] value = e.getValue();
            if (value == null) {
                value = PtBytes.EMPTY_BYTE_ARRAY;
            }
            colCache.put(e.getKey(), value);
        }
    }

    private LoadingCache<Cell, byte[]> getColCacheForTable(TableReference tableRef) {
        String tableName = tableRef.getQualifiedName();
        try {
            return columnTableCache.get(tableName, () -> createNewCacheForTable(tableRef));
        } catch (ExecutionException e) {
            throw Throwables.throwUncheckedException(e.getCause());
        }
    }

    private LoadingCache<Cell, byte[]> createNewCacheForTable(TableReference tableRef) {
        return CacheBuilder.newBuilder()
                .softValues()
                .maximumWeight(maxCacheWeightPerTable)
                .weigher((Weigher<Cell, byte[]>) (key, value) -> value.length)
                .build(getCacheLoader(tableRef));
    }

    private CacheLoader<Cell, byte[]> getCacheLoader(TableReference tableRef) {
        return new CacheLoader<Cell, byte[]>() {

            @Override
            public byte[] load(Cell key) throws Exception {
                byte[] res = CachingTransaction.super.get(tableRef, ImmutableSet.of(key)).get(key);
                if (res == null) {
                    return PtBytes.EMPTY_BYTE_ARRAY;
                } else {
                    return res;
                }
            }

            @Override
            public Map<Cell, byte[]> loadAll(Iterable<? extends Cell> keys) throws Exception {
                Map<Cell, byte[]> internalResult = CachingTransaction.super.get(tableRef, ImmutableSet.copyOf(keys));
                Map<Cell, byte[]> res = Maps.newConcurrentMap();
                for (Cell key : keys) {
                    byte[] value = internalResult.get(key);
                    if (value == null) {
                        res.put(key, PtBytes.EMPTY_BYTE_ARRAY);
                    } else {
                        res.put(key, value);
                    }
                }

                return res;
            }
        };
    }
}