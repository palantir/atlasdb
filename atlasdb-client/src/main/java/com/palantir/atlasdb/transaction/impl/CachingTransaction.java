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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.Throwables;
import com.palantir.util.Pair;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachingTransaction extends ForwardingTransaction {

    private static final Logger log = LoggerFactory.getLogger(CachingTransaction.class);
    private static final long DEFAULT_MAX_CACHED_CELLS = 10_000_000;

    private final Transaction delegate;
    private final Cache<Pair<String, Cell>, byte[]> cellCache;

    public CachingTransaction(Transaction delegate) {
        this(delegate, DEFAULT_MAX_CACHED_CELLS);
    }

    public CachingTransaction(Transaction delegate, long maxCachedCells) {
        this.delegate = delegate;
        cellCache = CacheBuilder.newBuilder()
                .maximumSize(maxCachedCells)
                .softValues()
                .recordStats()
                .build();
    }

    @Override
    public Transaction delegate() {
        return delegate;
    }

    @Override
    public NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection) {
        if (Iterables.isEmpty(rows)) {
            return AbstractTransaction.EMPTY_SORTED_ROWS;
        }

        if (columnSelection.allColumnsSelected()) {
            NavigableMap<byte[], RowResult<byte[]>> loaded = super.getRows(tableRef, rows, columnSelection);
            cacheLoadedRows(tableRef, loaded.values());
            return loaded;
        } else {
            Set<byte[]> toLoad = new HashSet<>();
            ImmutableSortedMap.Builder<byte[], RowResult<byte[]>> inCache =
                    ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
            for (byte[] row : rows) {
                ImmutableSortedMap.Builder<byte[], byte[]> matches =
                        ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
                boolean nonEmpty = false;
                boolean shouldLoad = false;
                for (byte[] col : columnSelection.getSelectedColumns()) {
                    byte[] val = getCachedCellIfPresent(tableRef, Cell.create(row, col));
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
            cacheLoadedRows(tableRef, toLoad, columnSelection.getSelectedColumns(), results);
            inCache.putAll(results);
            return inCache.build();
        }
    }

    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        try {
            return getWithLoader(
                            tableRef,
                            cells,
                            (tableReference, toRead) -> Futures.immediateFuture(super.get(tableReference, toRead)))
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        }
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getAsync(TableReference tableRef, Set<Cell> cells) {
        return getWithLoader(tableRef, cells, super::getAsync);
    }

    private ListenableFuture<Map<Cell, byte[]>> getWithLoader(
            TableReference tableRef, Set<Cell> cells, CellLoader cellLoader) {
        if (cells.isEmpty()) {
            return Futures.immediateFuture(ImmutableMap.of());
        }

        Set<Cell> toLoad = new HashSet<>();
        Map<Cell, byte[]> cacheHit = Maps.newHashMapWithExpectedSize(cells.size());
        for (Cell cell : cells) {
            byte[] val = getCachedCellIfPresent(tableRef, cell);
            if (val != null) {
                if (val.length > 0) {
                    cacheHit.put(cell, val);
                }
            } else {
                toLoad.add(cell);
            }
        }

        return Futures.transform(
                cellLoader.load(tableRef, toLoad),
                loadedCells -> {
                    cacheLoadedCells(tableRef, toLoad, loadedCells);
                    cacheHit.putAll(loadedCells);
                    return cacheHit;
                },
                MoreExecutors.directExecutor());
    }

    @Override
    public final void delete(TableReference tableRef, Set<Cell> cells) {
        super.delete(tableRef, cells);
        addToCache(tableRef, Cells.constantValueMap(cells, PtBytes.EMPTY_BYTE_ARRAY));
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        super.put(tableRef, values);
        addToCache(tableRef, values);
    }

    private void addToCache(TableReference tableRef, Map<Cell, byte[]> values) {
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            byte[] value = e.getValue();
            if (value == null) {
                value = PtBytes.EMPTY_BYTE_ARRAY;
            }
            cacheLoadedCell(tableRef, e.getKey(), value);
        }
    }

    private void cacheLoadedRows(TableReference tableRef, Iterable<RowResult<byte[]>> rowView) {
        for (RowResult<byte[]> loadedRow : rowView) {
            for (Map.Entry<Cell, byte[]> e : loadedRow.getCells()) {
                cacheLoadedCell(tableRef, e.getKey(), e.getValue());
            }
        }
    }

    private void cacheLoadedRows(
            TableReference tableRef,
            Iterable<byte[]> toLoad,
            Collection<byte[]> columnNames,
            SortedMap<byte[], RowResult<byte[]>> toCache) {
        for (byte[] row : toLoad) {
            SortedMap<byte[], byte[]> columnValues =
                    toCache.get(row) != null ? toCache.get(row).getColumns() : ImmutableSortedMap.of();
            for (byte[] columnName : columnNames) {
                byte[] value = columnValues.get(columnName);
                if (value == null) {
                    value = PtBytes.EMPTY_BYTE_ARRAY;
                }
                cacheLoadedCell(tableRef, Cell.create(row, columnName), value);
            }
        }
    }

    private void cacheLoadedCells(TableReference tableRef, Set<Cell> toLoad, Map<Cell, byte[]> toCache) {
        for (Cell key : toLoad) {
            byte[] value = toCache.get(key);
            if (value == null) {
                value = PtBytes.EMPTY_BYTE_ARRAY;
            }
            cacheLoadedCell(tableRef, key, value);
        }
    }

    private byte[] getCachedCellIfPresent(TableReference tableRef, Cell cell) {
        return cellCache.getIfPresent(Pair.create(tableRef.getQualifiedName(), cell));
    }

    private void cacheLoadedCell(TableReference tableRef, Cell cell, byte[] value) {
        cellCache.put(Pair.create(tableRef.getQualifiedName(), cell), value);
    }

    // Log cache stats on commit or abort.
    // Note we check for logging enabled because actually getting stats is not necessarily trivial
    // (it must aggregate stats from all cache segments)
    @Override
    public void commit() throws TransactionFailedException {
        try {
            super.commit();
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("CachingTransaction cache stats on commit: {}", cellCache.stats());
            }
        }
    }

    @Override
    public void commit(TransactionService txService) throws TransactionFailedException {
        try {
            super.commit(txService);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("CachingTransaction cache stats on commit(txService): {}", cellCache.stats());
            }
        }
    }

    @Override
    public void abort() {
        try {
            super.abort();
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("CachingTransaction cache stats on abort: {}", cellCache.stats());
            }
        }
    }

    @FunctionalInterface
    private interface CellLoader {
        ListenableFuture<Map<Cell, byte[]>> load(TableReference tableReference, Set<Cell> toRead);
    }
}
