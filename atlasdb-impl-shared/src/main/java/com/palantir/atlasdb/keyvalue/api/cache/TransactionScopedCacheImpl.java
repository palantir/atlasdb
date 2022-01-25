/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.cache;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.RowResults;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.watch.CommitUpdate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.immutables.value.Value;

@ThreadSafe
final class TransactionScopedCacheImpl implements TransactionScopedCache {
    private final TransactionCacheValueStore valueStore;
    private final CacheMetrics metrics;
    private volatile boolean finalised = false;

    private TransactionScopedCacheImpl(TransactionCacheValueStore valueStore, CacheMetrics metrics) {
        this.valueStore = valueStore;
        this.metrics = metrics;
    }

    static TransactionScopedCache create(ValueCacheSnapshot snapshot, CacheMetrics metrics) {
        return new TransactionScopedCacheImpl(new TransactionCacheValueStoreImpl(snapshot), metrics);
    }

    @Override
    public synchronized void write(TableReference tableReference, Map<Cell, byte[]> values) {
        ensureNotFinalised();
        values.keySet().forEach(cell -> valueStore.recordRemoteWrite(tableReference, cell));
    }

    @Override
    public synchronized void delete(TableReference tableReference, Set<Cell> cells) {
        ensureNotFinalised();
        cells.forEach(cell -> valueStore.recordRemoteWrite(tableReference, cell));
    }

    @Override
    public Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            Function<Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        ensureNotFinalised();
        return AtlasFutures.getUnchecked(getAsync(tableReference, cells, valueLoader));
    }

    @Override
    public synchronized ListenableFuture<Map<Cell, byte[]>> getAsync(
            TableReference tableReference,
            Set<Cell> cells,
            Function<Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        ensureNotFinalised();
        // Short-cut all the logic below if the table is not watched.
        if (!valueStore.isWatched(tableReference)) {
            return valueLoader.apply(cells);
        }

        CacheLookupResult cacheLookup = cacheLookup(tableReference, cells);

        if (cacheLookup.missedCells().isEmpty()) {
            return Futures.immediateFuture(filterEmptyValues(cacheLookup.cacheHits()));
        } else {
            return Futures.transform(
                    valueLoader.apply(cacheLookup.missedCells()),
                    remoteReadValues -> processRemoteRead(
                            tableReference, cacheLookup.cacheHits(), cacheLookup.missedCells(), remoteReadValues),
                    MoreExecutors.directExecutor());
        }
    }

    @Override
    public NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            Function<Set<Cell>, Map<Cell, byte[]>> cellLoader,
            Function<Iterable<byte[]>, NavigableMap<byte[], RowResult<byte[]>>> rowLoader) {
        ensureNotFinalised();

        if (!valueStore.isWatched(tableRef)) {
            return rowLoader.apply(rows);
        }

        Set<Cell> cells = columnSelection.asCellsForRows(rows);
        CacheLookupResult cached = cacheLookup(tableRef, cells);

        NavigableMap<byte[], Set<Cell>> cacheMisses = Cells.groupCellsByRow(cached.missedCells());

        List<byte[]> completelyMissedRows = new ArrayList<>();
        Set<Cell> completelyMissedRowsCells = new HashSet<>();
        Set<Cell> missedCells = new HashSet<>();
        cacheMisses.forEach((row, cellsForRow) -> {
            if (cellsForRow.size() == columnSelection.getSelectedColumns().size()) {
                completelyMissedRows.add(row);
                completelyMissedRowsCells.addAll(cellsForRow);
            } else {
                missedCells.addAll(cellsForRow);
            }
        });
        metrics.increaseGetRowsHits(cached.cacheHits().size());
        metrics.increaseGetRowsCellLookups(missedCells.size());
        metrics.increaseGetRowsRowLookups(completelyMissedRows.size());

        Map<Cell, byte[]> remoteDirectRead = missedCells.isEmpty() ? ImmutableMap.of() : cellLoader.apply(missedCells);
        Map<Cell, byte[]> cellLookups = processRemoteRead(tableRef, cached.cacheHits(), missedCells, remoteDirectRead);

        NavigableMap<byte[], RowResult<byte[]>> remoteRowRead = completelyMissedRows.isEmpty()
                ? new TreeMap<>(UnsignedBytes.lexicographicalComparator())
                : rowLoader.apply(completelyMissedRows);
        NavigableMap<byte[], RowResult<byte[]>> rowReads =
                processRemoteReadRows(tableRef, completelyMissedRowsCells, remoteRowRead);

        rowReads.putAll(RowResults.viewOfSortedMap(Cells.breakCellsUpByRow(cellLookups)));
        return rowReads;
    }

    @Override
    public synchronized ValueDigest getValueDigest() {
        ensureFinalised();
        return ValueDigest.of(valueStore.getValueDigest());
    }

    @Override
    public synchronized HitDigest getHitDigest() {
        ensureFinalised();
        return HitDigest.of(valueStore.getHitDigest());
    }

    @Override
    public TransactionScopedCache createReadOnlyCache(CommitUpdate commitUpdate) {
        return ReadOnlyTransactionScopedCache.create(
                new TransactionScopedCacheImpl(valueStore.createWithFilteredSnapshot(commitUpdate), metrics));
    }

    @Override
    public void finalise() {
        finalised = true;
    }

    private void ensureFinalised() {
        if (!finalised) {
            throw new TransactionLockWatchFailedException(
                    "Cannot compute value or hit digest unless the cache has been finalised");
        }
    }

    private void ensureNotFinalised() {
        if (finalised) {
            throw new TransactionLockWatchFailedException(
                    "Cannot get or write to a transaction scoped cache that has already been closed");
        }
    }

    private synchronized Map<Cell, byte[]> processRemoteRead(
            TableReference tableReference,
            Map<Cell, CacheValue> cacheHits,
            Set<Cell> cacheMisses,
            Map<Cell, byte[]> remoteReadValues) {
        valueStore.cacheRemoteReads(tableReference, remoteReadValues);
        cacheEmptyReads(tableReference, cacheMisses, remoteReadValues);
        return ImmutableMap.<Cell, byte[]>builder()
                .putAll(remoteReadValues)
                .putAll(filterEmptyValues(cacheHits))
                .build();
    }

    private synchronized NavigableMap<byte[], RowResult<byte[]>> processRemoteReadRows(
            TableReference tableReference, Set<Cell> cacheMisses, Map<byte[], RowResult<byte[]>> remoteReadValues) {
        Map<Cell, byte[]> rowsReadAsCells = remoteReadValues.values().stream()
                .map(RowResult::getCells)
                .flatMap(Streams::stream)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        valueStore.cacheRemoteReads(tableReference, rowsReadAsCells);
        cacheEmptyReads(tableReference, cacheMisses, rowsReadAsCells);
        NavigableMap<byte[], RowResult<byte[]>> result = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        result.putAll(remoteReadValues);
        return result;
    }

    private synchronized CacheLookupResult cacheLookup(TableReference table, Set<Cell> cells) {
        Map<Cell, CacheValue> cachedValues = valueStore.getCachedValues(table, cells);
        Set<Cell> uncachedCells = Sets.difference(cells, cachedValues.keySet());
        metrics.registerHits(cachedValues.size());
        metrics.registerMisses(uncachedCells.size());
        return CacheLookupResult.of(cachedValues, uncachedCells);
    }

    private synchronized void cacheEmptyReads(
            TableReference tableReference, Set<Cell> uncachedCells, Map<Cell, byte[]> remoteReadValues) {
        // The get method does not return an entry if a value is absent; we want to cache this fact
        Set<Cell> emptyCells = Sets.difference(uncachedCells, remoteReadValues.keySet());
        valueStore.cacheEmptyReads(tableReference, emptyCells);
    }

    private static Map<Cell, byte[]> filterEmptyValues(Map<Cell, CacheValue> snapshotCachedValues) {
        return KeyedStream.stream(snapshotCachedValues)
                .filter(value -> value.value().isPresent())
                .map(value -> value.value().get())
                .collectToMap();
    }

    @Value.Immutable
    interface CacheLookupResult {
        Map<Cell, CacheValue> cacheHits();

        Set<Cell> missedCells();

        static CacheLookupResult of(Map<Cell, CacheValue> cachedValues, Set<Cell> missedCells) {
            return ImmutableCacheLookupResult.builder()
                    .cacheHits(cachedValues)
                    .missedCells(missedCells)
                    .build();
        }
    }
}
