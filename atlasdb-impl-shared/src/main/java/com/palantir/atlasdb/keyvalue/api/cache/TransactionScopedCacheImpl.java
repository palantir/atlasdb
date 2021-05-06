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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.AtlasLockDescriptorUtils;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.CommitUpdate.Visitor;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.immutables.value.Value;

@ThreadSafe
final class TransactionScopedCacheImpl implements TransactionScopedCache {
    private final TransactionCacheValueStore valueStore;
    private volatile boolean finalised = false;

    private TransactionScopedCacheImpl(ValueCacheSnapshot snapshot) {
        valueStore = new TransactionCacheValueStoreImpl(snapshot);
    }

    static TransactionScopedCache create(ValueCacheSnapshot snapshot) {
        return new TransactionScopedCacheImpl(snapshot);
    }

    @Override
    public synchronized void write(TableReference tableReference, Map<Cell, byte[]> values) {
        ensureNotFinalised();
        KeyedStream.stream(values)
                .map(CacheValue::of)
                .forEach((cell, value) -> valueStore.cacheRemoteWrite(tableReference, cell, value));
    }

    @Override
    public synchronized void delete(TableReference tableReference, Set<Cell> cells) {
        ensureNotFinalised();
        cells.forEach(cell -> valueStore.cacheRemoteWrite(tableReference, cell, CacheValue.empty()));
    }

    @Override
    public Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        ensureNotFinalised();
        return AtlasFutures.getUnchecked(getAsync(tableReference, cells, valueLoader));
    }

    @Override
    public synchronized ListenableFuture<Map<Cell, byte[]>> getAsync(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        ensureNotFinalised();
        // Short-cut all the logic below if the table is not watched.
        if (!valueStore.isWatched(tableReference)) {
            return valueLoader.apply(tableReference, cells);
        }

        CacheLookupResult cacheLookup = cacheLookup(tableReference, cells);

        if (cacheLookup.missedCells().isEmpty()) {
            return Futures.immediateFuture(filterEmptyValues(cacheLookup.cacheHits()));
        } else {
            return Futures.transform(
                    valueLoader.apply(tableReference, cacheLookup.missedCells()),
                    remoteReadValues -> processRemoteRead(tableReference, cacheLookup, remoteReadValues),
                    MoreExecutors.directExecutor());
        }
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
        ValueDigest valueDigest = getValueDigest();
        ValueCacheSnapshot delegateSnapshot = valueStore.getSnapshot();

        LockedCells lockedCells = commitUpdate.accept(new Visitor<>() {
            @Override
            public LockedCells invalidateAll() {
                return ImmutableLockedCells.of(true, ImmutableSet.of());
            }

            @Override
            public LockedCells invalidateSome(Set<LockDescriptor> invalidatedLocks) {
                return ImmutableLockedCells.of(
                        false,
                        invalidatedLocks.stream()
                                .map(AtlasLockDescriptorUtils::candidateCells)
                                .flatMap(List::stream)
                                .collect(Collectors.toSet()));
            }
        });

        ValueCacheSnapshot snapshot = new ValueCacheSnapshot() {
            @Override
            public Optional<CacheEntry> getValue(CellReference cellReference) {
                return delegateSnapshot.getValue(cellReference);
            }

            @Override
            public boolean isUnlocked(CellReference cellReference) {
                return lockedCells.isUnlocked(cellReference) && delegateSnapshot.isUnlocked(cellReference);
            }

            @Override
            public boolean isWatched(TableReference tableReference) {
                return delegateSnapshot.isWatched(tableReference);
            }
        };

        TransactionScopedCacheImpl delegateCache = new TransactionScopedCacheImpl(snapshot);
        KeyedStream.stream(valueDigest.loadedValues())
                .filter(value -> value.value().isPresent())
                .map(value -> value.value().get())
                .forEach(((cellReference, value) ->
                        delegateCache.write(cellReference.tableRef(), ImmutableMap.of(cellReference.cell(), value))));
        return new ReadOnlyTransactionScopedCache(delegateCache);
    }

    @Value.Immutable
    interface LockedCells {
        @Value.Parameter
        boolean allLocked();

        @Value.Parameter
        Set<CellReference> lockedCells();

        default boolean isUnlocked(CellReference cellReference) {
            return !allLocked() && !lockedCells().contains(cellReference);
        }
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
            TableReference tableReference, CacheLookupResult cacheLookup, Map<Cell, byte[]> remoteReadValues) {
        valueStore.cacheRemoteReads(tableReference, remoteReadValues);
        cacheEmptyReads(tableReference, cacheLookup.missedCells(), remoteReadValues);
        return ImmutableMap.<Cell, byte[]>builder()
                .putAll(remoteReadValues)
                .putAll(filterEmptyValues(cacheLookup.cacheHits()))
                .build();
    }

    private synchronized CacheLookupResult cacheLookup(TableReference table, Set<Cell> cells) {
        Map<Cell, CacheValue> cachedValues = valueStore.getCachedValues(table, cells);
        Set<Cell> uncachedCells = Sets.difference(cells, cachedValues.keySet());
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
