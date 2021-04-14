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
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCacheImpl.LocalCacheEntry.Status;
import com.palantir.common.streams.KeyedStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public final class TransactionScopedCacheImpl implements TransactionScopedCache {
    private final ValueCacheSnapshot snapshot;
    private final Map<CellReference, LocalCacheEntry> localUpdates;

    public TransactionScopedCacheImpl(ValueCacheSnapshot snapshot) {
        this.snapshot = snapshot;
        localUpdates = new HashMap<>();
    }

    @Override
    public synchronized void write(TableReference tableReference, Cell cell, CacheValue value) {
        if (snapshot.isWatched(tableReference)) {
            localUpdates.put(CellReference.of(tableReference, cell), LocalCacheEntry.write(value));
        }
    }

    @Override
    public synchronized Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, Map<Cell, byte[]>> valueLoader) {
        Set<CellReference> cellReferences = cells.stream()
                .map(cell -> CellReference.of(tableReference, cell))
                .collect(Collectors.toSet());

        // Read values from the snapshot. For the hits, mark as hit in the local map.
        Map<CellReference, CacheValue> snapshotCachedValues = KeyedStream.of(cellReferences.stream())
                .map(snapshot::getValue)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(CacheEntry::isUnlocked)
                .map(CacheEntry::value)
                .collectToMap();
        snapshotCachedValues.forEach(
                (cellReference, value) -> localUpdates.put(cellReference, LocalCacheEntry.hit(value)));

        // Filter out which values have not been read yet
        Set<CellReference> thusFarUncachedValues = Sets.difference(cellReferences, snapshotCachedValues.keySet());

        // Similar to above, except we check the local values next
        Map<CellReference, CacheValue> localCachedValues = KeyedStream.of(thusFarUncachedValues)
                .map(localUpdates::get)
                .filter(Objects::nonNull)
                .map(LocalCacheEntry::value)
                .collectToMap();

        // Now we need to read the remaining values from the DB
        Set<CellReference> nowActuallyUncachedValues =
                Sets.difference(thusFarUncachedValues, localCachedValues.keySet());

        Map<Cell, byte[]> remoteReadValues = valueLoader.apply(
                tableReference,
                nowActuallyUncachedValues.stream().map(CellReference::cell).collect(Collectors.toSet()));

        remoteReadValues.forEach((cell, value) ->
                localUpdates.put(CellReference.of(tableReference, cell), LocalCacheEntry.read(CacheValue.of(value))));

        // The map does not return an entry if a value is absent; we want to cache this fact.
        Set<CellReference> emptyCells = Sets.difference(nowActuallyUncachedValues, remoteReadValues.keySet());
        emptyCells.forEach(cell -> localUpdates.put(cell, LocalCacheEntry.read(CacheValue.empty())));

        // Now we need to combine all the maps

        ImmutableMap.Builder<Cell, byte[]> output = ImmutableMap.builder();
        output.putAll(remoteReadValues);
        output.putAll(filterEmptyValues(snapshotCachedValues));
        output.putAll(filterEmptyValues(localCachedValues));

        return output.build();
    }

    @Override
    public synchronized TransactionDigest getDigest() {
        // The digest doesn't contain values cached locally due to writes because they will be filtered out based on
        // locks anyway.
        Map<CellReference, CacheValue> loadedValues = KeyedStream.stream(localUpdates)
                .filter(entry -> entry.status().equals(Status.READ))
                .map(LocalCacheEntry::value)
                .collectToMap();
        return TransactionDigest.of(loadedValues);
    }

    private Map<Cell, byte[]> filterEmptyValues(Map<CellReference, CacheValue> snapshotCachedValues) {
        return KeyedStream.stream(snapshotCachedValues)
                .filter(value -> value.value().isPresent())
                .map(value -> value.value().get())
                .mapKeys(CellReference::cell)
                .collectToMap();
    }
    // private final ValueCacheSnapshot snapshot;
    // private final ValueStore localChanges;
    //
    // public TransactionScopedCacheImpl(ValueCacheSnapshot snapshot) {
    //     this.snapshot = snapshot;
    //     this.localChanges = new ValueStoreImpl();
    // }
    //
    // @Override
    // public void invalidate(TableReference tableReference, Cell cell) {
    //     localChanges.putLockedCell(CellReference.of(tableReference, cell));
    // }
    //
    // @Override
    // public Map<Cell, byte[]> get(
    //         TableReference tableReference,
    //         Set<Cell> cells,
    //         BiFunction<TableReference, Set<Cell>, java.util.Map<Cell, byte[]>> valueLoader) {
    //     if (!snapshot.canCache(tableReference)) {
    //         return valueLoader.apply(tableReference, cells);
    //     }
    //
    //     Map<Cell, Optional<byte[]>> cachedCells = readFromCache(tableReference, cells);
    //     Set<Cell> uncachedCells = Sets.difference(cells, cachedCells.keySet());
    //
    //     Map<Cell, byte[]> loadedValues = valueLoader.apply(tableReference, uncachedCells);
    //     loadedValues.entrySet().stream()
    //             .filter(entry -> filterLockedCells(tableReference, entry.getKey()))
    //             .forEach(entry -> cacheLocally(tableReference, entry.getKey(), entry.getValue()));
    //
    //     Sets.difference(uncachedCells, loadedValues.keySet()).stream()
    //             .filter(cell -> filterLockedCells(tableReference, cell))
    //             .forEach(cell -> cacheLocally(tableReference, cell, new byte[0]));
    //
    //     ImmutableMap.Builder<Cell, byte[]> builder = ImmutableMap.builder();
    //     builder.putAll(loadedValues);
    //     cachedCells.entrySet().stream()
    //             .filter(entry -> entry.getValue().isPresent())
    //             .forEach(entry -> builder.put(entry.getKey(), entry.getValue().get()));
    //
    //     return builder.build();
    // }
    //
    // private void cacheLocally(TableReference tableReference, Cell key, byte[] value) {
    //     localChanges.putValue(CellReference.of(tableReference, key), CacheValue.of(value));
    // }
    //
    // private boolean filterLockedCells(TableReference tableReference, Cell cell) {
    //     Optional<CacheEntry> value = getEntryFromCache(CellReference.of(tableReference, cell));
    //     Preconditions.checkState(
    //             !value.isPresent() || !value.get().status().isUnlocked(),
    //             "Value must either not be "
    //                     + "cached, or must be locked. Otherwise, this value should have been read from the cache!");
    //     return !value.isPresent();
    // }
    //
    // @Override
    // public TransactionDigest getDigest() {
    //     return localChanges.getSnapshot() // need to think of a better thing here
    // }
    //
    // private Optional<CacheEntry> getEntryFromCache(CellReference cellReference) {
    //     Optional<CacheEntry> snapshotRead = this.snapshot.getValue(cellReference);
    //
    //     if (snapshotRead.isPresent()) {
    //         return snapshotRead;
    //     } else {
    //         return localChanges.getSnapshot().getValue(cellReference);
    //     }
    // }
    //
    // private Map<Cell, Optional<byte[]>> readFromCache(TableReference tableReference, Set<Cell> cells) {
    //     Set<CellReference> cellReferences = cells.stream()
    //             .map(cell -> CellReference.of(tableReference, cell))
    //             .collect(Collectors.toSet());
    //     return null;
    // }

    @Value.Immutable
    public interface LocalCacheEntry {
        Status status();

        CacheValue value();

        static LocalCacheEntry read(CacheValue value) {
            return ImmutableLocalCacheEntry.builder()
                    .status(Status.READ)
                    .value(value)
                    .build();
        }

        static LocalCacheEntry write(CacheValue value) {
            return ImmutableLocalCacheEntry.builder()
                    .status(Status.WRITE)
                    .value(value)
                    .build();
        }

        static LocalCacheEntry hit(CacheValue value) {
            return ImmutableLocalCacheEntry.builder()
                    .status(Status.HIT)
                    .value(value)
                    .build();
        }

        enum Status {
            READ,
            WRITE,
            HIT;
        }
    }
}
