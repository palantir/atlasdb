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

    // TODO(jshah): figure out how we can improve perf given that this is now synchronised. Maybe its fine, but maybe
    //  we should instead autobatch?
    @Override
    public synchronized void write(TableReference tableReference, Cell cell, CacheValue value) {
        CellReference cellReference = CellReference.of(tableReference, cell);
        if (snapshot.isWatched(tableReference) && snapshot.isUnlocked(cellReference)) {
            localUpdates.put(cellReference, LocalCacheEntry.write(value));
        }
    }

    // TODO(jshah): as above. Equally, we should probably use the async value loader, then have it get afterwards,
    //  thus reducing the time spent in this method.
    @Override
    public synchronized Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, Map<Cell, byte[]>> valueLoader) {
        Set<CellReference> cellReferences = cells.stream()
                .map(cell -> CellReference.of(tableReference, cell))
                .collect(Collectors.toSet());

        // Read local values first - they may be different in the case of writes.
        Map<CellReference, CacheValue> localCachedValues = getLocallyCachedValues(cellReferences);

        // Filter out which values have not been read yet
        Set<CellReference> thusFarUncachedValues = Sets.difference(cellReferences, localCachedValues.keySet());

        // Read values from the snapshot. For the hits, mark as hit in the local map.
        Map<CellReference, CacheValue> snapshotCachedValues = getSnapshotValues(thusFarUncachedValues);
        snapshotCachedValues.forEach(
                (cellReference, value) -> localUpdates.put(cellReference, LocalCacheEntry.hit(value)));

        // Now we need to read the remaining values from the DB
        Set<CellReference> nowActuallyUncachedValues =
                Sets.difference(thusFarUncachedValues, localCachedValues.keySet());

        Map<Cell, byte[]> remoteReadValues = valueLoader.apply(
                tableReference,
                nowActuallyUncachedValues.stream().map(CellReference::cell).collect(Collectors.toSet()));

        KeyedStream.stream(remoteReadValues)
                .mapKeys(cell -> CellReference.of(tableReference, cell))
                .map(CacheValue::of)
                .filterKeys(snapshot::isUnlocked)
                .forEach((cell, value) -> localUpdates.put(cell, LocalCacheEntry.read(value)));

        // The get method does not return an entry if a value is absent; we want to cache this fact.
        Set<CellReference> emptyCells = Sets.difference(nowActuallyUncachedValues, remoteReadValues.keySet());
        emptyCells.stream()
                .filter(snapshot::isUnlocked)
                .forEach(cell -> localUpdates.put(cell, LocalCacheEntry.read(CacheValue.empty())));

        // Now we need to combine all the maps, filtering out empty values
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

    private Map<CellReference, CacheValue> getSnapshotValues(Set<CellReference> thusFarUncachedValues) {
        return KeyedStream.of(thusFarUncachedValues)
                .map(snapshot::getValue)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(CacheEntry::isUnlocked)
                .map(CacheEntry::value)
                .collectToMap();
    }

    private Map<CellReference, CacheValue> getLocallyCachedValues(Set<CellReference> cellReferences) {
        return KeyedStream.of(cellReferences)
                .map(localUpdates::get)
                .filter(Objects::nonNull)
                .map(LocalCacheEntry::value)
                .collectToMap();
    }

    private Map<Cell, byte[]> filterEmptyValues(Map<CellReference, CacheValue> snapshotCachedValues) {
        return KeyedStream.stream(snapshotCachedValues)
                .filter(value -> value.value().isPresent())
                .map(value -> value.value().get())
                .mapKeys(CellReference::cell)
                .collectToMap();
    }

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
