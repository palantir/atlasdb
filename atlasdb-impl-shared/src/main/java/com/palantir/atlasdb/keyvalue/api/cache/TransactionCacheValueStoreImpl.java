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
import com.palantir.atlasdb.keyvalue.api.cache.TransactionCacheValueStoreImpl.LocalCacheEntry.Status;
import com.palantir.common.streams.KeyedStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

final class TransactionCacheValueStoreImpl implements TransactionCacheValueStore {
    private final ValueCacheSnapshot snapshot;
    private final Map<CellReference, LocalCacheEntry> localUpdates;

    TransactionCacheValueStoreImpl(ValueCacheSnapshot snapshot) {
        this.snapshot = snapshot;
        this.localUpdates = new HashMap<>();
    }

    @Override
    public void cacheLocalWrite(TableReference tableReference, Cell cell, CacheValue value) {
        CellReference cellReference = CellReference.of(tableReference, cell);
        if (snapshot.isWatched(tableReference) && snapshot.isUnlocked(cellReference)) {
            localUpdates.put(cellReference, LocalCacheEntry.write(value));
        }
    }

    @Override
    public void updateLocalReads(TableReference tableReference, Map<Cell, byte[]> remoteReadValues) {
        KeyedStream.stream(remoteReadValues)
                .mapKeys(cell -> CellReference.of(tableReference, cell))
                .map(CacheValue::of)
                .filterKeys(snapshot::isUnlocked)
                .forEach((cell, value) -> localUpdates.put(cell, LocalCacheEntry.read(value)));
    }

    @Override
    public void updateEmptyReads(TableReference tableReference, Set<CellReference> emptyCells) {
        emptyCells.stream()
                .filter(snapshot::isUnlocked)
                .forEach(cell -> localUpdates.put(cell, LocalCacheEntry.read(CacheValue.empty())));
    }

    @Override
    public Map<CellReference, CacheValue> getCachedValues(Set<CellReference> cellReferences) {
        Map<CellReference, CacheValue> locallyCachedValues = getLocallyCachedValues(cellReferences);

        // Filter out which values have not been read yet
        Set<CellReference> remainingCells = Sets.difference(cellReferences, locallyCachedValues.keySet());

        // Read values from the snapshot. For the hits, mark as hit in the local map.
        Map<CellReference, CacheValue> snapshotCachedValues = getSnapshotValues(remainingCells);
        snapshotCachedValues.forEach(
                (cellReference, value) -> localUpdates.put(cellReference, LocalCacheEntry.hit(value)));

        return ImmutableMap.<CellReference, CacheValue>builder()
                .putAll(locallyCachedValues)
                .putAll(snapshotCachedValues)
                .build();
    }

    @Override
    public Map<CellReference, CacheValue> getTransactionDigest() {
        return KeyedStream.stream(localUpdates)
                .filter(entry -> entry.status().equals(Status.READ))
                .map(LocalCacheEntry::value)
                .collectToMap();
    }

    private Map<CellReference, CacheValue> getLocallyCachedValues(Set<CellReference> cellReferences) {
        return KeyedStream.of(cellReferences)
                .map(localUpdates::get)
                .filter(Objects::nonNull)
                .map(LocalCacheEntry::value)
                .collectToMap();
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
