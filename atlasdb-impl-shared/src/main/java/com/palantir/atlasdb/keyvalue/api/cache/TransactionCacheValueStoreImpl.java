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
import com.palantir.lock.watch.CommitUpdate;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Value;

@NotThreadSafe
final class TransactionCacheValueStoreImpl implements TransactionCacheValueStore {
    private final ValueCacheSnapshot snapshot;
    private final Map<CellReference, LocalCacheEntry> localUpdates;

    TransactionCacheValueStoreImpl(ValueCacheSnapshot snapshot) {
        this.snapshot = snapshot;
        this.localUpdates = new HashMap<>();
    }

    @Override
    public boolean isWatched(TableReference table) {
        return snapshot.isWatched(table);
    }

    @Override
    public void cacheRemoteWrite(TableReference table, Cell cell, CacheValue value) {
        CellReference cellReference = CellReference.of(table, cell);
        cacheRemoteWriteInternal(cellReference, value);
    }

    @Override
    public void cacheRemoteReads(TableReference table, Map<Cell, byte[]> remoteReadValues) {
        if (snapshot.isWatched(table)) {
            KeyedStream.stream(remoteReadValues)
                    .mapKeys(cell -> CellReference.of(table, cell))
                    .map(CacheValue::of)
                    .forEach(this::cacheRemoteReadInternal);
        }
    }

    @Override
    public void cacheEmptyReads(TableReference table, Set<Cell> emptyCells) {
        if (snapshot.isWatched(table)) {
            emptyCells.stream()
                    .map(cell -> CellReference.of(table, cell))
                    .filter(snapshot::isUnlocked)
                    .forEach(cell -> localUpdates.put(cell, LocalCacheEntry.read(CacheValue.empty())));
        }
    }

    @Override
    public TransactionCacheValueStore createWithFilteredSnapshot(CommitUpdate commitUpdate) {
        TransactionCacheValueStoreImpl newStore =
                new TransactionCacheValueStoreImpl(FilteringValueCacheSnapshot.create(snapshot, commitUpdate));

        localUpdates.forEach((cell, cacheEntry) -> {
            switch (cacheEntry.status()) {
                case READ:
                    newStore.cacheRemoteReadInternal(cell, cacheEntry.value());
                    break;
                case WRITE:
                    newStore.cacheRemoteWriteInternal(cell, cacheEntry.value());
                    break;
                case HIT:
                default:
                    // no-op - hits contain the same values as stored in the snapshot
            }
        });

        return newStore;
    }

    @Override
    public Map<Cell, CacheValue> getCachedValues(TableReference table, Set<Cell> cells) {
        Map<Cell, CacheValue> locallyCachedValues = getLocallyCachedValues(table, cells);

        // Filter out which values have not been read yet
        Set<Cell> remainingCells = Sets.difference(cells, locallyCachedValues.keySet());

        // Read values from the snapshot. For the hits, mark as hit in the local map.
        Map<Cell, CacheValue> snapshotCachedValues = getSnapshotValues(table, remainingCells);
        snapshotCachedValues.forEach(
                (cell, value) -> localUpdates.put(CellReference.of(table, cell), LocalCacheEntry.hit(value)));

        return ImmutableMap.<Cell, CacheValue>builder()
                .putAll(locallyCachedValues)
                .putAll(snapshotCachedValues)
                .build();
    }

    @Override
    public Map<CellReference, CacheValue> getValueDigest() {
        return KeyedStream.stream(localUpdates)
                .filter(entry -> entry.status().equals(Status.READ))
                .map(LocalCacheEntry::value)
                .collectToMap();
    }

    @Override
    public Set<CellReference> getHitDigest() {
        return KeyedStream.stream(localUpdates)
                .filter(entry -> entry.status().equals(Status.HIT))
                .keys()
                .collect(Collectors.toSet());
    }

    private Map<Cell, CacheValue> getLocallyCachedValues(TableReference table, Set<Cell> cells) {
        return KeyedStream.of(cells)
                .map(cell -> localUpdates.get(CellReference.of(table, cell)))
                .filter(Objects::nonNull)
                .map(LocalCacheEntry::value)
                .collectToMap();
    }

    private void cacheRemoteWriteInternal(CellReference cellReference, CacheValue value) {
        if (snapshot.isWatched(cellReference.tableRef()) && snapshot.isUnlocked(cellReference)) {
            localUpdates.put(cellReference, LocalCacheEntry.write(value));
        }
    }

    private void cacheRemoteReadInternal(CellReference cell, CacheValue value) {
        if (snapshot.isUnlocked(cell)) {
            localUpdates.put(cell, LocalCacheEntry.read(value));
        }
    }

    private Map<Cell, CacheValue> getSnapshotValues(TableReference table, Set<Cell> cells) {
        return KeyedStream.of(cells)
                .map(cell -> snapshot.getValue(CellReference.of(table, cell)))
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
