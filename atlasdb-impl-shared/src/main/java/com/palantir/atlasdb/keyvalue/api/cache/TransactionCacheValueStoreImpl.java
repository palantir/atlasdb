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
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
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
    public void recordRemoteWrite(TableReference table, Cell cell) {
        CellReference cellReference = CellReference.of(table, cell);
        recordRemoteWriteInternal(cellReference);
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
                    .forEach(cell -> cacheRemoteReadInternal(cell, CacheValue.empty()));
        }
    }

    @Override
    public TransactionCacheValueStore createWithFilteredSnapshot(CommitUpdate commitUpdate) {
        TransactionCacheValueStoreImpl newStore =
                new TransactionCacheValueStoreImpl(FilteringValueCacheSnapshot.create(snapshot, commitUpdate));

        localUpdates.forEach((cell, cacheEntry) -> {
            switch (cacheEntry.status()) {
                case READ:
                    newStore.cacheRemoteReadInternal(cell, getReadValue(cacheEntry.value()));
                    break;
                case WRITE:
                    newStore.recordRemoteWriteInternal(cell);
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
        Set<Cell> locallyWrittenCells = getLocallyWrittenCells(table, cells);
        Set<Cell> cacheableCells = Sets.difference(cells, locallyWrittenCells);

        Map<Cell, CacheValue> locallyCachedValues = getLocallyCachedValues(table, cacheableCells);

        // Filter out which values have not been read yet
        Set<Cell> remainingCells = Sets.difference(cacheableCells, locallyCachedValues.keySet());

        // Read values from the snapshot. For the hits, mark as hit in the local map.
        Map<Cell, CacheValue> snapshotCachedValues = getSnapshotValues(table, remainingCells);
        snapshotCachedValues.forEach((cell, value) -> cacheHitInternal(table, cell, value));

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
                .map(TransactionCacheValueStoreImpl::getReadValue)
                .collectToMap();
    }

    @Override
    public Set<CellReference> getHitDigest() {
        return KeyedStream.stream(localUpdates)
                .filter(entry -> entry.status().equals(Status.HIT))
                .keys()
                .collect(Collectors.toSet());
    }

    private void cacheHitInternal(TableReference table, Cell cell, CacheValue value) {
        localUpdates.compute(CellReference.of(table, cell), (_unused, previousValue) -> {
            if (previousValue != null) {
                throw new SafeIllegalStateException(
                        "Should not be attempting to record hits for keys that have been written to or read from the"
                                + " KVS",
                        UnsafeArg.of("table", table),
                        UnsafeArg.of("cell", cell),
                        UnsafeArg.of("previousValue", previousValue));
            } else {
                return LocalCacheEntry.hit(value);
            }
        });
    }

    private Map<Cell, CacheValue> getLocallyCachedValues(TableReference table, Set<Cell> cells) {
        return KeyedStream.of(cells)
                .map(cell -> localUpdates.get(CellReference.of(table, cell)))
                .filter(Objects::nonNull)
                .filter(value -> !value.status().equals(Status.WRITE))
                .map(LocalCacheEntry::value)
                .map(TransactionCacheValueStoreImpl::getReadValue)
                .collectToMap();
    }

    private Set<Cell> getLocallyWrittenCells(TableReference table, Set<Cell> cells) {
        return KeyedStream.of(cells)
                .map(cell -> localUpdates.get(CellReference.of(table, cell)))
                .filter(Objects::nonNull)
                .filter(value -> value.status().equals(Status.WRITE))
                .keys()
                .collect(Collectors.toSet());
    }

    private void recordRemoteWriteInternal(CellReference cellReference) {
        if (snapshot.isUnlocked(cellReference)) {
            localUpdates.put(cellReference, LocalCacheEntry.write());
        }
    }

    private void cacheRemoteReadInternal(CellReference cell, CacheValue value) {
        if (snapshot.isUnlocked(cell)) {
            localUpdates.putIfAbsent(cell, LocalCacheEntry.read(value));
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

    private static CacheValue getReadValue(Optional<CacheValue> cacheValue) {
        return cacheValue.orElseThrow(() -> new SafeIllegalStateException("Reads must have a cache value present"));
    }

    @Value.Immutable
    interface LocalCacheEntry {

        Status status();

        Optional<CacheValue> value();

        static LocalCacheEntry read(CacheValue value) {
            return ImmutableLocalCacheEntry.builder()
                    .status(Status.READ)
                    .value(value)
                    .build();
        }

        static LocalCacheEntry write() {
            return ImmutableLocalCacheEntry.builder().status(Status.WRITE).build();
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
