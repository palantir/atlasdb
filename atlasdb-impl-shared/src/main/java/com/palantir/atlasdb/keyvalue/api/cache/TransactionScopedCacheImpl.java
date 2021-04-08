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
import com.palantir.logsafe.Preconditions;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public final class TransactionScopedCacheImpl implements TransactionScopedCache {
    private final ValueCacheSnapshot snapshot;
    private final ValueStore localChanges;

    public TransactionScopedCacheImpl(ValueCacheSnapshot snapshot) {
        this.snapshot = snapshot;
        this.localChanges = new ValueStoreImpl();
    }

    @Override
    public void invalidate(TableReference tableReference, Cell cell) {
        localChanges.putLockedCell(CellReference.of(tableReference, cell));
    }

    @Override
    public Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, java.util.Map<Cell, byte[]>> valueLoader) {
        if (!snapshot.canCache(tableReference)) {
            return valueLoader.apply(tableReference, cells);
        }

        Map<Cell, Optional<byte[]>> cachedCells = readFromCache(tableReference, cells);
        Set<Cell> uncachedCells = Sets.difference(cells, cachedCells.keySet());

        Map<Cell, byte[]> loadedValues = valueLoader.apply(tableReference, uncachedCells);
        loadedValues.entrySet().stream()
                .filter(entry -> filterLockedCells(tableReference, entry.getKey()))
                .forEach(entry -> cacheLocally(tableReference, entry.getKey(), entry.getValue()));

        Sets.difference(uncachedCells, loadedValues.keySet()).stream()
                .filter(cell -> filterLockedCells(tableReference, cell))
                .forEach(cell -> cacheLocally(tableReference, cell, new byte[0]));

        ImmutableMap.Builder<Cell, byte[]> builder = ImmutableMap.builder();
        builder.putAll(loadedValues);
        cachedCells.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .forEach(entry -> builder.put(entry.getKey(), entry.getValue().get()));

        return builder.build();
    }

    private void cacheLocally(TableReference tableReference, Cell key, byte[] value) {
        localChanges.putValue(CellReference.of(tableReference, key), CacheValue.of(value));
    }

    private boolean filterLockedCells(TableReference tableReference, Cell cell) {
        Optional<CacheEntry> value = getEntryFromCache(CellReference.of(tableReference, cell));
        Preconditions.checkState(
                !value.isPresent() || !value.get().status().isUnlocked(),
                "Value must either not be "
                        + "cached, or must be locked. Otherwise, this value should have been read from the cache!");
        return !value.isPresent();
    }

    @Override
    public TransactionDigest getDigest() {
        return localChanges.getSnapshot() // need to think of a better thing here
    }

    private Optional<CacheEntry> getEntryFromCache(CellReference cellReference) {
        Optional<CacheEntry> snapshotRead = this.snapshot.getValue(cellReference);

        if (snapshotRead.isPresent()) {
            return snapshotRead;
        } else {
            return localChanges.getSnapshot().getValue(cellReference);
        }
    }

    private Map<Cell, Optional<byte[]>> readFromCache(TableReference tableReference, Set<Cell> cells) {
        Set<CellReference> cellReferences = cells.stream()
                .map(cell -> CellReference.of(tableReference, cell))
                .collect(Collectors.toSet());
        return null;
    }
}
