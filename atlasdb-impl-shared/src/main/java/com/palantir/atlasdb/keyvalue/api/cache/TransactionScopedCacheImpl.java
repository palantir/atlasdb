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
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public final class TransactionScopedCacheImpl implements TransactionScopedCache {
    private final TransactionCacheValueStore valueStore;

    public TransactionScopedCacheImpl(ValueCacheSnapshot snapshot) {
        valueStore = new TransactionCacheValueStoreImpl(snapshot);
    }

    // TODO(jshah): figure out how we can improve perf given that this is now synchronised. Maybe its fine, but maybe
    //  we should instead autobatch?
    @Override
    public synchronized void write(TableReference tableReference, Cell cell, CacheValue value) {
        valueStore.cacheLocalWrite(tableReference, cell, value);
    }

    // TODO(jshah): as above. Equally, we should probably use the async value loader, then have it get afterwards,
    //  thus reducing the time spent in this method.
    @Override
    public synchronized Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, Map<Cell, byte[]>> valueLoader) {
        // We can potentially short-cut all the logic below if the table is not watched.
        if (!valueStore.isWatched(tableReference)) {
            return valueLoader.apply(tableReference, cells);
        }

        Set<CellReference> cellReferences = cells.stream()
                .map(cell -> CellReference.of(tableReference, cell))
                .collect(Collectors.toSet());
        Map<CellReference, CacheValue> cachedValues = valueStore.getCachedValues(cellReferences);

        // Now we need to read the remaining values from the DB
        Set<CellReference> uncachedCells = Sets.difference(cellReferences, cachedValues.keySet());

        Map<Cell, byte[]> remoteReadValues = valueLoader.apply(
                tableReference, uncachedCells.stream().map(CellReference::cell).collect(Collectors.toSet()));

        valueStore.updateLocalReads(tableReference, remoteReadValues);

        // The get method does not return an entry if a value is absent; we want to cache this fact.
        Set<CellReference> emptyCells = Sets.difference(
                uncachedCells,
                remoteReadValues.keySet().stream()
                        .map(cell -> CellReference.of(tableReference, cell))
                        .collect(Collectors.toSet()));

        valueStore.updateEmptyReads(tableReference, emptyCells);

        // Now we need to combine all the maps, filtering out empty values
        return ImmutableMap.<Cell, byte[]>builder()
                .putAll(remoteReadValues)
                .putAll(filterEmptyValues(cachedValues))
                .build();
    }

    @Override
    public synchronized TransactionDigest getDigest() {
        return TransactionDigest.of(valueStore.getTransactionDigest());
    }

    private Map<Cell, byte[]> filterEmptyValues(Map<CellReference, CacheValue> snapshotCachedValues) {
        return KeyedStream.stream(snapshotCachedValues)
                .filter(value -> value.value().isPresent())
                .map(value -> value.value().get())
                .mapKeys(CellReference::cell)
                .collectToMap();
    }
}
