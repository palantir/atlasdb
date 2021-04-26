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
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.immutables.value.Value;

final class TransactionScopedCacheImpl implements TransactionScopedCache {
    private final TransactionCacheValueStore valueStore;

    private TransactionScopedCacheImpl(ValueCacheSnapshot snapshot) {
        valueStore = new TransactionCacheValueStoreImpl(snapshot);
    }

    static TransactionScopedCache create(ValueCacheSnapshot snapshot) {
        return new TransactionScopedCacheImpl(snapshot);
    }

    // TODO(jshah): figure out how we can improve perf given that this is now synchronised (although maybe its fine
    //  because this operation is fast?) If not, we could autobatch.
    @Override
    public synchronized void write(TableReference tableReference, Cell cell, CacheValue value) {
        valueStore.cacheRemoteWrite(tableReference, cell, value);
    }

    // TODO(jshah): as above. Equally, we should probably use the async value loader, then have it get afterwards,
    //  thus reducing the time spent in this method.
    @Override
    public synchronized Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, Map<Cell, byte[]>> valueLoader) {
        // Short-cut all the logic below if the table is not watched.
        if (!valueStore.isWatched(tableReference)) {
            return valueLoader.apply(tableReference, cells);
        }

        CacheLookupResult cacheLookup = cacheLookup(tableReference, cells);

        Map<Cell, byte[]> remoteReadValues =
                getAndCacheRemoteReads(tableReference, cacheLookup.missedCells(), valueLoader);
        cacheEmptyReads(tableReference, cacheLookup.missedCells(), remoteReadValues);

        return ImmutableMap.<Cell, byte[]>builder()
                .putAll(remoteReadValues)
                .putAll(filterEmptyValues(cacheLookup.cacheHits()))
                .build();
    }

    @Override
    public synchronized ValueDigest getValueDigest() {
        return ValueDigest.of(valueStore.getValueDigest());
    }

    @Override
    public synchronized HitDigest getHitDigest() {
        return HitDigest.of(valueStore.getHitDigest());
    }

    private CacheLookupResult cacheLookup(TableReference table, Set<Cell> cells) {
        Map<Cell, CacheValue> cachedValues = valueStore.getCachedValues(table, cells);
        Set<Cell> uncachedCells = Sets.difference(cells, cachedValues.keySet());
        return CacheLookupResult.of(cachedValues, uncachedCells);
    }

    private void cacheEmptyReads(
            TableReference tableReference, Set<Cell> uncachedCells, Map<Cell, byte[]> remoteReadValues) {
        // The get method does not return an entry if a value is absent; we want to cache this fact
        Set<Cell> emptyCells = Sets.difference(uncachedCells, remoteReadValues.keySet());
        valueStore.cacheEmptyReads(tableReference, emptyCells);
    }

    private Map<Cell, byte[]> getAndCacheRemoteReads(
            TableReference tableReference,
            Set<Cell> uncachedCells,
            BiFunction<TableReference, Set<Cell>, Map<Cell, byte[]>> valueLoader) {
        Map<Cell, byte[]> remoteReadValues = valueLoader.apply(tableReference, uncachedCells);
        valueStore.cacheRemoteReads(tableReference, remoteReadValues);
        return remoteReadValues;
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
