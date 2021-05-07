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

import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.streams.KeyedStream;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface ValueCacheSnapshotImpl extends ValueCacheSnapshot {
    Map<CellReference, CacheEntry> values();

    Set<TableReference> enabledTables();

    @Override
    default Optional<CacheEntry> getValue(CellReference tableAndCell) {
        return values().get(tableAndCell).toJavaOptional();
    }

    @Override
    default boolean isUnlocked(CellReference tableAndCell) {
        return isWatched(tableAndCell.tableRef())
                && getValue(tableAndCell).map(CacheEntry::isUnlocked).orElse(true);
    }

    @Override
    default boolean isWatched(TableReference tableReference) {
        return enabledTables().contains(tableReference);
    }

    static ValueCacheSnapshot of(Map<CellReference, CacheEntry> values, Set<TableReference> enabledTables) {
        return ImmutableValueCacheSnapshotImpl.builder()
                .values(values)
                .enabledTables(enabledTables)
                .build();
    }

    default ValueCacheSnapshot withLockedCells(java.util.Set<CellReference> lockedCells) {
        java.util.Map<CellReference, CacheEntry> lockedValues =
                KeyedStream.of(lockedCells).map(_unused -> CacheEntry.locked()).collectToMap();

        Map<CellReference, CacheEntry> values = values();
        for (java.util.Map.Entry<CellReference, CacheEntry> entry : lockedValues.entrySet()) {
            values = values.put(entry.getKey(), entry.getValue());
        }

        return ImmutableValueCacheSnapshotImpl.builder()
                .from(this)
                .values(values)
                .build();
    }
}
