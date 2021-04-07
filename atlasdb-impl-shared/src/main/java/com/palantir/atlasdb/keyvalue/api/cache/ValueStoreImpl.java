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
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.UnsafeArg;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

public final class ValueStoreImpl implements ValueStore {
    private final StructureHolder<Map<CellReference, CacheEntry>> values;

    public ValueStoreImpl() {
        values = StructureHolder.create(HashMap.empty());
    }

    @Override
    public void reset() {
        values.resetToInitialValue();
    }

    @Override
    public void putLockedCell(CellReference cellReference) {
        values.with(map -> map.put(cellReference, CacheEntry.locked()));
    }

    @Override
    public void clearLockedCell(CellReference cellReference) {
        values.with(map -> map.get(cellReference)
                .toJavaOptional()
                .filter(entry -> !entry.status().isUnlocked())
                .map(_unused -> map.remove(cellReference))
                .orElse(map));
    }

    @Override
    public void putValue(CellReference cellReference, CacheValue value) {
        values.with(map -> map.put(cellReference, CacheEntry.unlocked(value), (oldValue, newValue) -> {
            Preconditions.checkState(
                    oldValue.status().isUnlocked() && oldValue.equals(newValue),
                    "Trying to cache a value which is " + "either locked or is not equal to a currently cached value",
                    UnsafeArg.of("table", cellReference.tableRef()),
                    UnsafeArg.of("cell", cellReference.cell()),
                    UnsafeArg.of("oldValue", oldValue),
                    UnsafeArg.of("newValue", newValue));
            return newValue;
        }));
    }

    @Override
    public ValueCacheSnapshot getSnapshot() {
        return ValueCacheSnapshotImpl.of(values.getStructure());
    }
}
