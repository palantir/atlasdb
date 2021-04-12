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

import com.palantir.atlasdb.keyvalue.api.AtlasLockDescriptorUtils;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchReferencesVisitor;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.UnsafeArg;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import java.util.stream.Stream;

public final class ValueStoreImpl implements ValueStore {
    private final StructureHolder<Map<CellReference, CacheEntry>> values;
    private final StructureHolder<Set<TableReference>> watchedTables;
    private final LockWatchVisitor visitor = new LockWatchVisitor();

    public ValueStoreImpl() {
        values = StructureHolder.create(HashMap.empty());
        watchedTables = StructureHolder.create(HashSet.empty());
    }

    @Override
    public void reset() {
        values.resetToInitialValue();
        watchedTables.resetToInitialValue();
    }

    @Override
    public void applyEvent(LockWatchEvent event) {
        event.accept(visitor);
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
        return ValueCacheSnapshotImpl.of(values.getStructure(), watchedTables.getStructure());
    }

    private Stream<CellReference> extractTableAndCell(LockDescriptor descriptor) {
        return AtlasLockDescriptorUtils.candidateCells(descriptor).stream();
    }

    private void applyLockedDescriptors(java.util.Set<LockDescriptor> lockDescriptors) {
        lockDescriptors.stream().flatMap(this::extractTableAndCell).forEach(this::putLockedCell);
    }

    private TableReference extractTableReference(LockWatchReference lockWatchReference) {
        return lockWatchReference
                .accept(LockWatchReferencesVisitor.INSTANCE)
                .orElseThrow(() -> new RuntimeException("Failed to parse table reference from lock watch reference"));
    }

    private final class LockWatchVisitor implements LockWatchEvent.Visitor<Void> {
        @Override
        public Void visit(LockEvent lockEvent) {
            applyLockedDescriptors(lockEvent.lockDescriptors());
            return null;
        }

        @Override
        public Void visit(UnlockEvent unlockEvent) {
            unlockEvent.lockDescriptors().stream()
                    .flatMap(ValueStoreImpl.this::extractTableAndCell)
                    .forEach(ValueStoreImpl.this::clearLockedCell);
            return null;
        }

        @Override
        public Void visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            lockWatchCreatedEvent.references().stream()
                    .map(ValueStoreImpl.this::extractTableReference)
                    .forEach(tableReference -> watchedTables.with(tables -> tables.add(tableReference)));
            applyLockedDescriptors(lockWatchCreatedEvent.lockDescriptors());
            return null;
        }
    }
}
