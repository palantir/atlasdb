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

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.UnlockEvent;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import java.util.List;

public final class LockWatchValueCacheImpl implements LockWatchValueCache {
    private final ValueStore valueStore;
    private final StructureHolder<Set<TableReference>> watchedTables;

    public LockWatchValueCacheImpl() {
        valueStore = new ValueStoreImpl();
        watchedTables = StructureHolder.create(HashSet.empty());
    }

    @Override
    public void applyEvents(List<LockWatchEvent> events) {
        events.forEach(event -> event.accept(new LockWatchVisitor()));
    }

    @Override
    public void updateCache(Void digest, long startTs) {}

    @Override
    public void createTransactionScopedCache(long startTs) {}

    private TableReference extractTableReference(LockWatchReference lockWatchReference) {
        // todo(jshah): implement
        return null;
    }

    private TableAndCell extractTableAndCell(LockDescriptor descriptor) {
        // todo(jshah): implement
        return null;
    }

    private final class LockWatchVisitor implements LockWatchEvent.Visitor<Void> {
        @Override
        public Void visit(LockEvent lockEvent) {
            lockEvent.lockDescriptors().stream()
                    .map(LockWatchValueCacheImpl.this::extractTableAndCell)
                    .forEach(valueStore::putLockedCell);
            return null;
        }

        @Override
        public Void visit(UnlockEvent unlockEvent) {
            unlockEvent.lockDescriptors().stream()
                    .map(LockWatchValueCacheImpl.this::extractTableAndCell)
                    .forEach(valueStore::clearLockedCell);
            return null;
        }

        @Override
        public Void visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            lockWatchCreatedEvent.references().stream()
                    .map(LockWatchValueCacheImpl.this::extractTableReference)
                    .forEach(tableReference -> watchedTables.with(tables -> tables.add(tableReference)));
            lockWatchCreatedEvent.lockDescriptors().stream()
                    .map(LockWatchValueCacheImpl.this::extractTableAndCell)
                    .forEach(valueStore::putLockedCell);
            return null;
        }
    }
}
