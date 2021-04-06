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
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.CommitUpdate.Visitor;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchReferencesVisitor;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LockWatchValueCacheImpl implements LockWatchValueCache {
    private final ValueStore valueStore;
    private final StructureHolder<Set<TableReference>> watchedTables;
    private final LockWatchEventCache eventCache;

    // todo(jshah): implement version tracking
    private volatile Optional<LockWatchVersion> currentVersion = Optional.empty();

    public LockWatchValueCacheImpl(LockWatchEventCache eventCache) {
        this.eventCache = eventCache;
        this.valueStore = new ValueStoreImpl();
        this.watchedTables = StructureHolder.create(HashSet.empty());
    }

    @Override
    public void processStartTransactions(java.util.Set<Long> startTimestamps) {
        TransactionsLockWatchUpdate updateForTransactions =
                eventCache.getUpdateForTransactions(startTimestamps, currentVersion);
        // update current version
        updateForTransactions.events().forEach(event -> event.accept(new LockWatchVisitor()));
    }

    @Override
    public void updateCache(TransactionDigest digest, long startTs) {
        CommitUpdate commitUpdate = eventCache.getCommitUpdate(startTs);
        commitUpdate.accept(new Visitor<Void>() {
            @Override
            public Void invalidateAll() {
                // If this is an election, we should throw. If not and we are not a serialisable transaction, we are
                // *technically* ok to go on here.
                return null;
            }

            @Override
            public Void invalidateSome(java.util.Set<LockDescriptor> invalidatedLocks) {
                java.util.Set<CellReference> invalidatedCells = invalidatedLocks.stream()
                        .flatMap(LockWatchValueCacheImpl.this::extractTableAndCell)
                        .collect(Collectors.toSet());
                KeyedStream.stream(digest.loadedValues())
                        .filterKeys(cellReference -> !invalidatedCells.contains(cellReference))
                        .forEach((valueStore::putValue));
                return null;
            }
        });
    }

    @Override
    public TransactionScopedCache createTransactionScopedCache(long startTs) {
        // todo(jshah): implement
        return new TransactionScopedCacheImpl();
    }

    private TableReference extractTableReference(LockWatchReference lockWatchReference) {
        return lockWatchReference
                .accept(LockWatchReferencesVisitor.INSTANCE)
                .orElseThrow(() -> new RuntimeException("Failed to parse table reference from lock watch reference"));
    }

    private Stream<CellReference> extractTableAndCell(LockDescriptor descriptor) {
        return AtlasLockDescriptorUtils.candidateCells(descriptor).stream();
    }

    private void applyLockedDescriptors(java.util.Set<LockDescriptor> lockDescriptors) {
        lockDescriptors.stream()
                .flatMap(LockWatchValueCacheImpl.this::extractTableAndCell)
                .forEach(valueStore::putLockedCell);
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
                    .flatMap(LockWatchValueCacheImpl.this::extractTableAndCell)
                    .forEach(valueStore::clearLockedCell);
            return null;
        }

        @Override
        public Void visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            lockWatchCreatedEvent.references().stream()
                    .map(LockWatchValueCacheImpl.this::extractTableReference)
                    .forEach(tableReference -> watchedTables.with(tables -> tables.add(tableReference)));
            applyLockedDescriptors(lockWatchCreatedEvent.lockDescriptors());
            return null;
        }
    }
}
