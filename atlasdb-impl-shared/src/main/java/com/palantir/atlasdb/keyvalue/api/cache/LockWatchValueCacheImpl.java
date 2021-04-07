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

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.AtlasLockDescriptorUtils;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.CommitUpdate.Visitor;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LockWatchValueCacheImpl implements LockWatchValueCache {
    private final ValueStore valueStore;
    private final LockWatchEventCache eventCache;
    private final SnapshotStore snapshotStore;

    private volatile Optional<LockWatchVersion> currentVersion = Optional.empty();

    public LockWatchValueCacheImpl(LockWatchEventCache eventCache) {
        this.eventCache = eventCache;
        this.valueStore = new ValueStoreImpl();
        this.snapshotStore = new SnapshotStoreImpl();
    }

    @Override
    public void processStartTransactions(java.util.Set<Long> startTimestamps) {
        TransactionsLockWatchUpdate updateForTransactions =
                eventCache.getUpdateForTransactions(startTimestamps, currentVersion);
        updateCurrentVersion(updateForTransactions);
        updateStores(updateForTransactions);
    }

    private void updateStores(TransactionsLockWatchUpdate updateForTransactions) {
        Multimap<Sequence, StartTimestamp> reversedMap = createSequenceTimestampMultimap(updateForTransactions);
        updateForTransactions.events().forEach(event -> {
            valueStore.applyEvent(event);
            Sequence sequence = Sequence.of(event.sequence());
            reversedMap
                    .get(sequence)
                    .forEach(timestamp -> snapshotStore.storeSnapshot(sequence, timestamp, valueStore.getSnapshot()));
        });
    }

    private Multimap<Sequence, StartTimestamp> createSequenceTimestampMultimap(
            TransactionsLockWatchUpdate updateForTransactions) {
        return KeyedStream.stream(updateForTransactions.startTsToSequence())
                .mapKeys(StartTimestamp::of)
                .map(LockWatchVersion::version)
                .map(Sequence::of)
                .mapEntries((timestamp, sequence) -> Maps.immutableEntry(sequence, timestamp))
                .collectToSetMultimap();
    }

    private void updateCurrentVersion(TransactionsLockWatchUpdate updateForTransactions) {
        Optional<LockWatchVersion> maybeLatestVersion = updateForTransactions.startTsToSequence().values().stream()
                .max(Comparator.comparingLong(LockWatchVersion::version));

        maybeLatestVersion.ifPresent(latestVersion -> {
            if (!currentVersion.isPresent()) {
                // first update after a snapshot or creation of cache
                currentVersion = maybeLatestVersion;
            } else {
                LockWatchVersion current = currentVersion.get();

                if (!current.id().equals(latestVersion.id())) {
                    // leader election
                    clearCache();
                    currentVersion = maybeLatestVersion;
                } else if (current.version() < latestVersion.version()) {
                    // normal update
                    currentVersion = maybeLatestVersion;
                }
            }
        });
    }

    private void clearCache() {
        valueStore.reset();
        snapshotStore.reset();
    }

    @Override
    public void updateCache(TransactionDigest digest, long startTs) {
        CommitUpdate commitUpdate = eventCache.getCommitUpdate(startTs);
        commitUpdate.accept(new Visitor<Void>() {
            @Override
            public Void invalidateAll() {
                // If this is an election, we should throw. If not and we are not a serialisable transaction, we are
                // *technically* ok to go on here. However, we cannot determine which of the two we are in, so I think
                // we just need to throw anyway.
                throw new TransactionLockWatchFailedException("Leader election by commit time, must retry.");
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

    private Stream<CellReference> extractTableAndCell(LockDescriptor descriptor) {
        return AtlasLockDescriptorUtils.candidateCells(descriptor).stream();
    }
}
