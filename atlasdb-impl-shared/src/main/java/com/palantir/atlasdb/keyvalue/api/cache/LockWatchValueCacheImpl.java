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
import com.palantir.atlasdb.keyvalue.api.watch.Sequence;
import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The operations on this class *must* be synchronised to protect against concurrent modification of the cache.
 */
public final class LockWatchValueCacheImpl implements LockWatchValueCache {
    private final LockWatchEventCache eventCache;
    private final ValueStore valueStore;
    private final SnapshotStore snapshotStore;

    private volatile Optional<LockWatchVersion> currentVersion = Optional.empty();

    public LockWatchValueCacheImpl(LockWatchEventCache eventCache) {
        this.eventCache = eventCache;
        this.valueStore = new ValueStoreImpl();
        this.snapshotStore = new SnapshotStoreImpl();
    }

    @Override
    public synchronized void processStartTransactions(Set<Long> startTimestamps) {
        TransactionsLockWatchUpdate updateForTransactions =
                eventCache.getUpdateForTransactions(startTimestamps, currentVersion);
        updateCurrentVersion(updateForTransactions);
        updateStores(updateForTransactions);
    }

    // TODO(jshah): This needs to be *very* carefully wired to ensure that the synchronised aspect here is not an
    //  issue. Chances are that this may need to be re-jigged to take a batch, and be connected to the batched commit
    //  timestamp call.
    @Override
    public synchronized void updateCacheOnCommit(TransactionDigest digest, long startTs) {
        try {
            CommitUpdate commitUpdate = eventCache.getCommitUpdate(startTs);
            commitUpdate.accept(new Visitor<Void>() {
                @Override
                public Void invalidateAll() {
                    // If this is an election, we should throw. If not and we are not a serialisable transaction, we are
                    // *technically* ok to go on here. However, we cannot determine which of the two we are in, so we
                    // throw anyway.
                    throw new TransactionLockWatchFailedException("A Timelock leader election has occurred between the"
                        + " start and commit time of this transaction, and thus all events have been invalidated. This"
                        + " transaction will be retried");
                }

                @Override
                public Void invalidateSome(Set<LockDescriptor> invalidatedLocks) {
                    Set<CellReference> invalidatedCells = invalidatedLocks.stream()
                            .flatMap(LockWatchValueCacheImpl::extractTableAndCell)
                            .collect(Collectors.toSet());
                    KeyedStream.stream(digest.loadedValues())
                            .filterKeys(cellReference -> !invalidatedCells.contains(cellReference))
                            .forEach(valueStore::putValue);
                    return null;
                }
            });
        } finally {
            // Remove the timestamp, and if the sequence has not updated, update the snapshot with the reads from
            // this transaction.
            snapshotStore
                    .removeTimestamp(StartTimestamp.of(startTs))
                    .ifPresent(sequence -> currentVersion.ifPresent(current -> {
                        if (sequence.value() == current.version()) {
                            snapshotStore.updateSnapshot(sequence, valueStore.getSnapshot());
                        }
                    }));
        }
    }

    @Override
    public TransactionScopedCache createTransactionScopedCache(long startTs) {
        // Chances are that, instead of throwing, we should just return a no-op cache that doesn't cache anything;
        // however, this does retry, so maybe this is fine too (this should only happen around leader elections).
        return new TransactionScopedCacheImpl(snapshotStore
                .getSnapshot(StartTimestamp.of(startTs))
                .orElseThrow(() -> new TransactionLockWatchFailedException("Snapshot missing for timestamp")));
    }

    /**
     * In order to maintain the necessary invariants, we need to do the following:
     *
     *  1. For each new event, we apply it to the cache. The effects of this application is described in
     *     {@link LockWatchValueCache}.
     *  2. For each transaction, we must ensure that we store a snapshot of the cache at the sequence corresponding
     *     to the transaction's start timestamp. Note that not every sequence will have a corresponding timestamp, so we
     *     don't bother storing a snapshot for those sequences. Also note that we know that each call here will only
     *     ever have new events, and that consecutive calls to this method will *always* have increasing sequences
     *     (without this last guarantee, we'd need to store snapshots for all sequences).
     */
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

    private static Stream<CellReference> extractTableAndCell(LockDescriptor descriptor) {
        // TODO(jshah): this has potentially large blow-up for cells with lots of zero bytes. We should probably make
        //  users opt-in or warn when this is the case.
        return AtlasLockDescriptorUtils.candidateCells(descriptor).stream();
    }
}
