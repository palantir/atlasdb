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
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.cache.CellUpdate.Visitor;
import com.palantir.atlasdb.keyvalue.api.watch.Sequence;
import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
import com.palantir.atlasdb.transaction.api.TransactionCacheConflictException;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class LockWatchValueScopingCacheImpl implements LockWatchValueScopingCache {
    private final LockWatchEventCache eventCache;
    private final ValueStore valueStore;
    private final SnapshotStore snapshotStore;
    private final CacheStore cacheStore;
    private final UpdateStore updateStore;
    private final double validationProbability;

    private volatile Optional<LockWatchVersion> currentVersion = Optional.empty();

    public LockWatchValueScopingCacheImpl(
            LockWatchEventCache eventCache, long maxCacheSize, double validationProbability) {
        this.eventCache = eventCache;
        this.valueStore = new ValueStoreImpl(maxCacheSize);
        this.validationProbability = validationProbability;
        this.snapshotStore = new SnapshotStoreImpl();
        this.cacheStore = new CacheStoreImpl(snapshotStore);
        this.updateStore = new UpdateStoreImpl();
    }

    @Override
    public synchronized void processStartTransactions(Set<Long> startTimestamps) {
        TransactionsLockWatchUpdate updateForTransactions =
                eventCache.getUpdateForTransactions(startTimestamps, currentVersion);

        if (updateForTransactions.clearCache()) {
            clearCache();
        }

        updateStores(updateForTransactions);
        updateCurrentVersion(updateForTransactions);
    }

    @Override
    public synchronized void updateCacheAndRemoveTransactionState(long startTimestamp) {
        processCommitUpdate(startTimestamp);
        removeTransactionState(startTimestamp);
    }

    @Override
    public synchronized void removeTransactionState(long startTimestamp) {
        StartTimestamp startTs = StartTimestamp.of(startTimestamp);
        // Remove the timestamps, and if the sequence has not updated, update the snapshot with the reads from
        // these transactions (there is no guarantee that all the start timestamps have the same sequence).
        snapshotStore
                .removeTimestamp(startTs)
                .filter(sequence -> currentVersion.isPresent()
                        && sequence.value() == currentVersion.get().version())
                .ifPresent(sequence -> snapshotStore.updateSnapshot(sequence, valueStore.getSnapshot()));
        cacheStore.removeCache(startTs);
        updateStore.removeUpdate(startTs);
    }

    @Override
    public TransactionScopedCache createTransactionScopedCache(long startTs) {
        // Snapshots may be missing due to leader elections. In this case, the transaction will not read from the
        // cache or publish anything to the cache at commit time.
        return ValidatingTransactionScopedCache.create(
                cacheStore.createCache(StartTimestamp.of(startTs)).orElseGet(NoOpTransactionScopedCache::create),
                validationProbability);
    }

    @Override
    public void verifySerializableReads(long startTimestamp) {
        StartTimestamp startTs = StartTimestamp.of(startTimestamp);
        TransactionScopedCache cache = cacheStore
                .getCache(startTs)
                .orElseThrow(() -> new TransactionLockWatchFailedException(
                        "Cannot verify serializability of transaction if cache is missing"));
        cache.close();

        updateStore.getUpdate(eventCache, startTs).accept(new Visitor<Void>() {
            @Override
            public Void invalidateAll() {
                throw new TransactionCacheConflictException("Cannot validate serializable conflicts due to a Timelock"
                        + " leader election, or some other reason.");
            }

            @Override
            public Void invalidateSome(Set<CellReference> invalidatedCells) {
                if (!Sets.intersection(cache.getHitDigest().hitCells(), invalidatedCells)
                        .isEmpty()) {
                    throw new TransactionCacheConflictException(
                            "Values read from the cache have conflicted with a remote write in a different "
                                    + "transaction");
                }
                return null;
            }
        });
    }

    private synchronized void processCommitUpdate(long startTimestamp) {
        StartTimestamp startTs = StartTimestamp.of(startTimestamp);
        Optional<TransactionScopedCache> cache = cacheStore.getCache(startTs);
        cache.ifPresent(TransactionScopedCache::close);

        updateStore.getUpdate(eventCache, startTs).accept(new Visitor<Void>() {
            @Override
            public Void invalidateAll() {
                // This might happen due to an election or if we exceeded the maximum number of events held in
                // memory. Either way, the values are just not pushed to the central cache. Read-write conflict
                // handling is managed separately.
                return null;
            }

            @Override
            public Void invalidateSome(Set<CellReference> invalidatedCells) {
                KeyedStream.stream(cache.map(TransactionScopedCache::getValueDigest)
                                .map(ValueDigest::loadedValues)
                                .orElseGet(ImmutableMap::of))
                        .filterKeys(cellReference -> !invalidatedCells.contains(cellReference))
                        .forEach(valueStore::putValue);
                return null;
            }
        });
    }

    /**
     * In order to maintain the necessary invariants, we need to do the following:
     *
     *  1. For each new event, we apply it to the cache. The effects of this application is described in
     *     {@link LockWatchValueScopingCache}.
     *  2. For each transaction, we must ensure that we store a snapshot of the cache at the sequence corresponding
     *     to the transaction's start timestamp. Note that not every sequence will have a corresponding timestamp, so we
     *     don't bother storing a snapshot for those sequences. Also note that we know that each call here will only
     *     ever have new events, and that consecutive calls to this method will *always* have increasing sequences
     *     (without this last guarantee, we'd need to store snapshots for all sequences).
     */
    private synchronized void updateStores(TransactionsLockWatchUpdate updateForTransactions) {
        Multimap<Sequence, StartTimestamp> reversedMap = createSequenceTimestampMultimap(updateForTransactions);

        // Without this block, updates with no events would not store a snapshot.
        currentVersion.map(LockWatchVersion::version).map(Sequence::of).ifPresent(sequence -> Optional.ofNullable(
                        reversedMap.get(sequence))
                .ifPresent(startTimestamps ->
                        snapshotStore.storeSnapshot(sequence, startTimestamps, valueStore.getSnapshot())));

        updateForTransactions.events().stream().filter(this::isNewEvent).forEach(event -> {
            valueStore.applyEvent(event);
            Sequence sequence = Sequence.of(event.sequence());
            snapshotStore.storeSnapshot(sequence, reversedMap.get(sequence), valueStore.getSnapshot());
        });

        assertNoSnapshotsMissing(reversedMap.keySet());
    }

    private synchronized boolean isNewEvent(LockWatchEvent event) {
        return currentVersion
                .map(LockWatchVersion::version)
                .map(current -> current < event.sequence())
                .orElse(true);
    }

    private synchronized void assertNoSnapshotsMissing(Set<Sequence> sequences) {
        if (sequences.stream()
                .map(snapshotStore::getSnapshotForSequence)
                .anyMatch(maybeSnapshot -> !maybeSnapshot.isPresent())) {
            throw new TransactionLockWatchFailedException("snapshots were not taken for all sequences; this update "
                    + "must have been lost and is now too old to process. Transactions should be retried.");
        }
    }

    private synchronized void updateCurrentVersion(TransactionsLockWatchUpdate updateForTransactions) {
        Optional<LockWatchVersion> maybeUpdateVersion = updateForTransactions.startTsToSequence().values().stream()
                .max(Comparator.comparingLong(LockWatchVersion::version));

        maybeUpdateVersion
                .filter(this::shouldUpdateVersion)
                .ifPresent(updateVersion -> currentVersion = Optional.of(updateVersion));
    }

    private synchronized boolean shouldUpdateVersion(LockWatchVersion updateVersion) {
        return !currentVersion.isPresent() || currentVersion.get().version() < updateVersion.version();
    }

    private synchronized void clearCache() {
        valueStore.reset();
        snapshotStore.reset();
        cacheStore.reset();
        updateStore.reset();
        currentVersion = Optional.empty();
    }

    private static Multimap<Sequence, StartTimestamp> createSequenceTimestampMultimap(
            TransactionsLockWatchUpdate updateForTransactions) {
        return KeyedStream.stream(updateForTransactions.startTsToSequence())
                .mapKeys(StartTimestamp::of)
                .map(LockWatchVersion::version)
                .map(Sequence::of)
                .mapEntries((timestamp, sequence) -> Maps.immutableEntry(sequence, timestamp))
                .collectToSetMultimap();
    }
}
