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
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public final class LockWatchValueCacheImpl implements LockWatchValueCache {
    private static final Logger log = LoggerFactory.getLogger(LockWatchValueCacheImpl.class);
    private static final int CELL_BLOWUP_THRESHOLD = 100;

    // TODO(jshah): this should be configurable.
    private static final long MAX_CACHE_SIZE = 20_000;

    private final LockWatchEventCache eventCache;
    private final ValueStore valueStore;
    private final SnapshotStore snapshotStore;

    private volatile Optional<LockWatchVersion> currentVersion = Optional.empty();

    public LockWatchValueCacheImpl(LockWatchEventCache eventCache) {
        this.eventCache = eventCache;
        this.valueStore = new ValueStoreImpl(MAX_CACHE_SIZE);
        this.snapshotStore = new SnapshotStoreImpl();
    }

    @Override
    public synchronized void processStartTransactions(Set<Long> startTimestamps) {
        TransactionsLockWatchUpdate updateForTransactions =
                eventCache.getUpdateForTransactions(startTimestamps, currentVersion);
        updateStores(updateForTransactions);
        updateCurrentVersion(updateForTransactions);
    }

    // TODO(jshah): This needs to be *very* carefully wired to ensure that the synchronised aspect here is not an
    //  issue. Chances are that this may need to be re-jigged to take a batch, and be connected to the batched commit
    //  timestamp call.
    @Override
    public synchronized void updateCacheOnCommit(ValueDigest digest, long startTs) {
        try {
            CommitUpdate commitUpdate = eventCache.getCommitUpdate(startTs);
            commitUpdate.accept(new Visitor<Void>() {
                @Override
                public Void invalidateAll() {
                    // This might happen due to an election or if we exceeded the maximum number of events held in
                    // memory. Either way, the values are just not pushed to the central cache. If it needs to throw
                    // because of read-write conflicts, that is handled in the PreCommitCondition.
                    return null;
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
        // Snapshots may be missing due to leader elections. In this case, the transaction will not read from the
        // cache or publish anything to the cache at commit time.
        return ValidatingTransactionScopedCache.create(snapshotStore
                .getSnapshot(StartTimestamp.of(startTs))
                .map(TransactionScopedCacheImpl::create)
                .orElseGet(NoOpTransactionScopedCache::create));
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

    private boolean isNewEvent(LockWatchEvent event) {
        return currentVersion
                .map(LockWatchVersion::version)
                .map(current -> current < event.sequence())
                .orElse(true);
    }

    private void assertNoSnapshotsMissing(Set<Sequence> sequences) {
        if (sequences.stream()
                .map(snapshotStore::getSnapshotForSequence)
                .anyMatch(maybeSnapshot -> !maybeSnapshot.isPresent())) {
            throw new TransactionLockWatchFailedException("snapshots were not taken for all sequences; this update "
                    + "must have been lost and is now too old to process. Transactions should be retried.");
        }
    }

    private void updateCurrentVersion(TransactionsLockWatchUpdate updateForTransactions) {
        Optional<LockWatchVersion> maybeUpdateVersion = updateForTransactions.startTsToSequence().values().stream()
                .max(Comparator.comparingLong(LockWatchVersion::version));

        maybeUpdateVersion.ifPresent(updateVersion -> {
            if (firstUpdateAfterCreation()) {
                currentVersion = maybeUpdateVersion;
            } else {
                LockWatchVersion current = currentVersion.get();
                if (leaderElection(current, updateVersion)) {
                    clearCache();
                    currentVersion = maybeUpdateVersion;
                } else if (newUpdate(current, updateVersion)) {
                    currentVersion = maybeUpdateVersion;
                }
            }
        });
    }

    private boolean newUpdate(LockWatchVersion current, LockWatchVersion updateVersion) {
        return current.version() < updateVersion.version();
    }

    private boolean leaderElection(LockWatchVersion current, LockWatchVersion updateVersion) {
        return !current.id().equals(updateVersion.id());
    }

    private boolean firstUpdateAfterCreation() {
        return !currentVersion.isPresent();
    }

    private void clearCache() {
        valueStore.reset();
        snapshotStore.reset();
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

    private static Stream<CellReference> extractTableAndCell(LockDescriptor descriptor) {
        List<CellReference> candidateCells = AtlasLockDescriptorUtils.candidateCells(descriptor);

        if (candidateCells.size() > CELL_BLOWUP_THRESHOLD) {
            log.warn(
                    "Lock descriptor produced a large number of candidate cells - this is due to the descriptor "
                            + "containing many zero bytes. If this message is logged frequently, this table may be "
                            + "inappropriate for caching",
                    SafeArg.of("candidateCellSize", candidateCells.size()),
                    UnsafeArg.of("lockDescriptor", descriptor));
        }

        return candidateCells.stream();
    }
}
