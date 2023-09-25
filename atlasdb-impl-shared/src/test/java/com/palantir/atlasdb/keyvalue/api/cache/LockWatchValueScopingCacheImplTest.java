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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchEventCacheImpl;
import com.palantir.atlasdb.keyvalue.api.watch.Sequence;
import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.assertj.core.api.IterableAssert;
import org.junit.Before;
import org.junit.Test;

public final class LockWatchValueScopingCacheImplTest {
    private static final long TIMESTAMP_1 = 5L;
    private static final long TIMESTAMP_2 = 123123123L;
    private static final Long TIMESTAMP_3 = 88888888L;
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("t.table");
    private static final Cell CELL_1 = createCell(1);
    private static final Cell CELL_2 = createCell(2);
    private static final Cell CELL_3 = createCell(3);
    private static final CacheValue VALUE_1 = createValue(10);
    private static final CacheValue VALUE_2 = createValue(20);
    private static final UUID LEADER = UUID.randomUUID();
    private static final ImmutableMap<Cell, byte[]> VALUES = ImmutableMap.<Cell, byte[]>builder()
            .put(CELL_1, VALUE_1.value().get())
            .put(CELL_2, VALUE_2.value().get())
            .buildOrThrow();

    private static final LockWatchEvent LOCK_EVENT = createLockEvent();
    private static final LockWatchEvent WATCH_EVENT = createWatchEvent();
    private static final LockWatchEvent UNLOCK_EVENT = createUnlockEvent();
    private static final LockWatchStateUpdate.Snapshot LOCK_WATCH_SNAPSHOT = LockWatchStateUpdate.snapshot(
            LEADER,
            WATCH_EVENT.sequence(),
            ImmutableSet.of(),
            ImmutableSet.of(LockWatchReferences.entireTable(TABLE.getQualifiedName())));
    private static final LockWatchStateUpdate.Success SUCCESS_WITH_NO_UPDATES = successWithNoUpdates(0L);
    private static final LockWatchStateUpdate.Success LOCK_WATCH_LOCK_SUCCESS =
            LockWatchStateUpdate.success(LEADER, 1L, ImmutableList.of(LOCK_EVENT));
    private static final LockWatchStateUpdate.Success LOCK_WATCH_UNLOCK_SUCCESS =
            LockWatchStateUpdate.success(LEADER, 2L, ImmutableList.of(UNLOCK_EVENT));

    private static final int MAX_EVENTS = 10_000;

    private final CacheMetrics metrics = mock(CacheMetrics.class);
    private LockWatchEventCache eventCache;
    private LockWatchValueScopingCache valueCache;
    private SnapshotStore snapshotStore;

    @Before
    public void before() {
        snapshotStore = SnapshotStoreImpl.create(metrics);
        eventCache = LockWatchEventCacheImpl.create(metrics, MAX_EVENTS);
        valueCache = LockWatchValueScopingCacheImpl.create(
                eventCache, 20_000, 0.0, ImmutableSet.of(TABLE), snapshotStore, () -> {}, metrics);
    }

    @Test
    public void tableNotWatchedInSchemaDoesNotCache() {
        valueCache = LockWatchValueScopingCacheImpl.create(
                eventCache, 20_000, 0.0, ImmutableSet.of(), snapshotStore, () -> {}, metrics);
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1, TIMESTAMP_2);

        TransactionScopedCache scopedCache = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThatRemotelyReadCells(scopedCache, TABLE, CELL_1).containsExactlyInAnyOrder(CELL_1);
        assertThatRemotelyReadCells(scopedCache, TABLE, CELL_1).containsExactlyInAnyOrder(CELL_1);

        assertNoRowsCached(scopedCache);
        assertNoRowsCached(scopedCache);

        scopedCache.finalise();
        assertThat(scopedCache.getHitDigest().hitCells()).isEmpty();
        assertThat(scopedCache.getValueDigest().loadedValues()).isEmpty();
    }

    @Test
    public void valueCacheCreatesValidatingTransactionCaches() {
        valueCache = LockWatchValueScopingCacheImpl.create(
                eventCache, 20_000, 1.0, ImmutableSet.of(TABLE), snapshotStore, () -> {}, metrics);
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1, TIMESTAMP_2);

        TransactionScopedCache scopedCache = valueCache.getTransactionScopedCache(TIMESTAMP_1);

        // This confirms that we always read from remote when validation is set to 1.0.
        assertThatRemotelyReadCells(scopedCache, TABLE, CELL_1).containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(1)).registerHits(0);
        verify(metrics, times(1)).registerMisses(1);

        assertThatRemotelyReadCells(scopedCache, TABLE, CELL_1).containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(1)).registerHits(1);
        verify(metrics, times(1)).registerMisses(0);
    }

    @Test
    public void updateCacheOnCommitFlushesValuesToCentralCache() {
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1);

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThatRemotelyReadCells(scopedCache1, TABLE, CELL_1).containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(1)).registerHits(0);
        verify(metrics, times(1)).registerMisses(1);

        processSuccessfulCommit(TIMESTAMP_1, 0L);

        processStartTransactionsUpdate(SUCCESS_WITH_NO_UPDATES, TIMESTAMP_2);

        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);
        assertThatRemotelyReadCells(scopedCache2, TABLE, CELL_1).isEmpty();
        verify(metrics, times(1)).registerHits(1);
        verify(metrics, times(1)).registerMisses(0);
    }

    @Test
    public void updateCacheOnCommitThrowsOnLeaderElection() {
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1);

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThatRemotelyReadCells(scopedCache1, TABLE, CELL_1).containsExactlyInAnyOrder(CELL_1);
        processSuccessfulCommit(TIMESTAMP_1, 0L);

        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_2),
                LockWatchStateUpdate.snapshot(UUID.randomUUID(), -1L, ImmutableSet.of(), ImmutableSet.of()));

        // Throws this message because the leader election cleared the info entirely (as opposed to us knowing that
        // there was an election)
        assertThatThrownBy(() -> valueCache.updateCacheWithCommitTimestampsInformation(ImmutableSet.of(TIMESTAMP_1)))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("start or commit info not processed for start timestamp");
    }

    @Test
    public void readOnlyTransactionCacheFiltersOutNewlyLockedValues() {
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1);

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        // The cache is initially empty, and thus both reads read from remote
        assertThatRemotelyReadCells(scopedCache1, TABLE, CELL_1, CELL_2).containsExactlyInAnyOrder(CELL_1, CELL_2);
        verify(metrics, times(1)).registerHits(0);
        verify(metrics, times(1)).registerMisses(2);

        // This update has a lock taken out for CELL_1, and so all reads must be remote for it
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_2), LOCK_WATCH_LOCK_SUCCESS);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2));
        processEventCacheCommit(TIMESTAMP_1, 1L);
        valueCache.updateCacheWithCommitTimestampsInformation(ImmutableSet.of(TIMESTAMP_1));

        // The read only cache cannot cache CELL_1 due to it being locked, but can cache CELL_2 as it was read locally.
        // The new scoped cache cannot cache CELL_1 either, but also cannot initially read CELL_2 from the cache as it
        // has not yet been flushed to the central one.
        TransactionScopedCache readOnlyCache = valueCache.getReadOnlyTransactionScopedCacheForCommit(TIMESTAMP_1);
        assertThatRemotelyReadCells(readOnlyCache, TABLE, CELL_1, CELL_2).containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(1)).registerHits(1);
        verify(metrics, times(1)).registerMisses(1);

        // While this commit does update the central cache, the second cache has already been created before this point
        // and thus does not receive the update
        valueCache.onSuccessfulCommit(TIMESTAMP_1);

        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);
        assertThatRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_2).containsExactlyInAnyOrder(CELL_1, CELL_2);

        // This confirms that CELL_1 remains uncacheable for this cache due to there having been a lock taken out when
        // this cache was created
        assertThatRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_2).containsExactlyInAnyOrder(CELL_1);
    }

    @Test
    public void lockUpdatesPreventCachingAndUnlockUpdatesAllowItAgain() {
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1);

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThatRemotelyReadCells(scopedCache1, TABLE, CELL_1, CELL_3).containsExactlyInAnyOrder(CELL_1, CELL_3);
        processSuccessfulCommit(TIMESTAMP_1, 0L);
        assertThat(scopedCache1.getHitDigest().hitCells()).isEmpty();
        verify(metrics, times(1)).registerHits(0);
        verify(metrics, times(1)).registerMisses(2);

        processStartTransactionsUpdate(LOCK_WATCH_LOCK_SUCCESS, TIMESTAMP_2);
        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);

        assertThatRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_2, CELL_3)
                .containsExactlyInAnyOrder(CELL_1, CELL_2);
        verify(metrics, times(1)).registerHits(1);
        verify(metrics, times(2)).registerMisses(2);

        assertThatRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_2, CELL_3).containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(1)).registerHits(2);
        verify(metrics, times(1)).registerMisses(1);

        processSuccessfulCommit(TIMESTAMP_2, 1L);
        assertThat(scopedCache2.getHitDigest().hitCells()).containsExactly(CellReference.of(TABLE, CELL_3));

        processStartTransactionsUpdate(LOCK_WATCH_UNLOCK_SUCCESS, TIMESTAMP_3);

        TransactionScopedCache scopedCache3 = valueCache.getTransactionScopedCache(TIMESTAMP_3);
        assertThatRemotelyReadCells(scopedCache3, TABLE, CELL_1, CELL_2, CELL_3).containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(2)).registerHits(2);
        verify(metrics, times(2)).registerMisses(1);

        assertThatRemotelyReadCells(scopedCache3, TABLE, CELL_1, CELL_2, CELL_3).isEmpty();
        verify(metrics, times(1)).registerHits(3);
        verify(metrics, times(1)).registerMisses(0);

        scopedCache3.finalise();
        assertThat(scopedCache3.getValueDigest().loadedValues())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CellReference.of(TABLE, CELL_1), VALUE_1));
        assertThat(scopedCache3.getHitDigest().hitCells())
                .containsExactlyInAnyOrder(CellReference.of(TABLE, CELL_2), CellReference.of(TABLE, CELL_3));
    }

    @Test
    public void processingTransactionsOutOfOrderThrows() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), LOCK_WATCH_SNAPSHOT);
        processStartTransactionsUpdate(LOCK_WATCH_LOCK_SUCCESS, TIMESTAMP_2);

        // This throws inside the eventCache because we're trying to get an update yet our version is later than the
        // transaction's.
        assertThatThrownBy(() -> valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1)))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasRootCauseExactlyInstanceOf(SafeIllegalStateException.class)
                .hasRootCauseMessage(
                        "Cannot get update for transactions when the last known version is more recent than the "
                                + "transactions");
    }

    @Test
    public void createTransactionScopedCacheWithMissingSnapshotReturnsNoOpCache() {
        TransactionScopedCache scopedCache = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThatRemotelyReadCells(scopedCache, TABLE, CELL_1, CELL_2, CELL_3)
                .containsExactlyInAnyOrder(CELL_1, CELL_2, CELL_3);
        assertThatRemotelyReadCells(scopedCache, TABLE, CELL_1, CELL_2, CELL_3)
                .containsExactlyInAnyOrder(CELL_1, CELL_2, CELL_3);

        assertThat(scopedCache.getValueDigest().loadedValues()).isEmpty();
        assertThat(scopedCache.getHitDigest().hitCells()).isEmpty();
    }

    @Test
    public void leaderElectionCausesCacheToBeCleared() {
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1);

        // Stores CELL_1 -> VALUE_1 in central cache
        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThatRemotelyReadCells(scopedCache1, TABLE, CELL_1).containsExactlyInAnyOrder(CELL_1);
        processSuccessfulCommit(TIMESTAMP_1, 0L);

        processStartTransactionsUpdate(SUCCESS_WITH_NO_UPDATES, TIMESTAMP_2);

        // Confirms entry is present
        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);
        assertThatRemotelyReadCells(scopedCache2, TABLE, CELL_1).isEmpty();
        processSuccessfulCommit(TIMESTAMP_2, 0L);

        UUID newLeader = UUID.randomUUID();
        processStartTransactionsUpdate(
                LockWatchStateUpdate.snapshot(newLeader, -1L, ImmutableSet.of(), ImmutableSet.of()), TIMESTAMP_3);

        // Confirms entry is no longer present
        TransactionScopedCache scopedCache3 = valueCache.getTransactionScopedCache(TIMESTAMP_3);
        assertThatRemotelyReadCells(scopedCache3, TABLE, CELL_1).containsExactlyInAnyOrder(CELL_1);
    }

    @Test
    public void failedValidationCausesCacheToFallback() {
        valueCache = LockWatchValueScopingCacheImpl.create(eventCache, metrics, 20_000, 1.0, ImmutableSet.of(TABLE));

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1));

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThatRemotelyReadCells(scopedCache1, TABLE, CELL_1).containsExactlyInAnyOrder(CELL_1);
        assertThatThrownBy(() -> scopedCache1.get(
                        TABLE, ImmutableSet.of(CELL_1), _cells -> Futures.immediateFuture(ImmutableMap.of())))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Failed lock watch cache validation - will retry without caching");

        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2));

        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);
        assertThatRemotelyReadCells(scopedCache1, TABLE, CELL_1).containsExactlyInAnyOrder(CELL_1);
        assertThatRemotelyReadCells(scopedCache1, TABLE, CELL_1).containsExactlyInAnyOrder(CELL_1);
        assertThat(scopedCache2).isExactlyInstanceOf(NoOpTransactionScopedCache.class);
    }

    @Test
    public void locksAfterCommitTimeAreNotMissedWhenFlushingValues() {
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1);

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThatRemotelyReadCells(scopedCache1, TABLE, CELL_1, CELL_3).containsExactlyInAnyOrder(CELL_1, CELL_3);
        processEventCacheCommit(TIMESTAMP_1, 0L);
        valueCache.updateCacheWithCommitTimestampsInformation(ImmutableSet.of(TIMESTAMP_1));
        verify(metrics, times(1)).registerHits(0);
        verify(metrics, times(1)).registerMisses(2);

        // Simulate the case where the transaction gets a commit timestamp but has not yet committed. This start
        // transactions update introduces the lock taken out on CELL_1
        processStartTransactionsUpdate(LOCK_WATCH_LOCK_SUCCESS, TIMESTAMP_2);
        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);

        assertThatRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_3).containsExactlyInAnyOrder(CELL_1, CELL_3);
        verify(metrics, times(2)).registerHits(0);
        verify(metrics, times(2)).registerMisses(2);

        // Confirm that the read only cache ignores the new lock, since it happened after commit time
        TransactionScopedCache readOnlyCache1 = valueCache.getReadOnlyTransactionScopedCacheForCommit(TIMESTAMP_1);
        assertThatRemotelyReadCells(readOnlyCache1, TABLE, CELL_1, CELL_3).isEmpty();

        // Finally, the first transaction commits, but only after a lock has been taken out on one of the cached cells
        valueCache.onSuccessfulCommit(TIMESTAMP_1);

        // New transaction caches should have CELL_3 which was never locked, but CELL_1 should have been filtered out
        processStartTransactionsUpdate(LOCK_WATCH_UNLOCK_SUCCESS, TIMESTAMP_3);
        TransactionScopedCache scopedCache3 = valueCache.getTransactionScopedCache(TIMESTAMP_3);

        assertThatRemotelyReadCells(scopedCache3, TABLE, CELL_1, CELL_3).containsExactlyInAnyOrder(CELL_1);
    }

    @Test
    public void ensureStateRemovedDoesNotFlushValuesToCentralCache() {
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1);

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThatRemotelyReadCells(scopedCache1, TABLE, CELL_1, CELL_3).containsExactlyInAnyOrder(CELL_1, CELL_3);
        processEventCacheCommit(TIMESTAMP_1, 0L);
        valueCache.updateCacheWithCommitTimestampsInformation(ImmutableSet.of(TIMESTAMP_1));
        valueCache.ensureStateRemoved(TIMESTAMP_1);
        verify(metrics, times(1)).registerHits(0);
        verify(metrics, times(1)).registerMisses(2);

        processStartTransactionsUpdate(SUCCESS_WITH_NO_UPDATES, TIMESTAMP_2);

        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);
        assertThatRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_3).containsExactlyInAnyOrder(CELL_1, CELL_3);
        verify(metrics, times(2)).registerHits(0);
        verify(metrics, times(2)).registerMisses(2);
    }

    @Test
    public void leaderElectionsCauseValuesToNotBeFlushedToCentralCache() {
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1);

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThatRemotelyReadCells(scopedCache1, TABLE, CELL_1, CELL_2, CELL_3)
                .containsExactlyInAnyOrder(CELL_1, CELL_2, CELL_3);
        verify(metrics, times(1)).registerHits(0);
        verify(metrics, times(1)).registerMisses(3);

        processEventCacheCommit(TIMESTAMP_1, 0L);
        valueCache.updateCacheWithCommitTimestampsInformation(ImmutableSet.of(TIMESTAMP_1));

        processStartTransactionsUpdate(
                LockWatchStateUpdate.snapshot(
                        UUID.randomUUID(),
                        -1L,
                        ImmutableSet.of(),
                        ImmutableSet.of(LockWatchReferences.entireTable(TABLE.getQualifiedName()))),
                TIMESTAMP_2);
        valueCache.onSuccessfulCommit(TIMESTAMP_1);

        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);
        assertThatRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_2, CELL_3)
                .containsExactlyInAnyOrder(CELL_1, CELL_2, CELL_3);
        verify(metrics, times(2)).registerHits(0);
        verify(metrics, times(2)).registerMisses(3);
    }

    @Test
    public void moreEventsThanTimestampsCreatesOnlyNecessarySnapshots() {
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1);
        processStartTransactionsUpdate(
                LockWatchStateUpdate.success(LEADER, 2L, ImmutableList.of(LOCK_EVENT, UNLOCK_EVENT)),
                TIMESTAMP_2,
                TIMESTAMP_3);

        Stream.of(TIMESTAMP_1, TIMESTAMP_2, TIMESTAMP_3).forEach(timestamp -> {
            assertThatCode(() -> valueCache.getTransactionScopedCache(timestamp))
                    .doesNotThrowAnyException();
            assertThat(snapshotStore.getSnapshot(StartTimestamp.of(timestamp))).isPresent();
        });

        assertThat(snapshotStore.getSnapshotForSequence(Sequence.of(LOCK_EVENT.sequence())))
                .isEmpty();
    }

    @Test
    public void missingSnapshotsForSequenceDoesNotThrowWhenNoTablesAreWatched() {
        snapshotStore = new SnapshotStoreImpl(0, 20_000, metrics);
        valueCache = LockWatchValueScopingCacheImpl.create(
                eventCache, 20_000, 0.0, ImmutableSet.of(TABLE), snapshotStore, () -> {}, metrics);

        // This should cause the cache to progress to version 1 but without a snapshot stored at version 0
        processStartTransactionsUpdate(
                LockWatchStateUpdate.snapshot(LEADER, 0L, ImmutableSet.of(), ImmutableSet.of()), TIMESTAMP_1);
        long timestamp4 = 99L;
        processStartTransactionsUpdate(LOCK_WATCH_LOCK_SUCCESS, timestamp4);
        processSuccessfulCommit(TIMESTAMP_1, 1L);
        processSuccessfulCommit(timestamp4, 1L);

        // There are timestamps at version 0 (before we have a snapshot) and at version 1; this would previously throw,
        // but now it just causes the transaction to not cache.
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_2), successWithNoUpdates(0L));
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_3), successWithNoUpdates(1L));
        assertThatCode(() -> valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2, TIMESTAMP_3)))
                .doesNotThrowAnyException();
        assertThat(valueCache.getTransactionScopedCache(TIMESTAMP_2))
                .isExactlyInstanceOf(NoOpTransactionScopedCache.class);
        assertThat(valueCache.getTransactionScopedCache(TIMESTAMP_3))
                .isExactlyInstanceOf(ValidatingTransactionScopedCache.class);
    }

    @Test
    public void missingSnapshotsForSequenceThrowsWhenTablesAreWatched() {
        snapshotStore = new SnapshotStoreImpl(0, 20_000, metrics);
        valueCache = LockWatchValueScopingCacheImpl.create(
                eventCache, 20_000, 0.0, ImmutableSet.of(TABLE), snapshotStore, () -> {}, metrics);

        // This should cause the cache to progress to version 1 but without a snapshot stored at version 0
        processStartTransactionsUpdate(LOCK_WATCH_SNAPSHOT, TIMESTAMP_1);
        long timestamp4 = 99L;
        processStartTransactionsUpdate(LOCK_WATCH_LOCK_SUCCESS, timestamp4);
        processSuccessfulCommit(TIMESTAMP_1, 1L);
        processSuccessfulCommit(timestamp4, 1L);

        // There are timestamps at version 0 (before we have a snapshot) and at version 1; this would previously throw,
        // but now it just causes the transaction to not cache.
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_2), successWithNoUpdates(0L));
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_3), successWithNoUpdates(1L));
        assertThatThrownBy(() -> valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2, TIMESTAMP_3)))
                .isInstanceOf(TransactionFailedRetriableException.class)
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("snapshots were not taken for all sequences; this update must have been lost and is now too"
                        + " old to process. Transactions should be retried.");
    }

    private static void assertNoRowsCached(TransactionScopedCache scopedCache) {
        Set<Cell> remoteReads = new HashSet<>();
        Set<byte[]> remoteRowReads = Collections.newSetFromMap(new IdentityHashMap<>());
        scopedCache.getRows(
                TABLE,
                ImmutableSet.of(CELL_1.getRowName(), CELL_2.getRowName()),
                ColumnSelection.all(),
                cells -> {
                    remoteReads.addAll(cells);
                    return ImmutableMap.of();
                },
                rows -> {
                    rows.forEach(remoteRowReads::add);
                    return ImmutableSortedMap.of();
                });
        assertThat(remoteReads).isEmpty();
        assertThat(remoteRowReads).containsExactlyInAnyOrder(CELL_1.getRowName(), CELL_2.getRowName());
    }

    private void processStartTransactionsUpdate(LockWatchStateUpdate update, long... timestamps) {
        Set<Long> timestampsToProcess = LongStream.of(timestamps).boxed().collect(Collectors.toSet());
        eventCache.processStartTransactionsUpdate(timestampsToProcess, update);
        valueCache.processStartTransactions(timestampsToProcess);
    }

    private static IterableAssert<Cell> assertThatRemotelyReadCells(
            TransactionScopedCache scopedCache, TableReference table, Cell... cells) {
        return assertThat(getRemotelyReadCells(scopedCache, table, cells));
    }

    private void processSuccessfulCommit(long startTimestamp, long sequence) {
        processEventCacheCommit(startTimestamp, sequence);
        valueCache.updateCacheWithCommitTimestampsInformation(ImmutableSet.of(startTimestamp));
        valueCache.onSuccessfulCommit(startTimestamp);
    }

    private void processEventCacheCommit(long startTimestamp, long sequence) {
        eventCache.processGetCommitTimestampsUpdate(
                ImmutableList.of(TransactionUpdate.builder()
                        .startTs(startTimestamp)
                        .commitTs(startTimestamp + 1337L)
                        .writesToken(LockToken.of(UUID.randomUUID()))
                        .build()),
                successWithNoUpdates(sequence));
    }

    private static LockWatchStateUpdate.Success successWithNoUpdates(long sequence) {
        return LockWatchStateUpdate.success(LEADER, sequence, ImmutableList.of());
    }

    private static Iterable<Cell> getRemotelyReadCells(
            TransactionScopedCache cache, TableReference table, Cell... cells) {
        Set<Cell> remoteReads = new HashSet<>();
        cache.get(table, Stream.of(cells).collect(Collectors.toSet()), cellsToRead -> {
            remoteReads.addAll(cellsToRead);
            return remoteRead(cellsToRead);
        });
        return remoteReads;
    }

    private static ListenableFuture<Map<Cell, byte[]>> remoteRead(Set<Cell> cells) {
        return Futures.immediateFuture(KeyedStream.of(cells)
                .map(VALUES::get)
                .map(Optional::ofNullable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collectToMap());
    }

    private static LockWatchEvent createWatchEvent() {
        return LockWatchCreatedEvent.builder(
                        ImmutableSet.of(LockWatchReferences.entireTable(TABLE.getQualifiedName())), ImmutableSet.of())
                .build(0L);
    }

    private static LockWatchEvent createLockEvent() {
        return LockEvent.builder(
                        ImmutableSet.of(AtlasCellLockDescriptor.of(
                                TABLE.getQualifiedName(), CELL_1.getRowName(), CELL_1.getColumnName())),
                        LockToken.of(UUID.randomUUID()))
                .build(1L);
    }

    private static LockWatchEvent createUnlockEvent() {
        return UnlockEvent.builder(ImmutableSet.of(AtlasCellLockDescriptor.of(
                        TABLE.getQualifiedName(), CELL_1.getRowName(), CELL_1.getColumnName())))
                .build(2L);
    }

    private static CacheValue createValue(int value) {
        return CacheValue.of(createBytes(value));
    }

    private static Cell createCell(int value) {
        return Cell.create(createBytes(value), createBytes(value + 100));
    }

    private static byte[] createBytes(int value) {
        return new byte[] {(byte) value};
    }
}
