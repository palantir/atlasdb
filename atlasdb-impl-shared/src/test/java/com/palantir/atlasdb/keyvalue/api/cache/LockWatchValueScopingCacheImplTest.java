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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchEventCacheImpl;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
            .build();

    private static final LockWatchEvent LOCK_EVENT = createLockEvent();
    private static final LockWatchEvent WATCH_EVENT = createWatchEvent();
    private static final LockWatchEvent UNLOCK_EVENT = createUnlockEvent();
    private static final LockWatchStateUpdate.Snapshot LOCK_WATCH_SNAPSHOT = LockWatchStateUpdate.snapshot(
            LEADER,
            WATCH_EVENT.sequence(),
            ImmutableSet.of(),
            ImmutableSet.of(LockWatchReferences.entireTable(TABLE.getQualifiedName())));

    private final CacheMetrics metrics = mock(CacheMetrics.class);
    private LockWatchEventCache eventCache;
    private LockWatchValueScopingCache valueCache;

    @Before
    public void before() {
        eventCache = LockWatchEventCacheImpl.create(metrics);
        valueCache =
                new LockWatchValueScopingCacheImpl(eventCache, 20_000, 0.0, ImmutableSet.of(TABLE), () -> {}, metrics);
    }

    @Test
    public void tableNotWatchedInSchemaDoesNotCache() {
        valueCache = new LockWatchValueScopingCacheImpl(eventCache, 20_000, 0.0, ImmutableSet.of(), () -> {}, metrics);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2));

        TransactionScopedCache scopedCache = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(scopedCache, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        assertThat(getRemotelyReadCells(scopedCache, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        scopedCache.finalise();
        assertThat(scopedCache.getHitDigest().hitCells()).isEmpty();
        assertThat(scopedCache.getValueDigest().loadedValues()).isEmpty();
    }

    @Test
    public void valueCacheCreatesValidatingTransactionCaches() {
        valueCache =
                new LockWatchValueScopingCacheImpl(eventCache, 20_000, 1.0, ImmutableSet.of(TABLE), () -> {}, metrics);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2));

        TransactionScopedCache scopedCache = valueCache.getTransactionScopedCache(TIMESTAMP_1);

        // This confirms that we always read from remote when validation is set to 1.0.
        assertThat(getRemotelyReadCells(scopedCache, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(1)).registerHits(0);
        verify(metrics, times(1)).registerMisses(1);

        assertThat(getRemotelyReadCells(scopedCache, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(1)).registerHits(1);
        verify(metrics, times(1)).registerMisses(0);
    }

    @Test
    public void updateCacheOnCommitFlushesValuesToCentralCache() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1));

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(1)).registerHits(0);
        verify(metrics, times(1)).registerMisses(1);

        processSuccessfulCommit(TIMESTAMP_1, 0L);

        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_2), LockWatchStateUpdate.success(LEADER, 0L, ImmutableList.of()));
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2));

        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);
        assertThat(getRemotelyReadCells(scopedCache2, TABLE, CELL_1)).isEmpty();
        verify(metrics, times(1)).registerHits(1);
        verify(metrics, times(1)).registerMisses(0);
    }

    @Test
    public void updateCacheOnCommitThrowsOnLeaderElection() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1));

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
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
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1));

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        scopedCache1.write(TABLE, ImmutableMap.of(CELL_2, VALUE_2.value().get()));
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1, CELL_2)).containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(1)).registerHits(1);
        verify(metrics, times(1)).registerMisses(1);

        // This update has a lock taken out for CELL_1: this means that all reads for it must be remote.
        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_2), LockWatchStateUpdate.success(LEADER, 1L, ImmutableList.of(LOCK_EVENT)));
        processEventCacheCommit(TIMESTAMP_1, 1L);
        valueCache.updateCacheWithCommitTimestampsInformation(ImmutableSet.of(TIMESTAMP_1));

        // The difference between the read only cache and the new scoped cache, despite being at the same sequence,
        // is that the read-only cache contains all the locally cached values, including writes, whereas the fresh
        // cache only contains those published values from the first cache - and since one was a write, and the other
        // had a lock taken out during the transaction, none of the values were actually pushed centrally.
        TransactionScopedCache readOnlyCache = valueCache.getReadOnlyTransactionScopedCacheForCommit(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(readOnlyCache, TABLE, CELL_1, CELL_2)).containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(2)).registerHits(1);
        verify(metrics, times(2)).registerMisses(1);

        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);
        assertThat(getRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_2)).containsExactlyInAnyOrder(CELL_1, CELL_2);
        // noop cache
    }

    @Test
    public void lockUpdatesPreventCachingAndUnlockUpdatesAllowItAgain() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1));

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1, CELL_3)).containsExactlyInAnyOrder(CELL_1, CELL_3);
        processSuccessfulCommit(TIMESTAMP_1, 0L);
        assertThat(scopedCache1.getHitDigest().hitCells()).isEmpty();
        verify(metrics, times(1)).registerHits(0);
        verify(metrics, times(1)).registerMisses(2);

        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_2), LockWatchStateUpdate.success(LEADER, 1L, ImmutableList.of(LOCK_EVENT)));
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2));
        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);

        assertThat(getRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_2, CELL_3))
                .containsExactlyInAnyOrder(CELL_1, CELL_2);
        verify(metrics, times(1)).registerHits(1);
        verify(metrics, times(2)).registerMisses(2);

        assertThat(getRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_2, CELL_3))
                .containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(1)).registerHits(2);
        verify(metrics, times(1)).registerMisses(1);

        processSuccessfulCommit(TIMESTAMP_2, 1L);
        assertThat(scopedCache2.getHitDigest().hitCells()).containsExactly(CellReference.of(TABLE, CELL_3));

        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_3), LockWatchStateUpdate.success(LEADER, 2L, ImmutableList.of(UNLOCK_EVENT)));
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_3));

        TransactionScopedCache scopedCache3 = valueCache.getTransactionScopedCache(TIMESTAMP_3);
        assertThat(getRemotelyReadCells(scopedCache3, TABLE, CELL_1, CELL_2, CELL_3))
                .containsExactlyInAnyOrder(CELL_1);
        verify(metrics, times(2)).registerHits(2);
        verify(metrics, times(2)).registerMisses(1);

        assertThat(getRemotelyReadCells(scopedCache3, TABLE, CELL_1, CELL_2, CELL_3))
                .isEmpty();
        verify(metrics, times(1)).registerHits(3);
        verify(metrics, times(1)).registerMisses(0);

        scopedCache3.finalise();
        assertThat(scopedCache3.getValueDigest().loadedValues())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CellReference.of(TABLE, CELL_1), VALUE_1));
        assertThat(scopedCache3.getHitDigest().hitCells())
                .containsExactly(CellReference.of(TABLE, CELL_2), CellReference.of(TABLE, CELL_3));
    }

    @Test
    public void processingTransactionsOutOfOrderThrows() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), LOCK_WATCH_SNAPSHOT);
        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_2), LockWatchStateUpdate.success(LEADER, 1L, ImmutableList.of(LOCK_EVENT)));

        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2));

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
        assertThat(getRemotelyReadCells(scopedCache, TABLE, CELL_1, CELL_2, CELL_3))
                .containsExactlyInAnyOrder(CELL_1, CELL_2, CELL_3);
        assertThat(getRemotelyReadCells(scopedCache, TABLE, CELL_1, CELL_2, CELL_3))
                .containsExactlyInAnyOrder(CELL_1, CELL_2, CELL_3);

        assertThat(scopedCache.getValueDigest().loadedValues()).isEmpty();
        assertThat(scopedCache.getHitDigest().hitCells()).isEmpty();
    }

    @Test
    public void leaderElectionCausesCacheToBeCleared() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1));

        // Stores CELL_1 -> VALUE_1 in central cache
        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        processSuccessfulCommit(TIMESTAMP_1, 0L);

        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_2), LockWatchStateUpdate.success(LEADER, 0L, ImmutableList.of()));
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2));

        // Confirms entry is present
        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);
        assertThat(getRemotelyReadCells(scopedCache2, TABLE, CELL_1)).isEmpty();
        processSuccessfulCommit(TIMESTAMP_2, 0L);

        UUID newLeader = UUID.randomUUID();
        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_3),
                LockWatchStateUpdate.snapshot(newLeader, -1L, ImmutableSet.of(), ImmutableSet.of()));
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_3));

        // Confirms entry is no longer present
        TransactionScopedCache scopedCache3 = valueCache.getTransactionScopedCache(TIMESTAMP_3);
        assertThat(getRemotelyReadCells(scopedCache3, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
    }

    @Test
    public void failedValidationCausesCacheToFallback() {
        valueCache = LockWatchValueScopingCacheImpl.create(eventCache, metrics, 20_000, 1.0, ImmutableSet.of(TABLE));

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1));

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        assertThatThrownBy(() -> scopedCache1.get(
                        TABLE, ImmutableSet.of(CELL_1), (_table, _cells) -> Futures.immediateFuture(ImmutableMap.of())))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Failed lock watch cache validation - will retry without caching");

        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2));

        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        assertThat(scopedCache2).isExactlyInstanceOf(NoOpTransactionScopedCache.class);
    }

    @Test
    public void locksAfterCommitTimeAreNotMissedWhenFlushingValues() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1));

        TransactionScopedCache scopedCache1 = valueCache.getTransactionScopedCache(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1, CELL_3)).containsExactlyInAnyOrder(CELL_1, CELL_3);
        processEventCacheCommit(TIMESTAMP_1, 0L);
        valueCache.updateCacheWithCommitTimestampsInformation(ImmutableSet.of(TIMESTAMP_1));
        verify(metrics, times(1)).registerHits(0);
        verify(metrics, times(1)).registerMisses(2);

        // Simulate the case where the transaction gets a commit timestamp but has not yet committed. This start
        // transactions update introduces the lock taken out on CELL_1
        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_2), LockWatchStateUpdate.success(LEADER, 1L, ImmutableList.of(LOCK_EVENT)));
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2));
        TransactionScopedCache scopedCache2 = valueCache.getTransactionScopedCache(TIMESTAMP_2);

        assertThat(getRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_3)).containsExactlyInAnyOrder(CELL_1, CELL_3);
        verify(metrics, times(2)).registerHits(0);
        verify(metrics, times(2)).registerMisses(2);

        // Confirm that the read only cache ignores the new lock, since it happened after commit time
        TransactionScopedCache readOnlyCache1 = valueCache.getReadOnlyTransactionScopedCacheForCommit(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(readOnlyCache1, TABLE, CELL_1, CELL_3)).isEmpty();

        // Finally, the first transaction commits, but only after a lock has been taken out on one of the cached cells
        valueCache.onSuccessfulCommit(TIMESTAMP_1);

        // New transaction caches should have CELL_3 which was never locked, but CELL_1 should have been filtered out
        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_3), LockWatchStateUpdate.success(LEADER, 1L, ImmutableList.of(UNLOCK_EVENT)));
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_3));
        TransactionScopedCache scopedCache3 = valueCache.getTransactionScopedCache(TIMESTAMP_3);

        assertThat(getRemotelyReadCells(scopedCache3, TABLE, CELL_1, CELL_3)).containsExactlyInAnyOrder(CELL_1);
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
                LockWatchStateUpdate.success(LEADER, sequence, ImmutableList.of()));
    }

    private static Set<Cell> getRemotelyReadCells(TransactionScopedCache cache, TableReference table, Cell... cells) {
        Set<Cell> remoteReads = new HashSet<>();
        cache.get(table, Stream.of(cells).collect(Collectors.toSet()), (_unused, cellsToRead) -> {
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
