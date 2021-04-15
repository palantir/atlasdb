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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchEventCacheImpl;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchStateUpdate.Success;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.lock.watch.UnlockEvent;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

public final class LockWatchValueCacheImplTest {
    private static final long TIMESTAMP_1 = 5L;
    private static final long TIMESTAMP_2 = 123123123L;
    private static final long TIMESTAMP_3 = 999L;
    private static final long TIMESTAMP_4 = 10101L;
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("t.table");
    private static final Cell CELL_1 = createCell(1);
    private static final Cell CELL_2 = createCell(2);
    private static final Cell CELL_3 = createCell(3);
    private static final Cell CELL_4 = createCell(4);
    private static final Cell CELL_5 = createCell(5);
    private static final Cell CELL_6 = createCell(6);
    private static final CacheValue VALUE_1 = createValue(10);
    private static final CacheValue VALUE_2 = createValue(20);
    private static final CacheValue VALUE_3 = createValue(30);
    private static final CacheValue VALUE_4 = createValue(40);
    private static final CacheValue VALUE_5 = createValue(50);
    private static final CellReference TABLE_CELL = CellReference.of(TABLE, CELL_1);
    private static final UUID LEADER = UUID.randomUUID();
    private static final ImmutableMap<Cell, byte[]> VALUES = ImmutableMap.<Cell, byte[]>builder()
            .put(CELL_1, VALUE_1.value().get())
            .put(CELL_2, VALUE_2.value().get())
            .put(CELL_3, VALUE_3.value().get())
            .put(CELL_4, VALUE_4.value().get())
            .put(CELL_5, VALUE_5.value().get())
            .build();
    private static final CacheValue VALUE_EMPTY = CacheValue.empty();

    private static final LockWatchEvent LOCK_EVENT = createLockEvent();
    public static final Success LOCK_WATCH_SUCCESS =
            LockWatchStateUpdate.success(LEADER, 1L, ImmutableList.of(LOCK_EVENT));
    private static final LockWatchEvent WATCH_EVENT = createWatchEvent();
    public static final LockWatchStateUpdate.Snapshot LOCK_WATCH_SNAPSHOT = LockWatchStateUpdate.snapshot(
            LEADER,
            WATCH_EVENT.sequence(),
            ImmutableSet.of(),
            ImmutableSet.of(LockWatchReferences.entireTable(TABLE.getQualifiedName())));
    private static final LockWatchEvent UNLOCK_EVENT = createUnlockEvent();

    private LockWatchEventCache eventCache;
    private LockWatchValueCache valueCache;

    @Before
    public void before() {
        eventCache = LockWatchEventCacheImpl.create(MetricsManagers.createForTests());
        valueCache = new LockWatchValueCacheImpl(eventCache);
    }

    @Test
    public void updateCacheOnCommitFlushesValuesToLatestSequenceCache() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2));

        TransactionScopedCache scopedCache1 = valueCache.createTransactionScopedCache(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        processCommitTimestamp(TIMESTAMP_1, 0L);
        valueCache.updateCacheOnCommit(scopedCache1.getDigest(), TIMESTAMP_1);

        TransactionScopedCache scopedCache2 = valueCache.createTransactionScopedCache(TIMESTAMP_2);
        assertThat(getRemotelyReadCells(scopedCache2, TABLE, CELL_1)).isEmpty();
    }

    @Test
    public void updateCacheOnCommitFlushesValuesToCentralCache() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1));

        TransactionScopedCache scopedCache1 = valueCache.createTransactionScopedCache(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        processCommitTimestamp(TIMESTAMP_1, 0L);
        valueCache.updateCacheOnCommit(scopedCache1.getDigest(), TIMESTAMP_1);

        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_2), LockWatchStateUpdate.success(LEADER, 0L, ImmutableList.of()));
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2));

        TransactionScopedCache scopedCache2 = valueCache.createTransactionScopedCache(TIMESTAMP_2);
        assertThat(getRemotelyReadCells(scopedCache2, TABLE, CELL_1)).isEmpty();
    }

    @Test
    public void updateCacheOnCommitThrowsOnLeaderElection() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1));

        TransactionScopedCache scopedCache1 = valueCache.createTransactionScopedCache(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1)).containsExactlyInAnyOrder(CELL_1);
        processCommitTimestamp(TIMESTAMP_1, 0L);

        eventCache.processStartTransactionsUpdate(
                ImmutableSet.of(TIMESTAMP_2),
                LockWatchStateUpdate.snapshot(UUID.randomUUID(), -1L, ImmutableSet.of(), ImmutableSet.of()));

        // Throws this message because the leader election cleared the info entirely (as opposed to us knowing that
        // there was an election)
        assertThatThrownBy(() -> valueCache.updateCacheOnCommit(scopedCache1.getDigest(), TIMESTAMP_1))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("start or commit info not processed for start timestamp");
    }

    @Test
    public void lockUpdatesPreventCaching() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), LOCK_WATCH_SNAPSHOT);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2));

        TransactionScopedCache scopedCache1 = valueCache.createTransactionScopedCache(TIMESTAMP_1);
        assertThat(getRemotelyReadCells(scopedCache1, TABLE, CELL_1, CELL_6)).containsExactlyInAnyOrder(CELL_1, CELL_6);

        processCommitTimestamp(TIMESTAMP_1, 0L);
        valueCache.updateCacheOnCommit(scopedCache1.getDigest(), TIMESTAMP_1);

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(TIMESTAMP_2), LOCK_WATCH_SUCCESS);
        valueCache.processStartTransactions(ImmutableSet.of(TIMESTAMP_2));
        TransactionScopedCache scopedCache2 = valueCache.createTransactionScopedCache(TIMESTAMP_2);

        assertThat(getRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_2, CELL_6))
                .containsExactlyInAnyOrder(CELL_1, CELL_2);
        assertThat(getRemotelyReadCells(scopedCache2, TABLE, CELL_1, CELL_2, CELL_6))
                .containsExactlyInAnyOrder(CELL_1);

        assertThat(scopedCache2.getDigest().loadedValues())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CellReference.of(TABLE, CELL_2), VALUE_2));
    }

    @Test
    public void createTransactionScopedCacheWithoutProcessingTimestampThrows() {
        assertThatThrownBy(() -> valueCache.createTransactionScopedCache(TIMESTAMP_1))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Snapshot missing for timestamp");
    }

    private void processCommitTimestamp(long startTimestamp, long sequence) {
        eventCache.processGetCommitTimestampsUpdate(
                ImmutableList.of(TransactionUpdate.builder()
                        .startTs(startTimestamp)
                        .commitTs(startTimestamp + 1337L)
                        .writesToken(LockToken.of(UUID.randomUUID()))
                        .build()),
                LockWatchStateUpdate.success(LEADER, sequence, ImmutableList.of()));
    }

    private static Set<Cell> getRemotelyReadCells(TransactionScopedCache cache, TableReference table, Cell... cells) {
        Set<Cell> remoteReads = new java.util.HashSet<>();
        cache.get(table, Stream.of(cells).collect(Collectors.toSet()), (_unused, cellsToRead) -> {
            remoteReads.addAll(cellsToRead);
            return remoteRead(cellsToRead);
        });
        return remoteReads;
    }

    private static Map<Cell, byte[]> remoteRead(Set<Cell> cells) {
        return KeyedStream.of(cells)
                .map(VALUES::get)
                .map(Optional::ofNullable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collectToMap();
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
