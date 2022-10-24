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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.cache.ValueStoreImpl.EntryWeigher;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public final class ValueStoreImplTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("t.table");
    private static final TableReference ROW_WATCHED_TABLE = TableReference.createFromFullyQualifiedName("t.row-level");
    private static final byte[] ROW_NAME = createBytes(73);

    private static final Cell CELL_1 = createCell(1);
    private static final Cell CELL_2 = createCell(2);
    private static final Cell CELL_3 = createCell(3);
    private static final Cell ROW_LEVEL_CELL = Cell.create(ROW_NAME, createBytes(42));
    private static final CellReference TABLE_CELL = CellReference.of(TABLE, CELL_1);
    private static final CellReference ROW_LEVEL_CELL_REFERENCE = CellReference.of(ROW_WATCHED_TABLE, ROW_LEVEL_CELL);
    private static final CacheValue VALUE_1 = createValue(10);
    private static final CacheValue VALUE_2 = createValue(20);
    private static final CacheValue VALUE_3 = createValue(30);
    private static final LockWatchEvent LOCK_EVENT = createLockEvent();
    private static final LockWatchEvent ROW_LOCK_EVENT = createRowLockEvent();
    private static final LockWatchEvent WATCH_EVENTS = createWatchEvents();
    private static final LockWatchEvent UNLOCK_EVENT = createUnlockEvent();
    private static final int EXPECTED_SIZE = EntryWeigher.INSTANCE.weigh(TABLE_CELL, 1);

    private final CacheMetrics metrics = mock(CacheMetrics.class);

    private ValueStore valueStore;

    @Before
    public void before() {
        valueStore = new ValueStoreImpl(ImmutableSet.of(TABLE, ROW_WATCHED_TABLE), 1_000, metrics);
    }

    @Test
    public void lockEventInvalidatesValue() {
        valueStore.applyEvent(WATCH_EVENTS);
        valueStore.putValue(TABLE_CELL, VALUE_1);
        valueStore.putValue(CellReference.of(TABLE, CELL_2), VALUE_3);

        verify(metrics, times(2)).increaseCacheSize(EXPECTED_SIZE);

        assertExpectedValue(CELL_1, CacheEntry.unlocked(VALUE_1));
        assertExpectedValue(CELL_2, CacheEntry.unlocked(VALUE_3));

        valueStore.applyEvent(LOCK_EVENT);
        assertExpectedValue(CELL_1, CacheEntry.locked());
        assertExpectedValue(CELL_2, CacheEntry.unlocked(VALUE_3));

        verify(metrics).decreaseCacheSize(EXPECTED_SIZE);
    }

    @Test
    public void rowLevelReferencesExcludedFromValueStore() {
        valueStore.applyEvent(WATCH_EVENTS);
        valueStore.applyEvent(ROW_LOCK_EVENT);

        verify(metrics, never()).increaseCacheSize(anyLong());

        assertThat(valueStore.getSnapshot().getValue(ROW_LEVEL_CELL_REFERENCE)).isEmpty();
        assertThatCode(() -> valueStore.putValue(ROW_LEVEL_CELL_REFERENCE, VALUE_1))
                .doesNotThrowAnyException();
    }

    @Test
    public void unlockEventsClearLockedEntries() {
        valueStore.applyEvent(WATCH_EVENTS);
        valueStore.applyEvent(LOCK_EVENT);

        assertExpectedValue(CELL_1, CacheEntry.locked());

        valueStore.applyEvent(UNLOCK_EVENT);
        assertThat(valueStore.getSnapshot().getValue(TABLE_CELL)).isEmpty();
    }

    @Test
    public void resetClearsAllEntries() {
        valueStore.applyEvent(WATCH_EVENTS);
        valueStore.applyEvent(LOCK_EVENT);
        valueStore.applyEvent(ROW_LOCK_EVENT);

        assertExpectedValue(CELL_1, CacheEntry.locked());
        assertThat(valueStore.getSnapshot().getValue(ROW_LEVEL_CELL_REFERENCE)).isEmpty();

        valueStore.reset();
        assertThat(valueStore.getSnapshot().getValue(TABLE_CELL)).isEmpty();
        assertThat(valueStore.getSnapshot().getValue(ROW_LEVEL_CELL_REFERENCE)).isEmpty();
    }

    @Test
    public void putValueThrowsIfCurrentValueDiffers() {
        valueStore.applyEvent(WATCH_EVENTS);
        valueStore.putValue(TABLE_CELL, VALUE_1);

        assertThatCode(() -> valueStore.putValue(TABLE_CELL, VALUE_1)).doesNotThrowAnyException();
        assertPutThrows(VALUE_2);

        valueStore.applyEvent(LOCK_EVENT);
        assertPutThrows(VALUE_1);
    }

    @Test
    public void watchEventUpdatesWatchableTables() {
        assertThat(valueStore.getSnapshot().isWatched(TABLE)).isFalse();
        valueStore.applyEvent(WATCH_EVENTS);
        assertThat(valueStore.getSnapshot().isWatched(TABLE)).isTrue();
        assertThat(valueStore.getSnapshot().isWatched(ROW_WATCHED_TABLE)).isFalse();
    }

    @Test
    public void valuesEvictedOnceMaxSizeReached() {
        // size is in bytes; with overhead, this should keep 2 but not three values
        valueStore = new ValueStoreImpl(ImmutableSet.of(TABLE), 300, metrics);
        CellReference tableCell2 = CellReference.of(TABLE, CELL_2);

        valueStore.applyEvent(WATCH_EVENTS);
        valueStore.putValue(TABLE_CELL, VALUE_1);
        valueStore.putValue(tableCell2, VALUE_2);
        verify(metrics, times(2)).increaseCacheSize(EXPECTED_SIZE);

        valueStore.putValue(CellReference.of(TABLE, CELL_3), VALUE_3);
        verify(metrics, times(3)).increaseCacheSize(anyLong());
        verify(metrics).decreaseCacheSize(EXPECTED_SIZE);

        // Caffeine explicitly does *not* implement simple LRU, so we cannot reason on the actual entries here.
        assertThat(((ValueCacheSnapshotImpl) valueStore.getSnapshot()).values()).hasSize(2);
    }

    @Test
    public void lockedValuesDoNotCountToCacheSize() {
        valueStore = new ValueStoreImpl(ImmutableSet.of(TABLE), 300, metrics);
        valueStore.applyEvent(WATCH_EVENTS);
        valueStore.applyEvent(LOCK_EVENT);

        valueStore.putValue(CellReference.of(TABLE, CELL_2), VALUE_2);
        valueStore.putValue(CellReference.of(TABLE, CELL_3), VALUE_3);
        assertExpectedValue(CELL_2, CacheEntry.unlocked(VALUE_2));
        assertExpectedValue(CELL_3, CacheEntry.unlocked(VALUE_3));

        verify(metrics, times(2)).increaseCacheSize(EXPECTED_SIZE);
    }

    @Test
    public void metricsCorrectlyCountOverlappingPuts() {
        valueStore = new ValueStoreImpl(ImmutableSet.of(TABLE), 300, metrics);
        valueStore.applyEvent(WATCH_EVENTS);

        valueStore.putValue(CellReference.of(TABLE, CELL_2), VALUE_2);
        valueStore.putValue(CellReference.of(TABLE, CELL_2), VALUE_2);
        verify(metrics, times(2)).increaseCacheSize(EXPECTED_SIZE);
        verify(metrics).decreaseCacheSize(EXPECTED_SIZE);
    }

    @Test
    public void metricsCalculateSize() {
        valueStore = new ValueStoreImpl(ImmutableSet.of(TABLE), 300, metrics);
        valueStore.applyEvent(WATCH_EVENTS);

        CellReference cellRef = CellReference.of(TABLE, Cell.create(new byte[] {1, 2}, new byte[] {3, 4, 5}));

        valueStore.putValue(cellRef, CacheValue.of(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
        int expectedSize = ValueStoreImpl.CACHE_OVERHEAD + 7 + 2 + 3 + 8;
        verify(metrics).increaseCacheSize(expectedSize);
    }

    private void assertPutThrows(CacheValue value) {
        assertPutThrows(TABLE_CELL, value);
    }

    private void assertPutThrows(CellReference cellReference, CacheValue value) {
        assertThatThrownBy(() -> valueStore.putValue(cellReference, value))
                .isExactlyInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining(
                        "Trying to cache a value which is either locked or is not equal to a currently cached value");
    }

    private void assertExpectedValue(Cell cell, CacheEntry entry) {
        assertExpectedValue(TABLE, cell, entry);
    }

    private void assertExpectedValue(TableReference tableRef, Cell cell, CacheEntry entry) {
        assertThat(valueStore.getSnapshot().getValue(CellReference.of(tableRef, cell)))
                .hasValue(entry);
    }

    private static LockWatchEvent createWatchEvents() {
        LockWatchReferences.LockWatchReference entireTable = LockWatchReferences.entireTable(TABLE.getQualifiedName());
        LockWatchReferences.LockWatchReference exactRow =
                LockWatchReferences.exactRow(ROW_WATCHED_TABLE.getQualifiedName(), ROW_NAME);
        return LockWatchCreatedEvent.builder(ImmutableSet.of(entireTable, exactRow), ImmutableSet.of())
                .build(0L);
    }

    private static LockWatchEvent createRowLockEvent() {
        return createLockEvent(ROW_WATCHED_TABLE, ROW_LEVEL_CELL);
    }

    private static LockWatchEvent createLockEvent() {
        return createLockEvent(TABLE, CELL_1);
    }

    private static LockWatchEvent createLockEvent(TableReference table, Cell cell) {
        return LockEvent.builder(
                        ImmutableSet.of(AtlasCellLockDescriptor.of(
                                table.getQualifiedName(), cell.getRowName(), cell.getColumnName())),
                        LockToken.of(UUID.randomUUID()))
                .build(1L);
    }

    private static LockWatchEvent createUnlockEvent() {
        return UnlockEvent.builder(ImmutableSet.of(AtlasCellLockDescriptor.of(
                        TABLE.getQualifiedName(), CELL_1.getRowName(), CELL_1.getColumnName())))
                .build(1L);
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
