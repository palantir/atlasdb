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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
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
    private static final Cell CELL_1 = createCell(1);
    private static final Cell CELL_2 = createCell(3);
    private static final CellReference TABLE_CELL = CellReference.of(TABLE, CELL_1);
    private static final CacheValue VALUE_1 = createValue(10);
    private static final CacheValue VALUE_2 = createValue(20);
    private static final CacheValue VALUE_3 = createValue(30);
    private static final LockWatchEvent LOCK_EVENT = createLockEvent();
    private static final LockWatchEvent WATCH_EVENT = createWatchEvent();
    private static final LockWatchEvent UNLOCK_EVENT = createUnlockEvent();

    private ValueStore valueStore;

    @Before
    public void before() {
        valueStore = new ValueStoreImpl();
    }

    @Test
    public void lockEventInvalidatesValue() {
        valueStore.applyEvent(WATCH_EVENT);
        valueStore.putValue(TABLE_CELL, VALUE_1);
        valueStore.putValue(CellReference.of(TABLE, CELL_2), VALUE_3);

        assertExpectedValue(CELL_1, CacheEntry.unlocked(VALUE_1));
        assertExpectedValue(CELL_2, CacheEntry.unlocked(VALUE_3));

        valueStore.applyEvent(LOCK_EVENT);
        assertExpectedValue(CELL_1, CacheEntry.locked());
        assertExpectedValue(CELL_2, CacheEntry.unlocked(VALUE_3));
    }

    @Test
    public void unlockEventsClearLockedEntries() {
        valueStore.applyEvent(WATCH_EVENT);
        valueStore.applyEvent(LOCK_EVENT);

        assertExpectedValue(CELL_1, CacheEntry.locked());

        valueStore.applyEvent(UNLOCK_EVENT);
        assertThat(valueStore.getSnapshot().getValue(TABLE_CELL)).isEmpty();
    }

    @Test
    public void putValueThrowsIfCurrentValueDiffers() {
        valueStore.applyEvent(WATCH_EVENT);
        valueStore.putValue(TABLE_CELL, VALUE_1);

        assertThatCode(() -> valueStore.putValue(TABLE_CELL, VALUE_1)).doesNotThrowAnyException();
        assertPutThrows(VALUE_2);

        valueStore.applyEvent(LOCK_EVENT);
        assertPutThrows(VALUE_1);
    }

    @Test
    public void watchEventUpdatesWatchableTables() {
        assertThat(valueStore.getSnapshot().isWatched(TABLE)).isFalse();
        valueStore.applyEvent(WATCH_EVENT);
        assertThat(valueStore.getSnapshot().isWatched(TABLE)).isTrue();
    }

    private void assertPutThrows(CacheValue value) {
        assertThatThrownBy(() -> valueStore.putValue(TABLE_CELL, value))
                .isExactlyInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining(
                        "Trying to cache a value which is either locked or is not equal to a currently cached value");
    }

    private void assertExpectedValue(Cell cell, CacheEntry entry) {
        assertThat(valueStore.getSnapshot().getValue(CellReference.of(TABLE, cell)))
                .hasValue(entry);
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
