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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.watch.CommitUpdate;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import org.junit.Test;

public final class FilteringValueCacheSnapshotTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("t.table1");
    private static final Cell CELL_1 = createCell(1);
    private static final CellReference TABLE_CELL_1 = createTableCell(CELL_1);
    private static final CellReference TABLE_CELL_2 = createTableCell(createCell(2));
    private static final CellReference TABLE_CELL_3 = createTableCell(createCell(3));
    private static final CacheValue VALUE_1 = createValue(10);
    private static final CacheValue VALUE_2 = createValue(20);

    private final ValueCacheSnapshot delegate = ValueCacheSnapshotImpl.of(
            HashMap.of(TABLE_CELL_1, CacheEntry.unlocked(VALUE_1), TABLE_CELL_2, CacheEntry.unlocked(VALUE_2)),
            HashSet.of(TABLE));

    @Test
    public void invalidateAllReturnsAllLockedValues() {
        ValueCacheSnapshot filteredSnapshot =
                FilteringValueCacheSnapshot.create(delegate, CommitUpdate.invalidateAll());

        assertThatValueIsUnlocked(delegate, TABLE_CELL_1, VALUE_1);
        assertThatValueIsUnlocked(delegate, TABLE_CELL_2, VALUE_2);
        assertThatValueIsEmpty(delegate, TABLE_CELL_3);

        assertThatValueIsLocked(filteredSnapshot, TABLE_CELL_1);
        assertThatValueIsLocked(filteredSnapshot, TABLE_CELL_2);
        assertThatValueIsLocked(filteredSnapshot, TABLE_CELL_3);
    }

    @Test
    public void invalidateSomeReturnsLockedOnlyWhenCommitUpdateHasLocked() {
        ValueCacheSnapshot filteredSnapshot = FilteringValueCacheSnapshot.create(
                delegate,
                CommitUpdate.invalidateSome(ImmutableSet.of(AtlasCellLockDescriptor.of(
                        TABLE.getQualifiedName(), CELL_1.getRowName(), CELL_1.getColumnName()))));

        assertThatValueIsUnlocked(delegate, TABLE_CELL_1, VALUE_1);
        assertThatValueIsUnlocked(delegate, TABLE_CELL_2, VALUE_2);
        assertThatValueIsEmpty(delegate, TABLE_CELL_3);

        assertThatValueIsLocked(filteredSnapshot, TABLE_CELL_1);
        assertThatValueIsUnlocked(filteredSnapshot, TABLE_CELL_2, VALUE_2);
        assertThatValueIsEmpty(filteredSnapshot, TABLE_CELL_3);
    }

    private static void assertThatValueIsEmpty(ValueCacheSnapshot delegate, CellReference cell) {
        assertThat(delegate.isUnlocked(cell)).isTrue();
        assertThat(delegate.getValue(cell)).isEmpty();
    }

    private static void assertThatValueIsUnlocked(ValueCacheSnapshot delegate, CellReference cell, CacheValue value) {
        assertThat(delegate.isUnlocked(cell)).isTrue();
        assertThat(delegate.getValue(cell)).hasValue(CacheEntry.unlocked(value));
    }

    private static void assertThatValueIsLocked(ValueCacheSnapshot snapshot, CellReference cell) {
        assertThat(snapshot.isUnlocked(cell)).isFalse();
        assertThat(snapshot.getValue(cell)).hasValue(CacheEntry.locked());
    }

    private static CellReference createTableCell(Cell cell) {
        return CellReference.of(TABLE, cell);
    }

    private static CacheValue createValue(int value) {
        return CacheValue.of(createBytes(value));
    }

    private static byte[] createBytes(int value) {
        return new byte[] {(byte) value};
    }

    private static Cell createCell(int value) {
        return Cell.create(createBytes(value), createBytes(value + 100));
    }
}
