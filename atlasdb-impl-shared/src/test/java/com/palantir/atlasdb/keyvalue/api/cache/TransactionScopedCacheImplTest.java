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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.streams.KeyedStream;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public final class TransactionScopedCacheImplTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("t.table1");
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
    private static final ImmutableMap<Cell, byte[]> VALUES = ImmutableMap.<Cell, byte[]>builder()
            .put(CELL_1, VALUE_1.value().get())
            .put(CELL_2, VALUE_2.value().get())
            .put(CELL_3, VALUE_3.value().get())
            .put(CELL_4, VALUE_4.value().get())
            .put(CELL_5, VALUE_5.value().get())
            .build();
    private static final CacheValue VALUE_EMPTY = CacheValue.empty();

    @Test
    public void getReadsCachedValuesBeforeReadingFromDb() {
        TransactionScopedCache cache = new TransactionScopedCacheImpl(snapshotWithSingleValue());

        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_2)).containsExactlyInAnyOrder(CELL_2);
        cache.write(TABLE, CELL_3, VALUE_3);

        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_2, CELL_3, CELL_4, CELL_5))
                .containsExactlyInAnyOrder(CELL_4, CELL_5);
    }

    @Test
    public void emptyValuesAreCachedButFilteredOutOfResults() {
        TransactionScopedCache cache = new TransactionScopedCacheImpl(snapshotWithSingleValue());

        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_6)).containsExactlyInAnyOrder(CELL_6);
        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_6)).isEmpty();

        assertThat(cache.get(TABLE, ImmutableSet.of(CELL_1, CELL_6), (_unused, cells) -> remoteRead(cells)))
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(CELL_1, VALUE_1.value().get()));
        assertThat(cache.getValueDigest().loadedValues())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CellReference.of(TABLE, CELL_6), VALUE_EMPTY));
    }

    @Test
    public void lockedCellsAreNeverCached() {
        TransactionScopedCache cache = new TransactionScopedCacheImpl(ValueCacheSnapshotImpl.of(
                HashMap.of(CellReference.of(TABLE, CELL_1), CacheEntry.locked()), HashSet.of(TABLE)));

        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_2)).containsExactlyInAnyOrder(CELL_1, CELL_2);
        assertThat(getRemotelyReadCells(cache, TABLE, CELL_1, CELL_2)).containsExactlyInAnyOrder(CELL_1);

        assertThat(cache.getValueDigest().loadedValues())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CellReference.of(TABLE, CELL_2), VALUE_2));
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

    private static ValueCacheSnapshot snapshotWithSingleValue() {
        return ValueCacheSnapshotImpl.of(
                HashMap.of(CellReference.of(TABLE, CELL_1), CacheEntry.unlocked(VALUE_1)), HashSet.of(TABLE));
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
