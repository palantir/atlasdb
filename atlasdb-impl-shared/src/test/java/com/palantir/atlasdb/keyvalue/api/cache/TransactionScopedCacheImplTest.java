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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.streams.KeyedStream;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public final class TransactionScopedCacheImplTest {
    private static final TableReference TABLE_1 = TableReference.createFromFullyQualifiedName("t.table1");
    private static final TableReference TABLE_2 = TableReference.createFromFullyQualifiedName("t.table2");
    private static final Cell CELL_1 = createCell(1);
    private static final Cell CELL_2 = createCell(2);
    private static final Cell CELL_3 = createCell(3);
    private static final Cell CELL_4 = createCell(4);
    private static final Cell CELL_5 = createCell(5);
    private static final CellReference TABLE_CELL_1 = CellReference.of(TABLE_1, CELL_1);
    private static final CacheValue VALUE_1 = createValue(10);
    private static final CacheValue VALUE_2 = createValue(20);
    private static final CacheValue VALUE_3 = createValue(30);
    private static final CacheValue VALUE_4 = createValue(40);
    private static final CacheValue VALUE_5 = createValue(50);
    private static final ImmutableList<CacheValue> VALUES =
            ImmutableList.of(VALUE_1, VALUE_2, VALUE_3, VALUE_4, VALUE_5);
    private static final CacheValue VALUE_EMPTY = CacheValue.empty();

    @Test
    public void getReadsCachedValuesBeforeReadingFromDb() {
        TransactionScopedCache cache = new TransactionScopedCacheImpl(
                ValueCacheSnapshotImpl.of(HashMap.of(TABLE_CELL_1, CacheEntry.unlocked(VALUE_1)), HashSet.of(TABLE_1)));

        assertThat(getRemotelyReadCells(cache, TABLE_1, CELL_1, CELL_2)).containsExactlyInAnyOrder(CELL_2);
        cache.write(TABLE_1, CELL_3, VALUE_3);

        assertThat(getRemotelyReadCells(cache, TABLE_1, CELL_1, CELL_2, CELL_3, CELL_4, CELL_5))
                .containsExactlyInAnyOrder(CELL_4, CELL_5);
    }

    private static void assertCacheContainsValue(TransactionCacheValueStore valueStore, CacheValue value) {
        assertThat(valueStore.getCachedValues(ImmutableSet.of(TABLE_CELL_1)))
                .containsExactly(Maps.immutableEntry(TABLE_CELL_1, value));
    }

    private static TransactionCacheValueStoreImpl emptyCache() {
        return new TransactionCacheValueStoreImpl(ValueCacheSnapshotImpl.of(HashMap.empty(), HashSet.of(TABLE_1)));
    }

    private static TransactionCacheValueStoreImpl cacheWithSingleValue() {
        return new TransactionCacheValueStoreImpl(
                ValueCacheSnapshotImpl.of(HashMap.of(TABLE_CELL_1, CacheEntry.unlocked(VALUE_1)), HashSet.of(TABLE_1)));
    }

    private static void assertDigestContainsEntries(
            TransactionCacheValueStore valueStore, Map<CellReference, CacheValue> expectedValues) {
        assertThat(valueStore.getTransactionDigest()).containsExactlyInAnyOrderEntriesOf(expectedValues);
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

    private static Set<Cell> getRemotelyReadCells(TransactionScopedCache cache, TableReference table, Cell... cells) {
        Set<Cell> remoteReads = new java.util.HashSet<>();
        cache.get(table, Stream.of(cells).collect(Collectors.toSet()), (_unused, cellsToRead) -> {
            remoteReads.addAll(cellsToRead);
            return KeyedStream.of(Streams.zip(
                            cellsToRead.stream(),
                            VALUES.stream(),
                            (c, v) -> Maps.immutableEntry(c, v.value().get())))
                    .mapKeys(Entry::getKey)
                    .map(Entry::getValue)
                    .collectToMap();
        });
        return remoteReads;
    }
}
