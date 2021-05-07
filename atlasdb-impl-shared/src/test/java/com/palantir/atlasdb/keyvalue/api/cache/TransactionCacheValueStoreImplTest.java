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
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.CommitUpdate;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import java.util.Map;
import org.junit.Test;

public final class TransactionCacheValueStoreImplTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("t.table");
    private static final Cell CELL = Cell.create(createBytes(1), createBytes(1 + 100));
    private static final CellReference TABLE_CELL = CellReference.of(TABLE, CELL);
    private static final CacheValue VALUE_1 = createValue(10);
    private static final CacheValue VALUE_2 = createValue(20);
    private static final CacheValue VALUE_EMPTY = CacheValue.empty();

    @Test
    public void snapshotValuesAreRead() {
        TransactionCacheValueStore valueStore = cacheWithSingleValue();
        assertCacheContainsValue(valueStore, VALUE_1);
    }

    @Test
    public void localReadsAreStoredAndRead() {
        TransactionCacheValueStore valueStore = emptyCache();
        assertCacheIsEmpty(valueStore);

        valueStore.cacheRemoteReads(TABLE, ImmutableMap.of(CELL, VALUE_1.value().get()));

        assertCacheContainsValue(valueStore, VALUE_1);

        assertDigestContainsEntries(valueStore, ImmutableMap.of(TABLE_CELL, VALUE_1));
    }

    @Test
    public void localWritesAreStoredAndReadInsteadOfSnapshotReads() {
        TransactionCacheValueStore valueStore = cacheWithSingleValue();
        assertCacheContainsValue(valueStore, VALUE_1);

        valueStore.cacheRemoteWrite(TABLE, CELL, VALUE_2);

        assertCacheContainsValue(valueStore, VALUE_2);
        assertDigestContainsEntries(valueStore, ImmutableMap.of());
    }

    @Test
    public void emptyReadsAreCached() {
        TransactionCacheValueStore valueStore = emptyCache();
        assertCacheIsEmpty(valueStore);

        valueStore.cacheEmptyReads(TABLE, ImmutableSet.of(CELL));
        assertCacheContainsValue(valueStore, VALUE_EMPTY);

        assertDigestContainsEntries(valueStore, ImmutableMap.of(TABLE_CELL, VALUE_EMPTY));
    }

    @Test
    public void valuesNotCachedForUnwatchedTables() {
        TransactionCacheValueStore valueStore =
                new TransactionCacheValueStoreImpl(ValueCacheSnapshotImpl.of(HashMap.empty(), HashSet.empty()));

        valueStore.cacheRemoteWrite(TABLE, CELL, VALUE_1);
        assertCacheIsEmpty(valueStore);

        valueStore.cacheEmptyReads(TABLE, ImmutableSet.of(CELL));
        assertCacheIsEmpty(valueStore);

        valueStore.cacheRemoteReads(TABLE, ImmutableMap.of(CELL, VALUE_1.value().get()));
        assertCacheIsEmpty(valueStore);
    }

    @Test
    public void createWithFilteredUpdateTransfersWrites() {
        TransactionCacheValueStore valueStore = cacheWithSingleValue();

        valueStore.cacheRemoteWrite(TABLE, CELL, VALUE_1);
        assertCacheContainsValue(valueStore, VALUE_1);

        TransactionCacheValueStore filteredValueStore =
                valueStore.createWithFilteredSnapshot(CommitUpdate.invalidateSome(ImmutableSet.of()));
        assertCacheContainsValue(filteredValueStore, VALUE_1);
    }

    @Test
    public void createWithFilteredUpdateTransfersReads() {
        TransactionCacheValueStore valueStore = cacheWithSingleValue();

        valueStore.cacheRemoteReads(TABLE, ImmutableMap.of(CELL, VALUE_1.value().get()));
        assertCacheContainsValue(valueStore, VALUE_1);

        TransactionCacheValueStore filteredValueStore =
                valueStore.createWithFilteredSnapshot(CommitUpdate.invalidateSome(ImmutableSet.of()));
        assertCacheContainsValue(filteredValueStore, VALUE_1);
    }

    @Test
    public void newlyLockedCellsAreNotTransferred() {
        TransactionCacheValueStore valueStore = cacheWithSingleValue();

        valueStore.cacheRemoteReads(TABLE, ImmutableMap.of(CELL, VALUE_1.value().get()));
        assertCacheContainsValue(valueStore, VALUE_1);

        TransactionCacheValueStore filteredValueStore =
                valueStore.createWithFilteredSnapshot(CommitUpdate.invalidateAll());
        assertCacheIsEmpty(filteredValueStore);
    }

    private static void assertCacheIsEmpty(TransactionCacheValueStore valueStore) {
        assertThat(valueStore.getCachedValues(TABLE, ImmutableSet.of(CELL))).isEmpty();
    }

    private static void assertCacheContainsValue(TransactionCacheValueStore valueStore, CacheValue value) {
        assertThat(valueStore.getCachedValues(TABLE, ImmutableSet.of(CELL)))
                .containsExactly(Maps.immutableEntry(CELL, value));
    }

    private static TransactionCacheValueStore emptyCache() {
        return new TransactionCacheValueStoreImpl(ValueCacheSnapshotImpl.of(HashMap.empty(), HashSet.of(TABLE)));
    }

    private static TransactionCacheValueStore cacheWithSingleValue() {
        return new TransactionCacheValueStoreImpl(
                ValueCacheSnapshotImpl.of(HashMap.of(TABLE_CELL, CacheEntry.unlocked(VALUE_1)), HashSet.of(TABLE)));
    }

    private static void assertDigestContainsEntries(
            TransactionCacheValueStore valueStore, Map<CellReference, CacheValue> expectedValues) {
        assertThat(valueStore.getValueDigest()).containsExactlyInAnyOrderEntriesOf(expectedValues);
    }

    private static CacheValue createValue(int value) {
        return CacheValue.of(createBytes(value));
    }

    private static byte[] createBytes(int value) {
        return new byte[] {(byte) value};
    }
}
