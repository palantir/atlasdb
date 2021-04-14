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
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.UnlockEvent;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import java.util.UUID;
import org.junit.Test;

public final class TransactionCacheValueStoreImplTest {
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

    @Test
    public void snapshotValuesAreRead() {
        TransactionCacheValueStore valueStore = cacheWithSingleValue();
        assertCacheContainsValue(valueStore, VALUE_1);
    }

    @Test
    public void localReadsAreStoredAndRead() {
        TransactionCacheValueStore valueStore = emptyCache();
        assertThat(valueStore.getCachedValues(ImmutableSet.of(TABLE_CELL))).isEmpty();

        valueStore.updateLocalReads(
                TABLE, ImmutableMap.of(CELL_1, VALUE_1.value().get()));

        assertCacheContainsValue(valueStore, VALUE_1);

        assertThat(valueStore.getTransactionDigest()).containsExactly(Maps.immutableEntry(TABLE_CELL, VALUE_1));
    }

    @Test
    public void localWritesAreStoredAndReadInsteadOfSnapshotReads() {
        TransactionCacheValueStore valueStore = cacheWithSingleValue();
        assertCacheContainsValue(valueStore, VALUE_1);

        valueStore.cacheLocalWrite(TABLE, CELL_1, VALUE_2);

        assertCacheContainsValue(valueStore, VALUE_2);
    }

    private void assertCacheContainsValue(TransactionCacheValueStore valueStore, CacheValue value) {
        assertThat(valueStore.getCachedValues(ImmutableSet.of(TABLE_CELL)))
                .containsExactly(Maps.immutableEntry(TABLE_CELL, value));
    }

    private TransactionCacheValueStoreImpl emptyCache() {
        return new TransactionCacheValueStoreImpl(ValueCacheSnapshotImpl.of(HashMap.empty(), HashSet.of(TABLE)));
    }

    private TransactionCacheValueStoreImpl cacheWithSingleValue() {
        return new TransactionCacheValueStoreImpl(
                ValueCacheSnapshotImpl.of(HashMap.of(TABLE_CELL, CacheEntry.unlocked(VALUE_1)), HashSet.of(TABLE)));
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
