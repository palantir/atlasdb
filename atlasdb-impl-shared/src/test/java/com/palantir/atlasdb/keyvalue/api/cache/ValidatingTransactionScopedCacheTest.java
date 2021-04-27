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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.streams.KeyedStream;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import org.junit.Test;

public final class ValidatingTransactionScopedCacheTest {
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
    public void validationCausesAllCellsToBeReadRemotely() {
        TransactionScopedCache delegate = TransactionScopedCacheImpl.create(ValueCacheSnapshotImpl.of(
                HashMap.of(CellReference.of(TABLE, CELL_1), CacheEntry.unlocked(VALUE_1)), HashSet.of(TABLE)));
        TransactionScopedCache validatingCache = new ValidatingTransactionScopedCache(delegate, () -> true);
        Set<Cell> cells = ImmutableSet.of(CELL_1, CELL_2);

        BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader = mock(BiFunction.class);
        when(valueLoader.apply(TABLE, cells)).thenReturn(remoteRead(cells));

        validatingCache.get(TABLE, cells, valueLoader);
        verify(valueLoader).apply(TABLE, cells);
    }

    @Test
    public void validationFailsWhenMismatchingValuesReturned() {
        TransactionScopedCache delegate = mock(TransactionScopedCache.class);
        TransactionScopedCache validatingCache = new ValidatingTransactionScopedCache(delegate, () -> true);
        Set<Cell> cells = ImmutableSet.of(CELL_1, CELL_2);
        BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader = mock(BiFunction.class);
        when(valueLoader.apply(TABLE, cells)).thenReturn(remoteRead(cells));

        when(delegate.get(eq(TABLE), eq(cells), any())).thenReturn(AtlasFutures.getUnchecked(remoteRead(cells)));

        validatingCache.get(TABLE, cells, valueLoader);
    }

    private static ListenableFuture<Map<Cell, byte[]>> remoteRead(Set<Cell> cells) {
        return Futures.immediateFuture(KeyedStream.of(cells)
                .map(VALUES::get)
                .map(Optional::ofNullable)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collectToMap());
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
