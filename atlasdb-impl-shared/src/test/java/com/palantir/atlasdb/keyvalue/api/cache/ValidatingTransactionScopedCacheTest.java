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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.watch.CommitUpdate;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class ValidatingTransactionScopedCacheTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("t.table");
    private static final Cell CELL_1 = createCell(1);
    private static final Cell CELL_2 = createCell(2);
    private static final ImmutableSet<Cell> CELLS = ImmutableSet.of(CELL_1, CELL_2);
    private static final CacheValue VALUE_1 = createValue(10);
    private static final CacheValue VALUE_2 = createValue(20);
    private static final ImmutableMap<Cell, byte[]> VALUES = ImmutableMap.<Cell, byte[]>builder()
            .put(CELL_1, VALUE_1.value().get())
            .put(CELL_2, VALUE_2.value().get())
            .build();

    private final CacheMetrics metrics = mock(CacheMetrics.class);

    @Mock
    public Function<Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader;

    @Test
    public void validationCausesAllCellsToBeReadRemotely() {
        TransactionScopedCache delegate = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);
        TransactionScopedCache validatingCache = new ValidatingTransactionScopedCache(delegate, 1.0, () -> {});

        when(valueLoader.apply(CELLS)).thenReturn(remoteRead(CELLS));

        validatingCache.get(TABLE, CELLS, valueLoader);
        verify(valueLoader).apply(CELLS);
    }

    @Test
    public void validationDoesNotReadFromRemoteWhenItShouldNotValidate() {
        TransactionScopedCache delegate = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);
        TransactionScopedCache validatingCache = new ValidatingTransactionScopedCache(delegate, 0.0, () -> {});

        validatingCache.get(TABLE, ImmutableSet.of(CELL_1), valueLoader);
        verify(valueLoader, never()).apply(any());
    }

    @Test
    public void validationFailsWhenMismatchingValuesReturned() {
        TransactionScopedCache delegate = mock(TransactionScopedCache.class);
        Runnable failureCallback = mock(Runnable.class);
        TransactionScopedCache validatingCache = new ValidatingTransactionScopedCache(delegate, 1.0, failureCallback);
        when(valueLoader.apply(CELLS)).thenReturn(remoteRead(CELLS));

        when(delegate.getAsync(eq(TABLE), eq(CELLS), any())).thenReturn(Futures.immediateFuture(ImmutableMap.of()));
        assertThatThrownBy(() -> validatingCache.get(TABLE, CELLS, valueLoader))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Failed lock watch cache validation - will retry without caching");
        verify(failureCallback).run();
    }

    @Test
    public void readOnlyCacheAlsoValidates() {
        TransactionScopedCache delegate = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);
        TransactionScopedCache validatingCache = new ValidatingTransactionScopedCache(delegate, 1.0, () -> {});

        when(valueLoader.apply(CELLS)).thenReturn(remoteRead(CELLS));

        validatingCache.get(TABLE, CELLS, valueLoader);
        verify(valueLoader).apply(CELLS);

        TransactionScopedCache readOnlyCache =
                validatingCache.createReadOnlyCache(CommitUpdate.invalidateSome(ImmutableSet.of()));
        readOnlyCache.get(TABLE, CELLS, valueLoader);
        verify(valueLoader, times(2)).apply(CELLS);
    }

    @Test
    public void differentByteArrayReferencesDoNotCauseValidationToFail() {
        TransactionScopedCache delegate = TransactionScopedCacheImpl.create(snapshotWithSingleValue(), metrics);
        TransactionScopedCache validatingCache = new ValidatingTransactionScopedCache(delegate, 1.0, () -> {});

        when(valueLoader.apply(CELLS))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(
                        CELL_1,
                        createValue(10).value().get(),
                        CELL_2,
                        createValue(20).value().get())));
        assertThatCode(() -> validatingCache.get(TABLE, CELLS, valueLoader)).doesNotThrowAnyException();
    }

    @Test
    public void getRowsReadsRemotelyOnceWhenValidating() {
        ValueCacheSnapshot snapshot = ValueCacheSnapshotImpl.of(
                HashMap.of(CellReference.of(TABLE, createCell(1, 1)), CacheEntry.unlocked(VALUE_1)),
                HashSet.of(TABLE),
                ImmutableSet.of(TABLE));
        TransactionScopedCache delegate = TransactionScopedCacheImpl.create(snapshot, metrics);
        TransactionScopedCache validatingCache = new ValidatingTransactionScopedCache(delegate, 1.0, () -> {});

        ImmutableList<byte[]> rowsAndCols = ImmutableList.of(createBytes(1), createBytes(2));
        ColumnSelection columns = ColumnSelection.create(rowsAndCols);

        Function<Set<Cell>, Map<Cell, byte[]>> cellLoader = mock(Function.class);
        Function<Iterable<byte[]>, NavigableMap<byte[], RowResult<byte[]>>> rowLoader = mock(Function.class);

        // set up mock
        NavigableMap<byte[], RowResult<byte[]>> resultMap = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        SortedMap<byte[], byte[]> rowResults = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        rowResults.put(createBytes(1), VALUE_1.value().get());
        rowResults.put(createBytes(2), createBytes(2));
        resultMap.put(createBytes(1), RowResult.create(createBytes(1), rowResults));
        when(rowLoader.apply(any())).thenReturn(resultMap);

        validatingCache.getRows(TABLE, rowsAndCols, columns, cellLoader, rowLoader);

        verifyNoInteractions(cellLoader);
        verify(rowLoader, times(1)).apply(any());
    }

    @Test
    public void getRowsValidates() {
        ValueCacheSnapshot snapshot = ValueCacheSnapshotImpl.of(
                HashMap.of(CellReference.of(TABLE, createCell(1, 1)), CacheEntry.unlocked(VALUE_1)),
                HashSet.of(TABLE),
                ImmutableSet.of(TABLE));
        TransactionScopedCache delegate = TransactionScopedCacheImpl.create(snapshot, metrics);
        TransactionScopedCache validatingCache = new ValidatingTransactionScopedCache(delegate, 1.0, () -> {});

        ImmutableList<byte[]> rowsAndCols = ImmutableList.of(createBytes(1));
        ColumnSelection columns = ColumnSelection.create(rowsAndCols);

        Function<Set<Cell>, Map<Cell, byte[]>> cellLoader = mock(Function.class);
        Function<Iterable<byte[]>, NavigableMap<byte[], RowResult<byte[]>>> rowLoader = mock(Function.class);

        when(rowLoader.apply(any())).thenReturn(new TreeMap<>(UnsignedBytes.lexicographicalComparator()));

        assertThatThrownBy(() -> validatingCache.getRows(TABLE, rowsAndCols, columns, cellLoader, rowLoader))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Failed lock watch cache validation - will retry without caching");

        verifyNoInteractions(cellLoader);
        verify(rowLoader, times(1)).apply(any());
    }

    private static ValueCacheSnapshot snapshotWithSingleValue() {
        return ValueCacheSnapshotImpl.of(
                HashMap.of(CellReference.of(TABLE, CELL_1), CacheEntry.unlocked(VALUE_1)),
                HashSet.of(TABLE),
                ImmutableSet.of(TABLE));
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

    private static Cell createCell(int row, int col) {
        return Cell.create(createBytes(row), createBytes(col));
    }
}
