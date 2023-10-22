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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.CommitUpdate;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public final class ReadOnlyTransactionScopedCacheTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final Cell CELL = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("sanity"));
    private static final ImmutableSet<Cell> CELLS = ImmutableSet.of(CELL);
    private static final byte[] VALUE = PtBytes.toBytes("valuable");
    private static final CommitUpdate COMMIT_UPDATE = CommitUpdate.invalidateAll();

    @Mock
    public TransactionScopedCache delegate;

    @Mock
    public Function<Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader;

    @Mock
    public Function<Set<Cell>, Map<Cell, byte[]>> immediateValueLoader;

    @Mock
    public Function<Iterable<byte[]>, NavigableMap<byte[], RowResult<byte[]>>> rowLoader;

    @Test
    public void nonReadMethodsThrow() {
        TransactionScopedCache readOnlyCache = ReadOnlyTransactionScopedCache.create(delegate);

        assertThatThrownBy(() -> readOnlyCache.write(TABLE, ImmutableMap.of(CELL, VALUE)))
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Cannot write via the read only transaction cache");

        assertThatThrownBy(() -> readOnlyCache.delete(TABLE, ImmutableSet.of(CELL)))
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Cannot delete via the read only transaction cache");

        assertThatThrownBy(readOnlyCache::getValueDigest)
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Cannot get a value digest from the read only transaction cache");

        assertThatThrownBy(readOnlyCache::getHitDigest)
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Cannot get a hit digest from the read only transaction cache");

        assertThatThrownBy(readOnlyCache::finalise)
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Cannot finalise the read only transaction cache");

        assertThatThrownBy(() -> readOnlyCache.createReadOnlyCache(COMMIT_UPDATE))
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Cannot create a read only transaction cache from itself");
    }

    @Test
    public void readMethodsPassThrough() {
        TransactionScopedCache readOnlyCache = ReadOnlyTransactionScopedCache.create(delegate);

        readOnlyCache.get(TABLE, CELLS, valueLoader);
        verify(delegate).get(TABLE, CELLS, valueLoader);

        readOnlyCache.getAsync(TABLE, CELLS, valueLoader);
        verify(delegate).getAsync(TABLE, CELLS, valueLoader);

        Set<byte[]> rows = CELLS.stream().map(Cell::getRowName).collect(Collectors.toSet());
        ColumnSelection columnSelection = ColumnSelection.all();
        readOnlyCache.getRows(TABLE, rows, columnSelection, immediateValueLoader, rowLoader);
        verify(delegate).getRows(TABLE, rows, columnSelection, immediateValueLoader, rowLoader);
    }
}
