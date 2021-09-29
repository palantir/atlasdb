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

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.CommitUpdate;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Function;

public final class ReadOnlyTransactionScopedCache implements TransactionScopedCache {
    private final TransactionScopedCache delegate;

    private ReadOnlyTransactionScopedCache(TransactionScopedCache delegate) {
        this.delegate = delegate;
    }

    public static TransactionScopedCache create(TransactionScopedCache delegate) {
        return new ReadOnlyTransactionScopedCache(delegate);
    }

    @Override
    public void write(TableReference tableReference, Map<Cell, byte[]> values) {
        throw new UnsupportedOperationException("Cannot write via the read only transaction cache");
    }

    @Override
    public void delete(TableReference tableReference, Set<Cell> cells) {
        throw new UnsupportedOperationException("Cannot delete via the read only transaction cache");
    }

    @Override
    public Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            Function<Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        return delegate.get(tableReference, cells, valueLoader);
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getAsync(
            TableReference tableReference,
            Set<Cell> cells,
            Function<Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        return delegate.getAsync(tableReference, cells, valueLoader);
    }

    @Override
    public NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            Function<Set<Cell>, Map<Cell, byte[]>> cellLoader,
            Function<Iterable<byte[]>, NavigableMap<byte[], RowResult<byte[]>>> rowLoader) {
        return delegate.getRows(tableRef, rows, columnSelection, cellLoader, rowLoader);
    }

    @Override
    public void finalise() {
        throw new UnsupportedOperationException("Cannot finalise the read only transaction cache");
    }

    @Override
    public ValueDigest getValueDigest() {
        throw new UnsupportedOperationException("Cannot get a value digest from the read only transaction cache");
    }

    @Override
    public HitDigest getHitDigest() {
        throw new UnsupportedOperationException("Cannot get a hit digest from the read only transaction cache");
    }

    @Override
    public TransactionScopedCache createReadOnlyCache(CommitUpdate commitUpdate) {
        throw new UnsupportedOperationException("Cannot create a read only transaction cache from itself");
    }
}
