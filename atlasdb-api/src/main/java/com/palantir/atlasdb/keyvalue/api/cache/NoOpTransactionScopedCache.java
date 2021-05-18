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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.CommitUpdate;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

public final class NoOpTransactionScopedCache implements TransactionScopedCache {
    private NoOpTransactionScopedCache() {}

    public static TransactionScopedCache create() {
        return new NoOpTransactionScopedCache();
    }

    @Override
    public void write(TableReference tableReference, Map<Cell, byte[]> values) {}

    @Override
    public void delete(TableReference tableReference, Set<Cell> cells) {}

    @Override
    public Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        return AtlasFutures.getUnchecked(getAsync(tableReference, cells, valueLoader));
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getAsync(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        return valueLoader.apply(tableReference, cells);
    }

    @Override
    public NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            Function<Set<Cell>, Map<Cell, byte[]>> cellLoader,
            Function<Iterable<byte[]>, NavigableMap<byte[], RowResult<byte[]>>> rowLoader) {
        return rowLoader.apply(rows);
    }

    @Override
    public void finalise() {}

    @Override
    public ValueDigest getValueDigest() {
        return ValueDigest.of(ImmutableMap.of());
    }

    @Override
    public HitDigest getHitDigest() {
        return HitDigest.of(ImmutableSet.of());
    }

    @Override
    public TransactionScopedCache createReadOnlyCache(CommitUpdate commitUpdate) {
        return ReadOnlyTransactionScopedCache.create(NoOpTransactionScopedCache.create());
    }
}
