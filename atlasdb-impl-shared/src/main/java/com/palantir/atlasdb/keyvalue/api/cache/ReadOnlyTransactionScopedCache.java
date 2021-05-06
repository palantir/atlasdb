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
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.CommitUpdate;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

final class ReadOnlyTransactionScopedCache implements TransactionScopedCache {
    private final TransactionScopedCache delegate;

    ReadOnlyTransactionScopedCache(TransactionScopedCache delegate) {
        this.delegate = delegate;
    }

    @Override
    public void write(TableReference tableReference, Map<Cell, byte[]> values) {
        throw new UnsupportedOperationException("Cannot write to read only transaction scoped cache");
    }

    @Override
    public void delete(TableReference tableReference, Set<Cell> cells) {
        throw new UnsupportedOperationException("Cannot delete via read only transaction scoped cache");
    }

    @Override
    public Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        return delegate.get(tableReference, cells, valueLoader);
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getAsync(
            TableReference tableReference,
            Set<Cell> cells,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        return delegate.getAsync(tableReference, cells, valueLoader);
    }

    @Override
    public void finalise() {
        delegate.finalise();
    }

    @Override
    public ValueDigest getValueDigest() {
        return delegate.getValueDigest();
    }

    @Override
    public HitDigest getHitDigest() {
        return delegate.getHitDigest();
    }

    @Override
    public TransactionScopedCache createReadOnlyCache(CommitUpdate commitUpdate) {
        return delegate.createReadOnlyCache(commitUpdate);
    }
}
