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
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

final class NoOpTransactionScopedCache implements TransactionScopedCache {

    static TransactionScopedCache create() {
        return new NoOpTransactionScopedCache();
    }

    @Override
    public void write(TableReference tableReference, Cell cell, CacheValue value) {}

    @Override
    public Map<Cell, byte[]> get(
            TableReference tableReference,
            Set<Cell> cell,
            BiFunction<TableReference, Set<Cell>, ListenableFuture<Map<Cell, byte[]>>> valueLoader) {
        return AtlasFutures.getUnchecked(valueLoader.apply(tableReference, cell));
    }

    @Override
    public ValueDigest getValueDigest() {
        return ValueDigest.of(ImmutableMap.of());
    }

    @Override
    public HitDigest getHitDigest() {
        return HitDigest.of(ImmutableSet.of());
    }
}
