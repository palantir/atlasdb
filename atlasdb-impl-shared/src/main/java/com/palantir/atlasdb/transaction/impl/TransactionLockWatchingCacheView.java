/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.GuardedValue;
import com.palantir.atlasdb.keyvalue.api.ImmutableGuardedValue;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.LockWatch;

public class TransactionLockWatchingCacheView {
    public static final TransactionLockWatchingCacheView EMPTY = NoOpLockWatchingCache.INSTANCE
            .getView(0, ImmutableMap.of(), null);

    private final long startTimestamp;
    private final BiFunction<TableReference, Cell, LockWatch> cellToWatch;
    private final KeyValueService kvs;
    private final LockWatchingCache cache;

    public TransactionLockWatchingCacheView(long startTimestamp,
            BiFunction<TableReference, Cell, LockWatch> cellToWatch, KeyValueService kvs, LockWatchingCache cache) {
        this.startTimestamp = startTimestamp;
        this.cellToWatch = cellToWatch;
        this.kvs = kvs;
        this.cache = cache;
    }

    Map<Cell, byte[]> readCached(TableReference tableRef, Set<Cell> cells) {
        return cache.getCached(tableRef, cells).entrySet().stream()
                .filter(entry -> eligibleToReadCached(tableRef, entry))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().value()));
    }

    private boolean eligibleToReadCached(TableReference tableRef, Map.Entry<Cell, GuardedValue> entry) {
        LockWatch currentState = cellToWatch.apply(tableRef, entry.getKey());
        return currentState.fromCommittedTransaction() && entry.getValue().guardTimestamp() == currentState.timestamp();
    }

    void cacheNewValuesRead(TableReference tableRef, Map<Cell, byte[]> writes) {
        Map<Cell, GuardedValue> result = writes.entrySet().stream()
                .filter(entry -> eligibleToBeCached(tableRef, entry))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> ImmutableGuardedValue.of(entry.getValue(), startTimestamp)));
        cache.maybeCacheEntriesRead(tableRef, result);
    }

    private boolean eligibleToBeCached(TableReference tableRef, Map.Entry<Cell, byte[]> entry) {
        LockWatch currentState = cellToWatch.apply(tableRef, entry.getKey());
        return currentState.fromCommittedTransaction();
    }

    void cacheWrittenValues(TableReference tableRef, Map<Cell, byte[]> writes, long lockTimestamp) {
        cache.maybeCacheCommittedWrites(tableRef, writes, lockTimestamp);
    }
}
