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

package com.palantir.atlasdb.transaction.impl.illiteracy;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;

public class RowCacheReaderImpl implements RowCacheReader {
    private final WatchRegistry watchRegistry;
    private final RemoteLockWatchClient remoteLockWatchClient;
    private final RowStateCache rowStateCache;

    public RowCacheReaderImpl(
            WatchRegistry watchRegistry,
            RemoteLockWatchClient remoteLockWatchClient,
            RowStateCache rowStateCache) {
        this.watchRegistry = watchRegistry;
        this.remoteLockWatchClient = remoteLockWatchClient;
        this.rowStateCache = rowStateCache;
    }

    @Override
    public void ensureCacheFlushed() {
        rowStateCache.ensureCacheFlushed();
    }

    @Override
    public <T> RowCacheRowReadAttemptResult<T> attemptToRead(
            TableReference tableRef,
            Iterable<byte[]> rows,
            long readTimestamp,
            Function<Map<Cell, Value>, T> transform) {
        Set<RowReference> rowReferences = Sets.newHashSet();
        rows.forEach(row -> rowReferences.add(
                ImmutableRowReference.builder().tableReference(tableRef).row(row).build()));
        Map<RowReference, RowCacheReference> watchedRows = watchRegistry.filterToWatchedRows(rowReferences);
        Map<RowCacheReference, RowReference> watchedRowsInverse = invert(watchedRows);
        Map<RowCacheReference, WatchIdentifierAndState> watchStates
                = remoteLockWatchClient.getStateForRows(ImmutableSet.copyOf(watchedRows.values()));

        SortedSet<byte[]> successfullyCachedReads = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
        Map<Cell, Value> results = Maps.newHashMap();
        for (Map.Entry<RowCacheReference, WatchIdentifierAndState> entry : watchStates.entrySet()) {
            Optional<Map<Cell, Value>> maybeData = rowStateCache.get(entry.getKey(), entry.getValue(), readTimestamp);
            if (maybeData.isPresent()) {
                results.putAll(maybeData.get());
                successfullyCachedReads.add(watchedRowsInverse.get(entry.getKey()).row());
            }
        }
        return ImmutableRowCacheRowReadAttemptResult.<T>builder()
                .rowsSuccessfullyReadFromCache(successfullyCachedReads)
                .output(transform.apply(results))
                .build();
    }

    private Map<RowCacheReference, RowReference> invert(Map<RowReference, RowCacheReference> watchedRows) {
        Map<RowCacheReference, RowReference> result = Maps.newHashMap();
        watchedRows.forEach((rowRef, rowCacheRef) -> result.put(rowCacheRef, rowRef));
        return result;
    }
}
