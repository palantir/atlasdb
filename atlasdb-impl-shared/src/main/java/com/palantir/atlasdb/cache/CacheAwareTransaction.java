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

package com.palantir.atlasdb.cache;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.ForwardingTransaction;

/**
 * A transaction that can make use of a distributed cache to skip entire operations. Implementations of the cache
 * must be aware of the possibility of invalidation from other nodes.
 *
 * This is different from {@link com.palantir.atlasdb.transaction.impl.CachingTransaction} in that that caches
 * reads previously done in the same transaction, while this is able to skip entire reads if the cache allows for that.
 */
public class CacheAwareTransaction extends ForwardingTransaction {
    private final Transaction delegate;
    private final TransactionOperationsCache operationsCache;

    public CacheAwareTransaction(Transaction delegate, TransactionOperationsCache operationsCache) {
        this.delegate = delegate;
        this.operationsCache = operationsCache;
    }

    @Override
    public Transaction delegate() {
        return delegate;
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection) {
        SortedMap<byte[] , CacheResponse<RowResult<byte[]>>> cacheResponse
                = operationsCache.getRows(tableRef, rows, columnSelection, getTimestamp());
        SortedMap<byte[], RowResult<byte[]>> result = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        List<byte[]> rowsToQuery = Lists.newArrayList();

        for (Map.Entry<byte[], CacheResponse<RowResult<byte[]>>> entry : cacheResponse.entrySet()) {
            if (entry.getValue().validity() == CacheResponse.CacheValidityState.VALID) {
                result.put(entry.getKey(), entry.getValue().presentValue());
            } else {
                rowsToQuery.add(entry.getKey());
            }
        }

        SortedMap<byte[], RowResult<byte[]>> delegateResult
                = delegate().getRows(tableRef, rowsToQuery, columnSelection);
        result.putAll(delegateResult);

        // The cache cannot be updated for invalid entries here (since this may be a long running transaction that
        // doesn't actually have the latest version of the cell.
        return result;
    }

}
