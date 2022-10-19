/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.KvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class KeyValueServiceDataTracker {
    private final AtomicLongMap<TableReference> bytesReadByTable = AtomicLongMap.create();
    private final AtomicLongMap<TableReference> kvsCallsByTable = AtomicLongMap.create();

    private final ConcurrentMap<TableReference, KvsCallReadInfo> maximumBytesKvsCallInfoByTable =
            new ConcurrentHashMap<>();

    // we can keep separate tallies for these, not sure if it's worth the extra bookkeeping
    TransactionReadInfo getReadInfo() {
        // todo(aalouane) check Comparator.naturalOrder() does what I think it does
        return ImmutableTransactionReadInfo.builder()
                .bytesRead(bytesReadByTable.sum())
                .kvsCalls(kvsCallsByTable.sum())
                .maximumBytesKvsCallInfo(
                        maximumBytesKvsCallInfoByTable.values().stream().max(Comparator.naturalOrder()))
                .build();
    }

    // below method may be too jank
    // takes immutable snapshots of maps and uses only the common keys to construct a coherent return value
    // we could keep a ConcurrentMap<TableReference, TransactionReadInfo>, but the update would be slower
    // (creating immutables instead of updating atomic longs) and that
    // happens more frequently than the method below (only ran after transactions close on callback)
    ImmutableMap<TableReference, TransactionReadInfo> getReadInfoByTable() {
        ImmutableMap<TableReference, Long> bytesRead = ImmutableMap.copyOf(bytesReadByTable.asMap());
        ImmutableMap<TableReference, Long> kvsCalls = ImmutableMap.copyOf(kvsCallsByTable.asMap());
        ImmutableMap<TableReference, KvsCallReadInfo> maximumBytesKvsCall =
                ImmutableMap.copyOf(maximumBytesKvsCallInfoByTable);

        // omitting third map as it doesn't always have info (e.g. kvs iterator gets)
        Set<TableReference> tableRefs = Sets.intersection(bytesRead.keySet(), kvsCalls.keySet());

        // ugly!
        Builder<TableReference, TransactionReadInfo> mapBuilder = ImmutableMap.builder();
        for (TableReference tableRef : tableRefs) {
            mapBuilder = mapBuilder.put(
                    tableRef,
                    ImmutableTransactionReadInfo.builder()
                            .bytesRead(bytesRead.get(tableRef))
                            .kvsCalls(kvsCalls.get(tableRef))
                            .maximumBytesKvsCallInfo(Optional.ofNullable(maximumBytesKvsCall.get(tableRef)))
                            .build());
        }

        return mapBuilder.build();
    }

    void trackKvsGetMethodRead(TableReference tableRef, String methodName, long bytesRead) {
        bytesReadByTable.addAndGet(tableRef, bytesRead);
        kvsCallsByTable.incrementAndGet(tableRef);
        maximumBytesKvsCallInfoByTable.merge(
                tableRef,
                ImmutableKvsCallReadInfo.builder()
                        .bytesRead(bytesRead)
                        .kvsMethodName(methodName)
                        .build(),
                Comparators::max);
    }

    void trackKvsGetPartialRead(TableReference tableRef, long bytesRead) {
        bytesReadByTable.addAndGet(tableRef, bytesRead);
    }

    void incrementKvsGetCallCount(TableReference tableRef) {
        kvsCallsByTable.incrementAndGet(tableRef);
    }
}
