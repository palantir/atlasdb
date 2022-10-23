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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

public final class KeyValueServiceDataTracker {
    private final AtomicLongMap<TableReference> bytesReadByTable = AtomicLongMap.create();
    private final LongAdder bytesReadOverall = new LongAdder();
    private final AtomicLongMap<TableReference> kvsCallsByTable = AtomicLongMap.create();
    private final LongAdder kvsCallsOverall = new LongAdder();

    private final ConcurrentMap<TableReference, KvsCallReadInfo> maximumBytesKvsCallInfoByTable =
            new ConcurrentHashMap<>();

    private final AtomicReference<Optional<KvsCallReadInfo>> maximumBytesKvsCallInfoOverall =
            new AtomicReference<>(Optional.empty());

    TransactionReadInfo getReadInfo() {
        return ImmutableTransactionReadInfo.builder()
                .bytesRead(bytesReadOverall.longValue())
                .kvsCalls(kvsCallsOverall.longValue())
                .maximumBytesKvsCallInfo(maximumBytesKvsCallInfoOverall.get())
                .build();
    }

    // below method may be too jank
    // takes immutable snapshots of maps and uses only the common keys to construct a coherent return value
    // we could keep a ConcurrentMap<TableReference, TransactionReadInfo>, but the update would be slower
    // update is expected to run more frequently compared with this method (called on callback)
    // actually, as this is supposed to run post-commit, there should be no iteraction with the maps
    // so we can just synchronized (or maybe there are futures hanging around with a kvs reference??)
    ImmutableMap<TableReference, TransactionReadInfo> getReadInfoByTable() {
        ImmutableMap<TableReference, Long> bytesReadMap = ImmutableMap.copyOf(bytesReadByTable.asMap());
        ImmutableMap<TableReference, Long> kvsCallsMap = ImmutableMap.copyOf(kvsCallsByTable.asMap());
        ImmutableMap<TableReference, KvsCallReadInfo> maximumBytesKvsCallMap =
                ImmutableMap.copyOf(maximumBytesKvsCallInfoByTable);

        // omitting third map as it doesn't always have info (e.g. kvs iterator gets)
        Set<TableReference> tableRefs = Sets.intersection(bytesReadMap.keySet(), kvsCallsMap.keySet());

        // ugly!
        Builder<TableReference, TransactionReadInfo> mapBuilder = ImmutableMap.builder();
        for (TableReference tableRef : tableRefs) {
            mapBuilder = mapBuilder.put(
                    tableRef,
                    ImmutableTransactionReadInfo.builder()
                            .bytesRead(bytesReadMap.get(tableRef))
                            .kvsCalls(kvsCallsMap.get(tableRef))
                            .maximumBytesKvsCallInfo(Optional.ofNullable(maximumBytesKvsCallMap.get(tableRef)))
                            .build());
        }

        return mapBuilder.buildOrThrow();
    }

    void registerKvsGetMethodRead(TableReference tableRef, String methodName, long bytesRead) {
        KvsCallReadInfo callInfo = ImmutableKvsCallReadInfo.builder()
                .bytesRead(bytesRead)
                .methodName(methodName)
                .build();

        updateKvsMethodOverallTallies(callInfo);
        updateKvsMethodByTableTallies(tableRef, callInfo);
    }

    void registerKvsGetPartialRead(TableReference tableRef, long bytesRead) {
        bytesReadOverall.add(bytesRead);
        bytesReadByTable.addAndGet(tableRef, bytesRead);
    }

    private void updateKvsMethodByTableTallies(TableReference tableRef, KvsCallReadInfo callInfo) {
        bytesReadByTable.addAndGet(tableRef, callInfo.bytesRead());
        kvsCallsByTable.incrementAndGet(tableRef);
        maximumBytesKvsCallInfoByTable.merge(tableRef, callInfo, Comparators::max);
    }

    private void updateKvsMethodOverallTallies(KvsCallReadInfo callInfo) {
        bytesReadOverall.add(callInfo.bytesRead());
        kvsCallsOverall.increment();
        maximumBytesKvsCallInfoOverall.updateAndGet(currentMaybeCall -> Comparators.max(
                currentMaybeCall, Optional.of(callInfo), Comparators.emptiesFirst(Comparator.naturalOrder())));
    }

    void incrementKvsReadCallCount(TableReference tableRef) {
        kvsCallsOverall.increment();
        kvsCallsByTable.incrementAndGet(tableRef);
    }
}
