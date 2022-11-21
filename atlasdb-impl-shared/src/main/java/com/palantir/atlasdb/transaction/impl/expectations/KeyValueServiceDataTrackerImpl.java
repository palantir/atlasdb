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
import com.google.common.collect.ImmutableSet;
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
import java.util.function.Function;

public final class KeyValueServiceDataTrackerImpl implements KeyValueServiceDataTracker {
    private final AtomicLongMap<TableReference> bytesReadByTable = AtomicLongMap.create();
    private final LongAdder bytesReadOverall = new LongAdder();
    private final AtomicLongMap<TableReference> kvsCallByTable = AtomicLongMap.create();
    private final LongAdder kvsCallsOverall = new LongAdder();
    private final ConcurrentMap<TableReference, KvsCallReadInfo> maximumBytesKvsCallInfoByTable =
            new ConcurrentHashMap<>();
    private final AtomicReference<Optional<KvsCallReadInfo>> maximumBytesKvsCallInfoOverall =
            new AtomicReference<>(Optional.empty());

    private KeyValueServiceDataTrackerImpl() {}

    @Override
    public TransactionReadInfo getReadInfo() {
        return ImmutableTransactionReadInfo.builder()
                .bytesRead(bytesReadOverall.longValue())
                .kvsCalls(kvsCallsOverall.longValue())
                .maximumBytesKvsCallInfo(maximumBytesKvsCallInfoOverall.get())
                .build();
    }

    /**
     * This is un-synchronized as it is called after task completion and the abort/commit stage.
     */
    @Override
    public ImmutableMap<TableReference, TransactionReadInfo> getReadInfoByTable() {
        Set<TableReference> tableRefs =
                ImmutableSet.copyOf(kvsCallByTable.asMap().keySet());

        return tableRefs.stream()
                .collect(ImmutableMap.toImmutableMap(
                        Function.identity(), tableRef -> ImmutableTransactionReadInfo.builder()
                                .bytesRead(bytesReadByTable.get(tableRef))
                                .kvsCalls(kvsCallByTable.get(tableRef))
                                .maximumBytesKvsCallInfo(
                                        Optional.ofNullable(maximumBytesKvsCallInfoByTable.get(tableRef)))
                                .build()));
    }

    @Override
    public void recordReadForTable(TableReference tableRef, String methodName, long bytesRead) {
        updateKvsMethodTalliesByTable(tableRef, ImmutableKvsCallReadInfo.of(methodName, bytesRead));
    }

    @Override
    public void recordTableAgnosticRead(String methodName, long bytesRead) {
        updateKvsMethodTalliesOverall(ImmutableKvsCallReadInfo.of(methodName, bytesRead));
    }

    /**
     * For KVS read methods tracked here, they are not taken into account for {@link #maximumBytesKvsCallInfoOverall}
     * and {@link #maximumBytesKvsCallInfoByTable}.
     */
    @Override
    public BytesReadTracker recordCallForTable(TableReference tableRef) {
        kvsCallsOverall.increment();
        kvsCallByTable.incrementAndGet(tableRef);
        return bytesRead -> {
            bytesReadOverall.add(bytesRead);
            bytesReadByTable.addAndGet(tableRef, bytesRead);
        };
    }

    /**
     * Creates a {@link KeyValueServiceDataTracker} which handles exceptions thrown for record methods.
     */
    public static KeyValueServiceDataTracker createFailSafeKeyValueServiceDataTracker() {
        return new FailSafeKeyValueServiceDataTracker(new KeyValueServiceDataTrackerImpl());
    }

    private void updateKvsMethodTalliesByTable(TableReference tableRef, KvsCallReadInfo callInfo) {
        updateKvsMethodTalliesOverall(callInfo);

        kvsCallByTable.incrementAndGet(tableRef);
        bytesReadByTable.addAndGet(tableRef, callInfo.bytesRead());
        maximumBytesKvsCallInfoByTable.merge(tableRef, callInfo, Comparators::max);
    }

    private void updateKvsMethodTalliesOverall(KvsCallReadInfo callInfo) {
        kvsCallsOverall.increment();
        bytesReadOverall.add(callInfo.bytesRead());
        maximumBytesKvsCallInfoOverall.updateAndGet(currentMaybeCall -> Comparators.max(
                currentMaybeCall, Optional.of(callInfo), Comparators.emptiesFirst(Comparator.naturalOrder())));
    }
}
