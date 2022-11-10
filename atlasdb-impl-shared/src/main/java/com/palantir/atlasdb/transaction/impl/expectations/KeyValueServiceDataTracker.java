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
import java.util.function.Consumer;
import java.util.function.Function;

public final class KeyValueServiceDataTracker {
    private final AtomicLongMap<TableReference> bytesReadByTable = AtomicLongMap.create();
    private final LongAdder bytesReadOverall = new LongAdder();
    private final AtomicLongMap<TableReference> kvsCallByTable = AtomicLongMap.create();
    private final LongAdder kvsCallsOverall = new LongAdder();
    private final ConcurrentMap<TableReference, KvsCallReadInfo> maximumBytesKvsCallInfoByTable =
            new ConcurrentHashMap<>();
    private final AtomicReference<Optional<KvsCallReadInfo>> maximumBytesKvsCallInfoOverall =
            new AtomicReference<>(Optional.empty());

    public TransactionReadInfo getReadInfo() {
        return ImmutableTransactionReadInfo.builder()
                .bytesRead(bytesReadOverall.longValue())
                .kvsCalls(kvsCallsOverall.longValue())
                .maximumBytesKvsCallInfo(maximumBytesKvsCallInfoOverall.get())
                .build();
    }

    /**
     * This is un-synchronized as it is called after task completion and the abort/commit stage.
     * For returned statistics, no consistency guarantees are provided if the user interacts with the transaction
     * outside the context of {@link com.palantir.atlasdb.transaction.api.TransactionTask} or after commit/abort
     * stages (e.g. this can happen with futures/iterators).
     */
    public ImmutableMap<TableReference, TransactionReadInfo> getReadInfoByTable() {
        Set<TableReference> tableRefs = ImmutableSet.copyOf(Sets.intersection(
                bytesReadByTable.asMap().keySet(), kvsCallByTable.asMap().keySet()));

        return tableRefs.stream()
                .collect(ImmutableMap.toImmutableMap(
                        Function.identity(), tableRef -> ImmutableTransactionReadInfo.builder()
                                .bytesRead(bytesReadByTable.get(tableRef))
                                .kvsCalls(kvsCallByTable.get(tableRef))
                                .maximumBytesKvsCallInfo(
                                        Optional.ofNullable(maximumBytesKvsCallInfoByTable.get(tableRef)))
                                .build()));
    }

    /**
     * Tracks an effectively completed kvs read method call for some {@link TableReference}.
     * Effectively completed refers to an eager call (i.e. does not spawn futures or iterators for later consumption).
     */
    public void readForTable(TableReference tableRef, String methodName, long bytesRead) {
        KvsCallReadInfo callInfo = ImmutableKvsCallReadInfo.builder()
                .bytesRead(bytesRead)
                .methodName(methodName)
                .build();
        updateKvsMethodOverallTallies(callInfo);
        updateKvsMethodByTableTallies(tableRef, callInfo);
    }

    /**
     * Track a lazy kvs read method call for some {@link TableReference}.
     */
    public void partialReadForTable(TableReference tableRef, long bytesRead) {
        bytesReadOverall.add(bytesRead);
        bytesReadByTable.addAndGet(tableRef, bytesRead);
    }

    public Consumer<Long> callForTable(TableReference tableRef) {
        kvsCallsOverall.increment();
        kvsCallByTable.incrementAndGet(tableRef);
        return bytesRead -> partialReadForTable(tableRef, bytesRead);
    }

    /**
     * Track an effectively completed kvs read method call with no {@link TableReference} information.
     * Effectively completed refers to an eager call (i.e. does not spawn futures or iterators for later consumption).
     */
    public void tableAgnosticRead(String methodName, long bytesRead) {
        KvsCallReadInfo callInfo = ImmutableKvsCallReadInfo.builder()
                .bytesRead(bytesRead)
                .methodName(methodName)
                .build();
        updateKvsMethodOverallTallies(callInfo);
    }

    private void updateKvsMethodOverallTallies(KvsCallReadInfo callInfo) {
        bytesReadOverall.add(callInfo.bytesRead());
        kvsCallsOverall.increment();
        maximumBytesKvsCallInfoOverall.updateAndGet(currentMaybeCall -> Comparators.max(
                currentMaybeCall, Optional.of(callInfo), Comparators.emptiesFirst(Comparator.naturalOrder())));
    }

    private void updateKvsMethodByTableTallies(TableReference tableRef, KvsCallReadInfo callInfo) {
        bytesReadByTable.addAndGet(tableRef, callInfo.bytesRead());
        kvsCallByTable.incrementAndGet(tableRef);
        maximumBytesKvsCallInfoByTable.merge(tableRef, callInfo, Comparators::max);
    }
}
