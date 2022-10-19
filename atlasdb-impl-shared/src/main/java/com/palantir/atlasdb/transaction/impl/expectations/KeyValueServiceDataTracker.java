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
import com.google.common.util.concurrent.AtomicLongMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableKvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.KvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class KeyValueServiceDataTracker {
    AtomicLongMap<TableReference> bytesReadByTable = AtomicLongMap.create();
    AtomicLongMap<TableReference> kvsCallsByTable = AtomicLongMap.create();

    ConcurrentMap<TableReference, KvsCallReadInfo> maximumBytesKvsCallInfoByTable = new ConcurrentHashMap<>();

    // we can keep separate tallies for these, not sure if it's worth the extra bookkeeping
    TransactionReadInfo getReadInfo() {
        return ImmutableTransactionReadInfo.builder()
                .bytesRead(bytesReadByTable.sum())
                .kvsCalls(kvsCallsByTable.sum())
                .build();
    }

    ImmutableMap<TableReference, TransactionReadInfo> getReadInfoByTable() {
        return null;
    }

    void trackBytesRead(TableReference tableRef, String methodName, long bytesRead) {
        bytesReadByTable.addAndGet(tableRef, bytesRead);
        kvsCallsByTable.incrementAndGet(tableRef);

        maximumBytesKvsCallInfoByTable.merge(
                tableRef,
                ImmutableKvsCallReadInfo.builder()
                        .bytesRead(bytesRead)
                        .kvsMethodName(methodName)
                        .build(),
                (x, y) -> Comparators.max(x, y))
    }
}
