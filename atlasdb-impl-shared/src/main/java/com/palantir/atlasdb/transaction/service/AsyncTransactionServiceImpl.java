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

package com.palantir.atlasdb.transaction.service;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.atomic.AtomicTable;
import java.util.Map;

public class AsyncTransactionServiceImpl implements AsyncTransactionService {
    private final AtomicTable<Long, Long> txnTable;

    public AsyncTransactionServiceImpl(AtomicTable<Long, Long> txnTable) {
        this.txnTable = txnTable;
    }

    @Override
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        return txnTable.get(startTimestamp);
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        return txnTable.get(startTimestamps);
    }
}
