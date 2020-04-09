/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.threading;

import java.util.concurrent.Executor;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.v2.api.api.NewEndOperation;
import com.palantir.atlasdb.v2.api.api.NewGetOperation;
import com.palantir.atlasdb.v2.api.api.NewPutOperation;
import com.palantir.atlasdb.v2.api.api.NewTransaction;
import com.palantir.atlasdb.v2.api.transaction.SingleThreadedTransaction;

public class ExecutorTransaction implements NewTransaction {
    private final Executor executor;
    private final SingleThreadedTransaction realTransaction;

    public ExecutorTransaction(Executor executor, SingleThreadedTransaction realTransaction) {
        this.executor = executor;
        this.realTransaction = realTransaction;
    }

    @Override
    public void put(NewPutOperation put) {
        executor.execute(() -> realTransaction.put(put));
    }

    @Override
    public <T> ListenableFuture<T> get(NewGetOperation<T> get) {
        // todo have an extra layer of indirection w/ disruptor to avoid worker thread doing _any_ blocking
        return Futures.submitAsync(() -> realTransaction.get(get), executor);
    }

    @Override
    public ListenableFuture<?> end(NewEndOperation end) {
        return Futures.submitAsync(() -> realTransaction.end(end), executor);
    }
}
