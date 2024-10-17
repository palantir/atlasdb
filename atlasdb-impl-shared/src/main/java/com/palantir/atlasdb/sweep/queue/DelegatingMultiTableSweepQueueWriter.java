/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.exception.NotInitializedException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DelegatingMultiTableSweepQueueWriter implements MultiTableSweepQueueWriter {
    private final Future<MultiTableSweepQueueWriter> delegate;

    public DelegatingMultiTableSweepQueueWriter(Future<MultiTableSweepQueueWriter> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void enqueue(Map<TableReference, ? extends Map<Cell, byte[]>> writes, long timestamp) {
        delegate().enqueue(writes, timestamp);
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        delegate().enqueue(writes);
    }

    @Override
    public void initialize(TransactionManager txManager) {
        delegate().initialize(txManager);
    }

    @Override
    public void onInitializationFailureCleanup(TransactionManager resource, Throwable initFailure) {
        delegate().onInitializationFailureCleanup(resource, initFailure);
    }

    @Override
    public Callback<TransactionManager> singleAttemptCallback() {
        return delegate().singleAttemptCallback();
    }

    @Override
    public Callback<TransactionManager> retryUnlessCleanupThrowsCallback() {
        return delegate().retryUnlessCleanupThrowsCallback();
    }

    @Override
    public List<WriteInfo> toWriteInfos(Map<TableReference, ? extends Map<Cell, byte[]>> writes, long timestamp) {
        return delegate().toWriteInfos(writes, timestamp);
    }

    @Override
    public void close() {
        delegate().close();
    }

    @Override
    public SweeperStrategy getSweepStrategy(TableReference tableReference) {
        return delegate().getSweepStrategy(tableReference);
    }

    private MultiTableSweepQueueWriter delegate() {
        try {
            return delegate.get(0, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException | RuntimeException t) {
            throw new NotInitializedException(DelegatingMultiTableSweepQueueWriter.class.getSimpleName(), t);
        } catch (InterruptedException t) {
            Thread.currentThread().interrupt();
            throw new NotInitializedException(DelegatingMultiTableSweepQueueWriter.class.getSimpleName(), t);
        }
    }
}
