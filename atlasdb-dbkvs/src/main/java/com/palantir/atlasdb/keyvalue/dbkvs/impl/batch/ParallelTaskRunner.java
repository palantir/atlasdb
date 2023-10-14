/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.batch;

import com.google.common.base.Throwables;
import com.palantir.common.concurrent.BlockingWorkerPool;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

public class ParallelTaskRunner implements BatchingTaskRunner {
    private final ExecutorService executor;
    private final int batchSize;
    private final int executorQosSize;

    public ParallelTaskRunner(ExecutorService executor, int batchSize) {
        this(executor, batchSize, 0);
    }

    /**
     * Constructs a ParallelTaskRunner.
     *
     * @param executor the ExecutorService to use for running tasks
     * @param batchSize the batchSize to pass into the {@link BatchingStrategy}.
     * @param executorQosSize When set to a positive value, this is the maximum number of concurrent tasks that may run
     *                        spawning from a single thread. Multiple threads may each call {@link #runTask} and each
     *                        may have up to this number of tasks run concurrently (independently). If the executor has
     *                        a bounded number of threads, that limit is still applies which may result in lower
     *                        concurrency than this value. If this is set to zero or a negative number, then there is
     *                        no limit for the number of concurrent tasks which originate from the same thread.
     */
    public ParallelTaskRunner(ExecutorService executor, int batchSize, int executorQosSize) {
        this.executor = executor;
        this.batchSize = batchSize;
        this.executorQosSize = executorQosSize;
    }

    @Override
    public <InT, OutT> OutT runTask(
            InT input,
            BatchingStrategy<InT> batchingStrategy,
            ResultAccumulatorStrategy<OutT> resultAccumulatingStrategy,
            Function<InT, OutT> task) {
        Iterable<? extends InT> batches = batchingStrategy.partitionIntoBatches(input, batchSize);
        List<Future<OutT>> futures = new ArrayList<>();
        BlockingWorkerPool<OutT> pool = new BlockingWorkerPool<>(executor, executorQosSize);
        for (InT batch : batches) {
            Future<OutT> future = pool.submitCallableUnchecked(() -> task.apply(batch));
            futures.add(future);
        }
        OutT result = resultAccumulatingStrategy.createEmptyResult();
        for (Future<OutT> future : futures) {
            OutT batchResult = getFutureUnchecked(future);
            resultAccumulatingStrategy.accumulateResult(result, batchResult);
        }
        return result;
    }

    private static <T> T getFutureUnchecked(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
