/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.impl.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.google.common.base.Throwables;

public class ParallelTaskRunner implements BatchingTaskRunner {
    private final ExecutorService executor;
    private final int batchSize;

    public ParallelTaskRunner(ExecutorService executor, int batchSize) {
        this.executor = executor;
        this.batchSize = batchSize;
    }

    @Override
    public <InT, OutT> OutT runTask(InT input,
                                    BatchingStrategy<InT> batchingStrategy,
                                    ResultAccumulatorStrategy<OutT> resultAccumulatingStrategy,
                                    Function<InT, OutT> task) {
        Iterable<? extends InT> batches = batchingStrategy.partitionIntoBatches(input, batchSize);
        List<Future<OutT>> futures = new ArrayList<>();
        for (InT batch : batches) {
            Future<OutT> future = executor.submit(() -> task.apply(batch));
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
