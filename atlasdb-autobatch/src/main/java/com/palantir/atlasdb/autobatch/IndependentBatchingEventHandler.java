/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.lmax.disruptor.EventHandler;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

final class IndependentBatchingEventHandler<T, R> implements EventHandler<BatchElement<T, R>> {
    private final Consumer<List<BatchElement<T, R>>> batchFunction;
    private final List<BatchElement<T, R>> pending;
    private final Optional<ExecutorService> executor;
    private final Optional<Semaphore> runPermits;

    IndependentBatchingEventHandler(
            Consumer<List<BatchElement<T, R>>> batchFunction,
            int bufferSize,
            Optional<ExecutorService> executor,
            Optional<Semaphore> runPermits) {
        this.batchFunction = batchFunction;
        this.pending = new ArrayList<>(bufferSize);
        this.executor = executor;
        this.runPermits = runPermits;
    }

    static <T, R> IndependentBatchingEventHandler<T, R> create(
            Consumer<List<BatchElement<T, R>>> batchFunction, int bufferSize, int maxParallelBatches) {
        Preconditions.checkArgument(
                (maxParallelBatches >= 1) && (maxParallelBatches <= 5),
                "Up to 5 parallel batches can run but not more");
        if (maxParallelBatches == 1) {
            return new IndependentBatchingEventHandler<>(batchFunction, bufferSize, Optional.empty(), Optional.empty());
        } else {
            return new IndependentBatchingEventHandler<>(
                    batchFunction,
                    bufferSize,
                    Optional.of(PTExecutors.newFixedThreadPool(
                            maxParallelBatches, "independent-batching-event-handler-flush")),
                    Optional.of(new Semaphore(maxParallelBatches - 1, true)));
        }
    }

    @Override
    public void onEvent(BatchElement<T, R> event, long sequence, boolean endOfBatch) {
        pending.add(event);
        if (endOfBatch) {
            signalFlush();
            pending.clear();
        }
    }

    private void signalFlush() {
        if (executor.isEmpty()) {
            syncFlush(batchFunction, Collections.unmodifiableList(pending));
            return;
        }

        Semaphore sem = runPermits.orElseThrow();
        if (sem.tryAcquire()) {
            asyncFlush(batchFunction, ImmutableList.copyOf(pending), executor.get(), sem);
            return;
        }

        syncFlush(batchFunction, Collections.unmodifiableList(pending));
    }

    private static <T, R> void asyncFlush(
            Consumer<List<BatchElement<T, R>>> batchFunction,
            List<BatchElement<T, R>> elementsToFlush,
            ExecutorService executor,
            Semaphore semaphore) {
        // rejected execution exception semantics?
        Futures.submit(() -> syncFlush(batchFunction, elementsToFlush), executor)
                .addListener(semaphore::release, MoreExecutors.directExecutor());
    }

    private static <T, R> void syncFlush(
            Consumer<List<BatchElement<T, R>>> batchFunction, List<BatchElement<T, R>> elementsToFlush) {
        elementsToFlush.forEach(batchElement -> batchElement.result().running());
        try {
            batchFunction.accept(elementsToFlush);
        } catch (Throwable t) {
            elementsToFlush.forEach(p -> p.result().setException(t));
        }
    }
}
