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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.jetbrains.annotations.VisibleForTesting;

final class IndependentBatchingEventHandler<T, R> implements AutobatcherEventHandler<T, R> {
    private final Consumer<List<BatchElement<T, R>>> batchFunction;
    private final List<BatchElement<T, R>> pending;
    private final WorkerPool pool;

    @VisibleForTesting
    IndependentBatchingEventHandler(Consumer<List<BatchElement<T, R>>> batchFunction, int bufferSize, WorkerPool pool) {
        this.batchFunction = batchFunction;
        this.pending = new ArrayList<>(bufferSize);
        this.pool = pool;
    }

    static <T, R> AutobatcherEventHandler<T, R> createWithSequentialBatchProcessing(
            Consumer<List<BatchElement<T, R>>> batchFunction, int bufferSize) {
        return new IndependentBatchingEventHandler<>(batchFunction, bufferSize, NoOpWorkerPool.INSTANCE);
    }

    @Override
    public void onEvent(BatchElement<T, R> event, long sequence, boolean endOfBatch) {
        pending.add(event);
        if (endOfBatch) {
            processBatch();
            pending.clear();
        }
    }

    private void processBatch() {
        boolean flushWasSubmitted =
                pool.tryRun(() -> ImmutableList.copyOf(pending), batchCopy -> flush(batchFunction, batchCopy));

        if (flushWasSubmitted) {
            return;
        }

        List<BatchElement<T, R>> batchView = Collections.unmodifiableList(pending);
        flush(batchFunction, batchView);
    }

    private static <T, R> void flush(Consumer<List<BatchElement<T, R>>> batchFunction, List<BatchElement<T, R>> batch) {
        batch.forEach(element -> element.result().running());
        try {
            batchFunction.accept(batch);
        } catch (Throwable t) {
            batch.forEach(p -> p.result().setException(t));
        }
    }

    @Override
    public void close() {
        pool.close();
    }
}
