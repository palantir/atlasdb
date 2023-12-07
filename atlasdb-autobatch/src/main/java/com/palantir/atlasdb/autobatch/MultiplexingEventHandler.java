/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.annotations.VisibleForTesting;
import com.lmax.disruptor.EventHandler;
import com.palantir.common.concurrent.PTExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * Uses N instances of the delegate factory handler.
 * Each batch is processed on one of the handlers, asynchronously.
 * The handler for the next batch is chosen in a round-robin fashion.
 * Bear in mind: onEvent is called from the disruptor thread and invocations are not parallel and do not intersect in time.
 */
final class MultiplexingEventHandler<I, O> implements EventHandler<BatchElement<I, O>> {
    private final int concurrency;
    private final ExecutorService executorService;
    private final List<EventHandler<BatchElement<I, O>>> handlers = new ArrayList<>();
    private int pointer = 0;

    @VisibleForTesting
    MultiplexingEventHandler(
            int concurrency, ExecutorService executorService, Supplier<EventHandler<BatchElement<I, O>>> factory) {
        this.concurrency = concurrency;
        this.executorService = executorService;
        for (int index = 0; index < concurrency; index++) {
            this.handlers.add(factory.get());
        }
    }

    MultiplexingEventHandler(int concurrency, String purpose, Supplier<EventHandler<BatchElement<I, O>>> factory) {
        this(
                concurrency,
                PTExecutors.newFixedThreadPool(concurrency, "autobatchers.multiplexed-event-handler." + purpose),
                factory);
    }

    @Override
    public void onEvent(BatchElement<I, O> event, long sequence, boolean endOfBatch) throws Exception {
        EventHandler<BatchElement<I, O>> handler = handlers.get(pointer);
        if (endOfBatch) {
            pointer = (pointer + 1) % concurrency;
            executorService.execute(() -> {
                try {
                    handler.onEvent(event, sequence, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            handler.onEvent(event, sequence, false);
        }
    }
}
