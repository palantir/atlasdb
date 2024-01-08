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
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

final class MultiplexingEventHandler<I, O> implements EventHandler<BatchElement<I, O>> {
    private final int concurrency;
    private final ExecutorService executorService;
    private final List<EventHandler<BatchElement<I, O>>> handlers = new ArrayList<>();
    private final List<Semaphore> semaphores = new ArrayList<>();
    private int pointer = 0;

    @VisibleForTesting
    MultiplexingEventHandler(
            int concurrency, ExecutorService executorService, Supplier<EventHandler<BatchElement<I, O>>> factory) {
        this.concurrency = concurrency;
        this.executorService = executorService;
        for (int index = 0; index < concurrency; index++) {
            this.handlers.add(factory.get());
            this.semaphores.add(new Semaphore(1, true));
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
        Semaphore semaphore = semaphores.get(pointer);
        if (endOfBatch) {
            pointer = (pointer + 1) % concurrency;
            acquire(semaphore);
            executorService.execute(() -> {
                try {
                    handler.onEvent(event, sequence, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    semaphore.release();
                }
            });
        } else {
            acquire(semaphore);
            try {
                handler.onEvent(event, sequence, false);
            } finally {
                semaphore.release();
            }
        }
    }

    private static void acquire(Semaphore semaphore) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
