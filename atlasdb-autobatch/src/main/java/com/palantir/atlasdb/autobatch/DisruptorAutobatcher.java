/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import static com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * While this class is public, it shouldn't be used as API outside of AtlasDB because we
 * don't guarantee we won't break it.
 */
public final class DisruptorAutobatcher<T, R>
        implements AsyncFunction<T, R>, Function<T, ListenableFuture<R>>, Closeable {
    private static final int BUFFER_SIZE = 1024;
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("autobatcher-%d")
            .build();

    private final Disruptor<DefaultBatchElement<T, R>> disruptor;
    private final RingBuffer<DefaultBatchElement<T, R>> buffer;
    private volatile boolean closed = false;

    public DisruptorAutobatcher(
            Disruptor<DefaultBatchElement<T, R>> disruptor,
            RingBuffer<DefaultBatchElement<T, R>> buffer) {
        this.disruptor = disruptor;
        this.buffer = buffer;
    }

    @Override
    public ListenableFuture<R> apply(T argument) {
        checkState(!closed, "Autobatcher is already shut down");
        SettableFuture<R> result = SettableFuture.create();
        buffer.publishEvent((refresh, sequence) -> {
            refresh.result = result;
            refresh.argument = argument;
        });
        return result;
    }

    @Override
    public void close() {
        closed = true;
        disruptor.shutdown();
    }

    private static final class DefaultBatchElement<T, R> implements BatchElement<T, R> {
        private T argument;
        private SettableFuture<R> result;

        @Override
        public T argument() {
            return argument;
        }

        @Override
        public SettableFuture<R> result() {
            return result;
        }
    }

    private static final class BatchingEventHandler<T, R> implements EventHandler<DefaultBatchElement<T, R>> {
        private final Consumer<List<BatchElement<T, R>>> batchFunction;
        private final List<DefaultBatchElement<T, R>> pending = new ArrayList<>(BUFFER_SIZE);

        private BatchingEventHandler(Consumer<List<BatchElement<T, R>>> batchFunction) {
            this.batchFunction = batchFunction;
        }

        @Override
        public void onEvent(DefaultBatchElement<T, R> event, long sequence, boolean endOfBatch) {
            pending.add(event);
            if (endOfBatch) {
                flush();
            }
        }

        private void flush() {
            try {
                batchFunction.accept(Collections.unmodifiableList(pending));
            } catch (Throwable t) {
                pending.forEach(p -> p.result.setException(t));
            }
            pending.clear();
        }
    }

    public static <T, R> DisruptorAutobatcher<T, R> create(Consumer<List<BatchElement<T, R>>> batchFunction) {
        Disruptor<DefaultBatchElement<T, R>> disruptor =
                new Disruptor<>(DefaultBatchElement::new, BUFFER_SIZE, threadFactory);
        disruptor.handleEventsWith(new BatchingEventHandler<>(batchFunction));
        disruptor.start();
        return new DisruptorAutobatcher<>(disruptor, disruptor.getRingBuffer());
    }
}
