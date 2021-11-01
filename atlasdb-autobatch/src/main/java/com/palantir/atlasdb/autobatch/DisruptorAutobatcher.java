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

package com.palantir.atlasdb.autobatch;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.DetachedSpan;
import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * While this class is public, it shouldn't be used as API outside of AtlasDB because we
 * don't guarantee we won't break it.
 */
public final class DisruptorAutobatcher<T, R>
        implements AsyncFunction<T, R>, Function<T, ListenableFuture<R>>, Closeable {

    private static final SafeLogger log = SafeLoggerFactory.get(DisruptorAutobatcher.class);

    /*
       By memoizing thread factories per loggable purpose, the thread names are numbered uniquely for multiple
       instances of the same autobatcher function.
    */
    private static final ConcurrentMap<String, ThreadFactory> threadFactories = new ConcurrentHashMap<>();

    private static ThreadFactory threadFactory(String safeLoggablePurpose) {
        return threadFactories.computeIfAbsent(safeLoggablePurpose, DisruptorAutobatcher::createThreadFactory);
    }

    private static ThreadFactory createThreadFactory(String safeLoggablePurpose) {
        return new NamedThreadFactory("autobatcher." + safeLoggablePurpose, true);
    }

    private final Disruptor<DisruptorBatchElement<T, R>> disruptor;
    private final RingBuffer<DisruptorBatchElement<T, R>> buffer;
    private final String safeLoggablePurpose;
    private final Runnable closingCallback;

    private volatile boolean closed = false;

    DisruptorAutobatcher(
            Disruptor<DisruptorBatchElement<T, R>> disruptor,
            RingBuffer<DisruptorBatchElement<T, R>> buffer,
            String safeLoggablePurpose,
            Runnable closingCallback) {
        this.disruptor = disruptor;
        this.buffer = buffer;
        this.safeLoggablePurpose = safeLoggablePurpose;
        this.closingCallback = closingCallback;
    }

    @Override
    public ListenableFuture<R> apply(T argument) {
        Preconditions.checkState(!closed, "Autobatcher is already shut down");
        DisruptorFuture<R> result = new DisruptorFuture<R>(safeLoggablePurpose);
        buffer.publishEvent((refresh, _sequence) -> {
            refresh.result = result;
            refresh.argument = argument;
        });
        return result;
    }

    @Override
    public void close() {
        closed = true;
        try {
            disruptor.shutdown(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn(
                    "Disruptor took more than 10 seconds to shutdown. "
                            + "Ensure that handlers aren't uninterruptibly blocking and ensure that they are closed.",
                    e);
        }
        closingCallback.run();
    }

    private static final class DisruptorBatchElement<T, R> {
        private T argument;
        private DisruptorFuture<R> result;

        /**
         * This copies the batch element and clears the references, which reduces the lifetime of the argument
         * and result objects. Since our autobatchers can be large, this leads to reduced old gen memory pressure.
         */
        public BatchElement<T, R> consume() {
            BatchElement<T, R> res = BatchElement.of(argument, result);
            argument = null;
            result = null;
            return res;
        }
    }

    public static final class DisruptorFuture<R> extends AbstractFuture<R> {

        private final DetachedSpan parent;
        private final DetachedSpan waitingSpan;

        @Nullable
        private DetachedSpan runningSpan = null;

        public DisruptorFuture(String safeLoggablePurpose) {
            this.parent = DetachedSpan.start(safeLoggablePurpose + " disruptor task");
            this.waitingSpan = parent.childDetachedSpan("task waiting to be run");
            this.addListener(
                    () -> {
                        waitingSpan.complete();
                        if (runningSpan != null) {
                            runningSpan.complete();
                        }
                        parent.complete();
                    },
                    MoreExecutors.directExecutor());
        }

        void running() {
            waitingSpan.complete();
            runningSpan = parent.childDetachedSpan("running task");
        }

        @Override
        public boolean set(R value) {
            return super.set(value);
        }

        @Override
        public boolean setException(Throwable throwable) {
            return super.setException(throwable);
        }

        @Override
        public boolean setFuture(ListenableFuture<? extends R> future) {
            return super.setFuture(future);
        }
    }

    static <T, R> DisruptorAutobatcher<T, R> create(
            EventHandler<BatchElement<T, R>> eventHandler,
            int bufferSize,
            String safeLoggablePurpose,
            Optional<WaitStrategy> waitStrategy,
            Runnable closingCallback) {
        Disruptor<DisruptorBatchElement<T, R>> disruptor = new Disruptor<>(
                DisruptorBatchElement::new,
                bufferSize,
                threadFactory(safeLoggablePurpose),
                ProducerType.MULTI,
                waitStrategy.orElseGet(BlockingWaitStrategy::new));
        disruptor.handleEventsWith(
                (event, sequence, endOfBatch) -> eventHandler.onEvent(event.consume(), sequence, endOfBatch));
        disruptor.start();
        return new DisruptorAutobatcher<>(disruptor, disruptor.getRingBuffer(), safeLoggablePurpose, closingCallback);
    }
}
