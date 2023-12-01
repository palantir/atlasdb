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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
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
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.conjure.java.api.errors.QosReason;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.DetachedSpan;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
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

    private static final QosReason CLOSED_REASON = QosReason.of("autobatcher-closed");

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
    private final AutobatcherTelemetryComponents telemetryComponents;
    private final Runnable closingCallback;

    private volatile boolean closed = false;

    DisruptorAutobatcher(
            Disruptor<DisruptorBatchElement<T, R>> disruptor,
            RingBuffer<DisruptorBatchElement<T, R>> buffer,
            AutobatcherTelemetryComponents telemetryComponents,
            Runnable closingCallback) {
        this.disruptor = disruptor;
        this.buffer = buffer;
        this.telemetryComponents = telemetryComponents;
        this.closingCallback = closingCallback;
    }

    @Override
    public ListenableFuture<R> apply(T argument) {
        if (closed) {
            throw QosException.unavailable(CLOSED_REASON);
        }

        DisruptorFuture<R> result = new DisruptorFuture<R>(telemetryComponents);
        buffer.publishEvent((refresh, sequence) -> {
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

        private final Ticker ticker;
        private final DetachedSpan parent;
        private final TimedDetachedSpan waitingSpan;

        @Nullable
        private TimedDetachedSpan runningSpan = null;

        private volatile boolean valueSet = false;

        @VisibleForTesting
        DisruptorFuture(Ticker ticker, AutobatcherTelemetryComponents telemetryComponents) {
            this.ticker = ticker;
            this.parent = DetachedSpan.start(telemetryComponents.getSafeLoggablePurpose() + " disruptor task");
            this.waitingSpan = TimedDetachedSpan.from(ticker, parent.childDetachedSpan("task waiting to be run"));
            this.addListener(
                    () -> {
                        waitingSpan.complete();
                        if (runningSpan != null) {
                            runningSpan.complete();
                            parent.complete();

                            if (valueSet) {
                                telemetryComponents.markWaitingTimeAndRunningTimeMetrics(
                                        waitingSpan.getDurationOrThrowIfStillRunning(),
                                        runningSpan.getDurationOrThrowIfStillRunning());
                            } else {
                                telemetryComponents.markWaitingTimeMetrics(
                                        waitingSpan.getDurationOrThrowIfStillRunning());
                            }

                        } else {
                            parent.complete();
                        }
                    },
                    MoreExecutors.directExecutor());
        }

        public DisruptorFuture(AutobatcherTelemetryComponents telemetryComponents) {
            this(Ticker.systemTicker(), telemetryComponents);
        }

        void running() {
            waitingSpan.complete();
            runningSpan = TimedDetachedSpan.from(ticker, parent.childDetachedSpan("running task"));
        }

        @Override
        public boolean set(R value) {
            valueSet = true;
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
        return new DisruptorAutobatcher<>(
                disruptor,
                disruptor.getRingBuffer(),
                AutobatcherTelemetryComponents.create(safeLoggablePurpose, SharedTaggedMetricRegistries.getSingleton()),
                closingCallback);
    }
}
