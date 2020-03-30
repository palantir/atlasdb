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

import java.io.Closeable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.checkerframework.checker.nullness.compatqual.NullableDecl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.palantir.logsafe.Preconditions;
import com.palantir.tracing.DetachedSpan;

/**
 * While this class is public, it shouldn't be used as API outside of AtlasDB because we
 * don't guarantee we won't break it.
 */
public final class DisruptorAutobatcher<T, R>
        implements AsyncFunction<T, R>, Function<T, ListenableFuture<R>>, Closeable {

    private static final Logger log = LoggerFactory.getLogger(DisruptorAutobatcher.class);

    /*
        By memoizing thread factories per loggable purpose, the thread names are numbered uniquely for multiple
        instances of the same autobatcher function.
     */
    private static final ConcurrentMap<String, ThreadFactory> threadFactories = Maps.newConcurrentMap();

    private static ThreadFactory threadFactory(String safeLoggablePurpose) {
        return threadFactories.computeIfAbsent(safeLoggablePurpose, DisruptorAutobatcher::createThreadFactory);
    }

    private static ThreadFactory createThreadFactory(String safeLoggablePurpose) {
        String namePrefix = String.format("autobatcher.%s-", safeLoggablePurpose);
        return new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(namePrefix + "%d")
                .build();
    }

    private final Disruptor<DefaultBatchElement<T, R>> disruptor;
    private final RingBuffer<DefaultBatchElement<T, R>> buffer;
    private final String safeLoggablePurpose;
    private volatile boolean closed = false;

    DisruptorAutobatcher(
            Disruptor<DefaultBatchElement<T, R>> disruptor,
            RingBuffer<DefaultBatchElement<T, R>> buffer,
            String safeLoggablePurpose) {
        this.disruptor = disruptor;
        this.buffer = buffer;
        this.safeLoggablePurpose = safeLoggablePurpose;
    }

    @Override
    public ListenableFuture<R> apply(T argument) {
        Preconditions.checkState(!closed, "Autobatcher is already shut down");
        DisruptorFuture<R> result = new DisruptorFuture<R>(safeLoggablePurpose);
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
            log.warn("Disruptor took more than 10 seconds to shutdown. "
                    + "Ensure that handlers aren't uninterruptibly blocking and ensure that they are closed.", e);
        }
    }

    private static final class DefaultBatchElement<T, R> implements BatchElement<T, R> {
        private T argument;
        private DisruptorFuture<R> result;

        @Override
        public T argument() {
            return argument;
        }

        @Override
        public DisruptorFuture<R> result() {
            return result;
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
            this.addListener(() -> {
                waitingSpan.complete();
                if (runningSpan != null) {
                    runningSpan.complete();
                }
                parent.complete();
            }, MoreExecutors.directExecutor());
        }

        void running() {
            waitingSpan.complete();
            runningSpan = parent.childDetachedSpan("running task");
        }

        @Override
        public boolean set(@NullableDecl R value) {
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
            String safeLoggablePurpose) {
        Disruptor<DefaultBatchElement<T, R>> disruptor =
                new Disruptor<>(DefaultBatchElement::new, bufferSize, threadFactory(safeLoggablePurpose));
        disruptor.handleEventsWith(eventHandler);
        disruptor.start();
        return new DisruptorAutobatcher<>(disruptor, disruptor.getRingBuffer(), safeLoggablePurpose);
    }
}
