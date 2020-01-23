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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.common.concurrent.PTExecutors;

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

    private final BlockingQueue<DefaultBatchElement<T, R>> queue;
    private final ExecutorService executorService;
    private volatile boolean closed = false;

    DisruptorAutobatcher(BlockingQueue<DefaultBatchElement<T, R>> queue, ExecutorService executorService) {
        this.queue = queue;
        this.executorService = executorService;

    }

    @Override
    public ListenableFuture<R> apply(T argument) {
        Preconditions.checkState(!closed, "Autobatcher is already shut down");
        SettableFuture<R> result = SettableFuture.create();
        queue.add(new DefaultBatchElement<>(argument, result));
        return result;
    }

    @Override
    public void close() {
        closed = true;
        try {
            executorService.shutdownNow();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Disruptor took more than 10 seconds to shutdown. "
                        + "Ensure that handlers aren't uninterruptibly blocking and ensure that they are closed.");
            }
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static final class DefaultBatchElement<T, R> implements BatchElement<T, R> {
        private final T argument;
        private final SettableFuture<R> result;

        private DefaultBatchElement(T argument, SettableFuture<R> result) {
            this.argument = argument;
            this.result = result;
        }

        @Override
        public T argument() {
            return argument;
        }

        @Override
        public SettableFuture<R> result() {
            return result;
        }
    }

    static <T, R> DisruptorAutobatcher<T, R> create(
            EventHandler<BatchElement<T, R>> eventHandler,
            String safeLoggablePurpose) {
        ExecutorService executor = PTExecutors.newSingleThreadExecutor(threadFactory(safeLoggablePurpose));
        BlockingQueue<DefaultBatchElement<T, R>> queue = new LinkedBlockingQueue<>();
        DisruptorAutobatcher<T, R> autobatcher = new DisruptorAutobatcher<>(queue, executor);
        executor.submit(() -> {
            while (!autobatcher.closed) {
                try {
                    List<DefaultBatchElement<T, R>> results = new ArrayList<>();
                    try {
                        results.add(queue.take());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    queue.drainTo(results);
                    eventHandler.onEvents(Collections.unmodifiableList(results));
                } catch (Throwable t) {
                    log.warn("Caught a throwable", t);
                }
            }
        });
        return autobatcher;
    }
}
