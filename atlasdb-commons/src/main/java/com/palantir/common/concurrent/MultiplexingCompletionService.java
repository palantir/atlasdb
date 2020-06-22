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
package com.palantir.common.concurrent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A MultiplexingCompletionService is much like a {@link java.util.concurrent.ExecutorCompletionService}, but
 * supports multiple delegate {@link ExecutorService}s feeding in to a single shared {@link BlockingQueue}.
 * {@link #poll()} operations will retrieve the results of computations that are done first (regardless of which
 * actual underlying executor service they may have been scheduled on).
 *
 * Maintaining separate executors may see application in improving monitoring and bounding of thread pools that
 * have several distinct use cases.
 *
 * @param <K> key type
 * @param <V> return type of tasks that are to be submitted
 */
public class MultiplexingCompletionService<K, V> {
    private final ImmutableMap<K, ExecutorService> executors;
    private final BlockingQueue<Future<Map.Entry<K, V>>> taskQueue;

    private MultiplexingCompletionService(
            ImmutableMap<K, ExecutorService> executors, BlockingQueue<Future<Map.Entry<K, V>>> taskQueue) {
        this.executors = executors;
        this.taskQueue = taskQueue;
    }

    public static <K, V> MultiplexingCompletionService<K, V> create(
            Map<? extends K, ExecutorService> executors) {
        return new MultiplexingCompletionService<>(ImmutableMap.copyOf(executors), new LinkedBlockingQueue<>());
    }

    /**
     * Submits a task to be run on a specific executor.
     *
     * @param key to identify which executor the task should be run on
     * @param task to be run on the relevant executor
     * @return future associated with submitting the task to the correct executor
     *
     * @throws IllegalStateException if the key provided is not associated with any executor
     */
    public Future<Map.Entry<K, V>> submit(K key, Callable<V> task) {
        ExecutorService targetExecutor = executors.get(key);
        if (targetExecutor == null) {
            throw new SafeIllegalStateException("The key provided to the multiplexing completion service doesn't exist!");
        }
        return submitAndPrepareForQueueing(targetExecutor, key, task);
    }

    public Future<Map.Entry<K, V>> poll() {
        return taskQueue.poll();
    }

    public Future<Map.Entry<K, V>> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return taskQueue.poll(timeout, unit);
    }

    private Future<Map.Entry<K, V>> submitAndPrepareForQueueing(
            ExecutorService delegate,
            K key,
            Callable<V> callable) {
        FutureTask<Map.Entry<K, V>> futureTask = new FutureTask<>(() -> Maps.immutableEntry(key, callable.call()));
        delegate.submit(new QueueTask(futureTask), null);
        return futureTask;
    }

    private class QueueTask extends FutureTask<Map.Entry<K, V>> {
        private final RunnableFuture<Map.Entry<K, V>> runnable;

        private QueueTask(RunnableFuture<Map.Entry<K, V>> runnable) {
            super(runnable, null);
            this.runnable = runnable;
        }

        @Override
        protected void done() {
            taskQueue.add(runnable);
        }
    }
}
