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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

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
    private final ImmutableMap<K, ListeningExecutorService> executors;
    private final BlockingQueue<ListenableFuture<Map.Entry<K, V>>> taskQueue;

    private MultiplexingCompletionService(
            ImmutableMap<K, ListeningExecutorService> executors,
            BlockingQueue<ListenableFuture<Map.Entry<K, V>>> taskQueue) {
        this.executors = executors;
        this.taskQueue = taskQueue;
    }

    public static <K, V> MultiplexingCompletionService<K, V> create(
            Map<? extends K, ExecutorService> executors) {
        ImmutableMap<K, ListeningExecutorService> listeningExecutors = KeyedStream.stream(executors)
                .map(delegate -> MoreExecutors.listeningDecorator(delegate))
                .entries()
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        return new MultiplexingCompletionService<>(
                listeningExecutors,
                new LinkedBlockingQueue<>());
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
    public ListenableFuture<Map.Entry<K, V>> submit(K key, Callable<V> task) {
        ListeningExecutorService targetExecutor = executors.get(key);
        if (targetExecutor == null) {
            throw new SafeIllegalStateException("The key provided to the multiplexing completion service doesn't exist!");
        }
        return submitAndPrepareForQueueing(targetExecutor, key, task);
    }

    public ListenableFuture<Map.Entry<K, V>> poll() {
        return taskQueue.poll();
    }

    public ListenableFuture<Map.Entry<K, V>> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return taskQueue.poll(timeout, unit);
    }

    private ListenableFuture<Map.Entry<K, V>> submitAndPrepareForQueueing(
            ListeningExecutorService delegate,
            K key,
            Callable<V> callable) {
        ListenableFuture<Map.Entry<K, V>> futureTask = delegate.submit(() -> Maps.immutableEntry(key, callable.call()));
        futureTask.addListener(() -> taskQueue.add(futureTask), MoreExecutors.directExecutor());
        return futureTask;
    }

}
