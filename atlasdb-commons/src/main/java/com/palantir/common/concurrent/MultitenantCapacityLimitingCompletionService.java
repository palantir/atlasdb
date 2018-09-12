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

package com.palantir.common.concurrent;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

public class MultitenantCapacityLimitingCompletionService<T, V> {
    private final ImmutableMap<T, Semaphore> usageCount;
    private final ExecutorService delegate;

    private final BlockingQueue<Future<V>> taskQueue;

    private MultitenantCapacityLimitingCompletionService(
            ImmutableMap<T, Semaphore> usageCount,
            ExecutorService delegate,
            BlockingQueue<Future<V>> taskQueue) {
        this.usageCount = usageCount;
        this.delegate = delegate;
        this.taskQueue = taskQueue;
    }

    public static <T, V> MultitenantCapacityLimitingCompletionService<T, V> create(
            Collection<T> permittedKeys,
            int capacityPerKey,
            ExecutorService delegate) {
        Map<T, Semaphore> usageCount = permittedKeys.stream().collect(
                Collectors.toMap(key -> key, unused -> new Semaphore(capacityPerKey)));
        return new MultitenantCapacityLimitingCompletionService<>(
                ImmutableMap.copyOf(usageCount), delegate, new LinkedBlockingQueue<>());
    }

    // TODO (jkong): Metrics for rejections / usage counts

    public Future<V> execute(T key, Callable<V> task) {
        Semaphore individualUsageCounter = usageCount.get(key);
        if (individualUsageCounter == null) {
            throw new IllegalStateException("MultitenantCapacityLimitingCompletionService doesn't recognise this key");
        }

        if (individualUsageCounter.tryAcquire()) {
            return submitAndPrepareForQueueing(key, task);
        }
        throw new RejectedExecutionException("There are too many computations currently ongoing for this key");
    }

    public Future<V> poll() {
        return taskQueue.poll();
    }

    public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return taskQueue.poll(timeout, unit);
    }

    private Future<V> submitAndPrepareForQueueing(T key, Callable<V> callable) {
        return delegate.submit(new QueueTask(key, new FutureTask<>(callable)), null);
    }

    private class QueueTask extends FutureTask<V> {
        private final T key;
        private final RunnableFuture<V> runnable;

        private QueueTask(T key, RunnableFuture<V> runnable) {
            super(runnable, null);
            this.key = key;
            this.runnable = runnable;
        }

        protected void done() {
            usageCount.get(key).release();
            taskQueue.add(runnable);
        }
    }
}
