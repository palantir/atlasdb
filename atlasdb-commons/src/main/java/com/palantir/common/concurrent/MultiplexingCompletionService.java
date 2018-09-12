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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

public class MultiplexingCompletionService<T, V> {
    private final ImmutableMap<T, ExecutorService> executors;
    private final BlockingQueue<Future<V>> taskQueue;

    private MultiplexingCompletionService(
            ImmutableMap<T, ExecutorService> executors, BlockingQueue<Future<V>> taskQueue) {
        this.executors = executors;
        this.taskQueue = taskQueue;
    }

    public static <T, V> MultiplexingCompletionService<T, V> create(
            Map<T, ExecutorService> executors) {
        return new MultiplexingCompletionService<>(ImmutableMap.copyOf(executors), new LinkedBlockingQueue<>());
    }

    // TODO (jkong): Metrics for rejections / usage counts

    public Future<V> execute(T key, Callable<V> task) {
        ExecutorService targetExecutor = executors.get(key);
        if (targetExecutor == null) {
            throw new IllegalStateException("The key provided to the multiplexing completion service doesn't exist!");
        }
        return submitAndPrepareForQueueing(targetExecutor, key, task);
    }

    public Future<V> poll() {
        return taskQueue.poll();
    }

    public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return taskQueue.poll(timeout, unit);
    }

    private Future<V> submitAndPrepareForQueueing(ExecutorService delegate, T key, Callable<V> callable) {
        return delegate.submit(new QueueTask(new FutureTask<>(callable)), null);
    }

    private class QueueTask extends FutureTask<V> {
        private final RunnableFuture<V> runnable;

        private QueueTask(RunnableFuture<V> runnable) {
            super(runnable, null);
            this.runnable = runnable;
        }

        protected void done() {
            taskQueue.add(runnable);
        }
    }
}
