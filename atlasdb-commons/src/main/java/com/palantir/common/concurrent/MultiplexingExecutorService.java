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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class bears similarities to {@link java.util.concurrent.ExecutorCompletionService} in that it provides a means
 * for scheduling tasks along with polling for tasks as they complete.
 *
 * However, tasks submitted are invoked on all elements of type T known to the service, and are assumed to be
 * functions of T.
 * @param <T>
 */
public class MultiplexingExecutorService<T, V> {
    private final Map<T, ExecutorService> executors;
    private final BlockingQueue<Future<V>> taskQueue;

    public MultiplexingExecutorService(Map<T, ExecutorService> executors, BlockingQueue<Future<V>> taskQueue) {
        this.executors = executors;
        this.taskQueue = taskQueue;
    }

    public Map<T, Future<V>> execute(Function<T, V> function) {
        return executors.entrySet()
                .stream()
                .collect(Collectors.<Map.Entry<T, ExecutorService>, T, Future<V>>toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().submit(
                                new QueueTask(new FutureTask<>(() -> function.apply(entry.getKey()))), null)
                ));
    }

    public Future<V> poll() {
        return taskQueue.poll();
    }

    public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return taskQueue.poll(timeout, unit);
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
