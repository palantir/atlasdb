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

import com.google.common.base.Throwables;
import com.palantir.logsafe.Preconditions;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingWorkerPool<T> {
    private final CompletionService<T> service;
    private final int concurrentTaskLimit;

    private final AtomicInteger currentTaskCount = new AtomicInteger();

    /**
     * Construct a BlockingWorkerPool.
     *
     * @param executor The ExecutorService to use for tasks
     * @param concurrentTaskLimit The limit for concurrently running tasks. If this value is 0 or negative, then no
     *                            limit is enforced.
     */
    public BlockingWorkerPool(ExecutorService executor, int concurrentTaskLimit) {
        this.service = new ExecutorCompletionService<>(executor);
        this.concurrentTaskLimit = concurrentTaskLimit;
    }

    /**
     * Submits a task to the pool. If the pool is currently executing the
     * maximum number of tasks allowed, this call will block until one of the
     * currently executing tasks has finished.
     *
     * @throws RuntimeException wrapping an ExecutionException if a previously
     *                          submitted task threw an exception.
     */
    public synchronized void submitTask(Runnable task) throws InterruptedException {
        waitForAvailability();

        Preconditions.checkState(
                concurrentTaskLimit <= 0 || currentTaskCount.get() < concurrentTaskLimit,
                "currentTaskCount must be less than currentTaskLimit");
        service.submit(task, null);
        currentTaskCount.incrementAndGet();
    }

    /**
     * Submits a callable task to the pool. Behaves the same as {@link #submitTask(Runnable)}.
     */
    public synchronized Future<T> submitCallable(Callable<T> task) throws InterruptedException {
        waitForAvailability();

        Preconditions.checkState(
                concurrentTaskLimit <= 0 || currentTaskCount.get() < concurrentTaskLimit,
                "currentTaskCount must be less than currentTaskLimit");
        Future<T> result = service.submit(task);
        currentTaskCount.incrementAndGet();
        return result;
    }

    /**
     * Same as {@link #submitCallable(Callable)} but will wrap any InterruptedException in a RuntimeException.
     * If an InterruptedException was encountered, the thread interrupt flag will still be set.
     */
    public Future<T> submitCallableUnchecked(Callable<T> task) {
        try {
            return submitCallable(task);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void waitForSingleTask() throws InterruptedException {
        if (currentTaskCount.get() <= 0) {
            return;
        }

        Future<T> f = service.take();
        currentTaskCount.decrementAndGet();
        try {
            f.get();
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    /**
     * Waits for all tasks that have been previously submitted to the pool to
     * finish.
     *
     * @throws RuntimeException wrapping an ExecutionException if a previously
     *                          submitted task threw an exception.
     */
    public synchronized void waitForSubmittedTasks() throws InterruptedException {
        while (currentTaskCount.get() > 0) {
            waitForSingleTask();
        }
    }

    /**
     * Waits until the number of tasks drops below the concurrent task limit.
     * If the limit is 0 or negative, then there is no enforced limit and this will not wait.
     */
    public synchronized void waitForAvailability() throws InterruptedException {
        if (concurrentTaskLimit > 0 && currentTaskCount.get() >= concurrentTaskLimit) {
            waitForSingleTask();
        }
    }
}
