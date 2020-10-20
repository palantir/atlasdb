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
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingWorkerPool {
    private final CompletionService<Void> service;
    private final int concurrentTaskLimit;

    private final AtomicInteger currentTaskCount = new AtomicInteger();

    public BlockingWorkerPool(ExecutorService executor, int concurrentTaskLimit) {
        this.service = new ExecutorCompletionService<Void>(executor);
        this.concurrentTaskLimit = concurrentTaskLimit;
    }

    /**
     * Submits a task to the pool. If the pool is currently executing the
     * maximum number of tasks allowed, this call will block until one of the
     * currently executing tasks has finished.
     *
     * @throws RuntimeException wrapping an ExecutionException if a previously
     *         submitted task threw an exception.
     */
    public synchronized void submitTask(Runnable task) throws InterruptedException {
        waitForAvailability();

        assert currentTaskCount.get() < concurrentTaskLimit : "currentTaskCount must be less than currentTaskLimit";
        service.submit(task, null);
        currentTaskCount.incrementAndGet();
    }

    private void waitForSingleTask() throws InterruptedException {
        if (currentTaskCount.get() <= 0) {
            return;
        }

        Future<Void> f = service.take();
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
     *         submitted task threw an exception.
     */
    public synchronized void waitForSubmittedTasks() throws InterruptedException {
        while (currentTaskCount.get() > 0) {
            waitForSingleTask();
        }
    }

    /**
     * Waits until the number of tasks drops below the concurrent task limit.
     * @throws InterruptedException
     */
    public synchronized void waitForAvailability() throws InterruptedException {
        if (currentTaskCount.get() >= concurrentTaskLimit) {
            waitForSingleTask();
        }
    }
}
