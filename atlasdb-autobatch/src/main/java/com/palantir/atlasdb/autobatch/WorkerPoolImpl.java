/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.jetbrains.annotations.VisibleForTesting;

final class WorkerPoolImpl implements WorkerPool {
    private static final SafeLogger log = SafeLoggerFactory.get(WorkerPoolImpl.class);
    private final Semaphore semaphore;
    private final ExecutorService executor;
    private final String safeLoggablePurpose;

    @VisibleForTesting
    WorkerPoolImpl(Semaphore semaphore, ExecutorService executor, String safeLoggablePurpose) {
        this.semaphore = semaphore;
        this.executor = executor;
        this.safeLoggablePurpose = safeLoggablePurpose;
    }

    static WorkerPool create(int workerCount, String safeLoggablePurpose) {
        Preconditions.checkArgument(
                workerCount >= 0, "Worker count must be non-negative.", SafeArg.of("workerCount", workerCount));

        if (workerCount == 0) {
            return NoOpWorkerPool.INSTANCE;
        }

        return new WorkerPoolImpl(
                new Semaphore(workerCount, true),
                PTExecutors.newFixedThreadPool(workerCount, safeLoggablePurpose),
                safeLoggablePurpose);
    }

    /**
     * Tries to asynchronously submit the given task to one of the workers.
     * Returns true if the task was submitted and false otherwise.
     * If all workers are busy, no blocking occurs.
     */
    @Override
    public <T> boolean tryRun(Supplier<T> supplier, Consumer<T> task) {
        if (!semaphore.tryAcquire()) {
            return false;
        }

        Optional<T> input = consumeSupplierWithSemaphoreCleanupOnException(supplier);
        if (input.isEmpty()) {
            return false;
        }

        Optional<ListenableFuture<Void>> taskFuture =
                submitTaskWithSemaphoreCleanupOnException(() -> task.accept(input.get()));
        if (taskFuture.isEmpty()) {
            return false;
        }

        taskFuture.get().addListener(semaphore::release, executor);
        return true;
    }

    @Override
    public List<Runnable> close() {
        return executor.shutdownNow();
    }

    private Optional<ListenableFuture<Void>> submitTaskWithSemaphoreCleanupOnException(Runnable task) {
        try {
            return Optional.of(Futures.submit(task, executor));
        } catch (Exception e) {
            log.warn(
                    "Failed to submit task to executor, possibly due to resource constraints.",
                    SafeArg.of("purpose", safeLoggablePurpose),
                    e);
            semaphore.release();
            return Optional.empty();
        }
    }

    private <T> Optional<T> consumeSupplierWithSemaphoreCleanupOnException(Supplier<T> supplier) {
        try {
            return Optional.of(supplier.get());
        } catch (Exception e) {
            log.warn("Failed to get input from supplier.", SafeArg.of("purpose", safeLoggablePurpose), e);
            semaphore.release();
            return Optional.empty();
        }
    }

    private enum NoOpWorkerPool implements WorkerPool {
        INSTANCE;

        @Override
        public <T> boolean tryRun(Supplier<T> supplier, Consumer<T> task) {
            return false;
        }

        @Override
        public List<Runnable> close() {
            return List.of();
        }
    }
}
