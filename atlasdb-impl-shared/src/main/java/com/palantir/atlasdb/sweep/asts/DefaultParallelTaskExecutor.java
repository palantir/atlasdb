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

package com.palantir.atlasdb.sweep.asts;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class DefaultParallelTaskExecutor implements ParallelTaskExecutor {
    private final ExecutorService cachedExecutorService;
    private final Refreshable<Duration> semaphoreAcquireTimeout;

    private DefaultParallelTaskExecutor(
            ExecutorService cachedExecutorService, Refreshable<Duration> semaphoreAcquireTimeout) {
        this.cachedExecutorService = cachedExecutorService;
        this.semaphoreAcquireTimeout = semaphoreAcquireTimeout;
    }

    // We're assuming that people will pass in a cachedExecutorService.
    // TODO(mdaudali): Figure out if we want to enforce that by creating our own and managing it, or if there's another
    // way
    //  to make it explicit e.g. types.
    public static DefaultParallelTaskExecutor create(
            ExecutorService cachedExecutorService, Refreshable<Duration> semaphoreAcquireTimeout) {
        return new DefaultParallelTaskExecutor(cachedExecutorService, semaphoreAcquireTimeout);
    }

    @Override
    public <V, K> List<V> execute(Stream<K> arg, Function<K, V> task, int maxParallelism) {
        Semaphore semaphore = new Semaphore(maxParallelism);
        List<Future<V>> executedTasks = arg.map(k -> {
                    // This is outside of the executor otherwise we end up spinning up a tonne of threads
                    acquireSemaphore(semaphore);
                    try {
                        return cachedExecutorService.submit(() -> {
                            try {
                                return task.apply(k);
                            } finally {
                                semaphore.release();
                            }
                        });
                    } catch (Exception e) {
                        semaphore.release(); // Doesn't really matter, since this will cause the whole execute method
                        // to fail and we'll re-create a semaphore on the next execute call
                        // but good hygiene
                        throw e;
                    }
                })
                // Needed to force computation at this layer, rather than blocking on get
                .collect(Collectors.toList());
        return executedTasks.stream()
                .map(future -> {
                    try {
                        return future.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    private void acquireSemaphore(Semaphore semaphore) {
        Duration timeout = semaphoreAcquireTimeout.get();
        try {
            boolean result = semaphore.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!result) {
                throw new SafeRuntimeException(
                        "Failed to acquire semaphore within timeout", SafeArg.of("timeout", timeout));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
