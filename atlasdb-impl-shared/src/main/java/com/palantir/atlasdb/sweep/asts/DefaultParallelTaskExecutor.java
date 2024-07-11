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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class DefaultParallelTaskExecutor implements ParallelTaskExecutor {
    private final ExecutorService cachedExecutorService;

    private DefaultParallelTaskExecutor(ExecutorService cachedExecutorService) {
        this.cachedExecutorService = cachedExecutorService;
    }

    public static DefaultParallelTaskExecutor create(ExecutorService cachedExecutorService) {
        return new DefaultParallelTaskExecutor(cachedExecutorService);
    }

    @Override
    public <V, K> List<V> execute(Stream<K> arg, Function<K, V> task, int maxParallelism) {
        Semaphore semaphore = new Semaphore(maxParallelism);
        Stream<Future<V>> executedTasks = arg.map(k -> {
            acquireSemaphore(semaphore);
            try {
                return cachedExecutorService.submit(() -> {
                    try {
                        return task.apply(k);
                    } finally {
                        semaphore.release();
                    }
                });
            } finally {
                semaphore.release();
            }
        });
        return executedTasks
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
        try {
            semaphore.acquire(); // TODO: Don't wait indefinitely.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
