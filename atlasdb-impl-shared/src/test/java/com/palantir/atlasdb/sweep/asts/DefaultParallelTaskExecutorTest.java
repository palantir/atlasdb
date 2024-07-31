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

import static com.palantir.logsafe.testing.Assertions.assertThat;
import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.logsafe.SafeArg;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public final class DefaultParallelTaskExecutorTest {
    private final ThreadGroup threadGroup = new ThreadGroup("workers");

    private final ExecutorService cachedExecutorService =
            Executors.newCachedThreadPool(runnable -> new Thread(threadGroup, runnable));

    private final SettableRefreshable<Duration> semaphoreAcquireTimeout = Refreshable.create(Duration.ofSeconds(5));

    private final ParallelTaskExecutor taskExecutor =
            DefaultParallelTaskExecutor.create(cachedExecutorService, semaphoreAcquireTimeout);

    @AfterEach
    public void tearDown() {
        cachedExecutorService.shutdownNow();
    }

    @Test
    public void taskCalledInSequenceWithArgs() {
        Stream<Integer> args = IntStream.range(0, 10).boxed();
        Function<Integer, Integer> task = i -> i;
        List<Integer> result = taskExecutor.execute(args, task, 1);
        assertThat(result)
                .containsExactlyElementsOf(IntStream.range(0, 10).boxed().collect(toList()));
    }

    @Test
    public void executesAtMostMaxParallelismInParallel() throws ExecutionException, InterruptedException {
        int maxParallelism = 2;
        Stream<Integer> args = IntStream.range(0, 10).boxed();
        CountDownLatch continueTest = new CountDownLatch(1);
        CountDownLatch waitForTasksToStart = new CountDownLatch(maxParallelism);
        Set<Integer> executed = ConcurrentHashMap.newKeySet();
        Function<Integer, Integer> task = i -> {
            executed.add(i);
            waitForTasksToStart.countDown();
            awaitLatch(continueTest, Duration.ofSeconds(1));
            return i;
        };
        Future<List<Integer>> future =
                cachedExecutorService.submit(() -> taskExecutor.execute(args, task, maxParallelism));
        awaitLatch(waitForTasksToStart, Duration.ofMillis(100));
        assertThat(executed).hasSize(maxParallelism);
        continueTest.countDown();
        List<Integer> result = future.get();
        assertThat(result)
                .containsExactlyElementsOf(IntStream.range(0, 10).boxed().collect(toList()));
    }

    @Test
    public void spinsUpNoMoreThanMaxParallelismThreads() {
        int maxParallelism = 2;
        Stream<Integer> args = IntStream.range(0, 10).boxed();
        CountDownLatch waitForTasksToStart = new CountDownLatch(maxParallelism);
        CountDownLatch continueTest = new CountDownLatch(1);
        Function<Integer, Integer> task = i -> {
            waitForTasksToStart.countDown();

            // We don't actually want the task to ever progress, so we don't count down continueTest
            awaitLatch(continueTest, Duration.ofSeconds(1));
            return i;
        };
        Future<List<Integer>> _future =
                cachedExecutorService.submit(() -> taskExecutor.execute(args, task, maxParallelism));
        awaitLatch(waitForTasksToStart, Duration.ofMillis(100));

        // + 1 to include the taskExecutor call
        assertThat(threadGroup.activeCount()).isEqualTo(maxParallelism + 1);
    }

    @Test
    // i.e, we release the semaphore and don't block forever!
    public void continuesToExecuteEvenWhenTasksFail() {
        Stream<Integer> args = IntStream.range(0, 10).boxed();
        Set<Integer> executed = ConcurrentHashMap.newKeySet();
        Function<Integer, Integer> task = i -> {
            if (i == 5) {
                throw new RuntimeException("Task failed");
            }
            executed.add(i);
            return i;
        };
        assertThatThrownBy(() -> taskExecutor.execute(args, task, 1));
        assertThat(executed)
                .containsExactlyElementsOf(
                        IntStream.range(0, 10).filter(i -> i != 5).boxed().collect(toList()));
    }

    @Test
    public void eventuallyFailsToAcquireSemaphore() {
        Stream<Integer> args = IntStream.range(0, 10).boxed();
        CountDownLatch continueTest = new CountDownLatch(1);
        semaphoreAcquireTimeout.update(Duration.ofMillis(100));
        Function<Integer, Integer> task = i -> {
            awaitLatch(continueTest, Duration.ofSeconds(1));
            return i;
        };
        assertThatLoggableExceptionThrownBy(() -> taskExecutor.execute(args, task, 1))
                .hasLogMessage("Failed to acquire semaphore within timeout")
                .hasExactlyArgs(SafeArg.of("timeout", semaphoreAcquireTimeout.get()));
    }

    private void awaitLatch(CountDownLatch latch, Duration timeout) {
        try {
            boolean result = latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!result) {
                throw new RuntimeException("Latch did not count down in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
