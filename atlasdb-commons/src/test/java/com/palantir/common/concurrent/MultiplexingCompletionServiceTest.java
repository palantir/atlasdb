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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

public class MultiplexingCompletionServiceTest {
    private static final String KEY_1 = "key_1";
    private static final String KEY_2 = "key_2";

    private final DeterministicScheduler executor1 = new DeterministicScheduler();
    private final DeterministicScheduler executor2 = new DeterministicScheduler();

    private MultiplexingCompletionService<String, Integer> completionService;

    @Before
    public void setUp() {
        completionService = MultiplexingCompletionService.create(ImmutableMap.of(KEY_1, executor1, KEY_2, executor2));
    }

    @Test
    public void executorServicesFeedInToTheSameQueue()
            throws ExecutionException, InterruptedException, CheckedRejectedExecutionException {
        completionService.submit(KEY_1, () -> 31);
        completionService.submit(KEY_2, () -> 41);

        executor1.runUntilIdle();
        executor2.runUntilIdle();

        assertThat(completionService.poll().get()).isEqualTo(Maps.immutableEntry(KEY_1, 31));
        assertThat(completionService.poll().get()).isEqualTo(Maps.immutableEntry(KEY_2, 41));
    }

    @Test
    public void resultsAreTakenAsTheyBecomeAvailable()
            throws ExecutionException, InterruptedException, CheckedRejectedExecutionException {
        completionService.submit(KEY_1, () -> 5);
        completionService.submit(KEY_2, () -> 11);
        completionService.submit(KEY_1, () -> 42);

        executor1.runUntilIdle();
        executor2.runUntilIdle();

        // 42 is before 11, because executor 1 finishes its tasks first
        assertThat(completionService.poll().get()).isEqualTo(Maps.immutableEntry(KEY_1, 5));
        assertThat(completionService.poll().get()).isEqualTo(Maps.immutableEntry(KEY_1, 42));
        assertThat(completionService.poll().get()).isEqualTo(Maps.immutableEntry(KEY_2, 11));
        assertThat(completionService.poll()).isNull();
    }

    @Test
    public void propagatesFailingComputationResults()
            throws ExecutionException, InterruptedException, CheckedRejectedExecutionException {
        MultiplexingCompletionService<String, Integer> service =
                MultiplexingCompletionService.create(ImmutableMap.of(KEY_1, executor1, KEY_2, executor2));
        service.submit(KEY_1, () -> 5);
        service.submit(KEY_2, () -> {
            throw new IllegalArgumentException("bad");
        });

        executor1.runUntilIdle();
        executor2.runUntilIdle();
        assertThat(service.poll().get()).isEqualTo(Maps.immutableEntry(KEY_1, 5));
        assertThatThrownBy(() -> service.poll().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bad");
    }

    @Test
    public void throwsIfKeyDoesNotExist() {
        MultiplexingCompletionService<String, Integer> oneKeyService =
                MultiplexingCompletionService.create(ImmutableMap.of(KEY_1, executor1));
        assertThatThrownBy(() -> oneKeyService.submit(KEY_2, () -> 7)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void returnsResultsEvenIfOneExecutorIsSlow()
            throws ExecutionException, InterruptedException, CheckedRejectedExecutionException {
        completionService.submit(KEY_1, () -> 5);
        completionService.submit(KEY_2, () -> 11);
        completionService.submit(KEY_1, () -> 42);
        completionService.submit(KEY_2, () -> 18);

        // executor1 does not run any of its tasks
        executor2.runUntilIdle();

        assertThat(completionService.poll().get()).isEqualTo(Maps.immutableEntry(KEY_2, 11));
        assertThat(completionService.poll().get()).isEqualTo(Maps.immutableEntry(KEY_2, 18));
        assertThat(completionService.poll()).isNull();

        executor1.runUntilIdle();

        assertThat(completionService.poll().get()).isEqualTo(Maps.immutableEntry(KEY_1, 5));
        assertThat(completionService.poll().get()).isEqualTo(Maps.immutableEntry(KEY_1, 42));
        assertThat(completionService.poll()).isNull();
    }

    @Test
    public void rejectsExecutionsIfUnderlyingExecutorRejects() throws CheckedRejectedExecutionException {
        MultiplexingCompletionService<String, Integer> boundedService =
                MultiplexingCompletionService.create(ImmutableMap.of(KEY_1, createBoundedExecutor(1)));

        Callable<Integer> sleepForFiveSeconds = getSleepCallable(5_000);

        boundedService.submit(KEY_1, sleepForFiveSeconds);
        boundedService.submit(KEY_1, sleepForFiveSeconds);

        assertThat(boundedService.poll()).isNull();
        assertThatThrownBy(() -> boundedService.submit(KEY_1, sleepForFiveSeconds))
                .isInstanceOf(CheckedRejectedExecutionException.class);
    }

    @Test
    public void resilientToLargeNumberOfRequests()
            throws InterruptedException, ExecutionException, CheckedRejectedExecutionException {
        MultiplexingCompletionService<String, Integer> boundedService = MultiplexingCompletionService.create(
                ImmutableMap.of(KEY_1, createBoundedExecutor(2), KEY_2, createBoundedExecutor(2)));

        for (int i = 0; i < 4; i++) {
            try {
                boundedService.submit(KEY_1, getSleepCallable(5_000));
            } catch (CheckedRejectedExecutionException e) {
                // This is permissible
            }
        }

        boundedService.submit(KEY_2, getSleepCallable(1));

        assertThat(boundedService.poll(1, TimeUnit.SECONDS).get()).isEqualTo(Maps.immutableEntry(KEY_2, 1));
    }

    @Test
    public void valuesMayBeRetrievedFromFuturesReturned()
            throws ExecutionException, InterruptedException, CheckedRejectedExecutionException {
        DeterministicScheduler scheduler = new DeterministicScheduler();
        MultiplexingCompletionService<String, Integer> boundedService =
                MultiplexingCompletionService.create(ImmutableMap.of(KEY_1, scheduler));
        Future<Map.Entry<String, Integer>> returnedFuture = boundedService.submit(KEY_1, () -> 1234567);

        scheduler.runUntilIdle();

        assertThat(returnedFuture.get()).isEqualTo(Maps.immutableEntry(KEY_1, 1234567));
        assertThat(boundedService.poll().get()).isEqualTo(Maps.immutableEntry(KEY_1, 1234567));
    }

    private static Callable<Integer> getSleepCallable(int durationMillis) {
        return () -> {
            Thread.sleep(durationMillis);
            return durationMillis;
        };
    }

    private static ThreadPoolExecutor createBoundedExecutor(int poolSize) {
        return PTExecutors.newThreadPoolExecutor(poolSize, poolSize, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1));
    }
}
