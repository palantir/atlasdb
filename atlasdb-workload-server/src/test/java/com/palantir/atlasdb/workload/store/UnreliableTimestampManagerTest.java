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

package com.palantir.atlasdb.workload.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import com.palantir.lock.client.TimestampManager;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class UnreliableTimestampManagerTest {

    @Mock
    private TimelockService timelockService;

    @Test
    public void randomlyIncreaseTimestampRandomlyIncreasesTimestamp() {
        List<Long> allValues = new ArrayList<>(List.of(0L));
        UnreliableTimestampManager timestampManager =
                UnreliableTimestampManager.create(timelockService, () -> Iterables.getLast(allValues), allValues::add);
        for (int i = 0; i < 10; i++) {
            timestampManager.randomlyIncreaseTimestamp();
        }

        assertListRandomlyIncreasesInValue(allValues);
    }

    @Test
    public void randomlyIncreaseTimestampRandomlyIncreasesTimestampWithMultipleThreads() throws InterruptedException {
        ArrayList<Long> allValues = new ArrayList<>(List.of(0L));
        UnreliableTimestampManager timestampManager = createThreadsafeArrayBackedTimestampManager(allValues);

        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            for (int i = 0; i < 10; i++) {
                executor.execute(timestampManager::randomlyIncreaseTimestamp);
            }
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertListRandomlyIncreasesInValue(allValues);
    }

    @ParameterizedTest
    @MethodSource("timestampMethods")
    public void gettingTimestampsBlocksOnPendingFastForward(Consumer<TimestampManager> task)
            throws InterruptedException {
        CountDownLatch startingFastForward = new CountDownLatch(1);
        CountDownLatch blockFastForward = new CountDownLatch(1);

        UnreliableTimestampManager timestampManager =
                UnreliableTimestampManager.create(timelockService, () -> 0L, _value -> {
                    startingFastForward.countDown();
                    try {
                        blockFastForward.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            Future<?> increaseTimestampFuture = executor.submit(timestampManager::randomlyIncreaseTimestamp);
            startingFastForward.await();
            Future<?> getTimestamp = executor.submit(() -> task.accept(timestampManager));
            assertThat(getTimestamp).isNotDone();
            blockFastForward.countDown();
            assertThat(increaseTimestampFuture).succeedsWithin(Duration.ofSeconds(1));
            assertThat(getTimestamp).succeedsWithin(Duration.ofSeconds(1));

        } finally {
            executor.shutdown();
            boolean shutdownSuccessfully = executor.awaitTermination(10, TimeUnit.SECONDS);
            executor.shutdownNow();
            assertThat(shutdownSuccessfully).isTrue();
        }
    }

    private static void assertListRandomlyIncreasesInValue(List<Long> timestamps) {
        assertThat(timestamps).isSorted();
        assertThat(new HashSet<>(timestamps)).hasSameSizeAs(timestamps);

        List<Long> differencesBetweenValues = new ArrayList<>();
        for (int i = 1; i < timestamps.size(); i++) {
            differencesBetweenValues.add(timestamps.get(i) - timestamps.get(i - 1));
        }

        // Verifying things increase randomly. We can't assert that every delta is random, since it's possible that at
        // least one delta is the same as another, but there's a minuscule chance that, in the domain of all longs, we
        // roll each unique long that we draw, 4 times.
        assertThat(new HashSet<>(differencesBetweenValues)).hasSizeGreaterThan(differencesBetweenValues.size() / 4);
    }

    private UnreliableTimestampManager createThreadsafeArrayBackedTimestampManager(List<Long> timestampList) {
        ReadWriteLock rwlock = new ReentrantReadWriteLock();
        return UnreliableTimestampManager.create(
                timelockService,
                () -> {
                    Lock lock = rwlock.readLock();
                    lock.lock();
                    try {
                        return Iterables.getLast(timestampList);
                    } finally {
                        lock.unlock();
                    }
                },
                value -> {
                    Lock lock = rwlock.writeLock();
                    lock.lock();
                    try {
                        timestampList.add(value);
                    } finally {
                        lock.unlock();
                    }
                });
    }

    static Stream<Named<Consumer<TimestampManager>>> timestampMethods() {
        return Stream.of(
                namedTask("getFreshTimestamp", TimestampManager::getFreshTimestamp),
                namedTask(
                        "getCommitTimestamp",
                        service -> service.getCommitTimestamp(1, LockToken.of(UUID.randomUUID()))),
                namedTask("getFreshTimestamps", service -> service.getFreshTimestamps(10)));
    }

    private static Named<Consumer<TimestampManager>> namedTask(String name, Consumer<TimestampManager> task) {
        return Named.of(name, task);
    }
}
