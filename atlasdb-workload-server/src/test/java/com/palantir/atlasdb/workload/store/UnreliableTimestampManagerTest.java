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

import com.palantir.lock.client.RandomizedTimestampManager;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
public final class UnreliableTimestampManagerTest {

    @Mock
    private TimelockService timelockService;

    @Test
    @SuppressWarnings("JdkObsolete") // LinkedList has the API I need for this test, and I don't see the benefit in
    // using an ArrayDeque to then marshall it into a List here.
    public void randomlyIncreaseTimestampRandomlyIncreasesTimestamp() {
        LinkedList<Long> timestamps = new LinkedList<>(List.of(0L));
        UnreliableTimestampManager timestampManager =
                UnreliableTimestampManager.create(timelockService, timestamps::getLast, timestamps::add);
        for (int i = 0; i < 10; i++) {
            timestampManager.randomlyIncreaseTimestamp();
        }

        assertListRandomlyIncreasesInValue(timestamps);
    }

    @Test
    public void randomlyIncreaseTimestampRandomlyIncreasesTimestampWithMultipleThreads() throws InterruptedException {
        // Collections#synchronizedList gives us a List, not a LinkedList, which isn't suitable here as I want a
        // synchronized getLast method without handrolling it.
        Deque<Long> timestamps = new ConcurrentLinkedDeque<>(List.of(0L));
        UnreliableTimestampManager timestampManager =
                UnreliableTimestampManager.create(timelockService, timestamps::getLast, timestamps::add);

        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            for (int i = 0; i < 10; i++) {
                executor.execute(timestampManager::randomlyIncreaseTimestamp);
            }
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertListRandomlyIncreasesInValue(new ArrayList<>(timestamps));
    }

    @ParameterizedTest
    @MethodSource("timestampMethods")
    public void gettingTimestampsBlocksOnPendingFastForward(Consumer<RandomizedTimestampManager> task)
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
            assertThat(executor.shutdownNow()).isEmpty();
        }
    }

    private static void assertListRandomlyIncreasesInValue(List<Long> timestamps) {
        assertThat(timestamps).isSorted();
        assertThat(timestamps).doesNotHaveDuplicates();

        List<Long> differencesBetweenValues = new ArrayList<>();
        for (int i = 1; i < timestamps.size(); i++) {
            differencesBetweenValues.add(timestamps.get(i) - timestamps.get(i - 1));
        }

        // Verifying things increase randomly. We can't assert that every delta is random, since it's possible that at
        // least one delta is the same as another, but there's a minuscule chance that, in the domain of all longs, we
        // roll each unique long that we draw, 4 times.
        assertThat(new HashSet<>(differencesBetweenValues)).hasSizeGreaterThan(differencesBetweenValues.size() / 4);
    }

    static Stream<Named<Consumer<RandomizedTimestampManager>>> timestampMethods() {
        return Stream.of(
                namedTask("getFreshTimestamp", RandomizedTimestampManager::getFreshTimestamp),
                namedTask(
                        "getCommitTimestamp",
                        service -> service.getCommitTimestamp(1, LockToken.of(UUID.randomUUID()))),
                namedTask("getFreshTimestamps", service -> service.getFreshTimestamps(10)));
    }

    private static Named<Consumer<RandomizedTimestampManager>> namedTask(
            String name, Consumer<RandomizedTimestampManager> task) {
        return Named.of(name, task);
    }
}
