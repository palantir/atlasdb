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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.sweep.asts.ShardedSweepTimestampManager.SweepTimestamps;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ShardedSweepableBucketRetrieverTest {
    private static final SweepTimestamps SWEEP_TIMESTAMPS =
            SweepTimestamps.builder().sweepTimestamp(1).lastSweptTimestamp(12).build();
    private final SettableRefreshable<Integer> numShards = Refreshable.create(1);
    private final SettableRefreshable<Integer> maxParallelism = Refreshable.create(1);
    private final TestShardedRetrievalStrategy strategy = new TestShardedRetrievalStrategy();
    private final TestParallelTaskExecutor parallelTaskExecutor = new TestParallelTaskExecutor();
    private final ExecutorService executorService = PTExecutors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean wasSleepCalled = new AtomicBoolean(false);

    private SweepableBucketRetriever retriever;

    @BeforeEach
    public void before() {
        retriever = new ShardedSweepableBucketRetriever(
                numShards,
                SweeperStrategy.CONSERVATIVE,
                strategy,
                shardAndStrategy -> SWEEP_TIMESTAMPS,
                parallelTaskExecutor,
                maxParallelism,
                () -> wasSleepCalled.set(true));
    }

    @AfterEach
    public void after() {
        executorService.shutdown();
    }

    @Test
    public void everyShardIsRequested() {
        int shards = 10;
        numShards.update(shards);
        retriever.getSweepableBuckets();
        assertThat(strategy.getRequestedShards())
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, shards).boxed().collect(Collectors.toSet()));
    }

    @Test
    public void noMoreThanMaxParallelismRequestsAreMadeInParallel() {
        maxParallelism.update(23);
        retriever.getSweepableBuckets();
        assertThat(parallelTaskExecutor.getLastMaxParallelism()).contains(23);
    }

    @Test
    public void updatesToMaxParallelismAreReflectedInSubsequentRequests() {
        int newMaxParallelism = 9084;
        maxParallelism.update(12);
        retriever.getSweepableBuckets();
        maxParallelism.update(newMaxParallelism);
        retriever.getSweepableBuckets();
        assertThat(parallelTaskExecutor.getLastMaxParallelism()).contains(newMaxParallelism);
    }

    @Test
    public void callsSleeperBetweenRequests() {
        retriever.getSweepableBuckets();
        assertThat(wasSleepCalled).isTrue();
    }

    @Test
    public void defaultSleeperCanBeInterrupted() throws InterruptedException {
        retriever = ShardedSweepableBucketRetriever.create(
                numShards,
                SweeperStrategy.CONSERVATIVE,
                strategy,
                shardAndStrategy -> SWEEP_TIMESTAMPS,
                parallelTaskExecutor,
                maxParallelism,
                Refreshable.only(Duration.ofMinutes(10)));

        CountDownLatch latch = new CountDownLatch(1);
        Future<?> future = executorService.submit(() -> {
            latch.countDown();
            retriever.getSweepableBuckets();
        });
        latch.await(); // reduce flakes where task doesn't actually run
        future.cancel(true);
        executorService.shutdown();

        // Even though the backoff is up to 10 minutes, interrupting the task should make it finish much faster.
        Awaitility.await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertThat(
                        executorService.awaitTermination(0, TimeUnit.MILLISECONDS))
                .isTrue());
    }

    private static final class TestShardedRetrievalStrategy implements ShardedRetrievalStrategy {
        private final Set<Integer> shards = new HashSet<>();

        @Override
        public List<SweepableBucket> getSweepableBucketsForShard(
                ShardAndStrategy shard, SweepTimestamps _sweepTimestamps) {
            int shardId = shard.shard();
            shards.add(shardId);
            return generateList(shardId);
        }

        public Set<Integer> getRequestedShards() {
            return shards;
        }

        private static List<SweepableBucket> generateList(int shard) {
            return IntStream.range(0, new Random().nextInt(10))
                    .mapToObj(i -> SweepableBucket.of(ShardAndStrategy.of(shard, SweeperStrategy.CONSERVATIVE), i))
                    .collect(Collectors.toList());
        }
    }

    private static final class TestParallelTaskExecutor implements ParallelTaskExecutor {
        private Optional<Integer> lastMaxParallelism = Optional.empty();

        @Override
        public <V, K> List<V> execute(Stream<K> arg, Function<K, V> task, int maxParallelism) {
            lastMaxParallelism = Optional.of(maxParallelism);
            return arg.map(task).collect(Collectors.toList());
        }

        public Optional<Integer> getLastMaxParallelism() {
            return lastMaxParallelism;
        }
    }
}
