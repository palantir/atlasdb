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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.sweep.asts.ShardedSweepTimestampManager.SweepTimestamps;
import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.refreshable.Refreshable;
import com.palantir.tracing.CloseableTracer;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class ShardedSweepableBucketRetriever implements SweepableBucketRetriever {
    private final Refreshable<Integer> numShards;
    private final SweeperStrategy strategy;

    private final ShardedRetrievalStrategy shardedRetrievalStrategy;
    private final ShardedSweepTimestampManager sweepTimestampManager;
    private final ParallelTaskExecutor parallelTaskExecutor;

    private final Refreshable<Integer> maxParallelism;

    private final Refreshable<Duration> maxBackoff;

    // Exists to facilitate testing in unit tests, rather than needing to mock out ThreadLocalRandom.
    private final Supplier<Long> backoffMillisGenerator;

    @VisibleForTesting
    ShardedSweepableBucketRetriever(
            Refreshable<Integer> numShards,
            SweeperStrategy strategy,
            ShardedRetrievalStrategy shardedRetrievalStrategy,
            ShardedSweepTimestampManager sweepTimestampManager,
            ParallelTaskExecutor parallelTaskExecutor,
            Refreshable<Integer> maxParallelism,
            Refreshable<Duration> maxBackoff,
            Supplier<Long> backoffMillisGenerator) {
        this.numShards = numShards;
        this.shardedRetrievalStrategy = shardedRetrievalStrategy;
        this.strategy = strategy;
        this.sweepTimestampManager = sweepTimestampManager;
        this.parallelTaskExecutor = parallelTaskExecutor;
        this.maxParallelism = maxParallelism;
        this.maxBackoff = maxBackoff;
        this.backoffMillisGenerator = backoffMillisGenerator;
    }

    public static SweepableBucketRetriever create(
            Refreshable<Integer> numShards,
            SweeperStrategy strategy,
            ShardedRetrievalStrategy shardedRetrievalStrategy,
            ShardedSweepTimestampManager sweepTimestampManager,
            ParallelTaskExecutor parallelTaskExecutor,
            Refreshable<Integer> maxParallelism,
            Refreshable<Duration> maxBackoff) {
        return new ShardedSweepableBucketRetriever(
                numShards,
                strategy,
                shardedRetrievalStrategy,
                sweepTimestampManager,
                parallelTaskExecutor,
                maxParallelism,
                maxBackoff,
                // We want _some_ backoff, hence the minimum is 1, rather than the standard 0.
                () -> ThreadLocalRandom.current().nextLong(1, maxBackoff.get().toMillis()));
    }

    @Override
    public Set<SweepableBucket> getSweepableBuckets() {
        List<List<SweepableBucket>> sweepableBuckets;
        try (CloseableTracer tracer = CloseableTracer.startSpan("getSweepableBucketsAcrossAllShards")) {
            // TODO: Time it!
            sweepableBuckets = parallelTaskExecutor.execute(
                    IntStream.range(0, numShards.get()).boxed(),
                    this::getSweepableBucketsForShardWithJitter,
                    maxParallelism.get());
        }
        return sweepableBuckets.stream().flatMap(List::stream).collect(Collectors.toSet());
    }

    /**
     *
     * What we want (Essentially, don't kill the database):
     * Don't run more than maxParallelism tasks
     * Have a delay between tasks executing (and random) to avoid thundering herd
     *
     * maxBackoff must be small (e.g. like 5ms) - otherwise you'll end up massively serialising the requests and not
     * really achieving any parallelism.
     *
     * The "obvious" but partially wrong? solution to this is to add a delay before / after the semaphore acquisition
     * in parallel task executor.
     * If you do that, you won't actually delay from occurring - if you do it before the semaphore acquisition, you'll
     * run through your delay _in parallel_ to the current task executing, rather than after.
     * If you do it after the semaphore is released, the next task will execute whilst the jittering thread
     * is running through the delay, also resulting in limited actual delay occurring.
     */
    private List<SweepableBucket> getSweepableBucketsForShardWithJitter(int shard) {
        try {
            Thread.sleep(backoffMillisGenerator.get());
            return getSweepableBucketsForShard(shard);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private List<SweepableBucket> getSweepableBucketsForShard(int shard) {
        ShardAndStrategy shardAndStrategy = ShardAndStrategy.of(shard, strategy);
        SweepTimestamps sweepTimestamps = sweepTimestampManager.getSweepTimestamps(shardAndStrategy);
        return shardedRetrievalStrategy.getSweepableBucketsForShard(shardAndStrategy, sweepTimestamps);
    }
}
