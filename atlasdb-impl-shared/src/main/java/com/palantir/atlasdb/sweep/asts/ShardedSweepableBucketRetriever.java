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

import com.palantir.atlasdb.sweep.asts.ShardedSweepTimestampManager.SweepTimestamps;
import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.refreshable.Refreshable;
import com.palantir.tracing.CloseableTracer;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class ShardedSweepableBucketRetriever implements SweepableBucketRetriever {
    private final Refreshable<Integer> numShards;
    private final SweeperStrategy strategy; // TODO: Maybe this should be a parameter on getSweepableBuckets instead.
    // It depends on whether we want a _single_ coordinator to handle both thorough and conservative sweep, or separate

    private final ShardedRetrievalStrategy shardedRetrievalStrategy;
    private final ShardedSweepTimestampManager sweepTimestampManager;
    private final ParallelTaskExecutor parallelTaskExecutor;

    private final Refreshable<Integer> maxParallelism;

    private final Refreshable<Duration> minimumBackoff;
    private final Refreshable<Duration> maxJitter;

    private ShardedSweepableBucketRetriever(
            Refreshable<Integer> numShards,
            SweeperStrategy strategy,
            ShardedRetrievalStrategy shardedRetrievalStrategy,
            ShardedSweepTimestampManager sweepTimestampManager,
            ParallelTaskExecutor parallelTaskExecutor,
            Refreshable<Integer> maxParallelism,
            Refreshable<Duration> minimumBackoff,
            Refreshable<Duration> maxJitter) {
        this.numShards = numShards;
        this.shardedRetrievalStrategy = shardedRetrievalStrategy;
        this.strategy = strategy;
        this.sweepTimestampManager = sweepTimestampManager;
        this.parallelTaskExecutor = parallelTaskExecutor;
        this.maxParallelism = maxParallelism;
        this.minimumBackoff = minimumBackoff;
        this.maxJitter = maxJitter;
    }

    public static SweepableBucketRetriever create(
            Refreshable<Integer> numShards,
            SweeperStrategy strategy,
            ShardedRetrievalStrategy shardedRetrievalStrategy,
            ShardedSweepTimestampManager sweepTimestampManager,
            ParallelTaskExecutor parallelTaskExecutor,
            Refreshable<Integer> maxParallelism,
            Refreshable<Duration> minimumBackoff,
            Refreshable<Duration> maxJitter) {
        return new ShardedSweepableBucketRetriever(
                numShards,
                strategy,
                shardedRetrievalStrategy,
                sweepTimestampManager,
                parallelTaskExecutor,
                maxParallelism,
                minimumBackoff,
                maxJitter);
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

    // Allows us to slow down / speed up the rate of requests to the underlying storage.
    private List<SweepableBucket> getSweepableBucketsForShardWithJitter(int shard) {
        try {
            Duration backoff = minimumBackoff.get();
            Duration jitter =
                    Duration.ofMillis((long) (Math.random() * maxJitter.get().toMillis()));
            Thread.sleep(backoff.plus(jitter).toMillis());
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
