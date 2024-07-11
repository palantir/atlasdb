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

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.sweep.asts.ShardedSweepTimestampManager.SweepTimestamps;
import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.CloseableTracer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

// Perhaps this doesn't need to be a separate class. Did just make testing a bit easier
public final class FaultTolerantShardedRetrieverStrategy implements ShardedRetrieverStrategy {
    private final SafeLogger log = SafeLoggerFactory.get(FaultTolerantShardedRetrieverStrategy.class);
    private final ShardedRetrieverStrategy delegate;
    private final Consumer<ShardAndStrategy> failureTracker;
    private final Consumer<Duration> durationTracker;

    private FaultTolerantShardedRetrieverStrategy(
            ShardedRetrieverStrategy delegate,
            Consumer<ShardAndStrategy> failureTracker,
            Consumer<Duration> durationTracker) {
        this.delegate = delegate;
        this.failureTracker = failureTracker;
        this.durationTracker = durationTracker;
    }

    public static ShardedRetrieverStrategy create(
            ShardedRetrieverStrategy delegate,
            Consumer<ShardAndStrategy> failureTracker,
            Consumer<Duration> durationTracker) {
        return new FaultTolerantShardedRetrieverStrategy(delegate, failureTracker, durationTracker);
    }

    /**
     * Returns a list of sweepable buckets for a given shard from the underlying {@link ShardedRetrieverStrategy}.
     * If retrieving the list results in an error, instead returns an empty list so that we can make progress on
     * other shards.
     */
    @Override
    public List<SweepableBucket> getSweepableBucketsForShard(ShardAndStrategy shard, SweepTimestamps sweepTimestamps) {
        try (CloseableTracer tracer =
                CloseableTracer.startSpan("getSweepableBucketsForShard", metadataFromShardAndStrategy(shard))) {
            // Figure out how to do a timed context
            return delegate.getSweepableBucketsForShard(shard, sweepTimestamps);
        } catch (Exception e) {
            log.warn(
                    "Failed to retrieve sweepable buckets for shard. Swallowing and returning an empty candidate list for this shard.",
                    SafeArg.of("shardAndStrategy", shard),
                    e);
            failureTracker.accept(shard);
            return List.of();
        }
    }

    private Map<String, String> metadataFromShardAndStrategy(ShardAndStrategy shard) {
        return ImmutableMap.of(
                "shard", Integer.toString(shard.shard()),
                "strategy", shard.strategy().toString());
    }
}
