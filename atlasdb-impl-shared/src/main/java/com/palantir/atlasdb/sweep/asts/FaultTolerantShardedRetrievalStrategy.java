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
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

// Perhaps this doesn't need to be a separate class. Did just make testing a bit easier
public final class FaultTolerantShardedRetrievalStrategy implements ShardedRetrievalStrategy {
    private final SafeLogger log = SafeLoggerFactory.get(FaultTolerantShardedRetrievalStrategy.class);
    private final ShardedRetrievalStrategy delegate;
    private final Consumer<ShardAndStrategy> failureTracker;

    private FaultTolerantShardedRetrievalStrategy(
            ShardedRetrievalStrategy delegate, Consumer<ShardAndStrategy> failureTracker) {
        this.delegate = delegate;
        this.failureTracker = failureTracker;
    }

    public static ShardedRetrievalStrategy create(
            ShardedRetrievalStrategy delegate, Consumer<ShardAndStrategy> failureTracker) {
        return new FaultTolerantShardedRetrievalStrategy(delegate, failureTracker);
    }

    /**
     * Returns a list of sweepable buckets for a given shard from the underlying {@link ShardedRetrievalStrategy}.
     * If retrieving the list results in an error, instead returns an empty list so that we can make progress on
     * other shards.
     */
    @Override
    public List<SweepableBucket> getSweepableBucketsForShard(ShardAndStrategy shard, SweepTimestamps sweepTimestamps) {
        try (CloseableTracer tracer =
                CloseableTracer.startSpan("getSweepableBucketsForShard", metadataFromShardAndStrategy(shard))) {
            // TODO: Figure out how to do a timed context, and have a durationTracker
            return delegate.getSweepableBucketsForShard(shard, sweepTimestamps);
        } catch (Exception e) {
            log.warn(
                    "Failed to retrieve sweepable buckets for shard. Swallowing and returning an empty candidate list"
                            + " for this shard.",
                    SafeArg.of("shardAndStrategy", shard),
                    e);
            // TODO: This will likely feed in to whatever class manages all the configurable state here and tune things
            // accordingly (as well as metrics)
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
