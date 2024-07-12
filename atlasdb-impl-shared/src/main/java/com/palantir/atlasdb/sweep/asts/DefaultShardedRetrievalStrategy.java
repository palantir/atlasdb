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
import com.palantir.atlasdb.sweep.queue.SweepableTimestamps;
import com.palantir.refreshable.Refreshable;
import java.util.List;
import java.util.stream.Collectors;

public final class DefaultShardedRetrievalStrategy implements ShardedRetrievalStrategy {
    private final Refreshable<Integer> bucketLimit;
    private final SweepableTimestamps sweepableTimestamps;

    private DefaultShardedRetrievalStrategy(Refreshable<Integer> bucketLimit, SweepableTimestamps sweepableTimestamps) {
        this.bucketLimit = bucketLimit;
        this.sweepableTimestamps = sweepableTimestamps;
    }

    public static ShardedRetrievalStrategy create(
            Refreshable<Integer> bucketLimit, SweepableTimestamps sweepableTimestamps) {
        return new DefaultShardedRetrievalStrategy(bucketLimit, sweepableTimestamps);
    }

    @Override
    public List<SweepableBucket> getSweepableBucketsForShard(
            ShardAndStrategy shardAndStrategy, SweepTimestamps sweepTimestamps) {
        return sweepableTimestamps
                .nextTimestampPartitions(
                        shardAndStrategy,
                        sweepTimestamps.lastSweptTimestamp(),
                        sweepTimestamps.sweepTimestamp(),
                        bucketLimit.get())
                .stream()
                .map(partition -> SweepableBucket.of(shardAndStrategy, partition))
                .collect(Collectors.toList());
    }
}
