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

import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketPointerTable;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketsTable;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class DefaultSweepableBucketRetriever implements SweepableBucketRetriever {
    private final Set<ShardAndStrategy> shardsAndStrategies;
    private final SweepBucketPointerTable sweepBucketPointerTable;
    private final SweepBucketsTable sweepBucketsTable;

    private DefaultSweepableBucketRetriever(
            Set<ShardAndStrategy> shardsAndStrategies,
            SweepBucketPointerTable sweepBucketPointerTable,
            SweepBucketsTable sweepBucketsTable) {
        this.shardsAndStrategies = shardsAndStrategies;
        this.sweepBucketPointerTable = sweepBucketPointerTable;
        this.sweepBucketsTable = sweepBucketsTable;
    }

    public static SweepableBucketRetriever create(
            int numberOfShards,
            List<SweeperStrategy> strategies,
            SweepBucketPointerTable sweepBucketPointerTable,
            SweepBucketsTable sweepBucketsTable) {
        Set<ShardAndStrategy> shardsAndStrategies = IntStream.range(0, numberOfShards)
                .boxed()
                .flatMap(shard -> strategies.stream().map(strategy -> ShardAndStrategy.of(shard, strategy)))
                .collect(Collectors.toSet());
        return new DefaultSweepableBucketRetriever(shardsAndStrategies, sweepBucketPointerTable, sweepBucketsTable);
    }

    @Override
    public Set<SweepableBucket> getSweepableBuckets() {
        if (shardsAndStrategies.isEmpty()) {
            return Set.of();
        }
        Set<Bucket> startingBuckets = sweepBucketPointerTable.getStartingBucketsForShards(shardsAndStrategies);
        return sweepBucketsTable.getSweepableBuckets(startingBuckets);
    }
}
