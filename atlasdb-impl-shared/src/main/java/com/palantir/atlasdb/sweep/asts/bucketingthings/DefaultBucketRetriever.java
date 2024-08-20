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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.SweepableBucket;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class DefaultBucketRetriever implements BucketRetriever {
    private final ShardCountStore shardCountStore;
    private final SweepBucketsTable sweepBucketsTable;
    private final SweepBucketPointerTable sweepBucketPointerTable;
    private final SweeperStrategy sweeperStrategy;

    private DefaultBucketRetriever(
            ShardCountStore shardCountStore,
            SweepBucketsTable sweepBucketsTable,
            SweepBucketPointerTable sweepBucketPointerTable,
            SweeperStrategy sweeperStrategy) {
        this.shardCountStore = shardCountStore;
        this.sweepBucketsTable = sweepBucketsTable;
        this.sweeperStrategy = sweeperStrategy;
        this.sweepBucketPointerTable = sweepBucketPointerTable;
    }

    public static BucketRetriever create(
            ShardCountStore shardCountStore,
            SweepBucketsTable sweepBucketsTable,
            SweepBucketPointerTable sweepBucketPointerTable,
            SweeperStrategy sweeperStrategy) {
        return new DefaultBucketRetriever(shardCountStore, sweepBucketsTable, sweepBucketPointerTable, sweeperStrategy);
    }

    @Override
    public Set<SweepableBucket> getSweepableBuckets() {
        int shardCount = shardCountStore.getShardCount(sweeperStrategy);
        // Is this correct? Is this inclusive or exclusive?
        Set<Bucket> startBuckets = sweepBucketPointerTable.getStartingBucketsForShards(IntStream.range(0, shardCount)
                .mapToObj(i -> ShardAndStrategy.of(i, sweeperStrategy))
                .collect(Collectors.toSet()));
        return sweepBucketsTable.getSweepableBuckets(startBuckets);
    }
}
