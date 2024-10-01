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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.sweep.asts.SweepableBucket.TimestampRange;
import com.palantir.atlasdb.sweep.asts.bucketingthings.CompletelyClosedSweepBucketBoundRetriever;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketPointerTable;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketRecordsTable;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgress;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.sweep.queue.SweepQueueCleaner;
import java.util.Optional;
import java.util.Set;

public class DefaultShardProgressUpdateProposer implements ShardProgressUpdateProposer {
    private final ShardProgress shardProgress;
    private final BucketProgressStore bucketProgressStore;
    private final SweepQueueCleaner sweepQueueCleaner;
    private final SweepBucketRecordsTable recordsTable;
    private final CompletelyClosedSweepBucketBoundRetriever boundRetriever;
    private final SweepBucketPointerTable sweepBucketPointerTable;

    public DefaultShardProgressUpdateProposer(ShardProgress shardProgress, BucketProgressStore bucketProgressStore, SweepQueueCleaner sweepQueueCleaner, SweepBucketRecordsTable recordsTable, CompletelyClosedSweepBucketBoundRetriever boundRetriever, SweepBucketPointerTable sweepBucketPointerTable) {
        this.shardProgress = shardProgress;
        this.bucketProgressStore = bucketProgressStore;
        this.sweepQueueCleaner = sweepQueueCleaner;
        this.recordsTable = recordsTable;
        this.boundRetriever = boundRetriever;
        this.sweepBucketPointerTable = sweepBucketPointerTable;
    }

    @Override
    public ShardProgressUpdate proposeUpdate(ShardAndStrategy shardAndStrategy, long currentSweepTimestamp) {
        long lastPossibleSweepEndPoint = getStrictUpperBoundForSweptBuckets(shardAndStrategy);
        long highestCompletedBucket = lastPossibleSweepEndPoint - 1;
        TimestampRange highestCompletedRange = recordsTable.get(shardAndStrategy, highestCompletedBucket);

        long currentShardProgress = shardProgress.getLastSweptTimestamp(shardAndStrategy);
        if (currentShardProgress < highestCompletedRange.startInclusive()) {
            currentShardProgress = highestCompletedRange.endExclusive() - 1;
        }

        long end = lastPossibleSweepEndPoint + 100;
        long finalProgress = currentShardProgress;
        for (long currentBucket = lastPossibleSweepEndPoint; currentBucket < end; currentBucket++) {
            Optional<BucketProgress> bucketProgress = bucketProgressStore.getBucketProgress(Bucket.of(shardAndStrategy, currentBucket));
            long intraBucketProgress = 0;
            if (bucketProgress.isPresent()) {
                BucketProgress presentBucketProgress = bucketProgress.get();
                TimestampRange requiredRange = recordsTable.get(shardAndStrategy, currentBucket);
                if (presentBucketProgress.timestampProgress() == requiredRange.endExclusive() - requiredRange.startInclusive() - 1) {
                    // bucket is completed.
                    // find all fine partitions with buckets ENDING in this range, and clean them.
                    sweepQueueCleaner.cleanQueueMetadata(shardAndStrategy, /* partitions wat */, requiredRange.endExclusive() - 1);
                } else {
                    finalProgress = requiredRange.startInclusive() + presentBucketProgress.timestampProgress();
                    break;
                }
            }
        }

        //

        return null;
    }

    private long getStrictUpperBoundForSweptBuckets(ShardAndStrategy shardAndStrategy) {
        Set<Bucket> startingBuckets = sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(shardAndStrategy));
        if (startingBuckets.isEmpty()) {
            // Every bucket up to the last bucket guaranteed to be closed must have been fully swept.
            return boundRetriever.getStrictUpperBoundForCompletelyClosedBuckets();
        } else {
            return Iterables.getOnlyElement(startingBuckets).bucketIdentifier();
        }
    }
}
