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
import com.palantir.atlasdb.sweep.queue.SweepQueueProgressUpdater;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

public class DefaultShardProgressUpdater implements ShardProgressUpdater {
    private static final long MAX_BUCKETS_TO_CHECK_PER_ITERATION = 100L;

    private final BucketProgressStore bucketProgressStore;
    private final SweepQueueProgressUpdater sweepQueueProgressUpdater;
    private final SweepBucketRecordsTable recordsTable;
    private final CompletelyClosedSweepBucketBoundRetriever boundRetriever;
    private final SweepBucketPointerTable sweepBucketPointerTable;

    public DefaultShardProgressUpdater(BucketProgressStore bucketProgressStore, SweepQueueProgressUpdater sweepQueueProgressUpdater, SweepBucketRecordsTable recordsTable, CompletelyClosedSweepBucketBoundRetriever boundRetriever, SweepBucketPointerTable sweepBucketPointerTable) {
        this.bucketProgressStore = bucketProgressStore;
        this.sweepQueueProgressUpdater = sweepQueueProgressUpdater;
        this.recordsTable = recordsTable;
        this.boundRetriever = boundRetriever;
        this.sweepBucketPointerTable = sweepBucketPointerTable;
    }

    @Override
    public void updateProgress(ShardAndStrategy shardAndStrategy, long currentSweepTimestamp) {
        long bucketPointer = getStrictUpperBoundForSweptBuckets(shardAndStrategy);
        BucketProbe bucketProbe = findFirstUnsweptBucket(shardAndStrategy, bucketPointer);

        for (long bucket = bucketProbe.startInclusive(); bucket < bucketProbe.endExclusive(); bucket++) {
            bucketProgressStore.deleteBucketProgress(Bucket.of(shardAndStrategy, bucket));
        }

        sweepBucketPointerTable.updateStartingBucketForShardAndStrategy(
                Bucket.of(shardAndStrategy, bucketProbe.endExclusive()));
        sweepQueueProgressUpdater.progressTo(shardAndStrategy, bucketProbe.knownSweepProgress());
    }

    private BucketProbe findFirstUnsweptBucket(ShardAndStrategy shardAndStrategy, long searchStart) {
        for (long offset = 0; offset < MAX_BUCKETS_TO_CHECK_PER_ITERATION; offset++) {
            long currentBucket = searchStart + offset;
            Optional<BucketProgress> bucketProgress = bucketProgressStore.getBucketProgress(Bucket.of(shardAndStrategy, currentBucket));
            if (bucketProgress.isPresent()) {
                BucketProgress presentBucketProgress = bucketProgress.get();
                TimestampRange requiredRange = recordsTable.get(shardAndStrategy, currentBucket);
                if (presentBucketProgress.timestampProgress() != requiredRange.endExclusive() - requiredRange.startInclusive() - 1) {
                    // Bucket still has progress to go, so we can stop here.
                    return BucketProbe.builder()
                            .startInclusive(searchStart)
                            .endExclusive(currentBucket)
                            .knownSweepProgress(requiredRange.startInclusive() + presentBucketProgress.timestampProgress())
                            .build();
                } else {
                    // Bucket fully processed, keep going!
                    if (offset == MAX_BUCKETS_TO_CHECK_PER_ITERATION - 1) {
                        // We actually finished a full set of buckets and all were completed.
                        return BucketProbe.builder()
                                .startInclusive(searchStart)
                                .endExclusive(currentBucket + 1)
                                .knownSweepProgress(requiredRange.endExclusive() + 1)
                                .build();
                    }
                }
            } else {
                // No progress; we're ahead of the pointer, so interpret as unstarted. Return where we got
                return BucketProbe.builder()
                        .startInclusive(searchStart)
                        .endExclusive(currentBucket)
                        .knownSweepProgress(recordsTable.get(shardAndStrategy, currentBucket).startInclusive() - 1L)
                        .build();
            }
        }
        throw new SafeIllegalStateException("Didn't expect to get here");
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

    @Value.Immutable
    interface BucketProbe {
        long startInclusive();

        long endExclusive();

        long knownSweepProgress();

        static ImmutableBucketProbe.Builder builder() {
            return ImmutableBucketProbe.builder();
        }
    }
}
