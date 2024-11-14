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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketPointerTable;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketRecordsTable;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgress;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepQueueProgressUpdater;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

public class DefaultShardProgressUpdater implements ShardProgressUpdater {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultShardProgressUpdater.class);

    @VisibleForTesting
    static final long MAX_BUCKETS_TO_CHECK_PER_ITERATION = 100L;

    private final BucketProgressStore bucketProgressStore;
    private final SweepQueueProgressUpdater sweepQueueProgressUpdater;
    private final SweepBucketRecordsTable recordsTable;
    private final SweepBucketPointerTable sweepBucketPointerTable;

    public DefaultShardProgressUpdater(
            BucketProgressStore bucketProgressStore,
            SweepQueueProgressUpdater sweepQueueProgressUpdater,
            SweepBucketRecordsTable recordsTable,
            SweepBucketPointerTable sweepBucketPointerTable) {
        this.bucketProgressStore = bucketProgressStore;
        this.sweepQueueProgressUpdater = sweepQueueProgressUpdater;
        this.recordsTable = recordsTable;
        this.sweepBucketPointerTable = sweepBucketPointerTable;
    }

    @Override
    public void updateProgress(ShardAndStrategy shardAndStrategy) {
        long bucketPointer = getStrictUpperBoundForSweptBuckets(shardAndStrategy);
        BucketProbeResult bucketProbeResult = findCompletedBuckets(shardAndStrategy, bucketPointer);

        // This order of clearing the metadata is intentional:
        // (1) if bucket progress is deleted but the pointer is not updated, we might sweep the relevant buckets
        //     again, but that is acceptable because sweepable cells and timestamps were already cleared, and
        //     these tables are not accessed via row range scans, so the number of tombstones we read will be
        //     reasonably bounded.
        // (2) if the pointer is updated but progress is not, we will update progress to the right value on the
        //     next iteration (notice that we only use the pointer, and not the existing progress, to track where
        //     we are in the timeline).
        for (long bucket = bucketPointer; bucket < bucketProbeResult.endExclusive(); bucket++) {
            bucketProgressStore.deleteBucketProgress(Bucket.of(shardAndStrategy, bucket));
        }
        sweepBucketPointerTable.updateStartingBucketForShardAndStrategy(
                Bucket.of(shardAndStrategy, bucketProbeResult.endExclusive()));
        sweepQueueProgressUpdater.progressTo(shardAndStrategy, bucketProbeResult.knownSweepProgress());
    }

    /**
     * Returns a {@link BucketProbeResult} indicating a prefix of cells that have been swept successfully, starting
     * from a given point in time. It is assumed that all buckets before searchStart have been swept successfully;
     * if this is not the case, behaviour is undefined.
     */
    private BucketProbeResult findCompletedBuckets(ShardAndStrategy shardAndStrategy, long searchStart) {
        for (long offset = 0; offset < MAX_BUCKETS_TO_CHECK_PER_ITERATION; offset++) {
            long currentBucket = searchStart + offset;
            Optional<BucketProgress> bucketProgress =
                    bucketProgressStore.getBucketProgress(Bucket.of(shardAndStrategy, currentBucket));
            if (bucketProgress.isPresent()) {
                BucketProgress presentBucketProgress = bucketProgress.get();
                TimestampRange requiredRange = getTimestampRangeRecord(currentBucket);
                if (presentBucketProgress.timestampProgress()
                        != requiredRange.endExclusive() - requiredRange.startInclusive() - 1) {
                    // Bucket still has progress to go, so we can stop here.
                    return BucketProbeResult.builder()
                            .endExclusive(currentBucket)
                            .knownSweepProgress(
                                    requiredRange.startInclusive() + presentBucketProgress.timestampProgress())
                            .build();
                } else {
                    // Bucket fully processed, keep going.
                    if (offset == MAX_BUCKETS_TO_CHECK_PER_ITERATION - 1) {
                        // We finished the maximum number of buckets to check, and all were completed.
                        return BucketProbeResult.builder()
                                .endExclusive(currentBucket + 1)
                                .knownSweepProgress(requiredRange.endExclusive() + 1)
                                .build();
                    }
                }
            } else {
                // No progress; we're ahead of the read pointer, so interpret as unstarted.
                return BucketProbeResult.builder()
                        .endExclusive(currentBucket)
                        .knownSweepProgress(
                                getTimestampRangeRecord(currentBucket).startInclusive() - 1L)
                        .build();
            }
        }
        throw new SafeIllegalStateException("Didn't expect to get here");
    }

    // TODO(mdaudali): This method is still incorrect (a record does not exist for an open bucket, not just pre-init
    //  bucket 0). A follow up PR will address this.
    private TimestampRange getTimestampRangeRecord(long queriedBucket) {
        return recordsTable
                .getTimestampRangeRecord(queriedBucket)
                .orElseThrow(() -> new SafeIllegalStateException(
                        "Timestamp range record not found. If this has happened for bucket 0, this is possible when"
                            + " autoscaling sweep is initializing itself. Otherwise, this is potentially indicative of"
                            + " a bug in auto-scaling sweep. In either case, we will retry.",
                        SafeArg.of("queriedBucket", queriedBucket)));
    }

    private long getStrictUpperBoundForSweptBuckets(ShardAndStrategy shardAndStrategy) {
        Set<Bucket> startingBuckets =
                sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(shardAndStrategy));
        return Iterables.getOnlyElement(startingBuckets).bucketIdentifier();
    }

    @Value.Immutable
    interface BucketProbeResult {
        /**
         * Buckets from the starting point of the probe to this bucket, exclusive, have been fully processed.
         * It is possible for this to be equal to the initial bucket, meaning that the initial bucket itself
         * has not been fully processed.
         */
        long endExclusive();

        /**
         * AtlasDB logical time to which sweep is known to have progressed (possibly within the endExclusive bucket).
         */
        long knownSweepProgress();

        static ImmutableBucketProbeResult.Builder builder() {
            return ImmutableBucketProbeResult.builder();
        }
    }
}
