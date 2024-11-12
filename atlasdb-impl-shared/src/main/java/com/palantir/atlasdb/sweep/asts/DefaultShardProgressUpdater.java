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
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketsTable;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgress;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepQueueProgressUpdater;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
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
    private final SweepBucketsTable sweepBucketsTable;
    private final SweepBucketPointerTable sweepBucketPointerTable;

    public DefaultShardProgressUpdater(
            BucketProgressStore bucketProgressStore,
            SweepQueueProgressUpdater sweepQueueProgressUpdater,
            SweepBucketRecordsTable recordsTable,
            SweepBucketsTable sweepBucketsTable,
            SweepBucketPointerTable sweepBucketPointerTable) {
        this.bucketProgressStore = bucketProgressStore;
        this.sweepQueueProgressUpdater = sweepQueueProgressUpdater;
        this.recordsTable = recordsTable;
        this.sweepBucketsTable = sweepBucketsTable;
        this.sweepBucketPointerTable = sweepBucketPointerTable;
    }

    @Override
    public void updateProgress(ShardAndStrategy shardAndStrategy) {
        long bucketPointer = getStrictUpperBoundForSweptBuckets(shardAndStrategy);
        BucketProbeResult bucketProbeResult = findCompletedBuckets(shardAndStrategy, bucketPointer);

        // This order of clearing the metadata is intentional:
        // if the pointer is updated but progress is not, we will update progress to the right value on the
        // next iteration (notice that we only use the pointer, and not the existing progress, to track where
        // we are in the timeline).
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
        long lastEndExclusiveForCompleteBucket = -1;
        for (long offset = 0; offset < MAX_BUCKETS_TO_CHECK_PER_ITERATION; offset++) {
            long currentBucket = searchStart + offset;
            Optional<BucketProgress> bucketProgress =
                    bucketProgressStore.getBucketProgress(Bucket.of(shardAndStrategy, currentBucket));
            Optional<TimestampRange> record = recordsTable.getTimestampRangeRecord(currentBucket);
            Optional<SweepableBucket> writtenSweepableBucket =
                    sweepBucketsTable.getSweepableBucket(Bucket.of(shardAndStrategy, currentBucket));

            if (record.isPresent()) { // bucket has to have been closed
                Optional<BucketProbeResult> definitiveProbeResult =
                        getProbeResultForPotentiallyPartiallyCompleteClosedBucket(
                                record.orElseThrow(), bucketProgress, writtenSweepableBucket, currentBucket);
                if (definitiveProbeResult.isPresent()) {
                    return definitiveProbeResult.get();
                } else {
                    lastEndExclusiveForCompleteBucket = record.get().endExclusive();
                }
            } else {
                // No record; we're possibly in an open bucket, or not created yet.

                if (writtenSweepableBucket.isEmpty()) {
                    // there's no open bucket. This should be rare - it'll happen if the bucket assigner failed to open
                    // the next bucket after closing the last, of it the bucket assigner is behind and hit the cap
                    // creating closed buckets.

                    if (lastEndExclusiveForCompleteBucket == -1) {
                        throw new SafeRuntimeException(
                                "Failed to update shard progress as there are no buckets to"
                                        + " check sweep's progress. This should be rare and transient. If this error"
                                        + " occurs for more than 15 minutes and there are no new buckets being created,"
                                        + " it is likely that there is something preventing the bucket assigner"
                                        + " from progressing.",
                                SafeArg.of("shardAndStrategy", shardAndStrategy),
                                SafeArg.of("currentBucket", currentBucket));
                    }

                    return BucketProbeResult.builder()
                            .endExclusive(currentBucket)
                            .knownSweepProgress(lastEndExclusiveForCompleteBucket - 1)
                            .build();
                }
                SweepableBucket bucket = writtenSweepableBucket.orElseThrow();
                return getProbeResultForOpenBucket(bucketProgress, bucket, currentBucket);
            }
        }

        if (lastEndExclusiveForCompleteBucket == -1) {
            throw new SafeIllegalStateException("Didn't expect to get here");
        }
        return BucketProbeResult.builder()
                .endExclusive(searchStart + MAX_BUCKETS_TO_CHECK_PER_ITERATION)
                .knownSweepProgress(lastEndExclusiveForCompleteBucket - 1)
                .build();
    }

    private Optional<BucketProbeResult> getProbeResultForPotentiallyPartiallyCompleteClosedBucket(
            TimestampRange presentRecord,
            Optional<BucketProgress> bucketProgress,
            Optional<SweepableBucket> writtenSweepableBucket,
            long currentBucket) {
        // If there's progress, and it's not at the end, then it's incomplete.
        // If there's progress, and it's at the end, it's finished
        // If there's no progress and the sweepable bucket is present, then it's not started
        // If there's no progress and the sweepable bucket is not present, then it's finished
        // (all assuming the record is present, since the record is written at the end)
        if (bucketProgress.isPresent()) {
            BucketProgress presentBucketProgress = bucketProgress.get();
            if (presentBucketProgress.timestampProgress()
                    != presentRecord.endExclusive() - presentRecord.startInclusive() - 1) {
                // Progress, but not at the end
                return Optional.of(BucketProbeResult.builder()
                        .endExclusive(currentBucket)
                        .knownSweepProgress(presentRecord.startInclusive() + presentBucketProgress.timestampProgress())
                        .build());
            } else {
                // progress and it's at the end (perhaps we caught the foreground task between updating
                /// progress, and updating the record)
                return Optional.empty();
            }
        } else if (writtenSweepableBucket.isPresent()) {
            // no progress, record present _and_ sweepable bucket entry present implies we're unstarted
            return Optional.of(BucketProbeResult.builder()
                    .endExclusive(currentBucket)
                    .knownSweepProgress(presentRecord.startInclusive() - 1L)
                    .build());
        } else {
            // no progress and the sweepable bucket is not present, so it's finished.
            return Optional.empty();
        }
    }

    private BucketProbeResult getProbeResultForOpenBucket(
            Optional<BucketProgress> bucketProgress, SweepableBucket bucket, long currentBucket) {
        // No progress, then we haven't started yet.
        if (bucketProgress.isEmpty()) {
            return BucketProbeResult.builder()
                    .endExclusive(currentBucket)
                    .knownSweepProgress(bucket.timestampRange().startInclusive() - 1L)
                    .build();
        } else {
            // Progress in the open bucket!
            BucketProgress presentBucketProgress = bucketProgress.get();
            return BucketProbeResult.builder()
                    .endExclusive(currentBucket)
                    .knownSweepProgress(
                            bucket.timestampRange().startInclusive() + presentBucketProgress.timestampProgress())
                    .build();
        }
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
