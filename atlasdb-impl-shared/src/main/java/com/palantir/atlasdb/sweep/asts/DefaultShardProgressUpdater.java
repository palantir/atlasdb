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
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.NoSuchElementException;
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
        log.info(
                "Bucket pointer",
                SafeArg.of("bucketPointer", bucketPointer),
                SafeArg.of("shardAndStrategy", shardAndStrategy));
        BucketProbeResult bucketProbeResult = findCompletedBuckets(shardAndStrategy, bucketPointer);
        log.info(
                "Found completed buckets",
                SafeArg.of("endExclusive", bucketProbeResult.endExclusive()),
                SafeArg.of("shardAndStrategy", shardAndStrategy),
                SafeArg.of("knownSweepProgress", bucketProbeResult.knownSweepProgress()));

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
        log.info("Deleted bucket progress where necessary", SafeArg.of("shardAndStrategy", shardAndStrategy));
        sweepBucketPointerTable.updateStartingBucketForShardAndStrategy(
                Bucket.of(shardAndStrategy, bucketProbeResult.endExclusive()));
        log.info("Updated starting bucket", SafeArg.of("shardAndStrategy", shardAndStrategy));
        sweepQueueProgressUpdater.progressTo(shardAndStrategy, bucketProbeResult.knownSweepProgress());
        log.info("recorded new progress", SafeArg.of("shardAndStrategy", shardAndStrategy));
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
            Optional<TimestampRange> record = getTimestampRangeRecord(currentBucket);
            Optional<SweepableBucket> writtenSweepableBucket =
                    sweepBucketsTable.getSweepableBucket(Bucket.of(shardAndStrategy, currentBucket));
            log.info(
                    "Existing bucket progress",
                    SafeArg.of("shardAndStrategy", shardAndStrategy),
                    SafeArg.of("currentBucket", currentBucket),
                    SafeArg.of("bucketProgress", bucketProgress),
                    SafeArg.of("record", record));
            if (record.isPresent()) { // bucket has to have been closed
                TimestampRange presentRecord = record.get();
                // If there's progress, and it's not at the end, then it's incomplete.
                // If there's progress, and it's at the end, it's finished
                // If there's no progress and the sweepable bucket is present, then it's not started
                // If there's no progress and the sweepable bucket is not present, then it's finished
                // (all assuming the record is present, since the record is written at the end)

                if (bucketProgress.isPresent()) {
                    BucketProgress presentBucketProgress = bucketProgress.get();
                    if (presentBucketProgress.timestampProgress()
                            != presentRecord.endExclusive() - presentRecord.startInclusive() - 1) {
                        log.info(
                                "Incomplete bucket",
                                SafeArg.of("shardAndStrategy", shardAndStrategy),
                                SafeArg.of("currentBucket", currentBucket),
                                SafeArg.of(
                                        "knownSweepProgress",
                                        presentRecord.startInclusive() + presentBucketProgress.timestampProgress()));
                        return BucketProbeResult.builder()
                                .endExclusive(currentBucket)
                                .knownSweepProgress(
                                        presentRecord.startInclusive() + presentBucketProgress.timestampProgress())
                                .build();
                    } else {
                        if (offset == MAX_BUCKETS_TO_CHECK_PER_ITERATION - 1) {
                            log.info("max buckets checked");
                            return BucketProbeResult.builder()
                                    .endExclusive(currentBucket + 1)
                                    .knownSweepProgress(presentRecord.endExclusive() + 1)
                                    .build();
                        }
                    }
                } else if (writtenSweepableBucket.isPresent()) {
                    log.info("Unstarted bucket");
                    return BucketProbeResult.builder()
                            .endExclusive(currentBucket)
                            .knownSweepProgress(presentRecord.startInclusive() - 1L)
                            .build();
                } else {
                    if (offset == MAX_BUCKETS_TO_CHECK_PER_ITERATION - 1) {
                        log.info("max buckets checked");
                        return BucketProbeResult.builder()
                                .endExclusive(currentBucket + 1)
                                .knownSweepProgress(presentRecord.endExclusive() + 1)
                                .build();
                    }
                }
            } else {
                log.info("No record found, therefore this is an open bucket");
                // No record; we're possibly in an open bucket, or not created yet.

                // TODO: Do nicely.
                SweepableBucket bucket = writtenSweepableBucket.orElseThrow(() -> new SafeIllegalStateException(
                        "This is likely bucket 0 or starting the next bucket had a transient failure,"
                                + " otherwise the state machine would have opened a new bucket."));

                // No progress, then we haven't started yet.
                if (bucketProgress.isEmpty()) {
                    log.info("No progress found, therefore this is an unstarted open bucket");
                    return BucketProbeResult.builder()
                            .endExclusive(currentBucket)
                            .knownSweepProgress(bucket.timestampRange().startInclusive() - 1L)
                            .build();
                } else {
                    // Progress in the open bucket!
                    BucketProgress presentBucketProgress = bucketProgress.get();
                    return BucketProbeResult.builder()
                            .endExclusive(currentBucket)
                            .knownSweepProgress(bucket.timestampRange().startInclusive()
                                    + presentBucketProgress.timestampProgress())
                            .build();
                }
            }
        }
        throw new SafeIllegalStateException("Didn't expect to get here");
    }

    private Optional<TimestampRange> getTimestampRangeRecord(long queriedBucket) {
        try {
            return Optional.of(recordsTable.getTimestampRangeRecord(queriedBucket));
        } catch (NoSuchElementException exception) {
            return Optional.empty(); // TODO(mdaudali): Note down the guarantees.
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
