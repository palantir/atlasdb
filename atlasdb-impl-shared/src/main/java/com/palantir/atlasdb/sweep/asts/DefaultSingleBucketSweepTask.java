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

import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketCompletionListener;
import com.palantir.atlasdb.sweep.asts.bucketingthings.CompletelyClosedSweepBucketBoundRetriever;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgress;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepBatch;
import com.palantir.atlasdb.sweep.queue.SweepBatchWithPartitionInfo;
import com.palantir.atlasdb.sweep.queue.SweepQueueCleaner;
import com.palantir.atlasdb.sweep.queue.SweepQueueDeleter;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.function.LongSupplier;

public class DefaultSingleBucketSweepTask implements SingleBucketSweepTask {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultSingleBucketSweepTask.class);

    private final BucketProgressStore bucketProgressStore;
    private final SweepQueueReader sweepQueueReader;
    private final SweepQueueDeleter sweepQueueDeleter;
    private final SweepQueueCleaner sweepQueueCleaner;
    private final LongSupplier sweepTimestampSupplier;
    private final TargetedSweepMetrics targetedSweepMetrics;
    private final BucketCompletionListener bucketCompletionListener;
    private final CompletelyClosedSweepBucketBoundRetriever completelyClosedSweepBucketBoundRetriever;

    public DefaultSingleBucketSweepTask(
            BucketProgressStore bucketProgressStore,
            SweepQueueReader sweepQueueReader,
            SweepQueueDeleter sweepQueueDeleter,
            SweepQueueCleaner sweepQueueCleaner,
            LongSupplier sweepTimestampSupplier,
            TargetedSweepMetrics targetedSweepMetrics,
            BucketCompletionListener bucketCompletionListener,
            CompletelyClosedSweepBucketBoundRetriever completelyClosedSweepBucketBoundRetriever) {
        this.bucketProgressStore = bucketProgressStore;
        this.sweepQueueReader = sweepQueueReader;
        this.sweepQueueDeleter = sweepQueueDeleter;
        this.sweepQueueCleaner = sweepQueueCleaner;
        this.sweepTimestampSupplier = sweepTimestampSupplier;
        this.targetedSweepMetrics = targetedSweepMetrics;
        this.bucketCompletionListener = bucketCompletionListener;
        this.completelyClosedSweepBucketBoundRetriever = completelyClosedSweepBucketBoundRetriever;
    }

    @Override
    public long runOneIteration(SweepableBucket sweepableBucket) {
        long sweepTimestampForIteration = sweepTimestampSupplier.getAsLong();
        long bucketStartTimestamp = sweepableBucket.timestampRange().startInclusive();
        if (sweepTimestampForIteration <= bucketStartTimestamp) {
            // This means that the sweep timestamp has not entered this partition yet, so we do not need to process
            // anything. Note that sweep timestamps are exclusive, so <= is correct.
            return 0L;
        }

        BucketProgress existingBucketProgress =
                bucketProgressStore.getBucketProgress(sweepableBucket.bucket()).orElse(BucketProgress.INITIAL_PROGRESS);

        // This is inclusive.
        long lastSweptTimestampInBucket =
                sweepableBucket.timestampRange().startInclusive() + existingBucketProgress.timestampProgress();
        if (isCompletelySwept(sweepableBucket.timestampRange().endExclusive(), lastSweptTimestampInBucket)) {
            // The bucket is fully swept; it might still be returned here if we thought it was a candidate, or if
            // the bucket state machine is still doing things
            markBucketCompleteIfEligible(sweepableBucket);
            return 0L;
        }

        if (sweepTimestampForIteration <= lastSweptTimestampInBucket) {
            // The sweep timestamp has made progress in this partition, but we've swept everything up to it.
            return 0L;
        }

        // TODO (jkong): Make use of the partial progress within a timestamp.
        SweepBatchWithPartitionInfo sweepBatchWithPartitionInfo = sweepQueueReader.getNextBatchToSweep(
                sweepableBucket.bucket().shardAndStrategy(),
                lastSweptTimestampInBucket,
                getEndOfSweepRange(sweepableBucket, sweepTimestampForIteration));
        SweepBatch sweepBatch = sweepBatchWithPartitionInfo.sweepBatch();

        ShardAndStrategy shardAndStrategy = sweepableBucket.bucket().shardAndStrategy();
        sweepQueueDeleter.sweep(sweepBatch.writes(), Sweeper.of(shardAndStrategy));
        targetedSweepMetrics.registerEntriesReadInBatch(shardAndStrategy, sweepBatch.entriesRead());

        if (!sweepBatch.isEmpty()) {
<<<<<<< HEAD
            log.debug(
                    "Put {} ranged tombstones and swept up to timestamp {} for {}.",
                    SafeArg.of("tombstones", sweepBatch.writes().size()),
                    SafeArg.of("lastSweptTs", sweepBatch.lastSweptTimestamp()),
                    SafeArg.of("shardStrategy", shardAndStrategy.toText()));
=======
            if (log.isDebugEnabled()) {
                log.debug(
                        "Put {} ranged tombstones and swept up to timestamp {} for {}.",
                        SafeArg.of("tombstones", sweepBatch.writes().size()),
                        SafeArg.of("lastSweptTs", sweepBatch.lastSweptTimestamp()),
                        SafeArg.of("shardStrategy", shardAndStrategy.toText()));
            }
>>>>>>> develop
        }

        long lastTs = sweepBatch.lastSweptTimestamp();
        sweepQueueCleaner.clean(
                shardAndStrategy,
                sweepBatchWithPartitionInfo.partitionsForPreviousLastSweptTsWithMinimumBound(
                        lastSweptTimestampInBucket,
                        sweepableBucket.timestampRange().startInclusive()),
                lastTs,
                sweepBatch.dedicatedRows());

        long lastTsOffset = lastTs - sweepableBucket.timestampRange().startInclusive();

        bucketProgressStore.updateBucketProgressToAtLeast(
                sweepableBucket.bucket(), BucketProgress.createForTimestampProgress(lastTsOffset));
        if (isCompletelySwept(sweepableBucket.timestampRange().endExclusive(), lastTs)) {
            // we've finished the bucket!
            markBucketCompleteIfEligible(sweepableBucket);
        }

<<<<<<< HEAD
=======
        targetedSweepMetrics.updateNumberOfTombstones(
                shardAndStrategy, sweepBatch.writes().size());

>>>>>>> develop
        // No updating of overall progress; that's a responsibility of the background updating task
        return sweepBatch.entriesRead();
    }

    private void markBucketCompleteIfEligible(SweepableBucket sweepableBucket) {
        if (sweepableBucket.bucket().bucketIdentifier()
                < completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets()) {
            bucketCompletionListener.markBucketCompleteAndRemoveFromScheduling(sweepableBucket.bucket());
        }
    }

    private static long getEndOfSweepRange(SweepableBucket sweepableBucket, long sweepTimestampForIteration) {
        if (sweepableBucket.timestampRange().endExclusive() == -1) {
            return sweepTimestampForIteration;
        }
        return Math.min(
                sweepTimestampForIteration, sweepableBucket.timestampRange().endExclusive());
    }

    private static boolean isCompletelySwept(long rangeEndExclusive, long lastSweptTimestampInBucket) {
        if (rangeEndExclusive == -1) {
            // The bucket's not complete, in which case it is not completely swept.
            return false;
        }
        return lastSweptTimestampInBucket >= rangeEndExclusive - 1;
    }
}
