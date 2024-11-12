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

import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketCompletionListener;
import com.palantir.atlasdb.sweep.asts.bucketingthings.CompletelyClosedSweepBucketBoundRetriever;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgress;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.metrics.SweepOutcome;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;
import com.palantir.atlasdb.sweep.queue.SweepBatch;
import com.palantir.atlasdb.sweep.queue.SweepBatchWithPartitionInfo;
import com.palantir.atlasdb.sweep.queue.SweepQueueCleaner;
import com.palantir.atlasdb.sweep.queue.SweepQueueDeleter;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;

public class DefaultSingleBucketSweepTask implements SingleBucketSweepTask {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultSingleBucketSweepTask.class);

    private final BucketProgressStore bucketProgressStore;
    private final SweepQueueReader sweepQueueReader;
    private final SweepQueueDeleter sweepQueueDeleter;
    private final SweepQueueCleaner sweepQueueCleaner;
    private final SpecialTimestampsSupplier specialTimestampsSupplier;
    private final TargetedSweepMetrics targetedSweepMetrics;
    private final BucketCompletionListener bucketCompletionListener;
    private final CompletelyClosedSweepBucketBoundRetriever completelyClosedSweepBucketBoundRetriever;

    public DefaultSingleBucketSweepTask(
            BucketProgressStore bucketProgressStore,
            SweepQueueReader sweepQueueReader,
            SweepQueueDeleter sweepQueueDeleter,
            SweepQueueCleaner sweepQueueCleaner,
            SpecialTimestampsSupplier specialTimestampsSupplier,
            TargetedSweepMetrics targetedSweepMetrics,
            BucketCompletionListener bucketCompletionListener,
            CompletelyClosedSweepBucketBoundRetriever completelyClosedSweepBucketBoundRetriever) {
        this.bucketProgressStore = bucketProgressStore;
        this.sweepQueueReader = sweepQueueReader;
        this.sweepQueueDeleter = sweepQueueDeleter;
        this.sweepQueueCleaner = sweepQueueCleaner;
        this.specialTimestampsSupplier = specialTimestampsSupplier;
        this.targetedSweepMetrics = targetedSweepMetrics;
        this.bucketCompletionListener = bucketCompletionListener;
        this.completelyClosedSweepBucketBoundRetriever = completelyClosedSweepBucketBoundRetriever;
    }

    @Override
    public long runOneIteration(SweepableBucket sweepableBucket) {
        long sweepTimestampForIteration =
                Sweeper.of(sweepableBucket.bucket().shardAndStrategy()).getSweepTimestamp(specialTimestampsSupplier);
        long bucketStartTimestamp = sweepableBucket.timestampRange().startInclusive();

        // TODO(mdaudali): we do need a way to report back to the coordinator about what happened when we tried to
        //  sweep, so that the autoscaler can adjust as necessary and we can centralise metric reporting.
        if (sweepTimestampForIteration <= bucketStartTimestamp) {
            // This means that the sweep timestamp has not entered this partition yet, so we do not need to process
            // anything. Note that sweep timestamps are exclusive, so <= is correct.
            targetedSweepMetrics.registerOccurrenceOf(
                    sweepableBucket.bucket().shardAndStrategy(), SweepOutcome.NOTHING_TO_SWEEP);
            return 0L;
        }

        BucketProgress existingBucketProgress =
                bucketProgressStore.getBucketProgress(sweepableBucket.bucket()).orElse(BucketProgress.INITIAL_PROGRESS);
        log.info(
                "Existing bucket progress",
                SafeArg.of("bucket", sweepableBucket.bucket()),
                SafeArg.of("progress", existingBucketProgress));

        // This is inclusive.
        long lastSweptTimestampInBucket =
                sweepableBucket.timestampRange().startInclusive() + existingBucketProgress.timestampProgress();
        if (isCompletelySwept(sweepableBucket.timestampRange().endExclusive(), lastSweptTimestampInBucket)) {
            // The bucket is fully swept; it might still be returned here if we thought it was a candidate, or if
            // the bucket state machine is still doing things
            markBucketCompleteIfEligible(sweepableBucket);
            targetedSweepMetrics.registerOccurrenceOf(
                    sweepableBucket.bucket().shardAndStrategy(), SweepOutcome.NOTHING_TO_SWEEP);
            return 0L;
        }

        if (sweepTimestampForIteration <= lastSweptTimestampInBucket) {
            // The sweep timestamp has made progress in this partition, but we've swept everything up to it.
            targetedSweepMetrics.registerOccurrenceOf(
                    sweepableBucket.bucket().shardAndStrategy(), SweepOutcome.NOTHING_TO_SWEEP);
            return 0L;
        }

        try {
            return sweepBucket(sweepableBucket, lastSweptTimestampInBucket, sweepTimestampForIteration);
        } catch (InsufficientConsistencyException e) {
            targetedSweepMetrics.registerOccurrenceOf(
                    sweepableBucket.bucket().shardAndStrategy(), SweepOutcome.NOT_ENOUGH_DB_NODES_ONLINE);
            log.warn(
                    "Targeted sweep for {} failed and will be retried later.",
                    SafeArg.of("bucket", sweepableBucket),
                    e);
            return 0L;
        } catch (Throwable e) {
            targetedSweepMetrics.registerOccurrenceOf(sweepableBucket.bucket().shardAndStrategy(), SweepOutcome.ERROR);
            log.warn(
                    "Targeted sweep for {} failed and will be retried later.",
                    SafeArg.of("bucket", sweepableBucket),
                    e);
            return 0L;
        }
    }

    private long sweepBucket(
            SweepableBucket sweepableBucket, long lastSweptTimestampInBucket, long sweepTimestampForIteration) {
        // TODO (jkong): Make use of the partial progress within a timestamp.
        SweepBatchWithPartitionInfo sweepBatchWithPartitionInfo = sweepQueueReader.getNextBatchToSweep(
                sweepableBucket.bucket().shardAndStrategy(),
                lastSweptTimestampInBucket,
                getMaxExclusiveProcessableStartTimestamp(sweepableBucket, sweepTimestampForIteration),
                sweepTimestampForIteration);
        SweepBatch sweepBatch = sweepBatchWithPartitionInfo.sweepBatch();

        ShardAndStrategy shardAndStrategy = sweepableBucket.bucket().shardAndStrategy();
        sweepQueueDeleter.sweep(sweepBatch.writes(), Sweeper.of(shardAndStrategy));
        targetedSweepMetrics.registerEntriesReadInBatch(shardAndStrategy, sweepBatch.entriesRead());

        if (!sweepBatch.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Put {} ranged tombstones and swept up to timestamp {} for {}.",
                        SafeArg.of("tombstones", sweepBatch.writes().size()),
                        SafeArg.of("lastSweptTs", sweepBatch.lastSweptTimestamp()),
                        SafeArg.of("shardStrategy", shardAndStrategy.toText()));
            }
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

        log.info(
                "Updating bucket progress",
                SafeArg.of("bucket", sweepableBucket.bucket()),
                SafeArg.of("progress", lastTsOffset));
        bucketProgressStore.updateBucketProgressToAtLeast(
                sweepableBucket.bucket(), BucketProgress.createForTimestampProgress(lastTsOffset));

        if (isCompletelySwept(sweepableBucket.timestampRange().endExclusive(), lastTs)) {
            // we've finished the bucket!
            markBucketCompleteIfEligible(sweepableBucket);
        }

        targetedSweepMetrics.updateNumberOfTombstones(
                shardAndStrategy, sweepBatch.writes().size());

        if (sweepBatch.isEmpty()) {
            targetedSweepMetrics.registerOccurrenceOf(shardAndStrategy, SweepOutcome.NOTHING_TO_SWEEP);
        } else {
            targetedSweepMetrics.registerOccurrenceOf(shardAndStrategy, SweepOutcome.SUCCESS);
        }

        // No updating of overall progress; that's a responsibility of the background updating task
        return sweepBatch.entriesRead();
    }

    private void markBucketCompleteIfEligible(SweepableBucket sweepableBucket) {
        if (sweepableBucket.bucket().bucketIdentifier()
                < completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets()) {
            bucketCompletionListener.markBucketCompleteAndRemoveFromScheduling(sweepableBucket.bucket());
        }
    }

    private static long getMaxExclusiveProcessableStartTimestamp(SweepableBucket sweepableBucket, long sweepTimestamp) {
        if (sweepableBucket.timestampRange().endExclusive() == -1) {
            return sweepTimestamp;
        }
        return Math.min(sweepTimestamp, sweepableBucket.timestampRange().endExclusive());
    }

    private static boolean isCompletelySwept(long rangeEndExclusive, long lastSweptTimestampInBucket) {
        if (rangeEndExclusive == -1) {
            // The bucket's not complete, in which case it is not completely swept.
            return false;
        }
        return lastSweptTimestampInBucket >= rangeEndExclusive - 1;
    }
}
