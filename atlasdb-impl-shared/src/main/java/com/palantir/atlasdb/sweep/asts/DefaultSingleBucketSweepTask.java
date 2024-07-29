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
import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgress;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.metrics.SweepOutcome;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepBatch;
import com.palantir.atlasdb.sweep.queue.SweepBatchWithPartitionInfo;
import com.palantir.atlasdb.sweep.queue.SweepQueueCleaner;
import com.palantir.atlasdb.sweep.queue.SweepQueueDeleter;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
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

    public DefaultSingleBucketSweepTask(
            BucketProgressStore bucketProgressStore,
            SweepQueueReader sweepQueueReader,
            SweepQueueDeleter sweepQueueDeleter,
            SweepQueueCleaner sweepQueueCleaner,
            LongSupplier sweepTimestampSupplier,
            TargetedSweepMetrics targetedSweepMetrics) {
        this.bucketProgressStore = bucketProgressStore;
        this.sweepQueueReader = sweepQueueReader;
        this.sweepQueueDeleter = sweepQueueDeleter;
        this.sweepQueueCleaner = sweepQueueCleaner;
        this.sweepTimestampSupplier = sweepTimestampSupplier;
        this.targetedSweepMetrics = targetedSweepMetrics;
    }

    @Override
    public long runOneIteration(SweepableBucket sweepableBucket) {
        long sweepTimestampForIteration = sweepTimestampSupplier.getAsLong();
        long minTimestampForPartition = sweepableBucket.bucketIdentifier() * SweepQueueUtils.TS_FINE_GRANULARITY;
        if (sweepTimestampForIteration <= minTimestampForPartition) {
            // This means that the sweep timestamp has not entered this partition yet, so we do not need to process
            // anything. Note that sweep timestamps are exclusive, so <= is correct.
            return 0L;
        }

        BucketProgress existingBucketProgress =
                bucketProgressStore.getBucketProgress(sweepableBucket).orElse(BucketProgress.ZERO);
        long startTimestampForProgress = minTimestampForPartition + existingBucketProgress.timestampOffset();
        if (sweepTimestampForIteration <= startTimestampForProgress) {
            // There is nothing to sweep in this case too, though this is done as a separate check, because retrieving
            // timestamp progress requires a database read.
            return 0L;
        } else if (existingBucketProgress.timestampOffset() == SweepQueueUtils.TS_FINE_GRANULARITY) {
            // The bucket has been fully swept. It might still be returned here if we accidentally thought it was a
            // candidate. In any case, there is no work to be done here: this bucket is done.
            return 0L;
        }

        long maxTimestampForPartitionExclusive = minTimestampForPartition + SweepQueueUtils.TS_FINE_GRANULARITY;
        long partitionEndExclusive = Math.min(sweepTimestampForIteration, maxTimestampForPartitionExclusive);

        // TODO (jkong): Don't just use this, it does a lot of unnecessary stuff
        SweepBatchWithPartitionInfo sweepBatchWithPartitionInfo = sweepQueueReader.getNextBatchToSweep(
                sweepableBucket.shardAndStrategy(),
                startTimestampForProgress - 1, // startTimestampForProgress must still be swept
                partitionEndExclusive);
        SweepBatch sweepBatch = sweepBatchWithPartitionInfo.sweepBatch();

        ShardAndStrategy shardAndStrategy = sweepableBucket.shardAndStrategy();
        sweepQueueDeleter.sweep(sweepBatch.writes(), Sweeper.of(shardAndStrategy));
        targetedSweepMetrics.registerEntriesReadInBatch(shardAndStrategy, sweepBatch.entriesRead());

        if (!sweepBatch.isEmpty()) {
            log.debug(
                    "Put {} ranged tombstones and swept up to timestamp {} for {}.",
                    SafeArg.of("tombstones", sweepBatch.writes().size()),
                    SafeArg.of("lastSweptTs", sweepBatch.lastSweptTimestamp()),
                    SafeArg.of("shardStrategy", shardAndStrategy.toText()));
        }

        // Cleaning is now handled by someone else...
        // TODO (jkong): This is a bit wonky. NOTHING_TO_SWEEP means a given BUCKET is done, but not everything!
        if (sweepBatch.isEmpty()) {
            targetedSweepMetrics.registerOccurrenceOf(shardAndStrategy, SweepOutcome.NOTHING_TO_SWEEP);
        } else {
            targetedSweepMetrics.registerOccurrenceOf(shardAndStrategy, SweepOutcome.SUCCESS);
        }

        // New: Update per bucket progress
        long lastTs = sweepBatch.lastSweptTimestamp();
        long lastTsOffset = lastTs - minTimestampForPartition + 1;
        bucketProgressStore.updateBucketProgressToAtLeast(
                sweepableBucket, BucketProgress.createForTimestampOffset(lastTsOffset));

        // If I am completed, delete stuff for my partition
        if (lastTsOffset == SweepQueueUtils.TS_FINE_GRANULARITY) {
            sweepQueueCleaner.foregroundClean(
                    shardAndStrategy,
                    sweepBatchWithPartitionInfo.finePartitions(),
                    sweepBatchWithPartitionInfo.sweepBatch().dedicatedRows());
        }

        // No updating of overall progress. That is a responsibility of the background progress updating task

        return sweepBatch.entriesRead();
    }
}
