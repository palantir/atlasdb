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

package com.palantir.atlasdb.sweep.asts.progress;

import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.sweep.queue.SweepQueueCleaner;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.atlasdb.sweep.queue.SweepableTimestamps;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;

public class DefaultGlobalSweepProgressUpdatingTask implements GlobalSweepProgressUpdatingTask {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultGlobalSweepProgressUpdatingTask.class);

    private static final int MAX_PARTITIONS_TO_ADVANCE_PROGRESS_PER_ITERATION = 100;

    private final ShardProgress shardProgress;
    private final BucketProgressStore bucketProgressStore;
    private final SweepableTimestamps sweepableTimestamps;
    private final SweepQueueCleaner sweepQueueCleaner;
    private final LongSupplier sweepTimestampSupplier;

    public DefaultGlobalSweepProgressUpdatingTask(
            ShardProgress shardProgress,
            BucketProgressStore bucketProgressStore,
            SweepableTimestamps sweepableTimestamps,
            SweepQueueCleaner sweepQueueCleaner,
            LongSupplier sweepTimestampSupplier) {
        this.shardProgress = shardProgress;
        this.bucketProgressStore = bucketProgressStore;
        this.sweepableTimestamps = sweepableTimestamps;
        this.sweepQueueCleaner = sweepQueueCleaner;
        this.sweepTimestampSupplier = sweepTimestampSupplier;
    }

    @Override
    public void updateProgress(ShardAndStrategy shardAndStrategy) {
        long currentSweepTimestamp = sweepTimestampSupplier.getAsLong();
        long currentTimestampPointer = shardProgress.getLastSweptTimestamp(shardAndStrategy);

        int numPartitionsProcessed = 0;
        boolean shouldContinueProcessing = true;

        Set<Long> completedPartitions = new HashSet<>();

        // TODO (jkong): This is a mouthful.
        while (numPartitionsProcessed < MAX_PARTITIONS_TO_ADVANCE_PROGRESS_PER_ITERATION && shouldContinueProcessing) {
            Optional<Long> nextFinePartition = sweepableTimestamps.nextTimestampPartition(
                    shardAndStrategy, currentTimestampPointer, currentSweepTimestamp);
            if (nextFinePartition.isEmpty()) {
                // No more timestamp partitions!
                shouldContinueProcessing = false;
                currentTimestampPointer = currentSweepTimestamp;
            } else {
                numPartitionsProcessed++;
                long presentNextFinePartition = nextFinePartition.get();
                Optional<BucketProgress> partitionProgress = bucketProgressStore.getBucketProgress(
                        SweepableBucket.of(shardAndStrategy, presentNextFinePartition));
                if (partitionProgress.isEmpty()) {
                    // This partition was not started!
                    shouldContinueProcessing = false;
                } else {
                    BucketProgress presentProgress = partitionProgress.get();
                    boolean complete = presentProgress.timestampOffset() == SweepQueueUtils.TS_FINE_GRANULARITY;
                    if (!complete) {
                        shouldContinueProcessing = false;
                        currentTimestampPointer = SweepQueueUtils.minTsForFinePartition(presentNextFinePartition)
                                + presentProgress.timestampOffset();
                    } else {
                        completedPartitions.add(presentNextFinePartition);
                        currentTimestampPointer = SweepQueueUtils.minTsForFinePartition(presentNextFinePartition + 1);
                    }
                }
            }
        }

        // Deferring to today's cleaner.
        sweepQueueCleaner.backgroundClean(shardAndStrategy, completedPartitions, currentTimestampPointer);
    }
}
