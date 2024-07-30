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
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.atlasdb.sweep.queue.SweepableTimestamps;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class DefaultShardProgressUpdateProposer implements ShardProgressUpdateProposer {
    // At perhaps 5 millis per call to sweepableTimestamps#nextTimestampPartition, this will mean an iteration of
    // this call will take half a second.
    private static final int MAX_PARTITIONS_TO_ADVANCE_PROGRESS_PER_ITERATION = 100;

    private final ShardAndStrategy shardAndStrategy;
    private final ShardProgress shardProgress;
    private final BucketProgressStore bucketProgressStore;
    private final SweepableTimestamps sweepableTimestamps;

    public DefaultShardProgressUpdateProposer(
            ShardAndStrategy shardAndStrategy,
            ShardProgress shardProgress,
            BucketProgressStore bucketProgressStore,
            SweepableTimestamps sweepableTimestamps) {
        this.shardAndStrategy = shardAndStrategy;
        this.shardProgress = shardProgress;
        this.bucketProgressStore = bucketProgressStore;
        this.sweepableTimestamps = sweepableTimestamps;
    }

    @Override
    public ShardProgressUpdate proposeUpdate(long currentSweepTimestamp) {
        long currentTimestampPointer = shardProgress.getLastSweptTimestamp(shardAndStrategy);

        int numPartitionsProcessed = 0;
        Set<Long> completedPartitions = new HashSet<>();

        // TODO (jkong): This is a mouthful.
        while (numPartitionsProcessed < MAX_PARTITIONS_TO_ADVANCE_PROGRESS_PER_ITERATION) {
            Optional<Long> nextFinePartition = sweepableTimestamps.nextTimestampPartition(
                    shardAndStrategy, currentTimestampPointer, currentSweepTimestamp);
            if (nextFinePartition.isEmpty()) {
                // No more timestamp partitions in our range.
                return ShardProgressUpdate.builder()
                        .completedFinePartitions(completedPartitions)
                        .progressTimestamp(currentSweepTimestamp)
                        .build();
            } else {
                numPartitionsProcessed++;
                long presentNextFinePartition = nextFinePartition.get();
                Optional<BucketProgress> partitionProgress = bucketProgressStore.getBucketProgress(
                        SweepableBucket.of(shardAndStrategy, presentNextFinePartition));
                if (partitionProgress.isEmpty()) {
                    // This partition was not started. In most cases, we can say that we stopped at the end of the
                    // previous partition safely. However, I'm not convinced we want to rewind the sweep progress if
                    // we're already ahead of the start (e.g., because our last update might have ended on this
                    // partition and there might have been nothing, so we might have set sweep timestamp inside this
                    // partition).
                    return ShardProgressUpdate.builder()
                            .completedFinePartitions(completedPartitions)
                            .progressTimestamp(Math.max(
                                    SweepQueueUtils.maxTsForFinePartition(presentNextFinePartition - 1),
                                    currentTimestampPointer))
                            .build();
                } else {
                    BucketProgress presentProgress = partitionProgress.get();
                    if (!presentProgress.isBucketCompletelySwept()) {
                        // As in the empty case, we do not want to rewind the sweep progress if we're already
                        // ahead of the start. This seems less likely, but I don't want to rule out us writing
                        // a zero marker or something to that effect.
                        return ShardProgressUpdate.builder()
                                .completedFinePartitions(completedPartitions)
                                .progressTimestamp(Math.max(
                                        SweepQueueUtils.minTsForFinePartition(presentNextFinePartition)
                                                + presentProgress.timestampOffset(),
                                        currentTimestampPointer))
                                .build();
                    } else {
                        completedPartitions.add(presentNextFinePartition);
                        currentTimestampPointer = SweepQueueUtils.minTsForFinePartition(presentNextFinePartition + 1);
                    }
                }
            }
        }

        // We finished the max number of iterations, and identified MAX_PARTITIONS_TO_ADVANCE partitions.
        // We have to be careful here: currentTimestampPointer tells us where we would start our search for the next
        // partition. But, we don't actually know that that timestamp is processed.
        // We could just do currentTimestampPointer - 1, though that feels a bit too fragile for my liking.
        long highestCompletedPartition = completedPartitions.stream()
                .max(Comparator.naturalOrder())
                .orElseThrow(() -> new SafeIllegalStateException(
                        "Finished max iterations of shard progress update proposal, but no partitions were"
                                + " found. This is unexpected"));
        return ShardProgressUpdate.builder()
                .completedFinePartitions(completedPartitions)
                .progressTimestamp(SweepQueueUtils.maxTsForFinePartition(highestCompletedPartition))
                .build();
    }
}
