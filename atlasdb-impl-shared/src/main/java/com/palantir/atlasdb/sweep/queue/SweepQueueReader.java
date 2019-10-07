/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep.queue;

import java.util.Optional;
import java.util.function.IntSupplier;

class SweepQueueReader {
    private final SweepableTimestamps sweepableTimestamps;
    private final SweepableCells sweepableCells;
    private final IntSupplier maximumPartitionsInBatch;

    SweepQueueReader(SweepableTimestamps sweepableTimestamps,
            SweepableCells sweepableCells,
            IntSupplier maximumPartitionsInBatch) {
        this.sweepableTimestamps = sweepableTimestamps;
        this.sweepableCells = sweepableCells;
        this.maximumPartitionsInBatch = maximumPartitionsInBatch;
    }

    SweepBatchWithPartitionInfo getNextBatchToSweep(ShardAndStrategy shardStrategy, long lastSweptTs, long sweepTs) {
        SweepBatchAccumulator accumulator = new SweepBatchAccumulator(sweepTs, lastSweptTs);
        long previousProgress = lastSweptTs;
        for (int currentBatch = 0;
                currentBatch < maximumPartitionsInBatch.getAsInt() && accumulator.shouldAcceptAdditionalBatch();
                currentBatch++) {
            Optional<Long> nextFinePartition = sweepableTimestamps.nextSweepableTimestampPartition(
                    shardStrategy, previousProgress, sweepTs);
            if (!nextFinePartition.isPresent()) {
                return accumulator.toSweepBatch();
            }
            SweepBatch batch = sweepableCells.getBatchForPartition(
                    shardStrategy, nextFinePartition.get(), previousProgress, sweepTs);
            accumulator.accumulateBatch(batch);
            previousProgress = accumulator.getProgressTimestamp();
        }
        return accumulator.toSweepBatch();
    }
}
