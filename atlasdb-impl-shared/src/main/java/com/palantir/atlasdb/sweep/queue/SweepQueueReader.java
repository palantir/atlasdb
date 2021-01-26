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
import org.immutables.value.Value.Immutable;

public class SweepQueueReader {
    private final SweepableTimestamps sweepableTimestamps;
    private final SweepableCells sweepableCells;
    private final ReadBatchingRuntimeContext runtime;

    SweepQueueReader(
            SweepableTimestamps sweepableTimestamps,
            SweepableCells sweepableCells,
            ReadBatchingRuntimeContext runtime) {
        this.sweepableTimestamps = sweepableTimestamps;
        this.sweepableCells = sweepableCells;
        this.runtime = runtime;
    }

    SweepBatchWithPartitionInfo getNextBatchToSweep(ShardAndStrategy shardStrategy, long lastSweptTs, long sweepTs) {
        SweepBatchAccumulator accumulator =
                new SweepBatchAccumulator(sweepTs, runtime.cellsThreshold().getAsInt(), lastSweptTs);
        long previousProgress = lastSweptTs;
        for (int currentBatch = 0;
                currentBatch < runtime.maximumPartitions().getAsInt() && accumulator.shouldAcceptAdditionalBatch();
                currentBatch++) {
            Optional<Long> nextFinePartition =
                    sweepableTimestamps.nextSweepableTimestampPartition(shardStrategy, previousProgress, sweepTs);
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

    public static final ReadBatchingRuntimeContext DEFAULT_READ_BATCHING_RUNTIME_CONTEXT =
            ReadBatchingRuntimeContext.builder()
                    .maximumPartitions(() -> 1)
                    .cellsThreshold(() -> SweepQueueUtils.SWEEP_BATCH_SIZE)
                    .build();

    @Immutable
    public interface ReadBatchingRuntimeContext {

        IntSupplier maximumPartitions();

        IntSupplier cellsThreshold();

        static ImmutableReadBatchingRuntimeContext.Builder builder() {
            return ImmutableReadBatchingRuntimeContext.builder();
        }
    }
}
