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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.ImmutableCellReference;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.common.streams.KeyedStream;

class SweepQueueReader {
    private final SweepableTimestamps sweepableTimestamps;
    private final SweepableCells sweepableCells;
    private final BooleanSupplier batchReadsAcrossPartitions;

    SweepQueueReader(SweepableTimestamps sweepableTimestamps,
            SweepableCells sweepableCells,
            BooleanSupplier batchReadsAcrossPartitions) {
        this.sweepableTimestamps = sweepableTimestamps;
        this.sweepableCells = sweepableCells;
        this.batchReadsAcrossPartitions = batchReadsAcrossPartitions;
    }

    SweepBatch getNextBatchToSweep(ShardAndStrategy shardStrategy, long lastSweptTs, long sweepTs) {
        if (batchReadsAcrossPartitions.getAsBoolean()) {
            boolean shouldStop = false;

            List<WriteInfo> accumulatedWrites = Lists.newArrayList();
            List<SweepableCellsTable.SweepableCellsRow> accumulatedDedicatedRows = Lists.newArrayList();
            long progressTimestamp = lastSweptTs;

            while (!shouldStop) {
                Optional<Long> nextFinePartition = sweepableTimestamps.nextSweepableTimestampPartition(
                        shardStrategy, progressTimestamp, sweepTs);
                if (!nextFinePartition.isPresent()) {
                    progressTimestamp = sweepTs - 1;
                    shouldStop = true;
                } else {
                    System.out.println(nextFinePartition);
                    SweepBatch batch = sweepableCells.getBatchForPartition(
                            shardStrategy, nextFinePartition.get(), progressTimestamp, sweepTs);
                    System.out.println(batch);
                    accumulatedWrites.addAll(batch.writes());
                    accumulatedDedicatedRows.addAll(batch.dedicatedRows().getDedicatedRows());
                    progressTimestamp = batch.lastSweptTimestamp();
                    shouldStop = accumulatedWrites.size() > SweepQueueUtils.SWEEP_BATCH_SIZE
                            || batch.isEmpty()
                            || progressTimestamp >= (sweepTs - 1);
                }
            }
            return ImmutableSweepBatch.builder()
                    .writes(takeLatestByCellReference(accumulatedWrites))
                    .dedicatedRows(DedicatedRows.of(accumulatedDedicatedRows))
                    .lastSweptTimestamp(progressTimestamp)
                    .build();
        }
        return getBatchFromSingleFinePartition(shardStrategy, lastSweptTs, sweepTs);
    }

    private List<WriteInfo> takeLatestByCellReference(List<WriteInfo> allTheWrites) {
        Map<CellReference, List<WriteInfo>> writes = allTheWrites.stream()
                .collect(Collectors.groupingBy(writeInfo -> ImmutableCellReference.builder()
                        .cell(writeInfo.cell())
                        .tableRef(writeInfo.tableRef())
                        .build()));
        return KeyedStream.stream(writes)
                .map(list -> list.stream().max(Comparator.comparing(WriteInfo::timestamp)))
                .map(Optional::get) // groupingBy() won't return empty lists
                .values()
                .collect(Collectors.toList());
    }

    private SweepBatch getBatchFromSingleFinePartition(ShardAndStrategy shardStrategy, long lastSweptTs, long sweepTs) {
        return sweepableTimestamps.nextSweepableTimestampPartition(shardStrategy, lastSweptTs, sweepTs)
                .map(fine -> sweepableCells.getBatchForPartition(shardStrategy, fine, lastSweptTs, sweepTs))
                .orElseGet(() -> SweepBatch.of(ImmutableList.of(), DedicatedRows.of(ImmutableList.of()), sweepTs - 1L));
    }
}
