/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.ImmutableCellReference;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.common.streams.KeyedStream;

class SweepBatchAccumulator {
    private final List<WriteInfo> accumulatedWrites = Lists.newArrayList();
    private final List<SweepableCellsTable.SweepableCellsRow> accumulatedDedicatedRows = Lists.newArrayList();
    private final long sweepTimestamp;

    private long progressTimestamp;
    private boolean anyBatchesPresent = false;

    SweepBatchAccumulator(long sweepTimestamp, long progressTimestamp) {
        this.sweepTimestamp = sweepTimestamp;
        this.progressTimestamp = progressTimestamp;
    }

    void accumulateBatch(SweepBatch sweepBatch) {
        Preconditions.checkState(sweepBatch.lastSweptTimestamp() < sweepTimestamp,
                "Tried to accumulate a batch %s at timestamp %s that went beyond the sweep timestamp %s!"
                        + " This is unexpected, and suggests a bug in the way we read in targeted sweep."
                        + " This by itself does not mean that AtlasDB service is compromised, but targeted sweep"
                        + " may not be working.",
                sweepBatch,
                sweepBatch.lastSweptTimestamp(),
                sweepTimestamp);

        accumulatedWrites.addAll(sweepBatch.writes());
        accumulatedDedicatedRows.addAll(sweepBatch.dedicatedRows().getDedicatedRows());
        progressTimestamp = Math.max(progressTimestamp, sweepBatch.lastSweptTimestamp());
        anyBatchesPresent = true;
    }

    long getProgressTimestamp() {
        return progressTimestamp;
    }

    SweepBatch toSweepBatch() {
        return SweepBatch.of(
                getLatestWritesByCellReference(),
                DedicatedRows.of(accumulatedDedicatedRows),
                getLastSweptTimestamp());
    }

    boolean shouldAcceptAdditionalBatch() {
        return accumulatedWrites.size() < SweepQueueUtils.SWEEP_BATCH_SIZE
                && progressTimestamp < (sweepTimestamp - 1);
    }

    private List<WriteInfo> getLatestWritesByCellReference() {
        Map<CellReference, List<WriteInfo>> writes = accumulatedWrites.stream()
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

    private long getLastSweptTimestamp() {
        if (anyBatchesPresent) {
            return progressTimestamp;
        }
        return sweepTimestamp - 1;
    }
}
