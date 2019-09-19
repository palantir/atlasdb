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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class SweepBatchAccumulator {
    private final List<WriteInfo> accumulatedWrites = Lists.newArrayList();
    private final Set<Long> finePartitions = Sets.newHashSet();
    private final List<SweepableCellsTable.SweepableCellsRow> accumulatedDedicatedRows = Lists.newArrayList();
    private final long sweepTimestamp;

    private long progressTimestamp;
    private long entriesRead = 0;
    private boolean anyBatchesPresent = false;
    private boolean nextBatchAvailable = true;

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
        addRelevantFinePartitions(sweepBatch);
        progressTimestamp = Math.max(progressTimestamp, sweepBatch.lastSweptTimestamp());
        anyBatchesPresent = true;
        nextBatchAvailable = sweepBatch.hasNext();
        entriesRead += sweepBatch.entriesRead();
    }

    long getProgressTimestamp() {
        return progressTimestamp;
    }

    SweepBatchWithPartitionInfo toSweepBatch() {
        SweepBatch sweepBatch = SweepBatch.of(
                getLatestWritesByCellReference(),
                DedicatedRows.of(accumulatedDedicatedRows),
                getLastSweptTimestamp(),
                nextBatchAvailable,
                entriesRead);
        return SweepBatchWithPartitionInfo.of(sweepBatch, finePartitions);
    }

    boolean shouldAcceptAdditionalBatch() {
        return accumulatedWrites.size() < SweepQueueUtils.SWEEP_BATCH_SIZE
                && nextBatchAvailable
                && progressTimestamp < (sweepTimestamp - 1);
    }

    private List<WriteInfo> getLatestWritesByCellReference() {
        return ImmutableList.copyOf(
                accumulatedWrites.stream()
                .collect(Collectors.toMap(info -> info.writeRef().cellReference(), x -> x, WriteInfo::higherTimestamp))
                .values());
    }

    private long getLastSweptTimestamp() {
        if (anyBatchesPresent) {
            return progressTimestamp;
        }
        return sweepTimestamp - 1;
    }

    private void addRelevantFinePartitions(SweepBatch sweepBatch) {
        sweepBatch.writes().stream()
                .map(writeInfo -> SweepQueueUtils.tsPartitionFine(writeInfo.timestamp()))
                .forEach(finePartitions::add);
        sweepBatch.dedicatedRows().getDedicatedRows().stream()
                .map(SweepableCellsTable.SweepableCellsRow::getTimestampPartition)
                .map(SweepQueueUtils::tsPartitionFine) // Dedicated rows are written with the start timestamp
                .forEach(finePartitions::add);
    }
}
