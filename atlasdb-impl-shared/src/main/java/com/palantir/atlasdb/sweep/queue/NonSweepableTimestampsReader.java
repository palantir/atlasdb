/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class NonSweepableTimestampsReader {
    private final SweepableTimestamps sweepableTimestamps;
    private final SweepableCells sweepableCells;
    private final ShardProgress progress;

    NonSweepableTimestampsReader(
            SweepableTimestamps sweepableTimestamps, SweepableCells sweepableCells, ShardProgress progress) {
        this.sweepableTimestamps = sweepableTimestamps;
        this.sweepableCells = sweepableCells;
        this.progress = progress;
    }

    public void getNextBatch(long sweepTs) {
        long lastSweptTimestamp = progress.getLastSweptTimestamp(SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE);

        Optional<Long> nextFinePartition =
                sweepableTimestamps.nextNonSweepableTimestampPartition(lastSweptTimestamp, sweepTs);

        if (nextFinePartition.isEmpty()) {
            possiblyCleanTables(lastSweptTimestamp, sweepTs, false);
            progress.updateLastSweptTimestamp(SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE, sweepTs - 1);
            return;
        }

        NonSweepableBatchInfo batch =
                sweepableCells.getNonSweepableBatchForPartition(nextFinePartition.get(), lastSweptTimestamp, sweepTs);
        progress.updateLastSeenCommitTimestamp(
                SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE, batch.lastSeenCommitTimestamp());
        // todo (gmaretic) : update abandoned timestamps
        long previousLastSwept = lastSweptTimestamp;
        if (batch.processedAll()) {
            long maxPossibleSwept = SweepQueueUtils.maxTsForFinePartition(nextFinePartition.get());
            if (sweepTs > maxPossibleSwept) {
                lastSweptTimestamp = maxPossibleSwept;
            } else {
                lastSweptTimestamp = sweepTs - 1;
            }
        } else {
            lastSweptTimestamp = batch.lastSweptTimestamp();
        }
        possiblyCleanTables(previousLastSwept, lastSweptTimestamp, true);
        progress.updateLastSweptTimestamp(SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE, lastSweptTimestamp);
    }

    private void possiblyCleanTables(long previouslyLastSwept, long lastSwept, boolean foundAny) {
        long firstCandidateFinePartition = SweepQueueUtils.tsPartitionFine(previouslyLastSwept + 1);
        long lastSweptFinePartition = SweepQueueUtils.tsPartitionFine(lastSwept);
        long firstNonProcessedFinePartition = SweepQueueUtils.tsPartitionFine(lastSwept + 1);
        boolean startedAtBeginningOfRowForCells =
                SweepQueueUtils.minTsForFinePartition(firstCandidateFinePartition) == previouslyLastSwept + 1;

        Set<Long> finePartitionsToClean = new HashSet<>();
        if (!startedAtBeginningOfRowForCells && firstCandidateFinePartition < firstNonProcessedFinePartition) {
            finePartitionsToClean.add(firstCandidateFinePartition);
        }
        if (foundAny && lastSweptFinePartition < firstNonProcessedFinePartition) {
            finePartitionsToClean.add(lastSweptFinePartition);
        }
        if (!finePartitionsToClean.isEmpty()) {
            sweepableCells.deleteNonSweepableRows(finePartitionsToClean);
        }

        long firstCandidateCoarsePartition = SweepQueueUtils.tsPartitionCoarse(previouslyLastSwept + 1);
        long lastSweptCoarsePartition = SweepQueueUtils.tsPartitionCoarse(lastSwept);
        long firstNonProcessedCoarsePartition = SweepQueueUtils.tsPartitionCoarse(lastSwept + 1);
        boolean startedAtBeginningOfRowForTimestamps =
                SweepQueueUtils.minTsForCoarsePartition(firstCandidateCoarsePartition) == previouslyLastSwept + 1;

        Set<Long> coarsePartitionsToClean = new HashSet<>();
        if (!startedAtBeginningOfRowForTimestamps && firstCandidateCoarsePartition < firstNonProcessedCoarsePartition) {
            coarsePartitionsToClean.add(firstCandidateCoarsePartition);
        }
        if (foundAny && lastSweptCoarsePartition < firstNonProcessedCoarsePartition) {
            coarsePartitionsToClean.add(lastSweptCoarsePartition);
        }
        if (!coarsePartitionsToClean.isEmpty()) {
            sweepableTimestamps.deleteNonSweepableCoarsePartitions(coarsePartitionsToClean);
        }
    }
}
