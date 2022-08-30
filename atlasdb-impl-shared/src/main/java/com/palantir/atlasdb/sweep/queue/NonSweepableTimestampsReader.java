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
import java.util.function.Consumer;
import java.util.function.Function;

public final class NonSweepableTimestampsReader {
    private final SweepableTimestamps sweepableTimestamps;
    private final SweepableCells sweepableCells;
    private final ShardProgress progress;

    public NonSweepableTimestampsReader(
            SweepableTimestamps sweepableTimestamps, SweepableCells sweepableCells, ShardProgress progress) {
        this.sweepableTimestamps = sweepableTimestamps;
        this.sweepableCells = sweepableCells;
        this.progress = progress;
    }

    /**
     * Processes the next batch of transactions that only wrote to tables that are not being swept: this will abort any
     * transaction with a start timestamp before sweepTs encountered that has still not committed.
     */
    public void processNextBatch(long sweepTs) {
        long lastSweptTimestamp = progress.getLastSweptTimestamp(SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE);

        Optional<Long> nextFinePartition =
                sweepableTimestamps.nextNonSweepableTimestampPartition(lastSweptTimestamp, sweepTs);

        if (nextFinePartition.isEmpty()) {
            possiblyCleanTables(lastSweptTimestamp, sweepTs - 1, false);
            progress.updateLastSweptTimestamp(SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE, sweepTs - 1);
            return;
        }

        NonSweepableBatchInfo batch = sweepableCells.processNonSweepableBatchForPartition(
                nextFinePartition.get(), lastSweptTimestamp, sweepTs);
        progress.updateLastSeenCommitTimestamp(
                SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE, batch.greatestSeenCommitTimestamp());
        // todo (gmaretic) : update abandoned timestamps here
        possiblyCleanTables(lastSweptTimestamp, batch.lastSweptTimestamp(), true);
        progress.updateLastSweptTimestamp(SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE, batch.lastSweptTimestamp());
    }

    private void possiblyCleanTables(long previouslyLastSwept, long lastSwept, boolean readFromLastRow) {
        possiblyClean(
                SweepQueueUtils::tsPartitionFine,
                sweepableCells::deleteNonSweepableRows,
                previouslyLastSwept,
                lastSwept,
                readFromLastRow);
        possiblyClean(
                SweepQueueUtils::tsPartitionCoarse,
                sweepableTimestamps::deleteNonSweepableRows,
                previouslyLastSwept,
                lastSwept,
                readFromLastRow);
    }

    private void possiblyClean(
            Function<Long, Long> partitioner,
            Consumer<Set<Long>> cleaner,
            long previouslyLastSwept,
            long lastSwept,
            boolean readFromLastRow) {
        long previouslyProcessedPartition = previouslyLastSwept == -1 ? -1 : partitioner.apply(previouslyLastSwept);
        long firstPartition = partitioner.apply(previouslyLastSwept + 1);
        long lastPartitionInclusive = partitioner.apply(lastSwept);
        long firstNotFullyProcessedPartition = partitioner.apply(lastSwept + 1);

        Set<Long> partitionsToClean = new HashSet<>();
        // if we have seen anything in the last row, and we finished it, we must clean
        if (readFromLastRow && lastPartitionInclusive < firstNotFullyProcessedPartition) {
            partitionsToClean.add(lastPartitionInclusive);
        }
        // if we finished the first row, it's either also the last row which is already covered, or the portion we
        // processed now was empty. In this case, we clean if and only if we did not start at the beginning of the row
        if (firstPartition < firstNotFullyProcessedPartition && previouslyProcessedPartition == firstPartition) {
            partitionsToClean.add(firstPartition);
        }

        if (!partitionsToClean.isEmpty()) {
            cleaner.accept(partitionsToClean);
        }
    }
}
