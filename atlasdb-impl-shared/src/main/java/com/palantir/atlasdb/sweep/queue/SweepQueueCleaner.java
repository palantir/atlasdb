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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.logsafe.SafeArg;

public class SweepQueueCleaner {
    private static final Logger log = LoggerFactory.getLogger(SweepQueueCleaner.class);
    private SweepableCells sweepableCells;
    private SweepableTimestamps sweepableTimestamps;
    private ShardProgress progress;

    public SweepQueueCleaner(SweepableCells cells, SweepableTimestamps timestamps, ShardProgress progress) {
        this.sweepableCells = cells;
        this.sweepableTimestamps = timestamps;
        this.progress = progress;
    }

    /**
     * Cleans up all the sweep queue data from the last update of progress up to and including the given sweep
     * partition. Then, updates the sweep queue progress.
     * @param shardStrategy shard and strategy to clean for.
     * @param oldProgress last swept timestamp for the previous iteration of sweep.
     * @param newProgress last swept timestamp for this iteration of sweep.
     * @param dedicatedRows the dedicated rows that have now been swept that should now be removed.
     */
    public void clean(ShardAndStrategy shardStrategy, long oldProgress, long newProgress, DedicatedRows dedicatedRows) {
        cleanDedicatedRows(dedicatedRows);
        cleanSweepableCells(shardStrategy, oldProgress, newProgress);
        cleanSweepableTimestamps(shardStrategy, oldProgress, newProgress);
        progressTo(shardStrategy, newProgress);
    }

    private void cleanSweepableCells(ShardAndStrategy shardStrategy, long oldProgress, long newProgress) {
        if (firstIterationOfSweep(oldProgress)) {
            return;
        }
        long lastSweptPartitionPreviously = SweepQueueUtils.tsPartitionFine(oldProgress);
        long minimumSweepPartitionNextIteration = SweepQueueUtils.tsPartitionFine(newProgress + 1);
        if (minimumSweepPartitionNextIteration > lastSweptPartitionPreviously) {
            cleanNonDedicatedRow(shardStrategy, lastSweptPartitionPreviously);
            log.info("Deleted persisted sweep queue information in table {} for partition {}.",
                    LoggingArgs.tableRef(TargetedSweepTableFactory.of().getSweepableCellsTable(null).getTableRef()),
                    SafeArg.of("partition", lastSweptPartitionPreviously));
        }
    }

    private void cleanDedicatedRows(DedicatedRows dedicatedRows) {
        sweepableCells.deleteDedicatedRows(dedicatedRows);
    }

    private void cleanNonDedicatedRow(ShardAndStrategy shardStrategy, long partitionToDelete) {
        sweepableCells.deleteNonDedicatedRow(shardStrategy, partitionToDelete);
    }

    private void cleanSweepableTimestamps(ShardAndStrategy shardStrategy, long oldProgress, long newProgress) {
        if (firstIterationOfSweep(oldProgress)) {
            return;
        }
        long lastSweptPartitionPreviously = SweepQueueUtils.tsPartitionCoarse(oldProgress);
        long minimumSweepPartitionNextIteration = SweepQueueUtils.tsPartitionCoarse(newProgress + 1);
        if (minimumSweepPartitionNextIteration > lastSweptPartitionPreviously) {
            sweepableTimestamps.deleteRow(shardStrategy, lastSweptPartitionPreviously);
            log.info("Deleted persisted sweep queue information in table {} for partition {}.",
                    LoggingArgs.tableRef(TargetedSweepTableFactory.of()
                            .getSweepableTimestampsTable(null).getTableRef()),
                    SafeArg.of("partition", lastSweptPartitionPreviously));
        }
    }

    private void progressTo(ShardAndStrategy shardStrategy, long newProgress) {
        if (newProgress < 0) {
            log.warn("Wasn't able to progress targeted sweep for {} since last swept timestamp {} is negative.",
                    SafeArg.of("shardStrategy", shardStrategy.toText()), SafeArg.of("timestamp", newProgress));
            return;
        }
        progress.updateLastSweptTimestamp(shardStrategy, newProgress);
        log.debug("Progressed last swept timestamp for {} to {}.",
                SafeArg.of("shardStrategy", shardStrategy.toText()), SafeArg.of("timestamp", newProgress));

    }

    private boolean firstIterationOfSweep(long oldProgress) {
        return oldProgress == SweepQueueUtils.INITIAL_TIMESTAMP;
    }
}
