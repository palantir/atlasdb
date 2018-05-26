/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
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
     */
    public void clean(ShardAndStrategy shardStrategy, long oldProgress, long newProgress) {
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
            cleanDedicatedRows(shardStrategy, lastSweptPartitionPreviously);
            cleanNonDedicatedRow(shardStrategy, lastSweptPartitionPreviously);
            log.info("Deleted persisted sweep queue information in table SweepableCells for partition {}.",
                    lastSweptPartitionPreviously);
        }
    }

    private void cleanDedicatedRows(ShardAndStrategy shardStrategy, long partitionToDelete) {
        sweepableCells.deleteDedicatedRows(shardStrategy, partitionToDelete);
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
            log.info("Deleted persisted sweep queue information in table SweepableTimestamps for partition {}.",
                    lastSweptPartitionPreviously);
        }
    }

    private void progressTo(ShardAndStrategy shardStrategy, long newProgress) {
        if (newProgress < 0) {
            log.warn("Wasn't able to progress targeted sweep for {} since last swept timestamp {} is negative.",
                    shardStrategy.toText(), newProgress);
            return;
        }
        progress.updateLastSweptTimestamp(shardStrategy, newProgress);
        log.info("Progressed last swept timestamp for {} to {}.", shardStrategy.toText(), newProgress);
    }

    private boolean firstIterationOfSweep(long oldProgress) {
        return oldProgress == SweepQueueUtils.INITIAL_TIMESTAMP;
    }
}
