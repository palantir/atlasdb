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

public class KvsSweepQueueScrubber {
    private SweepableCells sweepableCells;
    private SweepableTimestamps sweepableTimestamps;
    private KvsSweepQueueProgress progress;

    public KvsSweepQueueScrubber(SweepableCells cells, SweepableTimestamps timestamps, KvsSweepQueueProgress progress) {
        this.sweepableCells = cells;
        this.sweepableTimestamps = timestamps;
        this.progress = progress;
    }

    /**
     * Cleans up all the sweep queue data from the last update of progress up to and including the given sweep
     * partition. Then, updates the sweep queue progress.
     * @param shardStrategy shard and strategy to scrub for.
     * @param oldProgress previous last partition sweep has processed.
     * @param newProgress last partition sweep has processed.
     */
    public void scrub(ShardAndStrategy shardStrategy, long oldProgress, long newProgress) {
        scrubSweepableCells(shardStrategy, oldProgress, newProgress);
        scrubSweepableTimestamps(shardStrategy, oldProgress, newProgress);
        progressTo(shardStrategy, newProgress);
    }

    private void scrubSweepableCells(ShardAndStrategy shardStrategy, long oldProgress, long newProgress) {
        long oldPartitionFine = SweepQueueUtils.tsPartitionFine(oldProgress);
        long newPartitionFine = SweepQueueUtils.tsPartitionFine(newProgress);
        if (newPartitionFine > oldPartitionFine) {
            scrubDedicatedRows(shardStrategy, oldPartitionFine);
            scrubNonDedicatedRow(shardStrategy, oldPartitionFine);
        }
    }

    private void scrubDedicatedRows(ShardAndStrategy shardStrategy, long partitionToDelete) {
        sweepableCells.deleteDedicatedRows(shardStrategy, partitionToDelete);
    }

    private void scrubNonDedicatedRow(ShardAndStrategy shardStrategy, long partitionToDelete) {
        sweepableCells.deleteNonDedicatedRow(shardStrategy, partitionToDelete);
    }

    private void scrubSweepableTimestamps(ShardAndStrategy shardStrategy, long oldProgress, long newProgress) {
        long oldPartitionCoarse = SweepQueueUtils.tsPartitionCoarse(oldProgress);
        long newPartitionCoarse = SweepQueueUtils.tsPartitionCoarse(newProgress);
        if (newPartitionCoarse > oldPartitionCoarse) {
            sweepableTimestamps.deleteRow(shardStrategy, oldProgress);
        }
    }

    private void progressTo(ShardAndStrategy shardStrategy, long lastSweptTs) {
        progress.updateLastSweptTimestamp(shardStrategy, lastSweptTs);
    }
}
