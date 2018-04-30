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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public class KvsSweepQueueScrubber {
    private KeyValueService kvs;
    private SweepableCellsWriter sweepableCellsWriter;
    private SweepableCellsReader sweepableCellsReader;
    private SweepableTimestampsWriter sweepableTimestampsWriter;
    private KvsSweepQueueProgress progress;

    /**
     * Cleans up all the sweep queue data from the last update of progress up to and including the given sweep
     * partition. Then, updates the sweep queue progress.
     * @param shardStrategy shard and strategy to scrub for.
     * @param previousProgress previous last partition sweep has processed.
     * @param newProgress last partition sweep has processed.
     */
    public void scrub(ShardAndStrategy shardStrategy, long previousProgress, long newProgress) {
        scrubSweepableCells(shardStrategy, newProgress);
        scrubSweepableTimestamps(shardStrategy, previousProgress, newProgress);
        progressTo(shardStrategy, newProgress);
    }

    private void scrubSweepableCells(ShardAndStrategy shardStrategy, long newProgress) {
        scrubDedicatedRows(shardStrategy, newProgress);
        scrubNonDedicatedRow(shardStrategy, newProgress);
    }

    private void scrubDedicatedRows(ShardAndStrategy shardStrategy, long partition) {
        SweepableCellsReader.rangeRequestsForDedicatedRows(kvs, shardStrategy, partition)
                .forEach(sweepableCellsWriter::deleteRange);
    }

    private void scrubNonDedicatedRow(ShardAndStrategy shardStrategy, long partition) {
        sweepableCellsWriter.deleteRange(sweepableCellsReader.rangeRequestForNonDedicatedRow(shardStrategy, partition));
    }

    private void scrubSweepableTimestamps(ShardAndStrategy shardStrategy, long oldPartition, long newPartition) {
        if (SweepQueueUtils.partitionFineToCoarse(newPartition) > SweepQueueUtils.partitionFineToCoarse(oldPartition)) {
            sweepableTimestampsWriter.deleteRow(PartitionInfo.of(shardStrategy, oldPartition));
        }
    }

    private void progressTo(ShardAndStrategy shardStrategy, long partition) {
        progress.updateLastSweptTimestampPartition(shardStrategy, partition);
    }
}
