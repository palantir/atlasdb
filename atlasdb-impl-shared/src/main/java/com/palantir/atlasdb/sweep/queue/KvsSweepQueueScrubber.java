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

import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.ImmutableTargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;

public class KvsSweepQueueScrubber {
    private KeyValueService kvs;

    /**
     * Cleans up all the sweep queue data from the last update of progress up to and including the given sweep
     * partition. Then, updates the sweep queue progress.
     * @param shardStrategy shard and strategy to scrub for.
     * @param previousProgress previous last partition sweep has processed.
     * @param newProgress last partition sweep has processed.
     */
    public void scrub(ShardAndStrategy shardStrategy, long previousProgress, long newProgress) {
        scrubSweepableCells(shardStrategy, previousProgress, newProgress);
        scrubSweepableTimestamps(shardStrategy, previousProgress, newProgress);
        progressTo(shardStrategy, newProgress);
    }

    private void scrubSweepableCells(ShardAndStrategy shardStrategy, long previousProgress, long newProgress) {
        TableReference sweepableCells = TargetedSweepTableFactory.of().getSweepableCellsTable(null).getTableRef();
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .shard(shardStrategy.shard())
                .conservative(shardStrategy.isConservative())
                .
        byte[] row = SweepableCellsTable.SweepableCellsRow.of(previousProgress)
        RangeRequest request = RangeRequest.builder()
                .startRowInclusive()
                .endRowExclusive()
                .retainColumns(ColumnSelection.all())
                .build();
        kvs.deleteRange(sweepableCells, );
    }

    private void scrubSweepableTimestamps(ShardAndStrategy shardStrategy, long previousProgress, long newProgress) {

    }

    private void progressTo(ShardAndStrategy shardStrategy, long timestampPartitionFine) {

    }
}
