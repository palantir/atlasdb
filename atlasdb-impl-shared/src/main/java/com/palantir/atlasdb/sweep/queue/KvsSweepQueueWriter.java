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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;

public abstract class KvsSweepQueueWriter implements SweepQueueWriter {
    final KeyValueService kvs;
    private final TableReference tableRef;
    private final WriteInfoPartitioner partitioner;
    final Optional<TargetedSweepMetrics> maybeMetrics;

    public KvsSweepQueueWriter(KeyValueService kvs, TableReference tableRef, WriteInfoPartitioner partitioner,
            @Nullable TargetedSweepMetrics metrics) {
        this.kvs = kvs;
        this.tableRef = tableRef;
        this.partitioner = partitioner;
        this.maybeMetrics = Optional.ofNullable(metrics);
    }

    @Override
    public void enqueue(List<WriteInfo> allWrites) {
        Map<Cell, byte[]> cellsToWrite = new HashMap<>();
        Map<PartitionInfo, List<WriteInfo>> partitionedWrites = partitioner.filterAndPartition(allWrites);
        partitionedWrites.forEach((partitionInfo, writes) -> cellsToWrite.putAll(populateCells(partitionInfo, writes)));
        if (!cellsToWrite.isEmpty()) {
            kvs.put(tableRef, cellsToWrite, SweepQueueUtils.WRITE_TS);
        }
        maybeMetrics.ifPresent(metrics ->
                partitionedWrites.forEach((info, writes) ->
                        metrics.updateEnqueuedWrites(ShardAndStrategy.fromInfo(info), writes.size())));
    }

    /**
     * Converts a list of write infos into the required format to be persisted into the kvs. This method assumes all
     * the writes correspond to the same partition and have the same start timestamp (they are all part of the same
     * transaction), as given by info.
     *
     * @param info information about the partition the writes fall into, and the start timestamp of the transaction the
     * writes correspond to.
     * @param writes list detailing the information for each of the writes
     * @return map of cell to byte array persisting the write infomations into the kvs
     */
    abstract Map<Cell, byte[]> populateCells(PartitionInfo info, List<WriteInfo> writes);

    RowColumnRangeIterator getRowsColumnRange(Iterable<byte[]> rows, ColumnRangeSelection columnRange, int batchSize) {
        return kvs.getRowsColumnRange(tableRef, rows, columnRange, batchSize, SweepQueueUtils.READ_TS);
    }

    void deleteRange(RangeRequest request) {
        kvs.deleteRange(tableRef, request);
    }
}
