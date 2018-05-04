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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public abstract class KvsSweepQueueWriter implements SweepQueueWriter {
    private final KeyValueService kvs;
    private final TableReference tableRef;
    private final WriteInfoPartitioner partitioner;

    public KvsSweepQueueWriter(KeyValueService kvs, TableReference tableRef, WriteInfoPartitioner partitioner) {
        this.kvs = kvs;
        this.tableRef = tableRef;
        this.partitioner = partitioner;
    }

    @Override
    public void enqueue(List<WriteInfo> allWrites) {
        Map<Cell, byte[]> cellsToWrite = new HashMap<>();
        Map<PartitionInfo, List<WriteInfo>> partitionedWrites = partitioner.filterAndPartition(allWrites);
        partitionedWrites.forEach((partitionInfo, writes) -> populateCells(partitionInfo, writes, cellsToWrite));
        if (!cellsToWrite.isEmpty()) {
            kvs.put(tableRef, cellsToWrite, SweepQueueUtils.WRITE_TS);
        }
    }

    abstract void populateCells(PartitionInfo info, List<WriteInfo> writes, Map<Cell, byte[]> cellsToWrite);

    RowColumnRangeIterator getRowsColumnRange(Iterable<byte[]> rows, ColumnRangeSelection columnRange, int batchSize) {
        return kvs.getRowsColumnRange(tableRef, rows, columnRange, batchSize, SweepQueueUtils.READ_TS);
    }

    void deleteRange(RangeRequest request) {
        kvs.deleteRange(tableRef, request);
    }
}
