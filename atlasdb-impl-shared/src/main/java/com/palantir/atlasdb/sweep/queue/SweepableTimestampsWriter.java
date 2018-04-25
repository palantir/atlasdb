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
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.generated.SweepableTimestampsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;

public class SweepableTimestampsWriter extends KvsSweepQueueWriter {
    private static final long TS_COARSE_GRANULARITY = 10_000_000L;
    private static final byte[] DUMMY = new byte[0];

    private final WriteInfoPartitioner partitioner;

    SweepableTimestampsWriter(KeyValueService kvs, WriteInfoPartitioner part) {
        super(kvs, TargetedSweepTableFactory.of().getSweepableTimestampsTable(null).getTableRef());
        this.partitioner = part;
    }

    @Override
    protected Map<Cell, byte[]> batchWrites(List<WriteInfo> writes) {
        Map<Cell, byte[]> result = new HashMap<>();
        Map<PartitionInfo, List<WriteInfo>> partitionedWrites = partitioner.filterAndPartition(writes);
        partitionedWrites.forEach((partitionInfo, writeInfos) -> putWrite(partitionInfo, result));
        return result;
    }

    private void putWrite(PartitionInfo partitionInfo, Map<Cell, byte[]> result) {
        SweepableTimestampsTable.SweepableTimestampsRow row = SweepableTimestampsTable.SweepableTimestampsRow.of(
                partitionInfo.shard(),
                tsPartitionCoarse(partitionInfo.timestamp()),
                partitionInfo.isConservative().persistToBytes());

        SweepableTimestampsTable.SweepableTimestampsColumn col = SweepableTimestampsTable.SweepableTimestampsColumn.of(
                SweepableCellsWriter.tsMod(partitionInfo.timestamp()));

        SweepableTimestampsTable.SweepableTimestampsColumnValue colVal =
                SweepableTimestampsTable.SweepableTimestampsColumnValue.of(col, DUMMY);

        result.put(toCell(row, colVal), colVal.persistValue());
    }

    private long tsPartitionCoarse(long timestamp) {
        return timestamp / TS_COARSE_GRANULARITY;
    }
}
