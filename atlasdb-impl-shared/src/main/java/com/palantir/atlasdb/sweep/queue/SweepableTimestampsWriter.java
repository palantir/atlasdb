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
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.schema.generated.SweepableTimestampsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;

public class SweepableTimestampsWriter extends KvsSweepQueueWriter {
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
        SweepableTimestampsTable.SweepableTimestampsRow row = toRow(partitionInfo);

        SweepableTimestampsTable.SweepableTimestampsColumn col = SweepableTimestampsTable.SweepableTimestampsColumn.of(
                SweepQueueUtils.tsPartitionFine(partitionInfo.timestamp()));

        SweepableTimestampsTable.SweepableTimestampsColumnValue colVal =
                SweepableTimestampsTable.SweepableTimestampsColumnValue.of(col, DUMMY);

        result.put(SweepQueueUtils.toCell(row, colVal), colVal.persistValue());
    }

    void deleteRow(PartitionInfo partitionInfo) {
        byte[] row = toRow(partitionInfo).persistToBytes();

        RangeRequest request = RangeRequest.builder()
                .startRowInclusive(row)
                .endRowExclusive(RangeRequests.nextLexicographicName(row))
                .retainColumns(ColumnSelection.all())
                .build();

        deleteRange(request);
    }

    private SweepableTimestampsTable.SweepableTimestampsRow toRow(PartitionInfo partitionInfo) {
        return SweepableTimestampsTable.SweepableTimestampsRow.of(
                partitionInfo.shard(),
                SweepQueueUtils.tsPartitionCoarse(partitionInfo.timestamp()),
                partitionInfo.isConservative().persistToBytes());
    }
}
