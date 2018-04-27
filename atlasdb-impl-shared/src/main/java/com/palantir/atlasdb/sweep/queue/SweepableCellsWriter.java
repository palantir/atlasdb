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
import com.palantir.atlasdb.keyvalue.api.ImmutableTargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;

public class SweepableCellsWriter extends KvsSweepQueueWriter {
    private static final long MAX_CELLS_GENERIC = 50L;
    static final long MAX_CELLS_DEDICATED = 100_000L;

    private final WriteInfoPartitioner partitioner;

    SweepableCellsWriter(KeyValueService kvs, WriteInfoPartitioner partitioner) {
        super(kvs, TargetedSweepTableFactory.of().getSweepableCellsTable(null).getTableRef());
        this.partitioner = partitioner;
    }

    @Override
    protected Map<Cell, byte[]> batchWrites(List<WriteInfo> writes) {
        Map<Cell, byte[]> result = new HashMap<>();
        Map<PartitionInfo, List<WriteInfo>> partitionedWrites = partitioner.filterAndPartition(writes);
        partitionedWrites.forEach((partitionInfo, writeInfos) -> putWrites(partitionInfo, writeInfos, result));
        return result;
    }

    private void putWrites(PartitionInfo partitionInfo, List<WriteInfo> writes, Map<Cell, byte[]> result) {
        boolean dedicate = writes.size() > MAX_CELLS_GENERIC;

        if (dedicate) {
            addReferenceToDedicatedRows(partitionInfo, writes, result);
        }

        long index = 0;
        for (WriteInfo write : writes) {
            addWrite(partitionInfo, write, dedicate, index, result);
            index++;
        }
    }

    private void addReferenceToDedicatedRows(PartitionInfo info, List<WriteInfo> writes, Map<Cell, byte[]> result) {
        insert(info, WriteReference.DUMMY, false, 0, -requiredDedicatedRows(writes), result);
    }

    private void addWrite(PartitionInfo info, WriteInfo write, boolean dedicate, long index, Map<Cell, byte[]> result) {
        insert(info, write.writeRef(), dedicate, index / MAX_CELLS_DEDICATED, index % MAX_CELLS_DEDICATED, result);
    }

    private void insert(PartitionInfo info, WriteReference writeRef, boolean dedicate, long dedicatedRow,
            long index, Map<Cell, byte[]> result) {
        SweepableCellsTable.SweepableCellsRow row = createRow(info, dedicate, dedicatedRow);
        SweepableCellsTable.SweepableCellsColumnValue colVal = createColVal(info.timestamp(), index, writeRef);
        result.put(SweepQueueUtils.toCell(row, colVal), colVal.persistValue());
    }

    private SweepableCellsTable.SweepableCellsRow createRow(PartitionInfo info, boolean dedicate, long dedicatedRow) {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(info.isConservative().isTrue())
                .dedicatedRow(dedicate)
                .shard(info.shard())
                .dedicatedRowNumber(dedicatedRow)
                .build();

        return SweepableCellsTable.SweepableCellsRow.of(
                SweepQueueUtils.tsPartitionFine(info.timestamp()), metadata.persistToBytes());
    }

    private SweepableCellsTable.SweepableCellsColumnValue createColVal(long ts, long index, WriteReference writeRef) {
        SweepableCellsTable.SweepableCellsColumn col = SweepableCellsTable.SweepableCellsColumn.of(tsMod(ts), index);
        return SweepableCellsTable.SweepableCellsColumnValue.of(col, writeRef);
    }

    private long requiredDedicatedRows(List<WriteInfo> writes) {
        return 1 + (writes.size() - 1) / MAX_CELLS_DEDICATED;
    }

    private static long tsMod(long timestamp) {
        return timestamp % SweepQueueUtils.TS_FINE_GRANULARITY;
    }
}

