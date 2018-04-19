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

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableTargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;


public class SweepableCellsWriter extends KvsSweepQueueWriter {
    private static final long PARTITION_FACTOR = 50_000L;
    private static final long MAX_CELLS_GENERIC = 50L;
    private static final long MAX_CELLS_DEDICATED = 100_000L;

    private final SweepStrategyCache strategyCache;

    SweepableCellsWriter(KeyValueService kvs, TargetedSweepTableFactory tableFactory, SweepStrategyCache strategyCache) {
        super(kvs, tableFactory.getSweepableCellsTable(null).getTableRef());
        this.strategyCache = strategyCache;
    }

    @Override
    protected Map<Cell, byte[]> batchWrites(List<WriteInfo> writes) {
        ImmutableMap.Builder<Cell, byte[]> resultBuilder = ImmutableMap.builder();
        boolean dedicated = writes.size() > MAX_CELLS_GENERIC;

        if (dedicated) {
            addReferenceToDedicatedRows(writes, resultBuilder);
        }

        long writeIndex = 0;
        for (WriteInfo write : writes) {
            SweepableCellsTable.SweepableCellsRow row = createRow(write, dedicated, writeIndex / MAX_CELLS_DEDICATED);
            SweepableCellsTable.SweepableCellsColumnValue colVal = createColVal(write, writeIndex);
            resultBuilder.put(toCell(row, colVal), colVal.persistValue());
            writeIndex++;
        }
        return resultBuilder.build();
    }

    private void addReferenceToDedicatedRows(List<WriteInfo> writes, ImmutableMap.Builder<Cell, byte[]> resultBuilder) {
        SweepableCellsTable.SweepableCellsRow row = createRow(writes.get(0), false, 0);
        SweepableCellsTable.SweepableCellsColumnValue colVal = createColVal(writes.get(0), -numDedicatedRows(writes));
        resultBuilder.put(toCell(row, colVal), colVal.persistValue());
    }

    private SweepableCellsTable.SweepableCellsRow createRow(WriteInfo write, boolean dedicated, long dedicatedRowNum) {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(strategyCache.getStrategy(write) == TableMetadataPersistence.SweepStrategy.CONSERVATIVE)
                .dedicatedRow(dedicated)
                .shard(KvsSweepQueuePersister.getShard(write))
                .dedicatedRowNumber(dedicatedRowNum)
                .build();

        return SweepableCellsTable.SweepableCellsRow.of(tsPartition(write.timestamp()), metadata.persistToBytes());
    }

    private SweepableCellsTable.SweepableCellsColumnValue createColVal(WriteInfo write, long writeIndex) {
        SweepableCellsTable.SweepableCellsColumn col = SweepableCellsTable.SweepableCellsColumn
                .of(tsModulus(write.timestamp()), writeIndex % MAX_CELLS_DEDICATED);
        return SweepableCellsTable.SweepableCellsColumnValue.of(col, write.tableRefCell());
    }

    private long numDedicatedRows(List<WriteInfo> writes) {
        return 1 + (writes.size() - 1) / MAX_CELLS_DEDICATED;
    }

    private long tsPartition(long timestamp) {
        return timestamp / PARTITION_FACTOR;
    }

    private long tsModulus(long timestamp) {
        return timestamp % PARTITION_FACTOR;
    }
}

