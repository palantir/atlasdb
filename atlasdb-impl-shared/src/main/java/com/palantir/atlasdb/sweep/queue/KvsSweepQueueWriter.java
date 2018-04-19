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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableTargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TableReferenceAndCell;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.schema.generated.SweepableTimestampsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.TableMetadata;

public class KvsSweepQueueWriter implements MultiTableSweepQueueWriter {
    private static final long PARTITION_FACTOR = 50_000L;
    // todo(gmaretic): temporarily constant
    private static final int SHARDS = 128;
    private static final byte[] DUMMY = new byte[0];

    private final KeyValueService kvs;
    private final TableReference sweepableCellsTableRef;
    private final TableReference sweepableTimestampsTableRef;

    private ConcurrentMap<TableReference, TableMetadataPersistence.SweepStrategy> sweepStrategyCache = new ConcurrentHashMap<>();

    public KvsSweepQueueWriter(KeyValueService kvs, TargetedSweepTableFactory tableFactory) {
        this.kvs = kvs;
        this.sweepableCellsTableRef = tableFactory.getSweepableCellsTable(null).getTableRef();
        this.sweepableTimestampsTableRef = tableFactory.getSweepableTimestampsTable(null).getTableRef();
        initialize();
    }

    public void initialize() {
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    @Override
    public void enqueue(TableReference tableRef, Collection<WriteInfo> writes) {
        if (getSweepStrategy(tableRef) == TableMetadataPersistence.SweepStrategy.NOTHING) {
            return;
        }
        kvs.put(sweepableCellsTableRef, batchWritesForSweepableCells(tableRef, writes), 0L);
        kvs.put(sweepableTimestampsTableRef, batchWritesForSweepableTimestamps(tableRef, writes), 0L);
    }

    private Map<Cell, byte[]> batchWritesForSweepableTimestamps(TableReference tableRef, Collection<WriteInfo> writes) {
        Map<Cell, byte[]> result = new HashMap<>();
        for (WriteInfo writeInfo : writes) {
            SweepableTimestampsTable.SweepableTimestampsRow row = SweepableTimestampsTable.SweepableTimestampsRow.of(
                    getShard(writeInfo.cell()), tsPartition(writeInfo.timestamp()), getSweepStrategy(tableRef).toString());

            SweepableTimestampsTable.SweepableTimestampsColumnValue column = SweepableTimestampsTable.SweepableTimestampsColumnValue.of(
                    SweepableTimestampsTable.SweepableTimestampsColumn.of(tsModulus(writeInfo.timestamp())),
                    DUMMY);

            Cell cell = Cell.create(row.persistToBytes(), column.persistColumnName());
            result.put(cell, column.persistValue());
        }
        return result;
    }

    Map<Cell, byte[]> batchWritesForSweepableCells(TableReference tableRef, Collection<WriteInfo> writes) {
        ImmutableMap.Builder resultBuilder = ImmutableMap.builder();
        long writeIndex = 0;
        for (WriteInfo writeInfo : writes) {
            TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                    .conservative(getSweepStrategy(tableRef) == TableMetadataPersistence.SweepStrategy.CONSERVATIVE)
                    .dedicatedRow(false)
                    .shard(getShard(writeInfo.cell()))
                    .dedicatedRowNumber(0)
                    .build();
            SweepableCellsTable.SweepableCellsRow row = SweepableCellsTable.SweepableCellsRow
                    .of(tsPartition(writeInfo.timestamp()), metadata.persistToBytes());

            SweepableCellsTable.SweepableCellsColumnValue columnValue = SweepableCellsTable.SweepableCellsColumnValue.of(
                    SweepableCellsTable.SweepableCellsColumn.of(tsModulus(writeInfo.timestamp()), writeIndex),
                    TableReferenceAndCell.of(tableRef, writeInfo.cell()).toString());

            Cell cell = Cell.create(row.persistToBytes(), columnValue.persistColumnName());
            resultBuilder.put(cell, columnValue.persistValue());
            writeIndex++;
        }
        return resultBuilder.build();
    }

    private int getShard(Cell cell) {
        int shard = cell.hashCode() % SHARDS;
        return (shard + SHARDS) % SHARDS;
    }

    private TableMetadataPersistence.SweepStrategy getSweepStrategy(TableReference tableRef) {
        return sweepStrategyCache.computeIfAbsent(tableRef, this::getSweepStrategyFromKvs);
    }

    private TableMetadataPersistence.SweepStrategy getSweepStrategyFromKvs(TableReference tableRef) {
        // todo(gmaretic): fail gracefully if we cannot hydrate?
        return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(kvs.getMetadataForTable(tableRef)).getSweepStrategy();
    }

    private long tsPartition(long timestamp) {
        return timestamp / PARTITION_FACTOR;
    }

    private long tsModulus(long timestamp) {
        return timestamp % PARTITION_FACTOR;
    }
}
