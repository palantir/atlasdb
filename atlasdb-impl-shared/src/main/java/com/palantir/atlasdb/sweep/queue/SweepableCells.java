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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.ImmutableTargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;

public class SweepableCells extends KvsSweepQueueWriter {
    static final int MAX_CELLS_GENERIC = 50;
    static final int MAX_CELLS_DEDICATED = 100_000;

    private final WriteInfoPartitioner partitioner;

    SweepableCells(KeyValueService kvs, WriteInfoPartitioner partitioner) {
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

        return SweepableCellsTable.SweepableCellsRow.of(getTimestampOrPartition(info, dedicate),
                metadata.persistToBytes());
    }

    private SweepableCellsTable.SweepableCellsColumnValue createColVal(long ts, long index, WriteReference writeRef) {
        SweepableCellsTable.SweepableCellsColumn col = SweepableCellsTable.SweepableCellsColumn.of(tsMod(ts), index);
        return SweepableCellsTable.SweepableCellsColumnValue.of(col, writeRef);
    }

    private long getTimestampOrPartition(PartitionInfo info, boolean dedicate) {
        return dedicate ? info.timestamp() : SweepQueueUtils.tsPartitionFine(info.timestamp());
    }

    private long requiredDedicatedRows(List<WriteInfo> writes) {
        return 1 + (writes.size() - 1) / MAX_CELLS_DEDICATED;
    }

    private static long tsMod(long timestamp) {
        return timestamp % SweepQueueUtils.TS_FINE_GRANULARITY;
    }

    private static final ColumnRangeSelection ALL_COLUMNS = allPossibleColumns();

    List<WriteInfo> getLatestWrites(long partitionFine, ShardAndStrategy shardAndStrategy) {
        SweepableCellsTable.SweepableCellsRow row = computeRow(partitionFine, shardAndStrategy);

        RowColumnRangeIterator resultIterator = getAllColumns(kvs, row);

        Map<WriteReference, Long> results = new HashMap<>();
        resultIterator.forEachRemaining(entry -> populateResults(kvs, row, entry, results));

        return results.entrySet().stream()
                .map(entry -> WriteInfo.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private static SweepableCellsTable.SweepableCellsRow computeRow(long partitionFine,
            ShardAndStrategy shardAndStrategy) {
        TargetedSweepMetadata metadata = getDefaultMetadata(shardAndStrategy);
        return SweepableCellsTable.SweepableCellsRow.of(partitionFine, metadata.persistToBytes());
    }

    private static TargetedSweepMetadata getDefaultMetadata(ShardAndStrategy shardAndStrategy) {
        return ImmutableTargetedSweepMetadata.builder()
                .conservative(shardAndStrategy.isConservative())
                .dedicatedRow(false)
                .shard(shardAndStrategy.shard())
                .dedicatedRowNumber(0)
                .build();
    }

    private RowColumnRangeIterator getAllColumns(KeyValueService kvs, SweepableCellsTable.SweepableCellsRow row) {
        return getAllColumns(kvs, ImmutableList.of(row.persistToBytes()));
    }

    private RowColumnRangeIterator getAllColumns(KeyValueService kvs, Iterable<byte[]> rows) {
        return kvs.getRowsColumnRange(tableRef, rows, ALL_COLUMNS, 100_000, SweepQueueUtils.CAS_TS);
    }

    private void populateResults(KeyValueService kvs, SweepableCellsTable.SweepableCellsRow row, Map.Entry<Cell, Value> entry,
            Map<WriteReference, Long> results) {
        SweepableCellsTable.SweepableCellsColumn col = computeColumn(entry);

        if (isReferenceToDedicatedRows(col)) {
            populateFromDedicatedRows(kvs, row, col, results);
        } else {
            populateFromValue(getTimestamp(row, col), entry.getValue(), results);
        }
    }

    private static SweepableCellsTable.SweepableCellsColumn computeColumn(Map.Entry<Cell, Value> entry) {
        return SweepableCellsTable.SweepableCellsColumn.BYTES_HYDRATOR
                .hydrateFromBytes(entry.getKey().getColumnName());
    }

    private static boolean isReferenceToDedicatedRows(SweepableCellsTable.SweepableCellsColumn col) {
        return col.getWriteIndex() < 0;
    }

    private void populateFromDedicatedRows(KeyValueService kvs, SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col, Map<WriteReference, Long> results) {
        List<byte[]> dedicatedRows = computeDedicatedRows(row, col);
        RowColumnRangeIterator iterator = getAllColumns(kvs, dedicatedRows);
        iterator.forEachRemaining(entry -> populateFromValue(getTimestamp(row, col), entry.getValue(), results));
    }

    RangeRequest rangeRequestForNonDedicatedRow(ShardAndStrategy shardAndStrategy, long partitionFine) {
        byte[] row = computeRow(partitionFine, shardAndStrategy).persistToBytes();
        return computeRangeRequestForRows(row, row);
    }

    List<RangeRequest> rangeRequestsForDedicatedRows(ShardAndStrategy shardAndStrategy, long partitionFine) {
        SweepableCellsTable.SweepableCellsRow startingRow = computeRow(partitionFine, shardAndStrategy);
        RowColumnRangeIterator rowIterator = getAllColumns(kvs, startingRow);
        List<RangeRequest> requests = new ArrayList<>();
        rowIterator.forEachRemaining(entry -> addRangeRequestIfDedicated(startingRow, computeColumn(entry), requests));

        return requests;

    }

    private static void addRangeRequestIfDedicated(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col, List<RangeRequest> requests) {
        if (!isReferenceToDedicatedRows(col)) {
            return;
        }
        List<byte[]> dedicatedRows = computeDedicatedRows(row, col);
        byte[] startRowInclusive = dedicatedRows.get(0);
        byte[] endRowInclusive = dedicatedRows.get(dedicatedRows.size() - 1);
        requests.add(computeRangeRequestForRows(startRowInclusive, endRowInclusive));
    }

    private static RangeRequest computeRangeRequestForRows(byte[] startRowInclusive, byte[] endRowInclusive) {
        return RangeRequest.builder()
                .startRowInclusive(startRowInclusive)
                .endRowExclusive(RangeRequests.nextLexicographicName(endRowInclusive))
                .retainColumns(ColumnSelection.all())
                .build();
    }

    static List<byte[]> computeDedicatedRows(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col) {
        TargetedSweepMetadata metadata = TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(row.getMetadata());
        long timestamp = getTimestamp(row, col);
        int numberOfDedicatedRows = (int) -col.getWriteIndex();
        List<byte[]> dedicatedRows = new ArrayList<>();

        for (int i = 0; i < numberOfDedicatedRows; i++) {
            byte[] metadataBytes = ImmutableTargetedSweepMetadata.builder()
                    .from(metadata)
                    .dedicatedRow(true)
                    .dedicatedRowNumber(i)
                    .build()
                    .persistToBytes();
            dedicatedRows.add(SweepableCellsTable.SweepableCellsRow.of(timestamp, metadataBytes).persistToBytes());
        }
        return dedicatedRows;
    }

    private static long getTimestamp(SweepableCellsTable.SweepableCellsRow row, SweepableCellsTable.SweepableCellsColumn col) {
        return row.getTimestampPartition() * SweepQueueUtils.TS_FINE_GRANULARITY + col.getTimestampModulus();
    }

    private static void populateFromValue(long timestamp, Value value, Map<WriteReference, Long> results) {
        WriteReference writeRef = SweepableCellsTable.SweepableCellsColumnValue.hydrateValue(value.getContents());
        addIfMaxForCell(timestamp, writeRef, results);
    }

    private static void addIfMaxForCell(long ts, WriteReference writeRef, Map<WriteReference, Long> result) {
        result.merge(writeRef, ts, Math::max);
    }

    private static ColumnRangeSelection allPossibleColumns() {
        byte[] startCol = SweepableCellsTable.SweepableCellsColumn.of(0L, -TargetedSweepMetadata.MAX_DEDICATED_ROWS)
                .persistToBytes();
        byte[] endCol = SweepableCellsTable.SweepableCellsColumn.of(SweepQueueUtils.TS_FINE_GRANULARITY, 0)
                .persistToBytes();
        return new ColumnRangeSelection(startCol, endCol);
    }
}

