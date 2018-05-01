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

import com.google.common.annotations.VisibleForTesting;
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
    @VisibleForTesting
    static final int MAX_CELLS_GENERIC = 50;
    @VisibleForTesting
    static final int MAX_CELLS_DEDICATED = 100_000;
    private static final ColumnRangeSelection ALL_COLUMNS = allPossibleColumns();

    public SweepableCells(KeyValueService kvs, WriteInfoPartitioner partitioner) {
        super(kvs, TargetedSweepTableFactory.of().getSweepableCellsTable(null).getTableRef(), partitioner);
    }

    @Override
    void populateCells(PartitionInfo partitionInfo, List<WriteInfo> writes, Map<Cell, byte[]> cells) {
        boolean dedicate = writes.size() > MAX_CELLS_GENERIC;

        if (dedicate) {
            addReferenceToDedicatedRows(partitionInfo, writes, cells);
        }

        long index = 0;
        for (WriteInfo write : writes) {
            addWrite(partitionInfo, write, dedicate, index, cells);
            index++;
        }
    }

    List<WriteInfo> getWritesFromPartition(long partitionFine, ShardAndStrategy shardAndStrategy) {
        SweepableCellsTable.SweepableCellsRow row = computeRow(partitionFine, shardAndStrategy);
        RowColumnRangeIterator resultIterator = getColumnRangeAllForRow(row);

        Map<WriteReference, Long> writes = new HashMap<>();
        resultIterator.forEachRemaining(entry -> populateWrites(row, entry, writes));

        return writes.entrySet().stream()
                .map(entry -> WriteInfo.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    void deleteDedicatedRows(ShardAndStrategy shardAndStrategy, long partitionFine) {
        rangeRequestsDedicatedRows(shardAndStrategy, partitionFine).forEach(this::deleteRange);
    }

    void deleteNonDedicatedRow(ShardAndStrategy shardAndStrategy, long partitionFine) {
        deleteRange(rangeRequestNonDedicatedRow(shardAndStrategy, partitionFine));
    }

    private void addReferenceToDedicatedRows(PartitionInfo info, List<WriteInfo> writes, Map<Cell, byte[]> cells) {
        addCell(info, WriteReference.DUMMY, false, 0, -numberDedicatedRows(writes), cells);
    }

    private void addWrite(PartitionInfo info, WriteInfo write, boolean dedicate, long index, Map<Cell, byte[]> cells) {
        addCell(info, write.writeRef(), dedicate, index / MAX_CELLS_DEDICATED, index % MAX_CELLS_DEDICATED, cells);
    }

    private void addCell(PartitionInfo info, WriteReference writeRef, boolean dedicate, long dedicatedRow,
            long index, Map<Cell, byte[]> cells) {
        SweepableCellsTable.SweepableCellsRow row = computeRow(info, dedicate, dedicatedRow);
        SweepableCellsTable.SweepableCellsColumnValue colVal = createColVal(info.timestamp(), index, writeRef);
        cells.put(SweepQueueUtils.toCell(row, colVal), colVal.persistValue());
    }

    private long numberDedicatedRows(List<WriteInfo> writes) {
        return 1 + (writes.size() - 1) / MAX_CELLS_DEDICATED;
    }

    private static long tsMod(long timestamp) {
        return timestamp % SweepQueueUtils.TS_FINE_GRANULARITY;
    }

    private SweepableCellsTable.SweepableCellsColumnValue createColVal(long ts, long index, WriteReference writeRef) {
        SweepableCellsTable.SweepableCellsColumn col = SweepableCellsTable.SweepableCellsColumn.of(tsMod(ts), index);
        return SweepableCellsTable.SweepableCellsColumnValue.of(col, writeRef);
    }

    private void populateWrites(SweepableCellsTable.SweepableCellsRow row, Map.Entry<Cell, Value> entry,
            Map<WriteReference, Long> writes) {
        SweepableCellsTable.SweepableCellsColumn col = computeColumn(entry);

        if (isReferenceToDedicatedRows(col)) {
            addWritesFromDedicated(row, col, writes);
        } else {
            addWriteFromValue(getTimestamp(row, col), entry.getValue(), writes);
        }
    }

    private boolean isReferenceToDedicatedRows(SweepableCellsTable.SweepableCellsColumn col) {
        return col.getWriteIndex() < 0;
    }

    private void addWritesFromDedicated(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col, Map<WriteReference, Long> writes) {
        List<byte[]> dedicatedRows = computeDedicatedRows(row, col);
        RowColumnRangeIterator iterator = getColumnRangeAll(dedicatedRows);
        iterator.forEachRemaining(entry -> addWriteFromValue(getTimestamp(row, col), entry.getValue(), writes));
    }

    private void addWriteFromValue(long timestamp, Value value, Map<WriteReference, Long> writes) {
        WriteReference writeRef = SweepableCellsTable.SweepableCellsColumnValue.hydrateValue(value.getContents());
        addIfMaxTimestampForCell(timestamp, writeRef, writes);
    }

    private void addIfMaxTimestampForCell(long ts, WriteReference writeRef, Map<WriteReference, Long> result) {
        result.merge(writeRef, ts, Math::max);
    }

    private List<RangeRequest> rangeRequestsDedicatedRows(ShardAndStrategy shardAndStrategy, long partitionFine) {
        SweepableCellsTable.SweepableCellsRow startingRow = computeRow(partitionFine, shardAndStrategy);
        RowColumnRangeIterator rowIterator = getColumnRangeAllForRow(startingRow);
        List<RangeRequest> requests = new ArrayList<>();
        rowIterator.forEachRemaining(entry -> addRangeRequestIfDedicated(startingRow, computeColumn(entry), requests));
        return requests;
    }

    private void addRangeRequestIfDedicated(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col, List<RangeRequest> requests) {
        if (!isReferenceToDedicatedRows(col)) {
            return;
        }
        List<byte[]> dedicatedRows = computeDedicatedRows(row, col);
        byte[] startRowInclusive = dedicatedRows.get(0);
        byte[] endRowInclusive = dedicatedRows.get(dedicatedRows.size() - 1);
        requests.add(computeRangeRequestForRows(startRowInclusive, endRowInclusive));
    }

    private RangeRequest rangeRequestNonDedicatedRow(ShardAndStrategy shardAndStrategy, long partitionFine) {
        byte[] row = computeRow(partitionFine, shardAndStrategy).persistToBytes();
        return computeRangeRequestForRows(row, row);
    }

    private RangeRequest computeRangeRequestForRows(byte[] startRowInclusive, byte[] endRowInclusive) {
        return RangeRequest.builder()
                .startRowInclusive(startRowInclusive)
                .endRowExclusive(RangeRequests.nextLexicographicName(endRowInclusive))
                .retainColumns(ColumnSelection.all())
                .build();
    }

    private SweepableCellsTable.SweepableCellsRow computeRow(PartitionInfo info, boolean dedicate, long dedicatedRow) {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(info.isConservative().isTrue())
                .dedicatedRow(dedicate)
                .shard(info.shard())
                .dedicatedRowNumber(dedicatedRow)
                .build();

        return computeRow(getTimestampOrPartition(info, dedicate), metadata);
    }

    private SweepableCellsTable.SweepableCellsRow computeRow(long partitionFine, ShardAndStrategy shardStrategy) {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(shardStrategy.isConservative())
                .dedicatedRow(false)
                .shard(shardStrategy.shard())
                .dedicatedRowNumber(0)
                .build();

        return computeRow(partitionFine, metadata);
    }

    private SweepableCellsTable.SweepableCellsRow computeRow(long timestamp, TargetedSweepMetadata metadata) {
        return SweepableCellsTable.SweepableCellsRow.of(timestamp, metadata.persistToBytes());
    }

    private List<byte[]> computeDedicatedRows(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col) {
        TargetedSweepMetadata metadata = TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(row.getMetadata());
        long timestamp = getTimestamp(row, col);
        int numberOfDedicatedRows = (int) -col.getWriteIndex();
        List<byte[]> dedicatedRows = new ArrayList<>();

        for (int i = 0; i < numberOfDedicatedRows; i++) {
            TargetedSweepMetadata dedicatedMetadata = ImmutableTargetedSweepMetadata.builder()
                    .from(metadata)
                    .dedicatedRow(true)
                    .dedicatedRowNumber(i)
                    .build();
            dedicatedRows.add(computeRow(timestamp, dedicatedMetadata).persistToBytes());
        }
        return dedicatedRows;
    }

    private SweepableCellsTable.SweepableCellsColumn computeColumn(Map.Entry<Cell, Value> entry) {
        return SweepableCellsTable.SweepableCellsColumn.BYTES_HYDRATOR
                .hydrateFromBytes(entry.getKey().getColumnName());
    }

    private RowColumnRangeIterator getColumnRangeAllForRow(SweepableCellsTable.SweepableCellsRow row) {
        return getColumnRangeAll(ImmutableList.of(row.persistToBytes()));
    }

    private RowColumnRangeIterator getColumnRangeAll(Iterable<byte[]> rows) {
        return getRowsColumnRange(rows, ALL_COLUMNS, MAX_CELLS_DEDICATED);
    }

    private long getTimestamp(SweepableCellsTable.SweepableCellsRow row, SweepableCellsTable.SweepableCellsColumn col) {
        return row.getTimestampPartition() * SweepQueueUtils.TS_FINE_GRANULARITY + col.getTimestampModulus();
    }

    private long getTimestampOrPartition(PartitionInfo info, boolean dedicate) {
        return dedicate ? info.timestamp() : SweepQueueUtils.tsPartitionFine(info.timestamp());
    }

    private static ColumnRangeSelection allPossibleColumns() {
        byte[] startCol = SweepableCellsTable.SweepableCellsColumn.of(0L, -TargetedSweepMetadata.MAX_DEDICATED_ROWS)
                .persistToBytes();
        byte[] endCol = SweepableCellsTable.SweepableCellsColumn.of(SweepQueueUtils.TS_FINE_GRANULARITY, 0L)
                .persistToBytes();
        return new ColumnRangeSelection(startCol, endCol);
    }
}

