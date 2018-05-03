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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
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
    public static final long SWEEP_BATCH_SIZE = 1000L;
    public static final int MINIMUM_WRITE_INDEX = -TargetedSweepMetadata.MAX_DEDICATED_ROWS;

    public SweepableCells(KeyValueService kvs, WriteInfoPartitioner partitioner) {
        super(kvs, TargetedSweepTableFactory.of().getSweepableCellsTable(null).getTableRef(), partitioner);
    }

    @Override
    void populateCells(PartitionInfo partitionInfo, List<WriteInfo> writes, Map<Cell, byte[]> cells) {
        boolean dedicate = writes.size() > MAX_CELLS_GENERIC;

        if (dedicate) {
            cells = addReferenceToDedicatedRows(partitionInfo, writes, cells);
        }

        long index = 0;
        for (WriteInfo write : writes) {
            addWrite(partitionInfo, write, dedicate, index, cells);
            index++;
        }
    }

    private Map<Cell, byte[]> addReferenceToDedicatedRows(PartitionInfo info, List<WriteInfo> writes,
            Map<Cell, byte[]> cells) {
        return addCell(info, WriteReference.DUMMY, false, 0, entryIndicatingNumberOfRequiredRows(writes), cells);
    }

    private long entryIndicatingNumberOfRequiredRows(List<WriteInfo> writes) {
        return -(1 + (writes.size() - 1) / MAX_CELLS_DEDICATED);
    }

    private Map<Cell, byte[]> addCell(PartitionInfo info, WriteReference writeRef, boolean isDedicatedRow,
            long dedicatedRowNumber, long writeIndex, Map<Cell, byte[]> cells) {
        SweepableCellsTable.SweepableCellsRow row = computeRow(info, isDedicatedRow, dedicatedRowNumber);
        SweepableCellsTable.SweepableCellsColumnValue colVal = createColVal(info.timestamp(), writeIndex, writeRef);
        cells.put(SweepQueueUtils.toCell(row, colVal), colVal.persistValue());
        return cells;
    }

    private SweepableCellsTable.SweepableCellsRow computeRow(PartitionInfo info, boolean isDedicatedRow,
            long dedicatedRowNumber) {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(info.isConservative().isTrue())
                .dedicatedRow(isDedicatedRow)
                .shard(info.shard())
                .dedicatedRowNumber(dedicatedRowNumber)
                .build();

        long tsOrPartition = getTimestampOrPartition(info, isDedicatedRow);
        return SweepableCellsTable.SweepableCellsRow.of(tsOrPartition, metadata.persistToBytes());
    }

    private long getTimestampOrPartition(PartitionInfo info, boolean isDedicatedRow) {
        return isDedicatedRow ? info.timestamp() : SweepQueueUtils.tsPartitionFine(info.timestamp());
    }

    private SweepableCellsTable.SweepableCellsColumnValue createColVal(long ts, long index, WriteReference writeRef) {
        SweepableCellsTable.SweepableCellsColumn col = SweepableCellsTable.SweepableCellsColumn.of(tsMod(ts), index);
        return SweepableCellsTable.SweepableCellsColumnValue.of(col, writeRef);
    }

    private static long tsMod(long timestamp) {
        return timestamp % SweepQueueUtils.TS_FINE_GRANULARITY;
    }

    SweepBatch getBatchForPartition(ShardAndStrategy shardStrategy, long partitionFine, long minTsExclusive,
            long maxTsExclusive) {
        if (inconsistentBounds(minTsExclusive, maxTsExclusive)) {
            return  SweepBatch.of(ImmutableList.of(), minTsExclusive);
        }

        SweepableCellsTable.SweepableCellsRow row = computeRow(partitionFine, shardStrategy);
        RowColumnRangeIterator resultIterator = getRowColumnRange(row, partitionFine, minTsExclusive, maxTsExclusive);

        Map<CellReference, WriteInfo> writes = new HashMap<>();
        long lastSweptTs = 0L;

        while (resultIterator.hasNext() && writes.size() < SWEEP_BATCH_SIZE) {
            Map.Entry<Cell, Value> entry = resultIterator.next();
            populateWrites(row, entry, writes);
            lastSweptTs = getTimestamp(row, computeColumn(entry));
        }

        if (exhaustedAllColumns(resultIterator)) {
            lastSweptTs = lastGuaranteedSwept(partitionFine, maxTsExclusive);
        }

        return SweepBatch.of(writes.values(), lastSweptTs);
    }

    private boolean inconsistentBounds(long minTsExclusive, long maxTsExclusive) {
        return minTsExclusive + 1 >= maxTsExclusive;
    }

    private SweepableCellsTable.SweepableCellsRow computeRow(long partitionFine, ShardAndStrategy shardStrategy) {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(shardStrategy.isConservative())
                .dedicatedRow(false)
                .shard(shardStrategy.shard())
                .dedicatedRowNumber(0)
                .build();

        return SweepableCellsTable.SweepableCellsRow.of(partitionFine, metadata.persistToBytes());
    }

    private RowColumnRangeIterator getRowColumnRange(SweepableCellsTable.SweepableCellsRow row, long partitionFine,
            long minTsExclusive, long maxTsExclusive) {
        return getRowsColumnRange(ImmutableList.of(row.persistToBytes()),
                columnsBetween(minTsExclusive + 1, maxTsExclusive, partitionFine), MAX_CELLS_DEDICATED);
    }


    private void populateWrites(SweepableCellsTable.SweepableCellsRow row, Map.Entry<Cell, Value> entry,
            Map<CellReference, WriteInfo> writes) {
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
            SweepableCellsTable.SweepableCellsColumn col, Map<CellReference, WriteInfo> writes) {
        List<byte[]> dedicatedRows = computeDedicatedRows(row, col);
        RowColumnRangeIterator iterator = getWithColumnRangeAll(dedicatedRows);
        iterator.forEachRemaining(entry -> addWriteFromValue(getTimestamp(row, col), entry.getValue(), writes));
    }

    private List<byte[]> computeDedicatedRows(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col) {
        TargetedSweepMetadata metadata = TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(row.getMetadata());
        long timestamp = getTimestamp(row, col);
        int numberOfDedicatedRows = writeIndexToNumberOfDedicatedRows(col.getWriteIndex());
        List<byte[]> dedicatedRows = new ArrayList<>();

        for (int i = 0; i < numberOfDedicatedRows; i++) {
            byte[]  dedicatedMetadata = ImmutableTargetedSweepMetadata.builder()
                    .from(metadata)
                    .dedicatedRow(true)
                    .dedicatedRowNumber(i)
                    .build()
                    .persistToBytes();
            dedicatedRows.add(SweepableCellsTable.SweepableCellsRow.of(timestamp, dedicatedMetadata).persistToBytes());
        }
        return dedicatedRows;
    }

    private long getTimestamp(SweepableCellsTable.SweepableCellsRow row, SweepableCellsTable.SweepableCellsColumn col) {
        return row.getTimestampPartition() * SweepQueueUtils.TS_FINE_GRANULARITY + col.getTimestampModulus();
    }

    private int writeIndexToNumberOfDedicatedRows(long writeIndex) {
        return (int) -writeIndex;
    }

    private RowColumnRangeIterator getWithColumnRangeAll(Iterable<byte[]> rows) {
        return getRowsColumnRange(rows, ALL_COLUMNS, MAX_CELLS_DEDICATED);
    }

    private void addWriteFromValue(long timestamp, Value value, Map<CellReference, WriteInfo> writes) {
        WriteReference writeRef = SweepableCellsTable.SweepableCellsColumnValue.hydrateValue(value.getContents());
        updateLatestForCell(timestamp, writeRef, writes);
    }

    private boolean exhaustedAllColumns(RowColumnRangeIterator resultIterator) {
        return !resultIterator.hasNext();
    }

    private long lastGuaranteedSwept(long partitionFine, long maxTsExclusive) {
        return Math.min(SweepQueueUtils.maxForFinePartition(partitionFine), maxTsExclusive - 1);
    }

    void deleteDedicatedRows(ShardAndStrategy shardAndStrategy, long partitionFine) {
        rangeRequestsDedicatedRows(shardAndStrategy, partitionFine).forEach(this::deleteRange);
    }

    void deleteNonDedicatedRow(ShardAndStrategy shardAndStrategy, long partitionFine) {
        deleteRange(rangeRequestNonDedicatedRow(shardAndStrategy, partitionFine));
    }

    private void addWrite(PartitionInfo info, WriteInfo write, boolean dedicate, long index, Map<Cell, byte[]> cells) {
        addCell(info, write.writeRef(), dedicate, index / MAX_CELLS_DEDICATED, index % MAX_CELLS_DEDICATED, cells);
    }

    private void updateLatestForCell(long ts, WriteReference writeRef, Map<CellReference, WriteInfo> writes) {
        WriteInfo newWrite = WriteInfo.of(writeRef, ts);
        writes.merge(writeRef.cellReference(), newWrite,
                (writeOne, writeTwo) -> writeOne.timestamp() > writeTwo.timestamp() ? writeOne : writeTwo);
    }

    private List<RangeRequest> rangeRequestsDedicatedRows(ShardAndStrategy shardAndStrategy, long partitionFine) {
        SweepableCellsTable.SweepableCellsRow startingRow = computeRow(partitionFine, shardAndStrategy);
        RowColumnRangeIterator rowIterator = getWithColumnRangeAllForRow(startingRow);
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

    private SweepableCellsTable.SweepableCellsColumn computeColumn(Map.Entry<Cell, Value> entry) {
        return SweepableCellsTable.SweepableCellsColumn.BYTES_HYDRATOR
                .hydrateFromBytes(entry.getKey().getColumnName());
    }

    private RowColumnRangeIterator getWithColumnRangeAllForRow(SweepableCellsTable.SweepableCellsRow row) {
        return getWithColumnRangeAll(ImmutableList.of(row.persistToBytes()));
    }

    private ColumnRangeSelection columnsBetween(long startTsInclusive, long endTsExclusive, long partitionFine) {
        long startIncl = exactColumnOrElseBeginningOfRow(startTsInclusive, partitionFine);
        byte[] startCol = SweepableCellsTable.SweepableCellsColumn.of(startIncl, MINIMUM_WRITE_INDEX).persistToBytes();
        long endExcl = exactColumnOrElseOneBeyondEndOfRow(endTsExclusive, partitionFine);
        byte[] endCol = SweepableCellsTable.SweepableCellsColumn.of(endExcl, MINIMUM_WRITE_INDEX).persistToBytes();
        return new ColumnRangeSelection(startCol, endCol);
    }

    private long exactColumnOrElseOneBeyondEndOfRow(long endTsExclusive, long partitionFine) {
        return Math.min(endTsExclusive - SweepQueueUtils.minForFinePartition(partitionFine),
                SweepQueueUtils.TS_FINE_GRANULARITY);
    }

    private long exactColumnOrElseBeginningOfRow(long startTsInclusive, long partitionFine) {
        return Math.max(startTsInclusive - SweepQueueUtils.minForFinePartition(partitionFine), 0);
    }

    private static ColumnRangeSelection allPossibleColumns() {
        byte[] startCol = SweepableCellsTable.SweepableCellsColumn.of(0L, MINIMUM_WRITE_INDEX)
                .persistToBytes();
        byte[] endCol = SweepableCellsTable.SweepableCellsColumn.of(SweepQueueUtils.TS_FINE_GRANULARITY, 0L)
                .persistToBytes();
        return new ColumnRangeSelection(startCol, endCol);
    }
}

