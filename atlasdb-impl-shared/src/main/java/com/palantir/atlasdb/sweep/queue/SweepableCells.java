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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.ImmutableTargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.CommitTsLoader;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionServices;

import gnu.trove.set.hash.TLongHashSet;

public class SweepableCells extends KvsSweepQueueWriter {
    private final CommitTsLoader commitTsLoader;

    public SweepableCells(KeyValueService kvs, WriteInfoPartitioner partitioner) {
        super(kvs, TargetedSweepTableFactory.of().getSweepableCellsTable(null).getTableRef(), partitioner);
        commitTsLoader = CommitTsLoader.create(TransactionServices.createTransactionService(kvs), new TLongHashSet());
    }

    @Override
    Map<Cell, byte[]> populateCells(PartitionInfo partitionInfo, List<WriteInfo> writes) {
        Map<Cell, byte[]> cells = new HashMap<>();
        boolean dedicate = writes.size() > SweepQueueUtils.MAX_CELLS_GENERIC;

        if (dedicate) {
            cells.putAll(addReferenceToDedicatedRows(partitionInfo, writes));
        }

        long index = 0;
        for (WriteInfo write : writes) {
            cells.putAll(addWrite(partitionInfo, write, dedicate, index));
            index++;
        }
        return cells;
    }

    private Map<Cell, byte[]> addReferenceToDedicatedRows(PartitionInfo info, List<WriteInfo> writes) {
        return addCell(info, WriteReference.DUMMY, false, 0, entryIndicatingNumberOfRequiredRows(writes));
    }

    private long entryIndicatingNumberOfRequiredRows(List<WriteInfo> writes) {
        return -(1 + (writes.size() - 1) / SweepQueueUtils.MAX_CELLS_DEDICATED);
    }

    private Map<Cell, byte[]> addCell(PartitionInfo info, WriteReference writeRef, boolean isDedicatedRow,
            long dedicatedRowNumber, long writeIndex) {
        SweepableCellsTable.SweepableCellsRow row = computeRow(info, isDedicatedRow, dedicatedRowNumber);
        SweepableCellsTable.SweepableCellsColumnValue colVal = createColVal(info.timestamp(), writeIndex, writeRef);
        return ImmutableMap.of(SweepQueueUtils.toCell(row, colVal), colVal.persistValue());
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

    private SweepableCellsTable.SweepableCellsRow computeRow(long partitionFine, ShardAndStrategy shardStrategy) {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(shardStrategy.isConservative())
                .dedicatedRow(false)
                .shard(shardStrategy.shard())
                .dedicatedRowNumber(0)
                .build();

        return SweepableCellsTable.SweepableCellsRow.of(partitionFine, metadata.persistToBytes());
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

        Multimap<Long, WriteInfo> startTsWrites = HashMultimap.create();
        long lastSweptTs = 0L;

        while (resultIterator.hasNext() && startTsWrites.size() < SweepQueueUtils.SWEEP_BATCH_SIZE) {
            Map.Entry<Cell, Value> entry = resultIterator.next();
            SweepableCellsTable.SweepableCellsColumn col = computeColumn(entry);

            lastSweptTs = getTimestamp(row, col);
            startTsWrites.putAll(lastSweptTs, getWrites(row, col, entry.getValue()));
        }

        List<Long> committedTs = getCommittedTimestampsAndCleanupAborted(startTsWrites);
        committedTs.sort(Comparator.reverseOrder());

        if (exhaustedAllColumns(resultIterator)) {
            lastSweptTs = lastGuaranteedSwept(partitionFine, maxTsExclusive);
        }

        Map<CellReference, WriteInfo> writesToSweepFor = new HashMap<>();

        committedTs.stream()
                .map(startTsWrites::get)
                .flatMap(Collection::stream)
                .forEach(write -> writesToSweepFor
                        .putIfAbsent(write.writeRef().cellReference(), write));

        return SweepBatch.of(writesToSweepFor.values(), lastSweptTs);
    }

    private boolean inconsistentBounds(long minTsExclusive, long maxTsExclusive) {
        return minTsExclusive + 1 >= maxTsExclusive;
    }

    private RowColumnRangeIterator getRowColumnRange(SweepableCellsTable.SweepableCellsRow row, long partitionFine,
            long minTsExclusive, long maxTsExclusive) {
        return getRowsColumnRange(ImmutableList.of(row.persistToBytes()),
                columnsBetween(minTsExclusive + 1, maxTsExclusive, partitionFine), SweepQueueUtils.MAX_CELLS_DEDICATED);
    }

    private List<Long> getCommittedTimestampsAndCleanupAborted(Multimap<Long, WriteInfo> startTsWrites) {
        Map<Long, Long> startToCommitTs = commitTsLoader.loadBatch(startTsWrites.keySet());
        Map<TableReference, Multimap<Cell, Long>> cellsToDelete = new HashMap<>();
        List<Long> committedTimestamps = new ArrayList<>();

        for (Map.Entry<Long, Long> entry: startToCommitTs.entrySet()) {
            if (entry.getValue() == TransactionConstants.FAILED_COMMIT_TS) {
                startTsWrites.get(entry.getKey())
                        .forEach(write -> cellsToDelete
                                .computeIfAbsent(write.writeRef().tableRef(), ignore -> HashMultimap.create())
                                .put(write.writeRef().cell(), write.timestamp()));
            } else {
                committedTimestamps.add(entry.getKey());
            }
        }
        cellsToDelete.forEach(kvs::delete);

        return committedTimestamps;
    }

    private List<WriteInfo> getWrites(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col, Value value) {
        List<WriteInfo> writes = new ArrayList<>();
        if (isReferenceToDedicatedRows(col)) {
            writes = addWritesFromDedicated(row, col, writes);
        } else {
            writes.add(getWriteInfo(getTimestamp(row, col), value));
        }
        return writes;
    }

    private boolean isReferenceToDedicatedRows(SweepableCellsTable.SweepableCellsColumn col) {
        return col.getWriteIndex() < 0;
    }

    private List<WriteInfo> addWritesFromDedicated(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col, List<WriteInfo> writes) {
        List<byte[]> dedicatedRows = computeDedicatedRows(row, col);
        RowColumnRangeIterator iterator = getWithColumnRangeAll(dedicatedRows);
        iterator.forEachRemaining(entry -> writes.add(getWriteInfo(getTimestamp(row, col), entry.getValue())));
        return writes;
    }

    private List<byte[]> computeDedicatedRows(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col) {
        TargetedSweepMetadata metadata = TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(row.getMetadata());
        long timestamp = getTimestamp(row, col);
        int numberOfDedicatedRows = writeIndexToNumberOfDedicatedRows(col.getWriteIndex());
        List<byte[]> dedicatedRows = new ArrayList<>();

        for (int i = 0; i < numberOfDedicatedRows; i++) {
            byte[] dedicatedMetadata = ImmutableTargetedSweepMetadata.builder()
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
        return getRowsColumnRange(rows, SweepQueueUtils.ALL_COLUMNS, SweepQueueUtils.MAX_CELLS_DEDICATED);
    }

    private WriteInfo getWriteInfo(long timestamp, Value value) {
        return WriteInfo.of(SweepableCellsTable.SweepableCellsColumnValue.hydrateValue(value.getContents()), timestamp);
    }

    private boolean exhaustedAllColumns(RowColumnRangeIterator resultIterator) {
        return !resultIterator.hasNext();
    }

    private long lastGuaranteedSwept(long partitionFine, long maxTsExclusive) {
        return Math.min(SweepQueueUtils.maxTsForFinePartition(partitionFine), maxTsExclusive - 1);
    }

    void deleteDedicatedRows(ShardAndStrategy shardAndStrategy, long partitionFine) {
        rangeRequestsDedicatedRows(shardAndStrategy, partitionFine).forEach(this::deleteRange);
    }

    void deleteNonDedicatedRow(ShardAndStrategy shardAndStrategy, long partitionFine) {
        deleteRange(rangeRequestNonDedicatedRow(shardAndStrategy, partitionFine));
    }

    private Map<Cell, byte[]> addWrite(PartitionInfo info, WriteInfo write, boolean dedicate, long index) {
        return addCell(info, write.writeRef(), dedicate, index / SweepQueueUtils.MAX_CELLS_DEDICATED, index % SweepQueueUtils.MAX_CELLS_DEDICATED);
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
        byte[] startCol = SweepableCellsTable.SweepableCellsColumn.of(startIncl, SweepQueueUtils.MINIMUM_WRITE_INDEX)
                .persistToBytes();
        long endExcl = exactColumnOrElseOneBeyondEndOfRow(endTsExclusive, partitionFine);
        byte[] endCol = SweepableCellsTable.SweepableCellsColumn.of(endExcl, SweepQueueUtils.MINIMUM_WRITE_INDEX)
                .persistToBytes();
        return new ColumnRangeSelection(startCol, endCol);
    }

    private long exactColumnOrElseOneBeyondEndOfRow(long endTsExclusive, long partitionFine) {
        return Math.min(endTsExclusive - SweepQueueUtils.minTsForFinePartition(partitionFine),
                SweepQueueUtils.TS_FINE_GRANULARITY);
    }

    private long exactColumnOrElseBeginningOfRow(long startTsInclusive, long partitionFine) {
        return Math.max(startTsInclusive - SweepQueueUtils.minTsForFinePartition(partitionFine), 0);
    }
}

