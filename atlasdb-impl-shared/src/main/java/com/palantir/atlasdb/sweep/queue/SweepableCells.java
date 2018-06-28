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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
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
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.CommitTsCache;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.logsafe.SafeArg;

public class SweepableCells extends KvsSweepQueueWriter {
    private final Logger log = LoggerFactory.getLogger(SweepableCells.class);
    private final CommitTsCache commitTsCache;

    public SweepableCells(KeyValueService kvs, WriteInfoPartitioner partitioner, TargetedSweepMetrics metrics) {
        super(kvs, TargetedSweepTableFactory.of().getSweepableCellsTable(null).getTableRef(), partitioner, metrics);
        this.commitTsCache = CommitTsCache.create(TransactionServices.createTransactionService(kvs));
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
            long sweepTs) {
        SweepableCellsTable.SweepableCellsRow row = computeRow(partitionFine, shardStrategy);
        RowColumnRangeIterator resultIterator = getRowColumnRange(row, partitionFine, minTsExclusive, sweepTs);
        PeekingIterator<Map.Entry<Cell, Value>> peekingResultIterator = Iterators.peekingIterator(resultIterator);
        Multimap<Long, WriteInfo> writesByStartTs = getBatchOfWrites(row, peekingResultIterator, sweepTs);
        maybeMetrics.ifPresent(metrics -> metrics.updateEntriesRead(shardStrategy, writesByStartTs.size()));
        log.info("Read {} entries from the sweep queue.", SafeArg.of("number", writesByStartTs.size()));
        TimestampsToSweep tsToSweep = getTimestampsToSweepDescendingAndCleanupAborted(shardStrategy,
                minTsExclusive, sweepTs, writesByStartTs);
        Collection<WriteInfo> writes = getWritesToSweep(writesByStartTs, tsToSweep.timestampsDescending());
        long lastSweptTs = getLastSweptTs(tsToSweep, peekingResultIterator, partitionFine, sweepTs);
        return SweepBatch.of(writes, lastSweptTs);
    }

    private Multimap<Long, WriteInfo> getBatchOfWrites(SweepableCellsTable.SweepableCellsRow row,
            PeekingIterator<Map.Entry<Cell, Value>> resultIterator, long sweepTs) {
        Multimap<Long, WriteInfo> writesByStartTs = HashMultimap.create();
        while (resultIterator.hasNext() && writesByStartTs.size() < SweepQueueUtils.SWEEP_BATCH_SIZE) {
            Map.Entry<Cell, Value> entry = resultIterator.next();
            SweepableCellsTable.SweepableCellsColumn col = computeColumn(entry);
            long startTs = getTimestamp(row, col);
            if (knownToBeCommittedAfterSweepTs(startTs, sweepTs)) {
                writesByStartTs.put(startTs, getWriteInfo(startTs, entry.getValue()));
                // at this point we know any writes with a greater start timestamp will be filtered out, so we stop
                return writesByStartTs;
            }
            writesByStartTs.putAll(startTs, getWrites(row, col, entry.getValue()));
        }
        // there may be entries remaining with the same start timestamp as the last processed one. If that is the case
        // we want to include these ones as well. This is OK since there are at most MAX_CELLS_GENERIC - 1 of them.
        while (resultIterator.hasNext()) {
            Map.Entry<Cell, Value> entry = resultIterator.peek();
            SweepableCellsTable.SweepableCellsColumn col = computeColumn(entry);
            long timestamp = getTimestamp(row, col);
            if (writesByStartTs.containsKey(timestamp)) {
                writesByStartTs.putAll(timestamp, getWrites(row, col, entry.getValue()));
                resultIterator.next();
            } else {
                break;
            }
        }
        return writesByStartTs;
    }

    private RowColumnRangeIterator getRowColumnRange(SweepableCellsTable.SweepableCellsRow row, long partitionFine,
            long minTsExclusive, long maxTsExclusive) {
        return getRowsColumnRange(ImmutableList.of(row.persistToBytes()),
                columnsBetween(minTsExclusive + 1, maxTsExclusive, partitionFine), SweepQueueUtils.BATCH_SIZE_KVS);
    }

    private TimestampsToSweep getTimestampsToSweepDescendingAndCleanupAborted(ShardAndStrategy shardStrategy,
            long minTsExclusive, long sweepTs, Multimap<Long, WriteInfo> writesByStartTs) {
        Map<Long, Long> startToCommitTs = commitTsCache.loadBatch(writesByStartTs.keySet());
        Map<TableReference, Multimap<Cell, Long>> cellsToDelete = new HashMap<>();
        List<Long> committedTimestamps = new ArrayList<>();
        long lastSweptTs = minTsExclusive;
        boolean processedAll = true;

        List<Long> sortedStartTimestamps = startToCommitTs.keySet().stream().sorted().collect(Collectors.toList());
        for (long startTs : sortedStartTimestamps) {
            long commitTs = startToCommitTs.get(startTs);
            if (commitTs == TransactionConstants.FAILED_COMMIT_TS) {
                lastSweptTs = startTs;
                writesByStartTs.get(startTs)
                        .forEach(write -> cellsToDelete
                                .computeIfAbsent(write.tableRef(), ignore -> HashMultimap.create())
                                .put(write.cell(), write.timestamp()));
            } else if (commitTs < sweepTs) {
                lastSweptTs = startTs;
                committedTimestamps.add(startTs);
            } else {
                processedAll = false;
                break;
            }
        }

        cellsToDelete.forEach((tableRef, multimap) -> {
            kvs.delete(tableRef, multimap);
            maybeMetrics.ifPresent(metrics -> metrics.updateAbortedWritesDeleted(shardStrategy, multimap.size()));
            log.info("Deleted {} aborted writes from table {}.", SafeArg.of("number", multimap.size()),
                    LoggingArgs.tableRef(tableRef));
        });

        return TimestampsToSweep.of(Lists.reverse(committedTimestamps), lastSweptTs, processedAll);
    }

    private Collection<WriteInfo> getWritesToSweep(Multimap<Long, WriteInfo> writesByStartTs, List<Long> startTs) {
        Map<CellReference, WriteInfo> writesToSweepFor = new HashMap<>();
        startTs.stream()
                .map(writesByStartTs::get)
                .flatMap(Collection::stream)
                .forEach(write -> writesToSweepFor
                        .putIfAbsent(write.writeRef().cellReference(), write));
        return writesToSweepFor.values();
    }

    private long getLastSweptTs(TimestampsToSweep startTsCommitted, Iterator<Map.Entry<Cell, Value>> resultIterator,
            long partitionFine, long maxTsExclusive) {
        if (startTsCommitted.processedAll() && exhaustedAllColumns(resultIterator)) {
            return lastGuaranteedSwept(partitionFine, maxTsExclusive);
        } else {
            return startTsCommitted.maxSwept();
        }
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

    private boolean knownToBeCommittedAfterSweepTs(long startTs, long sweepTs) {
        return commitTsCache.loadIfCached(startTs).map(commitTs -> commitTs >= sweepTs).orElse(false);
    }

    private int writeIndexToNumberOfDedicatedRows(long writeIndex) {
        return (int) -writeIndex;
    }

    private RowColumnRangeIterator getWithColumnRangeAll(Iterable<byte[]> rows) {
        return getRowsColumnRange(rows, SweepQueueUtils.ALL_COLUMNS, SweepQueueUtils.BATCH_SIZE_KVS);
    }

    private WriteInfo getWriteInfo(long timestamp, Value value) {
        return WriteInfo.of(SweepableCellsTable.SweepableCellsColumnValue.hydrateValue(value.getContents()), timestamp);
    }

    private boolean exhaustedAllColumns(Iterator<Map.Entry<Cell, Value>> resultIterator) {
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
        return addCell(info, write.writeRef(), dedicate, index / SweepQueueUtils.MAX_CELLS_DEDICATED,
                index % SweepQueueUtils.MAX_CELLS_DEDICATED);
    }

    private List<RangeRequest> rangeRequestsDedicatedRows(ShardAndStrategy shardAndStrategy, long partitionFine) {
        SweepableCellsTable.SweepableCellsRow row = computeRow(partitionFine, shardAndStrategy);
        RowColumnRangeIterator rowIterator = getWithColumnRangeAllForRow(row);
        List<RangeRequest> requests = new ArrayList<>();
        rowIterator.forEachRemaining(entry -> requests.addAll(rangeRequestsIfDedicated(row, computeColumn(entry))));
        return requests;
    }

    private Set<RangeRequest> rangeRequestsIfDedicated(SweepableCellsTable.SweepableCellsRow row,
            SweepableCellsTable.SweepableCellsColumn col) {
        if (!isReferenceToDedicatedRows(col)) {
            return ImmutableSet.of();
        }
        return computeDedicatedRows(row, col).stream()
                .map(bytes -> computeRangeRequestForRows(bytes, bytes))
                .collect(Collectors.toSet());
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

