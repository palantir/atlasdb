/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep.queue;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ImmutableTargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.keyvalue.api.WriteReferencePersister;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable.SweepableCellsColumnValue;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable.SweepableCellsRow;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.CommitTsCache;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.id.SweepTableIndices;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.logsafe.SafeArg;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SweepableCells extends SweepQueueTable {
    private static final Logger log = LoggerFactory.getLogger(SweepableCells.class);
    private final CommitTsCache commitTsCache;
    private final WriteReferencePersister writeReferencePersister;

    private static final WriteReference DUMMY = WriteReference.of(
            TableReference.createFromFullyQualifiedName("dum.my"), Cell.create(new byte[] {0}, new byte[] {0}), false);

    public SweepableCells(
            KeyValueService kvs,
            WriteInfoPartitioner partitioner,
            TargetedSweepMetrics metrics,
            TransactionService transactionService) {
        super(kvs, TargetedSweepTableFactory.of().getSweepableCellsTable(null).getTableRef(), partitioner, metrics);
        this.commitTsCache = CommitTsCache.create(transactionService);
        this.writeReferencePersister = new WriteReferencePersister(new SweepTableIndices(kvs));
    }

    @Override
    Map<Cell, byte[]> populateReferences(PartitionInfo partitionInfo, List<WriteInfo> writes) {
        boolean dedicate = writes.size() > SweepQueueUtils.MAX_CELLS_GENERIC;
        if (dedicate) {
            return addReferenceToDedicatedRows(partitionInfo, writes);
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    Map<Cell, byte[]> populateCells(PartitionInfo partitionInfo, List<WriteInfo> writes) {
        Map<Cell, byte[]> cells = new HashMap<>();
        boolean dedicate = writes.size() > SweepQueueUtils.MAX_CELLS_GENERIC;

        long index = 0;
        for (WriteInfo write : writes) {
            cells.putAll(addWrite(partitionInfo, write, dedicate, index));
            index++;
        }
        return cells;
    }

    private Map<Cell, byte[]> addReferenceToDedicatedRows(PartitionInfo info, List<WriteInfo> writes) {
        return addCell(info, DUMMY, false, 0, entryIndicatingNumberOfRequiredRows(writes));
    }

    private long entryIndicatingNumberOfRequiredRows(List<WriteInfo> writes) {
        return -(1 + (writes.size() - 1) / SweepQueueUtils.MAX_CELLS_DEDICATED);
    }

    private Map<Cell, byte[]> addCell(
            PartitionInfo info,
            WriteReference writeRef,
            boolean isDedicatedRow,
            long dedicatedRowNumber,
            long writeIndex) {
        SweepableCellsRow row = computeRow(info, isDedicatedRow, dedicatedRowNumber);
        SweepableCellsColumnValue colVal = createColVal(info.timestamp(), writeIndex, writeRef);
        return ImmutableMap.of(SweepQueueUtils.toCell(row, colVal), colVal.persistValue());
    }

    private SweepableCellsRow computeRow(PartitionInfo info, boolean isDedicatedRow, long dedicatedRowNumber) {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(info.isConservative().isTrue())
                .dedicatedRow(isDedicatedRow)
                .shard(info.shard())
                .dedicatedRowNumber(dedicatedRowNumber)
                .build();

        long tsOrPartition = getTimestampOrPartition(info, isDedicatedRow);
        return SweepableCellsRow.of(tsOrPartition, metadata.persistToBytes());
    }

    private SweepableCellsRow computeRow(long partitionFine, ShardAndStrategy shardStrategy) {
        TargetedSweepMetadata metadata = ImmutableTargetedSweepMetadata.builder()
                .conservative(shardStrategy.isConservative())
                .dedicatedRow(false)
                .shard(shardStrategy.shard())
                .dedicatedRowNumber(0)
                .build();

        return SweepableCellsRow.of(partitionFine, metadata.persistToBytes());
    }

    private long getTimestampOrPartition(PartitionInfo info, boolean isDedicatedRow) {
        return isDedicatedRow ? info.timestamp() : SweepQueueUtils.tsPartitionFine(info.timestamp());
    }

    private SweepableCellsColumnValue createColVal(long ts, long index, WriteReference writeRef) {
        SweepableCellsTable.SweepableCellsColumn col = SweepableCellsTable.SweepableCellsColumn.of(tsMod(ts), index);
        return SweepableCellsColumnValue.of(col, writeReferencePersister.persist(writeRef));
    }

    private static long tsMod(long timestamp) {
        return timestamp % SweepQueueUtils.TS_FINE_GRANULARITY;
    }

    SweepBatch getBatchForPartition(
            ShardAndStrategy shardStrategy, long partitionFine, long minTsExclusive, long sweepTs) {
        SweepableCellsRow row = computeRow(partitionFine, shardStrategy);
        RowColumnRangeIterator resultIterator = getRowColumnRange(row, partitionFine, minTsExclusive, sweepTs);
        PeekingIterator<Map.Entry<Cell, Value>> peekingResultIterator = Iterators.peekingIterator(resultIterator);
        WriteBatch writeBatch = getBatchOfWrites(row, peekingResultIterator, sweepTs);
        Multimap<Long, WriteInfo> writesByStartTs = writeBatch.writesByStartTs;
        int entriesRead = writesByStartTs.size();
        maybeMetrics.ifPresent(metrics -> metrics.updateEntriesRead(shardStrategy, entriesRead));
        log.debug("Read {} entries from the sweep queue.", SafeArg.of("number", entriesRead));
        TimestampsToSweep tsToSweep = getTimestampsToSweepDescendingAndCleanupAborted(
                shardStrategy, minTsExclusive, sweepTs, writesByStartTs);
        Collection<WriteInfo> writes = getWritesToSweep(writesByStartTs, tsToSweep.timestampsDescending());
        DedicatedRows filteredDedicatedRows = getDedicatedRowsToClear(writeBatch.dedicatedRows, tsToSweep);
        long lastSweptTs = getLastSweptTs(tsToSweep, peekingResultIterator, partitionFine, sweepTs);
        return SweepBatch.of(writes, filteredDedicatedRows, lastSweptTs, tsToSweep.processedAll(), entriesRead);
    }

    private DedicatedRows getDedicatedRowsToClear(List<SweepableCellsRow> rows, TimestampsToSweep tsToSweep) {
        return DedicatedRows.of(rows.stream()
                .filter(row -> {
                    TargetedSweepMetadata metadata =
                            TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(row.getMetadata());
                    checkState(metadata.dedicatedRow(), "Row not a dedicated row", SafeArg.of("row", row));
                    return tsToSweep.timestampsDescending().contains(row.getTimestampPartition());
                })
                .collect(Collectors.toList()));
    }

    private WriteBatch getBatchOfWrites(
            SweepableCellsRow row, PeekingIterator<Map.Entry<Cell, Value>> resultIterator, long sweepTs) {
        WriteBatch writeBatch = new WriteBatch();
        while (resultIterator.hasNext() && writeBatch.writesByStartTs.size() < SweepQueueUtils.SWEEP_BATCH_SIZE) {
            Map.Entry<Cell, Value> entry = resultIterator.next();
            SweepableCellsTable.SweepableCellsColumn col = computeColumn(entry);
            long startTs = getTimestamp(row, col);
            if (knownToBeCommittedAfterSweepTs(startTs, sweepTs)) {
                writeBatch.add(ImmutableList.of(getWriteInfo(startTs, entry.getValue())));
                return writeBatch;
            }
            writeBatch.merge(getWrites(row, col, entry.getValue()));
        }
        // there may be entries remaining with the same start timestamp as the last processed one. If that is the case
        // we want to include these ones as well. This is OK since there are at most MAX_CELLS_GENERIC - 1 of them.
        while (resultIterator.hasNext()) {
            Map.Entry<Cell, Value> entry = resultIterator.peek();
            SweepableCellsTable.SweepableCellsColumn col = computeColumn(entry);
            long timestamp = getTimestamp(row, col);
            if (writeBatch.writesByStartTs.containsKey(timestamp)) {
                writeBatch.merge(getWrites(row, col, entry.getValue()));
                resultIterator.next();
            } else {
                break;
            }
        }
        return writeBatch;
    }

    private static final class WriteBatch {
        private final Multimap<Long, WriteInfo> writesByStartTs = HashMultimap.create();
        private final List<SweepableCellsRow> dedicatedRows = new ArrayList<>();

        WriteBatch merge(WriteBatch other) {
            writesByStartTs.putAll(other.writesByStartTs);
            dedicatedRows.addAll(other.dedicatedRows);
            return this;
        }

        static WriteBatch single(WriteInfo writeInfo) {
            WriteBatch batch = new WriteBatch();
            return batch.add(ImmutableList.of(writeInfo));
        }

        WriteBatch add(List<SweepableCellsRow> newDedicatedRows, List<WriteInfo> writeInfos) {
            dedicatedRows.addAll(newDedicatedRows);
            return add(writeInfos);
        }

        WriteBatch add(List<WriteInfo> writeInfos) {
            writeInfos.forEach(info -> writesByStartTs.put(info.timestamp(), info));
            return this;
        }
    }

    private RowColumnRangeIterator getRowColumnRange(
            SweepableCellsRow row, long partitionFine, long minTsExclusive, long maxTsExclusive) {
        return getRowsColumnRange(
                ImmutableList.of(row.persistToBytes()),
                columnsBetween(minTsExclusive + 1, maxTsExclusive, partitionFine),
                SweepQueueUtils.BATCH_SIZE_KVS);
    }

    private TimestampsToSweep getTimestampsToSweepDescendingAndCleanupAborted(
            ShardAndStrategy shardStrategy,
            long minTsExclusive,
            long sweepTs,
            Multimap<Long, WriteInfo> writesByStartTs) {
        Map<Long, Long> startToCommitTs = commitTsCache.loadBatch(writesByStartTs.keySet());
        Map<TableReference, Multimap<Cell, Long>> cellsToDelete = new HashMap<>();
        List<Long> committedTimestamps = new ArrayList<>();
        long lastSweptTs = minTsExclusive;
        boolean processedAll = true;

        List<Long> sortedStartTimestamps =
                startToCommitTs.keySet().stream().sorted().collect(Collectors.toList());
        for (long startTs : sortedStartTimestamps) {
            long commitTs = startToCommitTs.get(startTs);
            if (commitTs == TransactionConstants.FAILED_COMMIT_TS) {
                lastSweptTs = startTs;
                writesByStartTs.get(startTs).forEach(write -> cellsToDelete
                        .computeIfAbsent(write.tableRef(), ignore -> HashMultimap.create())
                        .put(write.cell(), write.timestamp()));
            } else if (commitTs < sweepTs) {
                lastSweptTs = startTs;
                committedTimestamps.add(startTs);
            } else {
                processedAll = false;
                lastSweptTs = startTs - 1;
                break;
            }
        }

        cellsToDelete.forEach((tableRef, multimap) -> {
            try {
                kvs.delete(tableRef, multimap);
            } catch (Exception exception) {
                if (tableWasDropped(tableRef)) {
                    // this table no longer exists, but had work to do in the sweep queue still;
                    // don't error out on this batch so that the queue cleans up and doesn't constantly retry forever
                    log.info(
                            "Tried to delete {} aborted writes from table {}, "
                                    + "but instead found that the table no longer exists.",
                            SafeArg.of("number", multimap.size()),
                            LoggingArgs.tableRef(tableRef),
                            exception);
                } else {
                    throw exception;
                }
            }
            maybeMetrics.ifPresent(metrics -> metrics.updateAbortedWritesDeleted(shardStrategy, multimap.size()));
            log.info(
                    "Deleted {} aborted writes from table {}.",
                    SafeArg.of("number", multimap.size()),
                    LoggingArgs.tableRef(tableRef));
        });

        return TimestampsToSweep.of(
                ImmutableSortedSet.copyOf(committedTimestamps).descendingSet(), lastSweptTs, processedAll);
    }

    private boolean tableWasDropped(TableReference tableRef) {
        return Arrays.equals(kvs.getMetadataForTable(tableRef), AtlasDbConstants.EMPTY_TABLE_METADATA);
    }

    private Collection<WriteInfo> getWritesToSweep(Multimap<Long, WriteInfo> writesByStartTs, SortedSet<Long> startTs) {
        Map<CellReference, WriteInfo> writesToSweepFor = new HashMap<>();
        startTs.stream()
                .map(writesByStartTs::get)
                .flatMap(Collection::stream)
                .forEach(write -> writesToSweepFor.putIfAbsent(write.writeRef().cellReference(), write));
        return writesToSweepFor.values();
    }

    private long getLastSweptTs(
            TimestampsToSweep startTsCommitted,
            Iterator<Map.Entry<Cell, Value>> resultIterator,
            long partitionFine,
            long maxTsExclusive) {
        if (startTsCommitted.processedAll() && exhaustedAllColumns(resultIterator)) {
            return lastGuaranteedSwept(partitionFine, maxTsExclusive);
        } else {
            return startTsCommitted.maxSwept();
        }
    }

    private WriteBatch getWrites(SweepableCellsRow row, SweepableCellsTable.SweepableCellsColumn col, Value value) {
        if (isReferenceToDedicatedRows(col)) {
            return writesFromDedicated(row, col);
        } else {
            return WriteBatch.single(getWriteInfo(getTimestamp(row, col), value));
        }
    }

    private boolean isReferenceToDedicatedRows(SweepableCellsTable.SweepableCellsColumn col) {
        return col.getWriteIndex() < 0;
    }

    private WriteBatch writesFromDedicated(SweepableCellsRow row, SweepableCellsTable.SweepableCellsColumn col) {
        List<SweepableCellsRow> dedicatedRows = computeDedicatedRows(row, col);
        RowColumnRangeIterator iterator =
                getWithColumnRangeAll(Lists.transform(dedicatedRows, SweepableCellsRow::persistToBytes));
        WriteBatch batch = new WriteBatch();
        return batch.add(
                dedicatedRows,
                Streams.stream(iterator)
                        .map(entry -> getWriteInfo(getTimestamp(row, col), entry.getValue()))
                        .collect(Collectors.toList()));
    }

    private List<SweepableCellsRow> computeDedicatedRows(
            SweepableCellsRow row, SweepableCellsTable.SweepableCellsColumn col) {
        TargetedSweepMetadata metadata = TargetedSweepMetadata.BYTES_HYDRATOR.hydrateFromBytes(row.getMetadata());
        long timestamp = getTimestamp(row, col);
        int numberOfDedicatedRows = writeIndexToNumberOfDedicatedRows(col.getWriteIndex());
        List<SweepableCellsRow> dedicatedRows = new ArrayList<>();

        for (int i = 0; i < numberOfDedicatedRows; i++) {
            byte[] dedicatedMetadata = ImmutableTargetedSweepMetadata.builder()
                    .from(metadata)
                    .dedicatedRow(true)
                    .dedicatedRowNumber(i)
                    .build()
                    .persistToBytes();
            dedicatedRows.add(SweepableCellsRow.of(timestamp, dedicatedMetadata));
        }
        return dedicatedRows;
    }

    private long getTimestamp(SweepableCellsRow row, SweepableCellsTable.SweepableCellsColumn col) {
        return row.getTimestampPartition() * SweepQueueUtils.TS_FINE_GRANULARITY + col.getTimestampModulus();
    }

    private boolean knownToBeCommittedAfterSweepTs(long startTs, long sweepTs) {
        return commitTsCache
                .loadIfCached(startTs)
                .map(commitTs -> commitTs >= sweepTs)
                .orElse(false);
    }

    private int writeIndexToNumberOfDedicatedRows(long writeIndex) {
        return (int) -writeIndex;
    }

    private RowColumnRangeIterator getWithColumnRangeAll(Iterable<byte[]> rows) {
        return getRowsColumnRange(rows, SweepQueueUtils.ALL_COLUMNS, SweepQueueUtils.BATCH_SIZE_KVS);
    }

    private WriteInfo getWriteInfo(long timestamp, Value value) {
        return WriteInfo.of(
                writeReferencePersister.unpersist(SweepableCellsColumnValue.hydrateValue(value.getContents())),
                timestamp);
    }

    private boolean exhaustedAllColumns(Iterator<Map.Entry<Cell, Value>> resultIterator) {
        return !resultIterator.hasNext();
    }

    private long lastGuaranteedSwept(long partitionFine, long maxTsExclusive) {
        return Math.min(SweepQueueUtils.maxTsForFinePartition(partitionFine), maxTsExclusive - 1);
    }

    void deleteDedicatedRows(DedicatedRows dedicatedRows) {
        List<byte[]> rows = dedicatedRows.getDedicatedRows().stream()
                .map(SweepableCellsRow::persistToBytes)
                .collect(Collectors.toList());
        deleteRows(rows);
    }

    void deleteNonDedicatedRows(ShardAndStrategy shardAndStrategy, Iterable<Long> partitionsFine) {
        List<byte[]> rows = Streams.stream(partitionsFine)
                .map(partitionFine -> computeRow(partitionFine, shardAndStrategy))
                .map(SweepableCellsRow::persistToBytes)
                .collect(Collectors.toList());
        deleteRows(rows);
    }

    private Map<Cell, byte[]> addWrite(PartitionInfo info, WriteInfo write, boolean dedicate, long index) {
        return addCell(
                info,
                write.writeRef(),
                dedicate,
                index / SweepQueueUtils.MAX_CELLS_DEDICATED,
                index % SweepQueueUtils.MAX_CELLS_DEDICATED);
    }

    private SweepableCellsTable.SweepableCellsColumn computeColumn(Map.Entry<Cell, Value> entry) {
        return SweepableCellsTable.SweepableCellsColumn.BYTES_HYDRATOR.hydrateFromBytes(
                entry.getKey().getColumnName());
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
        return Math.min(
                endTsExclusive - SweepQueueUtils.minTsForFinePartition(partitionFine),
                SweepQueueUtils.TS_FINE_GRANULARITY);
    }

    private long exactColumnOrElseBeginningOfRow(long startTsInclusive, long partitionFine) {
        return Math.max(startTsInclusive - SweepQueueUtils.minTsForFinePartition(partitionFine), 0);
    }
}
