/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.impl;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TransactionService.TableCell;
import com.palantir.atlasdb.protos.generated.TransactionService.TableRange;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.timelock.hackweek.JamesTransactionService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.logging.CommitProfileProcessor;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.collect.IterableUtils;
import com.palantir.common.collect.Maps2;
import com.palantir.common.streams.KeyedStream;
import com.palantir.util.Pair;

/**
 * This class will track all reads to verify that there are no read-write conflicts at commit time.
 * A read-write conflict is one where the value we read at our startTs is different than the value at our
 * commitTs.  We ignore all cells that have been written to during this transaction because those will
 * result in write-write conflicts and we don't need to worry about them.
 * <p>
 * If every table was marked as Serializable then we wouldn't need to also do write write conflict checking.
 * However, it is very common that we will be running in a mixed mode so this implementation does the standard
 * write/write conflict checking as well as preventing read/write conflicts to attain serializability.
 */
public class SerializableTransaction extends SnapshotTransaction {
    private static final Logger log = LoggerFactory.getLogger(SerializableTransaction.class);

    private static final int BATCH_SIZE = 1000;

    final ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> readsByTable = Maps.newConcurrentMap();
    final ConcurrentMap<TableReference, ConcurrentMap<RangeRequest, byte[]>> rangeEndByTable = Maps.newConcurrentMap();
    final ConcurrentMap<TableReference, ConcurrentMap<ByteBuffer, ConcurrentMap<BatchColumnRangeSelection, byte[]>>>
            columnRangeEndsByTable = Maps.newConcurrentMap();
    final ConcurrentMap<TableReference, Set<Cell>> cellsRead = Maps.newConcurrentMap();
    final ConcurrentMap<TableReference, Set<RowRead>> rowsRead = Maps.newConcurrentMap();

    public SerializableTransaction(MetricsManager metricsManager,
                                   KeyValueService keyValueService,
                                   JamesTransactionService james,
                                   TransactionService transactionService,
                                   Cleaner cleaner,
                                   long startTimeStamp,
                                   ConflictDetectionManager conflictDetectionManager,
                                   SweepStrategyManager sweepStrategyManager,
                                   long immutableTimestamp,
                                   PreCommitCondition preCommitCondition,
                                   AtlasDbConstraintCheckingMode constraintCheckingMode,
                                   Long transactionTimeoutMillis,
                                   TransactionReadSentinelBehavior readSentinelBehavior,
                                   boolean allowHiddenTableAccess,
                                   TimestampCache timestampCache,
                                   long lockAcquireTimeoutMs,
                                   ExecutorService getRangesExecutor,
                                   int defaultGetRangesConcurrency,
                                   MultiTableSweepQueueWriter sweepQueue,
                                   ExecutorService deleteExecutor,
                                   CommitProfileProcessor commitProfileProcessor) {
        super(metricsManager,
              keyValueService,
              james,
              transactionService,
              cleaner,
              startTimeStamp,
              conflictDetectionManager,
              sweepStrategyManager,
              immutableTimestamp,
              preCommitCondition,
              constraintCheckingMode,
              transactionTimeoutMillis,
              readSentinelBehavior,
              allowHiddenTableAccess,
              timestampCache,
              lockAcquireTimeoutMs,
              getRangesExecutor,
              defaultGetRangesConcurrency,
              sweepQueue,
              deleteExecutor,
              commitProfileProcessor);
    }

    @Override
    @Idempotent
    public SortedMap<byte[], RowResult<byte[]>> getRows(TableReference tableRef,
                                                        Iterable<byte[]> rows,
                                                        ColumnSelection columnSelection) {
        SortedMap<byte[], RowResult<byte[]>> ret = super.getRows(tableRef, rows, columnSelection);
        markRowsRead(tableRef, rows, columnSelection, ret.values());
        return ret;
    }

    void addToReadSet(TableReference tableRef, Map<Cell, byte[]> toAdd) {
        markCellsRead(tableRef, toAdd.keySet(), toAdd);
    }

    @Override
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> ret =
                super.getRowsColumnRange(tableRef, rows, columnRangeSelection);
        return Maps.transformEntries(ret, (row, visitable) -> new BatchingVisitable<Entry<Cell, byte[]>>() {
            @Override
            public <K extends Exception> boolean batchAccept(
                    int batchSize,
                    AbortingVisitor<? super List<Entry<Cell, byte[]>>, K> visitor)
                    throws K {
                boolean hitEnd = visitable.batchAccept(batchSize, items -> {
                    if (items.size() < batchSize) {
                        reachedEndOfColumnRange(tableRef, row, columnRangeSelection);
                    }
                    markRowColumnRangeRead(tableRef, row, columnRangeSelection, items);
                    return visitor.visit(items);
                });
                if (hitEnd) {
                    reachedEndOfColumnRange(tableRef, row, columnRangeSelection);
                }
                return hitEnd;
            }
        });
    }

    @Override
    public Iterator<Entry<Cell, byte[]>> getRowsColumnRange(TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int batchHint) {
        if (isSerializableTable(tableRef)) {
            throw new UnsupportedOperationException("This method does not support serializable conflict handling");
        }
        return super.getRowsColumnRange(tableRef, rows, columnRangeSelection, batchHint);
    }

    @Override
    @Idempotent
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        Map<Cell, byte[]> ret = super.get(tableRef, cells);
        markCellsRead(tableRef, cells, ret);
        return ret;
    }

    @Override
    @Idempotent
    public BatchingVisitable<RowResult<byte[]>> getRange(TableReference tableRef, RangeRequest rangeRequest) {
        final BatchingVisitable<RowResult<byte[]>> ret = super.getRange(tableRef, rangeRequest);
        return wrapRange(tableRef, rangeRequest, ret);
    }

    @Override
    @Idempotent
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(final TableReference tableRef,
            Iterable<RangeRequest> rangeRequests) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> ret = super.getRanges(tableRef, rangeRequests);
        Iterable<Pair<RangeRequest, BatchingVisitable<RowResult<byte[]>>>> zip = IterableUtils.zip(rangeRequests, ret);
        return Iterables.transform(zip, pair -> wrapRange(tableRef, pair.lhSide, pair.rhSide));
    }

    private BatchingVisitable<RowResult<byte[]>> wrapRange(final TableReference tableRef,
                                                           final RangeRequest rangeRequest,
                                                           final BatchingVisitable<RowResult<byte[]>> ret) {
        return new BatchingVisitable<RowResult<byte[]>>() {
            @Override
            public <K extends Exception> boolean batchAccept(
                    int batchSize,
                    AbortingVisitor<? super List<RowResult<byte[]>>, K> visitor)
                    throws K {
                boolean hitEnd = ret.batchAccept(batchSize, items -> {
                    if (items.size() < batchSize) {
                        reachedEndOfRange(tableRef, rangeRequest);
                    }
                    markRangeRead(tableRef, rangeRequest, items);
                    return visitor.visit(items);
                });
                if (hitEnd) {
                    reachedEndOfRange(tableRef, rangeRequest);
                }
                return hitEnd;
            }
        };
    }

    private ConcurrentNavigableMap<Cell, byte[]> getReadsForTable(TableReference table) {
        ConcurrentNavigableMap<Cell, byte[]> reads = readsByTable.get(table);
        if (reads == null) {
            ConcurrentNavigableMap<Cell, byte[]> newMap = new ConcurrentSkipListMap<>();
            readsByTable.putIfAbsent(table, newMap);
            reads = readsByTable.get(table);
        }
        return reads;
    }

    private void setRangeEnd(TableReference table, RangeRequest range, byte[] maxRow) {
        Validate.notNull(maxRow, "maxRow cannot be null");
        ConcurrentMap<RangeRequest, byte[]> rangeEnds = rangeEndByTable.get(table);
        if (rangeEnds == null) {
            ConcurrentMap<RangeRequest, byte[]> newMap = Maps.newConcurrentMap();
            rangeEndByTable.putIfAbsent(table, newMap);
            rangeEnds = rangeEndByTable.get(table);
        }

        if (maxRow.length == 0) {
            rangeEnds.put(range, maxRow);
        }

        rangeEnds.compute(range, (unused, curVal) -> {
            if (curVal == null) {
                return maxRow;
            } else if (curVal.length == 0) {
                return curVal;
            }
            return Ordering.from(UnsignedBytes.lexicographicalComparator()).max(curVal, maxRow);
        });
    }

    private void setColumnRangeEnd(
            TableReference table,
            byte[] unwrappedRow,
            BatchColumnRangeSelection columnRangeSelection,
            byte[] maxCol) {
        Validate.notNull(maxCol, "maxCol cannot be null");
        ByteBuffer row = ByteBuffer.wrap(unwrappedRow);
        columnRangeEndsByTable.computeIfAbsent(table, unused -> new ConcurrentHashMap<>());
        ConcurrentMap<BatchColumnRangeSelection, byte[]> rangeEnds =
                columnRangeEndsByTable.get(table).computeIfAbsent(row, unused -> new ConcurrentHashMap<>());

        if (maxCol.length == 0) {
            rangeEnds.put(columnRangeSelection, maxCol);
        }

        rangeEnds.compute(columnRangeSelection, (range, curVal) -> {
            if (curVal == null) {
                return maxCol;
            } else if (curVal.length == 0) {
                return curVal;
            }
            return Ordering.from(UnsignedBytes.lexicographicalComparator()).max(curVal, maxCol);
        });
    }

    boolean isSerializableTable(TableReference table) {
        // If the metadata is null, we assume that the conflict handler is not SERIALIZABLE.
        // In that case the transaction will fail on commit if it has writes.
        ConflictHandler conflictHandler = conflictDetectionManager.get(table);
        return conflictHandler.checkReadWriteConflicts();
    }

    /**
     * This exists to transform the incoming byte[] to cloned one to ensure that all the byte array
     * comparisons are valid.
     */
    protected Map<Cell, byte[]> transformGetsForTesting(Map<Cell, byte[]> map) {
        return map;
    }

    private void markCellsRead(TableReference table, Set<Cell> searched, Map<Cell, byte[]> result) {
        if (!isSerializableTable(table)) {
            return;
        }
        getReadsForTable(table).putAll(transformGetsForTesting(result));
        Set<Cell> cellsForTable = cellsRead.get(table);
        if (cellsForTable == null) {
            cellsRead.putIfAbsent(table, Sets.newConcurrentHashSet());
            cellsForTable = cellsRead.get(table);
        }
        cellsForTable.addAll(searched);
    }

    private void markRangeRead(TableReference table, RangeRequest range, List<RowResult<byte[]>> result) {
        if (!isSerializableTable(table)) {
            return;
        }
        ConcurrentNavigableMap<Cell, byte[]> reads = getReadsForTable(table);
        for (RowResult<byte[]> row : result) {
            Map<Cell, byte[]> map = Maps2.fromEntries(row.getCells());
            map = transformGetsForTesting(map);
            reads.putAll(map);
        }
        setRangeEnd(table, range, Iterables.getLast(result).getRowName());
    }

    private void markRowColumnRangeRead(
            TableReference table,
            byte[] row,
            BatchColumnRangeSelection range,
            List<Entry<Cell, byte[]>> result) {
        if (!isSerializableTable(table)) {
            return;
        }
        ConcurrentNavigableMap<Cell, byte[]> reads = getReadsForTable(table);
        Map<Cell, byte[]> map = Maps2.fromEntries(result);
        map = transformGetsForTesting(map);
        reads.putAll(map);
        setColumnRangeEnd(table, row, range, Iterables.getLast(result).getKey().getColumnName());
    }

    static class RowRead {
        final ImmutableList<byte[]> rows;
        final ColumnSelection cols;

        RowRead(Iterable<byte[]> rows, ColumnSelection cols) {
            this.rows = ImmutableList.copyOf(rows);
            this.cols = cols;
        }
    }

    private void markRowsRead(
            TableReference table,
            Iterable<byte[]> rows,
            ColumnSelection cols,
            Iterable<RowResult<byte[]>> result) {
        if (!isSerializableTable(table)) {
            return;
        }
        ConcurrentNavigableMap<Cell, byte[]> reads = getReadsForTable(table);
        for (RowResult<byte[]> row : result) {
            Map<Cell, byte[]> map = Maps2.fromEntries(row.getCells());
            map = transformGetsForTesting(map);
            reads.putAll(map);
        }
        Set<RowRead> rowReads = rowsRead.get(table);
        if (rowReads == null) {
            rowsRead.putIfAbsent(table, Sets.<RowRead>newConcurrentHashSet());
            rowReads = rowsRead.get(table);
        }
        rowReads.add(new RowRead(rows, cols));
    }

    private void reachedEndOfRange(TableReference table, RangeRequest range) {
        if (!isSerializableTable(table)) {
            return;
        }
        setRangeEnd(table, range, PtBytes.EMPTY_BYTE_ARRAY);
    }

    private void reachedEndOfColumnRange(
            TableReference table,
            byte[] row,
            BatchColumnRangeSelection columnRangeSelection) {
        if (!isSerializableTable(table)) {
            return;
        }
        setColumnRangeEnd(table, row, columnRangeSelection, PtBytes.EMPTY_BYTE_ARRAY);
    }

    @Override
    @Idempotent
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        super.put(tableRef, values);
    }

    @Override
    protected List<TableCell> getReads() {
        return KeyedStream.stream(readsByTable)
                .mapKeys(SnapshotTransaction::toTable)
                .flatMap(map -> map.keySet().stream().map(SnapshotTransaction::cell))
                .map((table, cell) -> TableCell.newBuilder().setTable(table).setCell(cell).build())
                .values()
                .collect(Collectors.toList());
    }

    private static byte[] transformForFirst(byte[] row) {
        if (row.length == 0) {
            return RangeRequests.getFirstRowName();
        }
        return row;
    }

    @Override
    protected List<TableRange> getReadRanges() {
        Stream<TableRange> rangeRequests = KeyedStream.stream(rangeEndByTable)
                .mapKeys(SnapshotTransaction::toTable)
                .flatMap((table, map) -> KeyedStream.stream(map).map((range, endRow) -> {
                    TableRange.Builder builder = TableRange.newBuilder()
                            .setTable(table)
                            .setStart(cell(Cell.create(
                                    transformForFirst(range.getStartInclusive()), RangeRequests.getFirstRowName())))
                            .setHasColumnFilter(true)
                            .addAllColumnFilter(range.getColumnNames().stream()
                                    .map(ByteString::copyFrom)
                                    .collect(Collectors.toList()));
                    if (endRow.length != 0 && !RangeRequests.isTerminalRow(false, endRow)) {
                        builder.setEnd(
                                cell(Cell.create(RangeRequests.getNextStartRow(false, endRow),
                                        RangeRequests.getFirstRowName())));
                    } else if (range.getEndExclusive().length == 0) {
                        builder.setEnd(cell(Cell.create(
                                RangeRequests.getLastRowName(),
                                RangeRequests.getLastRowName())));
                    } else {
                        builder.setEnd(cell(Cell.create(range.getEndExclusive(), RangeRequests.getFirstRowName())));
                    }
                    return builder.build();
                }).values())
                .values();

        Stream<TableRange> columnRangeRequests = KeyedStream.stream(columnRangeEndsByTable)
                .mapKeys(SnapshotTransaction::toTable)
                .flatMap((table, map) -> KeyedStream.stream(map)
                        .flatMap((row, ranges) -> KeyedStream.stream(ranges)
                                .map((range, rangeEnd) -> {
                                    TableRange.Builder builder = TableRange.newBuilder()
                                            .setTable(table)
                                            .setHasColumnFilter(false)
                                            .setStart(cell(Cell.create(row.array(), transformForFirst(range.getStartCol()))));
                                    if (rangeEnd.length != 0 && RangeRequests.isTerminalRow(false, rangeEnd)) {
                                        builder.setEnd(cell(Cell.create(row.array(),
                                                RangeRequests.getNextStartRow(false, rangeEnd))));
                                    } else if (range.getEndCol().length == 0) {
                                        builder.setEnd(cell(Cell.create(row.array(), RangeRequests.getLastRowName())));
                                    } else {
                                        builder.setEnd(cell(Cell.create(row.array(), range.getEndCol())));
                                    }
                                    return builder.build();
                                }).values()).values()).values();

        return Stream.concat(rangeRequests, columnRangeRequests)
                .collect(Collectors.toList());
    }
}
