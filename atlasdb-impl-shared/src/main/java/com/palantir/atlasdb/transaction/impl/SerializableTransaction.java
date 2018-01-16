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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.sweep.queue.SweepQueueWriter;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionSerializableConflictException;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.collect.IterableUtils;
import com.palantir.common.collect.Maps2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
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
    final ConcurrentMap<TableReference, ConcurrentMap<byte[], ConcurrentMap<BatchColumnRangeSelection, byte[]>>>
            columnRangeEndsByTable = Maps.newConcurrentMap();
    final ConcurrentMap<TableReference, Set<Cell>> cellsRead = Maps.newConcurrentMap();
    final ConcurrentMap<TableReference, Set<RowRead>> rowsRead = Maps.newConcurrentMap();
    private final MetricRegistry metricRegistry = AtlasDbMetrics.getMetricRegistry();

    public SerializableTransaction(KeyValueService keyValueService,
                                   TimelockService timelockService,
                                   TransactionService transactionService,
                                   Cleaner cleaner,
                                   Supplier<Long> startTimeStamp,
                                   ConflictDetectionManager conflictDetectionManager,
                                   SweepStrategyManager sweepStrategyManager,
                                   long immutableTimestamp,
                                   Optional<LockToken> immutableTsLock,
                                   AdvisoryLockPreCommitCheck advisoryLockCheck,
                                   AtlasDbConstraintCheckingMode constraintCheckingMode,
                                   Long transactionTimeoutMillis,
                                   TransactionReadSentinelBehavior readSentinelBehavior,
                                   boolean allowHiddenTableAccess,
                                   TimestampCache timestampCache,
                                   long lockAcquireTimeoutMs,
                                   ExecutorService getRangesExecutor,
                                   int defaultGetRangesConcurrency,
                                   SweepQueueWriter sweepQueue) {
        super(keyValueService,
              timelockService,
              transactionService,
              cleaner,
              startTimeStamp,
              conflictDetectionManager,
              sweepStrategyManager,
              immutableTimestamp,
              immutableTsLock,
              advisoryLockCheck,
              constraintCheckingMode,
              transactionTimeoutMillis,
              readSentinelBehavior,
              allowHiddenTableAccess,
              timestampCache,
              lockAcquireTimeoutMs,
              getRangesExecutor,
              defaultGetRangesConcurrency,
              sweepQueue);
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

        while (true) {
            byte[] curVal = rangeEnds.get(range);
            if (curVal == null) {
                byte[] oldVal = rangeEnds.putIfAbsent(range, maxRow);
                if (oldVal == null) {
                    return;
                } else {
                    continue;
                }
            }
            if (curVal.length == 0) {
                return;
            }
            if (UnsignedBytes.lexicographicalComparator().compare(curVal, maxRow) >= 0) {
                return;
            }
            if (rangeEnds.replace(range, curVal, maxRow)) {
                return;
            }
        }
    }

    private void setColumnRangeEnd(
            TableReference table,
            byte[] row,
            BatchColumnRangeSelection columnRangeSelection,
            byte[] maxCol) {
        Validate.notNull(maxCol, "maxCol cannot be null");
        if (!columnRangeEndsByTable.containsKey(table)) {
            ConcurrentMap<byte[], ConcurrentMap<BatchColumnRangeSelection, byte[]>> newMap = Maps.newConcurrentMap();
            columnRangeEndsByTable.putIfAbsent(table, newMap);
        }
        if (!columnRangeEndsByTable.get(table).containsKey(row)) {
            ConcurrentMap<BatchColumnRangeSelection, byte[]> newMap = Maps.newConcurrentMap();
            columnRangeEndsByTable.get(table).putIfAbsent(row, newMap);
        }
        ConcurrentMap<BatchColumnRangeSelection, byte[]> rangeEnds = columnRangeEndsByTable.get(table).get(row);

        if (maxCol.length == 0) {
            rangeEnds.put(columnRangeSelection, maxCol);
        }

        while (true) {
            byte[] curVal = rangeEnds.get(columnRangeSelection);
            if (curVal == null) {
                byte[] oldVal = rangeEnds.putIfAbsent(columnRangeSelection, maxCol);
                if (oldVal == null) {
                    return;
                } else {
                    continue;
                }
            }
            if (curVal.length == 0) {
                return;
            }
            if (UnsignedBytes.lexicographicalComparator().compare(curVal, maxCol) >= 0) {
                return;
            }
            if (rangeEnds.replace(columnRangeSelection, curVal, maxCol)) {
                return;
            }
        }
    }

    boolean isSerializableTable(TableReference table) {
        // If the metadata is null, we assume that the conflict handler is not SERIALIZABLE.
        // In that case the transaction will fail on commit if it has writes.
        return conflictDetectionManager.get(table) == ConflictHandler.SERIALIZABLE;
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
    protected void throwIfReadWriteConflictForSerializable(long commitTimestamp) {
        Transaction ro = getReadOnlyTransaction(commitTimestamp);
        verifyRanges(ro);
        verifyColumnRanges(ro);
        verifyCells(ro);
        verifyRows(ro);
    }

    private void verifyRows(Transaction ro) {
        for (Map.Entry<TableReference, Set<RowRead>> tableAndRowsEntry : rowsRead.entrySet()) {
            TableReference table = tableAndRowsEntry.getKey();
            Set<RowRead> rows = tableAndRowsEntry.getValue();

            ConcurrentNavigableMap<Cell, byte[]> readsForTable = getReadsForTable(table);
            Multimap<ColumnSelection, byte[]> rowsReadByColumns = Multimaps.newSortedSetMultimap(
                    Maps.newHashMap(),
                    () -> Sets.newTreeSet(UnsignedBytes.lexicographicalComparator()));
            for (RowRead r : rows) {
                rowsReadByColumns.putAll(r.cols, r.rows);
            }
            for (ColumnSelection cols : rowsReadByColumns.keySet()) {
                verifyColumns(ro, table, readsForTable, rowsReadByColumns, cols);
            }

        }
    }

    private void verifyColumns(
            Transaction ro,
            TableReference table,
            ConcurrentNavigableMap<Cell, byte[]> readsForTable,
            Multimap<ColumnSelection, byte[]> rowsReadByColumns,
            ColumnSelection columns) {
        for (List<byte[]> batch : Iterables.partition(rowsReadByColumns.get(columns), BATCH_SIZE)) {
            SortedMap<byte[], RowResult<byte[]>> currentRows = ro.getRows(table, batch, columns);
            for (byte[] row : batch) {
                RowResult<byte[]> currentRow = currentRows.get(row);
                Map<Cell, byte[]> orignalReads = readsForTable
                        .tailMap(Cells.createSmallestCellForRow(row), true)
                        .headMap(Cells.createLargestCellForRow(row), true);

                // We want to filter out all our reads to just the set that matches our column selection.
                orignalReads = Maps.filterKeys(orignalReads, input -> columns.contains(input.getColumnName()));

                if (writesByTable.get(table) != null) {
                    // We don't want to verify any reads that we wrote to cause
                    // we will just read our own values.
                    // NB: We filter our write set out here because our normal SI
                    // checking handles this case to ensure the value hasn't changed.
                    orignalReads = Maps.filterKeys(
                            orignalReads,
                            Predicates.not(Predicates.in(writesByTable.get(table).keySet())));
                }

                if (currentRow == null && orignalReads.isEmpty()) {
                    continue;
                }

                if (currentRow == null) {
                    handleTransactionConflict(table);
                }

                Map<Cell, byte[]> currentCells = Maps2.fromEntries(currentRow.getCells());
                if (writesByTable.get(table) != null) {
                    // We don't want to verify any reads that we wrote to cause
                    // we will just read our own values.
                    // NB: We filter our write set out here because our normal SI
                    // checking handles this case to ensure the value hasn't changed.
                    currentCells = Maps.filterKeys(
                            currentCells,
                            Predicates.not(Predicates.in(writesByTable.get(table).keySet())));
                }
                if (!areMapsEqual(orignalReads, currentCells)) {
                    handleTransactionConflict(table);
                }
            }
        }
    }

    private boolean areMapsEqual(Map<Cell, byte[]> map1, Map<Cell, byte[]> map2) {
        if (map1.size() != map2.size()) {
            return false;
        }
        for (Map.Entry<Cell, byte[]> e : map1.entrySet()) {
            if (!map2.containsKey(e.getKey())) {
                return false;
            }
            if (UnsignedBytes.lexicographicalComparator().compare(e.getValue(), map2.get(e.getKey())) != 0) {
                return false;
            }
        }
        return true;
    }

    private void verifyCells(Transaction readOnlyTransaction) {
        for (Entry<TableReference, Set<Cell>> tableAndCellsEntry : cellsRead.entrySet()) {
            TableReference table = tableAndCellsEntry.getKey();
            Set<Cell> cells = tableAndCellsEntry.getValue();

            final ConcurrentNavigableMap<Cell, byte[]> readsForTable = getReadsForTable(table);
            for (Iterable<Cell> batch : Iterables.partition(cells, BATCH_SIZE)) {
                // We don't want to verify any reads that we wrote to cause we will just read our own values.
                // NB: If the value has changed between read and write, our normal SI checking handles this case
                Iterable<Cell> batchWithoutWrites = writesByTable.get(table) != null
                        ? Iterables.filter(batch, Predicates.not(Predicates.in(writesByTable.get(table).keySet())))
                        : batch;
                ImmutableSet<Cell> batchWithoutWritesSet = ImmutableSet.copyOf(batchWithoutWrites);
                Map<Cell, byte[]> currentBatch = readOnlyTransaction.get(table, batchWithoutWritesSet);
                ImmutableMap<Cell, byte[]> originalReads = Maps.toMap(
                        Sets.intersection(batchWithoutWritesSet, readsForTable.keySet()),
                        Functions.forMap(readsForTable));
                if (!areMapsEqual(currentBatch, originalReads)) {
                    handleTransactionConflict(table);
                }
            }
        }
    }

    private void verifyRanges(Transaction readOnlyTransaction) {
        // verify each set of reads to ensure they are the same.
        for (Entry<TableReference, ConcurrentMap<RangeRequest, byte[]>> tableAndRange : rangeEndByTable.entrySet()) {
            TableReference table = tableAndRange.getKey();
            Map<RangeRequest, byte[]> rangeEnds = tableAndRange.getValue();

            for (Entry<RangeRequest, byte[]> rangeAndRangeEndEntry : rangeEnds.entrySet()) {
                RangeRequest range = rangeAndRangeEndEntry.getKey();
                byte[] rangeEnd = rangeAndRangeEndEntry.getValue();

                if (rangeEnd.length != 0 && !RangeRequests.isTerminalRow(range.isReverse(), rangeEnd)) {
                    range = range.getBuilder()
                            .endRowExclusive(RangeRequests.getNextStartRow(range.isReverse(), rangeEnd))
                            .build();
                }

                ConcurrentNavigableMap<Cell, byte[]> writes = writesByTable.get(table);
                BatchingVisitableView<RowResult<byte[]>> bv = BatchingVisitableView.of(
                        readOnlyTransaction.getRange(table, range));
                NavigableMap<Cell, ByteBuffer> readsInRange = Maps.transformValues(
                        getReadsInRange(table, range),
                        ByteBuffer::wrap);
                if (!bv.transformBatch(input -> filterWritesFromRows(input, writes)).isEqual(readsInRange.entrySet())) {
                    handleTransactionConflict(table);
                }
            }
        }
    }

    private NavigableMap<Cell, byte[]> getReadsInColumnRange(TableReference table,
                                                             byte[] row,
                                                             BatchColumnRangeSelection range) {
        NavigableMap<Cell, byte[]> reads = getReadsForTable(table);
        Cell startCell = Cells.createSmallestCellForRow(row);
        if ((range.getStartCol() != null) && (range.getStartCol().length > 0)) {
            startCell = Cell.create(row, range.getStartCol());
        }
        reads = reads.tailMap(startCell, true);
        if ((range.getEndCol() != null) && (range.getEndCol().length > 0)) {
            Cell endCell = Cell.create(row, range.getEndCol());
            reads = reads.headMap(endCell, false);
        } else {
            if (!RangeRequests.isLastRowName(row)) {
                Cell endCell = Cells.createSmallestCellForRow(RangeRequests.nextLexicographicName(row));
                reads = reads.headMap(endCell, false);
            }
        }
        ConcurrentNavigableMap<Cell, byte[]> writes = writesByTable.get(table);
        if (writes != null) {
            reads = Maps.filterKeys(reads, Predicates.not(Predicates.in(writes.keySet())));
        }
        return reads;
    }

    private void verifyColumnRanges(Transaction readOnlyTransaction) {
        // verify each set of reads to ensure they are the same.
        for (Entry<TableReference,
                ConcurrentMap<byte[], ConcurrentMap<BatchColumnRangeSelection, byte[]>>> tableAndRange :
                columnRangeEndsByTable.entrySet()) {
            TableReference table = tableAndRange.getKey();
            Map<byte[], ConcurrentMap<BatchColumnRangeSelection, byte[]>> columnRangeEnds = tableAndRange.getValue();

            Map<Cell, byte[]> writes = writesByTable.get(table);
            Map<BatchColumnRangeSelection, List<byte[]>> rangesToRows = Maps.newHashMap();
            for (Entry<byte[], ConcurrentMap<BatchColumnRangeSelection, byte[]>> rowAndRangeEnds :
                    columnRangeEnds.entrySet()) {
                byte[] row = rowAndRangeEnds.getKey();
                Map<BatchColumnRangeSelection, byte[]> rangeEnds = columnRangeEnds.get(row);

                for (Entry<BatchColumnRangeSelection, byte[]> e : rangeEnds.entrySet()) {
                    BatchColumnRangeSelection range = e.getKey();
                    byte[] rangeEnd = e.getValue();
                    if (rangeEnd.length != 0 && !RangeRequests.isTerminalRow(false, rangeEnd)) {
                        range = BatchColumnRangeSelection.create(
                                range.getStartCol(),
                                RangeRequests.getNextStartRow(false, rangeEnd),
                                range.getBatchHint());
                    }
                    if (rangesToRows.get(range) != null) {
                        rangesToRows.get(range).add(row);
                    } else {
                        rangesToRows.put(range, ImmutableList.of(row));
                    }
                }
            }
            for (Entry<BatchColumnRangeSelection, List<byte[]>> e : rangesToRows.entrySet()) {
                BatchColumnRangeSelection range = e.getKey();
                List<byte[]> rows = e.getValue();
                Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> result =
                        readOnlyTransaction.getRowsColumnRange(table, rows, range);
                for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> res : result.entrySet()) {
                    byte[] row = res.getKey();
                    BatchingVisitableView<Entry<Cell, byte[]>> bv = BatchingVisitableView.of(res.getValue());
                    NavigableMap<Cell, ByteBuffer> readsInRange = Maps.transformValues(
                            getReadsInColumnRange(table, row, range),
                            input -> ByteBuffer.wrap(input));
                    boolean isEqual = bv.transformBatch(input -> filterWritesFromCells(input, writes))
                            .isEqual(readsInRange.entrySet());
                    if (!isEqual) {
                        handleTransactionConflict(table);
                    }
                }
            }
        }
    }

    private List<Entry<Cell, ByteBuffer>> filterWritesFromCells(
            Iterable<Entry<Cell, byte[]>> cells,
            Map<Cell, byte[]> writes) {
        List<Entry<Cell, ByteBuffer>> cellsWithoutWrites = Lists.newArrayList();
        for (Entry<Cell, byte[]> cell : cells) {
            // NB: We filter our write set out here because our normal SI
            // checking handles this case to ensure the value hasn't changed.
            if (writes == null || !writes.containsKey(cell.getKey())) {
                cellsWithoutWrites.add(Maps.immutableEntry(cell.getKey(), ByteBuffer.wrap(cell.getValue())));
            }
        }
        return cellsWithoutWrites;
    }

    private List<Entry<Cell, ByteBuffer>> filterWritesFromRows(
            Iterable<RowResult<byte[]>> rows,
            Map<Cell, byte[]> writes) {
        List<Entry<Cell, ByteBuffer>> rowsWithoutWrites = Lists.newArrayList();
        for (RowResult<byte[]> row : rows) {
            rowsWithoutWrites.addAll(filterWritesFromCells(row.getCells(), writes));
        }
        return rowsWithoutWrites;
    }

    private NavigableMap<Cell, byte[]> getReadsInRange(TableReference table,
                                                       RangeRequest range) {
        NavigableMap<Cell, byte[]> reads = getReadsForTable(table);
        if (range.getStartInclusive().length != 0) {
            reads = reads.tailMap(Cells.createSmallestCellForRow(range.getStartInclusive()), true);
        }
        if (range.getEndExclusive().length != 0) {
            reads = reads.headMap(Cells.createSmallestCellForRow(range.getEndExclusive()), false);
        }
        Map<Cell, byte[]> writes = writesByTable.get(table);
        if (writes != null) {
            reads = Maps.filterKeys(reads, Predicates.not(Predicates.in(writes.keySet())));
        }
        if (!range.getColumnNames().isEmpty()) {
            Predicate<Cell> columnInNames = Predicates.compose(
                    Predicates.in(range.getColumnNames()),
                    Cells.getColumnFunction());
            reads = Maps.filterKeys(reads, columnInNames);
        }
        return reads;
    }

    private Transaction getReadOnlyTransaction(final long commitTs) {
        return new SnapshotTransaction(
                keyValueService,
                timelockService,
                defaultTransactionService,
                NoOpCleaner.INSTANCE,
                Suppliers.ofInstance(commitTs + 1),
                ConflictDetectionManagers.createWithNoConflictDetection(),
                sweepStrategyManager,
                immutableTimestamp,
                Optional.empty(),
                AdvisoryLockPreCommitCheck.NO_OP,
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                transactionReadTimeoutMillis,
                getReadSentinelBehavior(),
                allowHiddenTableAccess,
                timestampValidationReadCache,
                lockAcquireTimeoutMs,
                getRangesExecutor,
                defaultGetRangesConcurrency,
                SweepQueueWriter.NO_OP) {
            @Override
            protected Map<Long, Long> getCommitTimestamps(TableReference tableRef,
                                                          Iterable<Long> startTimestamps,
                                                          boolean waitForCommitterToComplete) {
                Set<Long> beforeStart = Sets.newHashSet();
                Set<Long> afterStart = Sets.newHashSet();
                boolean containsMyStart = false;
                long myStart = SerializableTransaction.this.getTimestamp();
                for (long startTs : startTimestamps) {
                    if (startTs == myStart) {
                        containsMyStart = true;
                    } else if (startTs < myStart) {
                        beforeStart.add(startTs);
                    } else {
                        afterStart.add(startTs);
                    }
                }
                Map<Long, Long> ret = Maps.newHashMap();
                if (!afterStart.isEmpty()) {
                    // We do not block when waiting for results that were written after our
                    // start timestamp.  If we block here it may lead to deadlock if two transactions
                    // (or a cycle of any length) have all written their data and all doing checks before committing.
                    Map<Long, Long> afterResults = super.getCommitTimestamps(tableRef, afterStart, false);
                    if (!afterResults.keySet().containsAll(afterStart)) {
                        // If we do not get back all these results we may be in the deadlock case so we should just
                        // fail out early.  It may be the case that abort more transactions than needed to break the
                        // deadlock cycle, but this should be pretty rare.
                        getTransactionConflictsMeter().mark();
                        throw new TransactionSerializableConflictException("An uncommitted conflicting read was "
                                + "written after our start timestamp for table " + tableRef + ".  "
                                + "This case can cause deadlock and is very likely to be a read write conflict.");
                    } else {
                        ret.putAll(afterResults);
                    }
                }
                // We are ok to block here because if there is a cycle of transactions that could result in a deadlock,
                // then at least one of them will be in the ab
                ret.putAll(super.getCommitTimestamps(tableRef, beforeStart, waitForCommitterToComplete));
                if (containsMyStart) {
                    ret.put(myStart, commitTs);
                }
                return ret;
            }
        };
    }

    private void handleTransactionConflict(TableReference tableRef) {
        getTransactionConflictsMeter().mark();
        throw TransactionSerializableConflictException.create(tableRef, getTimestamp(),
                System.currentTimeMillis() - timeCreated);
    }

    private Meter getTransactionConflictsMeter() {
        // TODO(hsaraogi): add table names as a tag
        return metricRegistry.meter(
                MetricRegistry.name(SerializableTransaction.class, "SerializableTransactionConflict"));
    }
}
