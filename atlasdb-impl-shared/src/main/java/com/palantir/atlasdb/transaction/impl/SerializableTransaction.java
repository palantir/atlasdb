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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCache;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionSerializableConflictException;
import com.palantir.atlasdb.transaction.impl.metrics.TableLevelMetricsController;
import com.palantir.atlasdb.transaction.service.AsyncTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.IterableUtils;
import com.palantir.common.collect.Maps2;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.Preconditions;
import com.palantir.util.Pair;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    final ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> readsByTable = new ConcurrentHashMap<>();
    final ConcurrentMap<TableReference, ConcurrentMap<RangeRequest, byte[]>> rangeEndByTable =
            new ConcurrentHashMap<>();
    final ConcurrentMap<TableReference, ConcurrentMap<ByteBuffer, ConcurrentMap<BatchColumnRangeSelection, byte[]>>>
            columnRangeEndsByTable = new ConcurrentHashMap<>();
    final ConcurrentMap<GetSortedColumnsRequest, AtomicReference<Cell>> sortedColumnRangeEnds =
            new ConcurrentHashMap<>();
    final ConcurrentMap<TableReference, Set<Cell>> cellsRead = new ConcurrentHashMap<>();
    final ConcurrentMap<TableReference, Set<RowRead>> rowsRead = new ConcurrentHashMap<>();

    public SerializableTransaction(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
            TimelockService timelockService,
            LockWatchManagerInternal lockWatchManager,
            TransactionService transactionService,
            Cleaner cleaner,
            Supplier<Long> startTimeStamp,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            long immutableTimestamp,
            Optional<LockToken> immutableTsLock,
            PreCommitCondition preCommitCondition,
            AtlasDbConstraintCheckingMode constraintCheckingMode,
            Long transactionTimeoutMillis,
            TransactionReadSentinelBehavior readSentinelBehavior,
            boolean allowHiddenTableAccess,
            TimestampCache timestampCache,
            ExecutorService getRangesExecutor,
            int defaultGetRangesConcurrency,
            MultiTableSweepQueueWriter sweepQueue,
            ExecutorService deleteExecutor,
            boolean validateLocksOnReads,
            Supplier<TransactionConfig> transactionConfig,
            ConflictTracer conflictTracer,
            TableLevelMetricsController tableLevelMetricsController) {
        super(
                metricsManager,
                keyValueService,
                timelockService,
                lockWatchManager,
                transactionService,
                cleaner,
                startTimeStamp,
                conflictDetectionManager,
                sweepStrategyManager,
                immutableTimestamp,
                immutableTsLock,
                preCommitCondition,
                constraintCheckingMode,
                transactionTimeoutMillis,
                readSentinelBehavior,
                allowHiddenTableAccess,
                timestampCache,
                getRangesExecutor,
                defaultGetRangesConcurrency,
                sweepQueue,
                deleteExecutor,
                validateLocksOnReads,
                transactionConfig,
                conflictTracer,
                tableLevelMetricsController);
    }

    @Override
    @Idempotent
    public NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection) {
        NavigableMap<byte[], RowResult<byte[]>> ret = super.getRows(tableRef, rows, columnSelection);
        markRowsRead(tableRef, rows, columnSelection, ret.values());
        return ret;
    }

    @Override
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> ret =
                super.getRowsColumnRange(tableRef, rows, columnRangeSelection);
        return KeyedStream.stream(ret)
                .map((row, visitable) -> wrapWithColumnRangeChecking(tableRef, columnRangeSelection, row, visitable))
                .collectTo(() -> new TreeMap<>(UnsignedBytes.lexicographicalComparator()));
    }

    @Override
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        if (isSerializableTable(tableRef)) {
            throw new UnsupportedOperationException("This method does not support serializable conflict handling");
        }
        return super.getRowsColumnRange(tableRef, rows, columnRangeSelection, batchHint);
    }

    @Override
    public Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> getRowsColumnRangeIterator(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> ret =
                super.getRowsColumnRangeIterator(tableRef, rows, columnRangeSelection);
        return KeyedStream.stream(ret)
                .map((row, iterator) -> wrapIteratorWithBoundsChecking(tableRef, columnRangeSelection, row, iterator))
                .collectTo(() -> new TreeMap<>(UnsignedBytes.lexicographicalComparator()));
    }

    @Override
    public Iterator<Map.Entry<Cell, byte[]>> getSortedColumns(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection batchColumnRangeSelection) {
        if (Iterables.isEmpty(rows)) {
            return Collections.emptyIterator();
        }

        List<byte[]> distinctRows = getDistinctRows(rows);
        Iterator<Map.Entry<Cell, byte[]>> sortedColumns =
                super.getSortedColumns(tableRef, distinctRows, batchColumnRangeSelection);

        if (!isSerializableTable(tableRef)) {
            return sortedColumns;
        }

        // if only one row is passed in, this approach gives us one extra attempt at parallel conflict handling
        if (distinctRows.size() == 1) {
            return wrapIteratorWithBoundsChecking(
                    tableRef, batchColumnRangeSelection, Iterables.getOnlyElement(distinctRows), sortedColumns);
        }

        return wrapIteratorWithSortedColumnsBoundChecking(
                tableRef, distinctRows, batchColumnRangeSelection, sortedColumns);
    }

    private Iterator<Map.Entry<Cell, byte[]>> wrapIteratorWithSortedColumnsBoundChecking(
            TableReference tableRef,
            List<byte[]> distinctRows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            Iterator<Map.Entry<Cell, byte[]>> sortedColumns) {
        Comparator<Cell> cellComparator = columnOrderThenPreserveInputRowOrder(distinctRows);
        ByteBuffer maxRow = ByteBuffer.wrap(distinctRows.get(distinctRows.size() - 1));
        GetSortedColumnsRequest request =
                ImmutableGetSortedColumnsRequest.of(tableRef, distinctRows, batchColumnRangeSelection);
        AtomicReference<Cell> rangeEnd =
                sortedColumnRangeEnds.computeIfAbsent(request, _key -> new AtomicReference<>());
        ConcurrentNavigableMap<Cell, byte[]> readsForTable = getReadsForTable(tableRef);

        return new AbstractIterator<Map.Entry<Cell, byte[]>>() {
            @Override
            protected Map.Entry<Cell, byte[]> computeNext() {
                if (!sortedColumns.hasNext()) {
                    reachedEndOfRange();
                    return endOfData();
                }
                Map.Entry<Cell, byte[]> ret = sortedColumns.next();
                markReadUpTo(ret.getKey(), ret.getValue());
                return ret;
            }

            private void markReadUpTo(Cell cell, byte[] value) {
                readsForTable.put(cell, value);
                updateRangeEnd(Cell.create(cell.getRowName(), cell.getColumnName()));
            }

            private void reachedEndOfRange() {
                updateRangeEnd(Cell.create(maxRow.array(), RangeRequests.getLastColumnName()));
            }

            private void updateRangeEnd(Cell end) {
                if (RangeRequests.isLastColumnName(end.getColumnName())) {
                    rangeEnd.set(end);
                    return;
                }
                rangeEnd.accumulateAndGet(end, (curVal, newVal) -> {
                    if (curVal == null) {
                        return newVal;
                    } else if (RangeRequests.isLastColumnName(curVal.getColumnName())) {
                        return curVal;
                    }
                    return Ordering.from(cellComparator).max(curVal, newVal);
                });
            }
        };
    }

    @Override
    @Idempotent
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        try {
            return getWithLoader(
                            tableRef,
                            cells,
                            (tableReference, toRead) -> Futures.immediateFuture(super.get(tableRef, toRead)))
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        }
    }

    @Override
    @Idempotent
    public ListenableFuture<Map<Cell, byte[]>> getAsync(TableReference tableRef, Set<Cell> cells) {
        return getWithLoader(tableRef, cells, super::getAsync);
    }

    private ListenableFuture<Map<Cell, byte[]>> getWithLoader(
            TableReference tableRef, Set<Cell> cells, CellLoader cellLoader) {
        return Futures.transform(
                cellLoader.load(tableRef, cells),
                loadedCells -> {
                    markCellsRead(tableRef, cells, loadedCells);
                    return loadedCells;
                },
                MoreExecutors.directExecutor());
    }

    @FunctionalInterface
    private interface CellLoader {
        ListenableFuture<Map<Cell, byte[]>> load(TableReference tableReference, Set<Cell> toRead);
    }

    @Override
    @Idempotent
    public BatchingVisitable<RowResult<byte[]>> getRange(TableReference tableRef, RangeRequest rangeRequest) {
        final BatchingVisitable<RowResult<byte[]>> ret = super.getRange(tableRef, rangeRequest);
        return wrapRange(tableRef, rangeRequest, ret);
    }

    @Override
    @Idempotent
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(
            final TableReference tableRef, Iterable<RangeRequest> rangeRequests) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> ret = super.getRanges(tableRef, rangeRequests);
        Iterable<Pair<RangeRequest, BatchingVisitable<RowResult<byte[]>>>> zip = IterableUtils.zip(rangeRequests, ret);
        return Iterables.transform(zip, pair -> wrapRange(tableRef, pair.lhSide, pair.rhSide));
    }

    private BatchingVisitable<RowResult<byte[]>> wrapRange(
            final TableReference tableRef,
            final RangeRequest rangeRequest,
            final BatchingVisitable<RowResult<byte[]>> ret) {
        return new BatchingVisitable<RowResult<byte[]>>() {
            @Override
            public <K extends Exception> boolean batchAccept(
                    int batchSize, AbortingVisitor<? super List<RowResult<byte[]>>, K> visitor) throws K {
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
        return readsByTable.computeIfAbsent(table, unused -> new ConcurrentSkipListMap<>());
    }

    private void setRangeEnd(TableReference table, RangeRequest range, byte[] maxRow) {
        Preconditions.checkNotNull(maxRow, "maxRow cannot be null");
        ConcurrentMap<RangeRequest, byte[]> rangeEnds =
                rangeEndByTable.computeIfAbsent(table, unused -> new ConcurrentHashMap<>());

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
            TableReference table, byte[] unwrappedRow, BatchColumnRangeSelection columnRangeSelection, byte[] maxCol) {
        Preconditions.checkNotNull(maxCol, "maxCol cannot be null");
        ByteBuffer row = ByteBuffer.wrap(unwrappedRow);
        columnRangeEndsByTable.computeIfAbsent(table, unused -> new ConcurrentHashMap<>());
        ConcurrentMap<BatchColumnRangeSelection, byte[]> rangeEndsForRow =
                columnRangeEndsByTable.get(table).computeIfAbsent(row, unused -> new ConcurrentHashMap<>());

        if (maxCol.length == 0) {
            rangeEndsForRow.put(columnRangeSelection, maxCol);
        }

        rangeEndsForRow.compute(columnRangeSelection, (range, curVal) -> {
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
        Set<Cell> cellsForTable = cellsRead.computeIfAbsent(table, unused -> ConcurrentHashMap.newKeySet());
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
            TableReference table, byte[] row, BatchColumnRangeSelection range, List<Map.Entry<Cell, byte[]>> result) {
        if (!isSerializableTable(table)) {
            return;
        }
        ConcurrentNavigableMap<Cell, byte[]> reads = getReadsForTable(table);
        Map<Cell, byte[]> map = Maps2.fromEntries(result);
        reads.putAll(transformGetsForTesting(map));
        setColumnRangeEnd(table, row, range, Iterables.getLast(result).getKey().getColumnName());
    }

    private static class RowRead {
        final ImmutableList<byte[]> rows;
        final ColumnSelection cols;

        RowRead(Iterable<byte[]> rows, ColumnSelection cols) {
            this.rows = ImmutableList.copyOf(rows);
            this.cols = cols;
        }
    }

    private void markRowsRead(
            TableReference table, Iterable<byte[]> rows, ColumnSelection cols, Iterable<RowResult<byte[]>> result) {
        if (!isSerializableTable(table)) {
            return;
        }
        ConcurrentNavigableMap<Cell, byte[]> reads = getReadsForTable(table);
        for (RowResult<byte[]> row : result) {
            Map<Cell, byte[]> map = Maps2.fromEntries(row.getCells());
            reads.putAll(transformGetsForTesting(map));
        }

        Set<RowRead> rowReads = rowsRead.computeIfAbsent(table, unused -> ConcurrentHashMap.newKeySet());
        rowReads.add(new RowRead(rows, cols));
    }

    private void reachedEndOfRange(TableReference table, RangeRequest range) {
        if (!isSerializableTable(table)) {
            return;
        }
        setRangeEnd(table, range, PtBytes.EMPTY_BYTE_ARRAY);
    }

    private void reachedEndOfColumnRange(
            TableReference table, byte[] row, BatchColumnRangeSelection columnRangeSelection) {
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
        verifyGetSortedColumns(ro);
    }

    private void verifyRows(Transaction ro) {
        for (Map.Entry<TableReference, Set<RowRead>> tableAndRowsEntry : rowsRead.entrySet()) {
            TableReference table = tableAndRowsEntry.getKey();
            Set<RowRead> rows = tableAndRowsEntry.getValue();

            ConcurrentNavigableMap<Cell, byte[]> readsForTable = getReadsForTable(table);
            Multimap<ColumnSelection, byte[]> rowsReadByColumns = Multimaps.newSortedSetMultimap(
                    new HashMap<>(), () -> new TreeSet<>(UnsignedBytes.lexicographicalComparator()));
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
                Map<Cell, byte[]> originalReads = readsForTable
                        .tailMap(Cells.createSmallestCellForRow(row), true)
                        .headMap(Cells.createLargestCellForRow(row), true);

                // We want to filter out all our reads to just the set that matches our column selection.
                originalReads = Maps.filterKeys(originalReads, input -> columns.contains(input.getColumnName()));

                if (writesByTable.get(table) != null) {
                    // We don't want to verify any reads that we wrote to cause
                    // we will just read our own values.
                    // NB: We filter our write set out here because our normal SI
                    // checking handles this case to ensure the value hasn't changed.
                    originalReads = Maps.filterKeys(
                            originalReads,
                            Predicates.not(
                                    Predicates.in(writesByTable.get(table).keySet())));
                }

                if (currentRow == null && originalReads.isEmpty()) {
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
                            Predicates.not(
                                    Predicates.in(writesByTable.get(table).keySet())));
                }
                if (!areMapsEqual(originalReads, currentCells)) {
                    handleTransactionConflict(table);
                }
            }
        }
    }

    private static boolean areMapsEqual(Map<Cell, byte[]> map1, Map<Cell, byte[]> map2) {
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
        for (Map.Entry<TableReference, Set<Cell>> tableAndCellsEntry : cellsRead.entrySet()) {
            TableReference table = tableAndCellsEntry.getKey();
            Set<Cell> cells = tableAndCellsEntry.getValue();

            final ConcurrentNavigableMap<Cell, byte[]> readsForTable = getReadsForTable(table);
            for (Iterable<Cell> batch : Iterables.partition(cells, BATCH_SIZE)) {
                // We don't want to verify any reads that we wrote to cause we will just read our own values.
                // NB: If the value has changed between read and write, our normal SI checking handles this case
                Iterable<Cell> batchWithoutWrites = writesByTable.get(table) != null
                        ? Iterables.filter(
                                batch,
                                Predicates.not(
                                        Predicates.in(writesByTable.get(table).keySet())))
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
        for (Map.Entry<TableReference, ConcurrentMap<RangeRequest, byte[]>> tableAndRange :
                rangeEndByTable.entrySet()) {
            TableReference table = tableAndRange.getKey();
            Map<RangeRequest, byte[]> rangeEnds = tableAndRange.getValue();

            for (Map.Entry<RangeRequest, byte[]> rangeAndRangeEndEntry : rangeEnds.entrySet()) {
                RangeRequest range = rangeAndRangeEndEntry.getKey();
                byte[] rangeEnd = rangeAndRangeEndEntry.getValue();

                if (rangeEnd.length != 0 && !RangeRequests.isTerminalRow(range.isReverse(), rangeEnd)) {
                    range = range.getBuilder()
                            .endRowExclusive(RangeRequests.getNextStartRow(range.isReverse(), rangeEnd))
                            .build();
                }

                ConcurrentNavigableMap<Cell, byte[]> writes = writesByTable.get(table);
                BatchingVisitableView<RowResult<byte[]>> bv =
                        BatchingVisitableView.of(readOnlyTransaction.getRange(table, range));
                NavigableMap<Cell, ByteBuffer> readsInRange =
                        Maps.transformValues(getReadsInRange(table, range), ByteBuffer::wrap);
                if (!bv.transformBatch(input -> filterWritesFromRows(input, writes))
                        .isEqual(readsInRange.entrySet())) {
                    handleTransactionConflict(table);
                }
            }
        }
    }

    private NavigableMap<Cell, byte[]> getReadsInColumnRangeSkippingWrites(
            TableReference table, byte[] row, BatchColumnRangeSelection range) {
        NavigableMap<Cell, byte[]> reads = getReadsForTable(table);
        Cell startCell = Cells.createSmallestCellForRow(row);
        if ((range.getStartCol() != null) && (range.getStartCol().length > 0)) {
            startCell = Cell.create(row, range.getStartCol());
        }
        reads = reads.tailMap(startCell, true);
        if ((range.getEndCol() != null) && (range.getEndCol().length > 0)) {
            Cell endCell = Cell.create(row, range.getEndCol());
            reads = reads.headMap(endCell, false);
        } else if (!RangeRequests.isLastRowName(row)) {
            Cell endCell = Cells.createSmallestCellForRow(RangeRequests.nextLexicographicName(row));
            reads = reads.headMap(endCell, false);
        }
        ConcurrentNavigableMap<Cell, byte[]> writes = writesByTable.get(table);
        if (writes != null) {
            reads = Maps.filterKeys(reads, Predicates.not(Predicates.in(writes.keySet())));
        }
        return reads;
    }

    private void verifyColumnRanges(Transaction readOnlyTransaction) {
        // verify each set of reads to ensure they are the same.
        for (Map.Entry<TableReference, ConcurrentMap<ByteBuffer, ConcurrentMap<BatchColumnRangeSelection, byte[]>>>
                tableAndColumnRangeEnds : columnRangeEndsByTable.entrySet()) {

            Map<ByteBuffer, ConcurrentMap<BatchColumnRangeSelection, byte[]>> columnRangeEnds =
                    tableAndColumnRangeEnds.getValue();

            Multimap<BatchColumnRangeSelection, byte[]> rangesToRows = LinkedListMultimap.create();
            for (Map.Entry<ByteBuffer, ConcurrentMap<BatchColumnRangeSelection, byte[]>> rowAndRangeEnds :
                    columnRangeEnds.entrySet()) {
                byte[] row = rowAndRangeEnds.getKey().array();
                Map<BatchColumnRangeSelection, byte[]> rangeEnds = rowAndRangeEnds.getValue();
                for (Map.Entry<BatchColumnRangeSelection, byte[]> e : rangeEnds.entrySet()) {
                    BatchColumnRangeSelection range = e.getKey();
                    byte[] rangeEnd = e.getValue();
                    rangesToRows.put(getBatchColumnRangeSelectionForEntriesReadSoFar(range, rangeEnd), row);
                }
            }

            TableReference table = tableAndColumnRangeEnds.getKey();
            rangesToRows.asMap().forEach((columnRange, rows) -> {
                Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> result =
                        readOnlyTransaction.getRowsColumnRange(table, rows, columnRange);

                for (Map.Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> cellValuesForRow :
                        result.entrySet()) {
                    byte[] row = cellValuesForRow.getKey();
                    BatchingVisitableView<Map.Entry<Cell, byte[]>> visitable =
                            BatchingVisitableView.of(cellValuesForRow.getValue());
                    NavigableMap<Cell, ByteBuffer> readsInRange = Maps.transformValues(
                            getReadsInColumnRangeSkippingWrites(table, row, columnRange), ByteBuffer::wrap);
                    boolean isEqual = visitable
                            .transformBatch(cellValues -> filterWritesFromCells(cellValues, table))
                            .isEqual(readsInRange.entrySet());
                    if (!isEqual) {
                        handleTransactionConflict(table);
                    }
                }
            });
        }
    }

    private void verifyGetSortedColumns(Transaction readOnlyTransaction) {
        sortedColumnRangeEnds.forEach((request, endOfRangeReference) -> {
            Cell endOfRange = endOfRangeReference.get();
            // no checks required if no data has been read so far
            if (endOfRange == null) {
                return;
            }
            BatchColumnRangeSelection range = getBatchColumnRangeSelectionForEntriesReadSoFar(
                    request.getColumnRangeSelection(), endOfRange.getColumnName());
            Iterable<byte[]> rows = request.getRows();
            Comparator<Cell> comparator = columnOrderThenPreserveInputRowOrder(request.getRows());
            Iterator<Map.Entry<Cell, ByteBuffer>> readValues =
                    readSortedColumns(request.getTableRef(), rows, range, comparator);

            Iterator<Map.Entry<Cell, byte[]>> storedValues =
                    readOnlyTransaction.getSortedColumns(request.getTableRef(), rows, range);

            // handles the case where (r1, c), (r2, c) exists and we read only up to (r1, c).
            Iterator<Map.Entry<Cell, byte[]>> truncatedStoredValues = new AbstractIterator<Map.Entry<Cell, byte[]>>() {
                @Override
                protected Map.Entry<Cell, byte[]> computeNext() {
                    if (!storedValues.hasNext()) {
                        return endOfData();
                    }

                    Map.Entry<Cell, byte[]> ret = storedValues.next();
                    if (comparator.compare(ret.getKey(), endOfRange) > 0) {
                        return endOfData();
                    }
                    return ret;
                }
            };

            List<Map.Entry<Cell, ByteBuffer>> actualReadList =
                    Streams.stream(readValues).collect(Collectors.toList());
            List<Map.Entry<Cell, ByteBuffer>> storedValuesWithoutLocalWrites = filterWritesFromCells(
                    Streams.stream(truncatedStoredValues).collect(Collectors.toList()), request.getTableRef());

            if (!actualReadList.equals(storedValuesWithoutLocalWrites)) {
                handleTransactionConflict(request.getTableRef());
            }
        });
    }

    private Iterator<Map.Entry<Cell, ByteBuffer>> readSortedColumns(
            TableReference table, Iterable<byte[]> rows, BatchColumnRangeSelection range, Comparator<Cell> comparator) {
        Iterator<Map.Entry<Cell, byte[]>> merged = mergeByComparator(
                Iterables.transform(rows, row -> getReadsInColumnRangeSkippingWrites(table, row, range)
                        .entrySet()
                        .iterator()),
                comparator);
        return Iterators.transform(merged, this::getCellEntryWithByteBufferWrappedValue);
    }

    private Map.Entry<Cell, ByteBuffer> getCellEntryWithByteBufferWrappedValue(Map.Entry<Cell, byte[]> cellEntry) {
        return Maps.immutableEntry(cellEntry.getKey(), ByteBuffer.wrap(cellEntry.getValue()));
    }

    private static BatchColumnRangeSelection getBatchColumnRangeSelectionForEntriesReadSoFar(
            BatchColumnRangeSelection currentRange, byte[] greatestColumnSoFar) {
        if (allEntriesRead(greatestColumnSoFar)) {
            return currentRange;
        }
        return BatchColumnRangeSelection.create(
                currentRange.getStartCol(),
                RangeRequests.getNextStartRow(false, greatestColumnSoFar),
                currentRange.getBatchHint());
    }

    private static boolean allEntriesRead(byte[] greatestColumnSoFar) {
        return greatestColumnSoFar.length == 0 || RangeRequests.isLastColumnName(greatestColumnSoFar);
    }

    private List<Map.Entry<Cell, ByteBuffer>> filterWritesFromCells(
            Iterable<Map.Entry<Cell, byte[]>> cells, TableReference table) {
        return filterWritesFromCells(cells, writesByTable.get(table));
    }

    private static List<Map.Entry<Cell, ByteBuffer>> filterWritesFromCells(
            Iterable<Map.Entry<Cell, byte[]>> cells, @Nullable Map<Cell, byte[]> writes) {
        List<Map.Entry<Cell, ByteBuffer>> cellsWithoutWrites = new ArrayList<>();
        for (Map.Entry<Cell, byte[]> cell : cells) {
            // NB: We filter our write set out here because our normal SI
            // checking handles this case to ensure the value hasn't changed.
            if (writes == null || !writes.containsKey(cell.getKey())) {
                cellsWithoutWrites.add(Maps.immutableEntry(cell.getKey(), ByteBuffer.wrap(cell.getValue())));
            }
        }
        return cellsWithoutWrites;
    }

    private static List<Map.Entry<Cell, ByteBuffer>> filterWritesFromRows(
            Iterable<RowResult<byte[]>> rows, @Nullable Map<Cell, byte[]> writes) {
        List<Map.Entry<Cell, ByteBuffer>> rowsWithoutWrites = new ArrayList<>();
        for (RowResult<byte[]> row : rows) {
            rowsWithoutWrites.addAll(filterWritesFromCells(row.getCells(), writes));
        }
        return rowsWithoutWrites;
    }

    private NavigableMap<Cell, byte[]> getReadsInRange(TableReference table, RangeRequest range) {
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
            Predicate<Cell> columnInNames =
                    Predicates.compose(Predicates.in(range.getColumnNames()), Cell::getColumnName);
            reads = Maps.filterKeys(reads, columnInNames);
        }
        return reads;
    }

    private Transaction getReadOnlyTransaction(final long commitTs) {
        return new SnapshotTransaction(
                metricsManager,
                keyValueService,
                timelockService,
                lockWatchManager,
                defaultTransactionService,
                NoOpCleaner.INSTANCE,
                Suppliers.ofInstance(commitTs + 1),
                ConflictDetectionManagers.createWithNoConflictDetection(),
                sweepStrategyManager,
                immutableTimestamp,
                Optional.empty(),
                PreCommitConditions.NO_OP,
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                transactionReadTimeoutMillis,
                getReadSentinelBehavior(),
                allowHiddenTableAccess,
                timestampValidationReadCache,
                getRangesExecutor,
                defaultGetRangesConcurrency,
                sweepQueue,
                deleteExecutor,
                validateLocksOnReads,
                transactionConfig,
                conflictTracer,
                tableLevelMetricsController) {
            @Override
            protected TransactionScopedCache getCache() {
                return lockWatchManager.getReadOnlyTransactionScopedCache(SerializableTransaction.this.getTimestamp());
            }

            @Override
            protected ListenableFuture<Map<Long, Long>> getCommitTimestamps(
                    TableReference tableRef,
                    Iterable<Long> startTimestamps,
                    boolean shouldWaitForCommitterToComplete,
                    AsyncTransactionService asyncTransactionService) {
                long myStart = SerializableTransaction.this.getTimestamp();
                PartitionedTimestamps partitionedTimestamps = splitTransactionBeforeAndAfter(myStart, startTimestamps);

                ListenableFuture<Map<Long, Long>> postStartCommitTimestamps =
                        getCommitTimestampsForTransactionsStartedAfterMe(
                                tableRef, asyncTransactionService, partitionedTimestamps.afterStart());

                // We are ok to block here because if there is a cycle of transactions that could result in a deadlock,
                // then at least one of them will be in the ab
                ListenableFuture<Map<Long, Long>> preStartCommitTimestamps = super.getCommitTimestamps(
                        tableRef,
                        partitionedTimestamps.beforeStart(),
                        shouldWaitForCommitterToComplete,
                        asyncTransactionService);

                return Futures.whenAllComplete(postStartCommitTimestamps, preStartCommitTimestamps)
                        .call(
                                () -> ImmutableMap.<Long, Long>builder()
                                        .putAll(AtlasFutures.getDone(preStartCommitTimestamps))
                                        .putAll(AtlasFutures.getDone(postStartCommitTimestamps))
                                        .putAll(partitionedTimestamps.myCommittedTransaction())
                                        .build(),
                                MoreExecutors.directExecutor());
            }

            private ListenableFuture<Map<Long, Long>> getCommitTimestampsForTransactionsStartedAfterMe(
                    TableReference tableRef,
                    AsyncTransactionService asyncTransactionService,
                    Set<Long> startTimestamps) {
                if (startTimestamps.isEmpty()) {
                    return Futures.immediateFuture(ImmutableMap.of());
                }

                return Futures.transform(
                        // We do not block when waiting for results that were written after our start timestamp.
                        // If we block here it may lead to deadlock if two transactions (or a cycle of any length) have
                        // all written their data and all doing checks before committing.
                        super.getCommitTimestamps(tableRef, startTimestamps, false, asyncTransactionService),
                        startToCommitTimestamps -> {
                            if (startToCommitTimestamps.keySet().containsAll(startTimestamps)) {
                                return startToCommitTimestamps;
                            }
                            // If we do not get back all these results we may be in the deadlock case so we
                            // should just fail out early.  It may be the case that abort more transactions
                            // than needed to break the deadlock cycle, but this should be pretty rare.
                            transactionOutcomeMetrics.markReadWriteConflict(tableRef);
                            throw new TransactionSerializableConflictException("An uncommitted conflicting read was "
                                    + "written after our start timestamp for table "
                                    + tableRef + ".  "
                                    + "This case can cause deadlock and is very likely to be a "
                                    + "read write conflict.");
                        },
                        MoreExecutors.directExecutor());
            }

            /**
             * Partitions {@code startTimestamps} in two sets, based on their relation to the start timestamp provided.
             *
             * @param myStart start timestamp of this transaction
             * @param startTimestamps of transactions we are interested in
             * @return a {@link PartitionedTimestamps} object containing split timestamps
             */
            private PartitionedTimestamps splitTransactionBeforeAndAfter(long myStart, Iterable<Long> startTimestamps) {
                ImmutablePartitionedTimestamps.Builder builder =
                        ImmutablePartitionedTimestamps.builder().myCommitTimestamp(commitTs);
                startTimestamps.forEach(startTimestamp -> {
                    if (startTimestamp == myStart) {
                        builder.splittingStartTimestamp(myStart);
                    } else if (startTimestamp < myStart) {
                        builder.addBeforeStart(startTimestamp);
                    } else {
                        builder.addAfterStart(startTimestamp);
                    }
                });

                return builder.build();
            }
        };
    }

    private void handleTransactionConflict(TableReference tableRef) {
        transactionOutcomeMetrics.markReadWriteConflict(tableRef);
        log.info("Serializable conflict", LoggingArgs.tableRef(tableRef));
        throw TransactionSerializableConflictException.create(
                tableRef, getTimestamp(), System.currentTimeMillis() - timeCreated);
    }

    private BatchingVisitable<Map.Entry<Cell, byte[]>> wrapWithColumnRangeChecking(
            TableReference tableRef,
            BatchColumnRangeSelection columnRangeSelection,
            byte[] row,
            BatchingVisitable<Map.Entry<Cell, byte[]>> visitable) {
        return new BatchingVisitable<Map.Entry<Cell, byte[]>>() {
            @Override
            public <K extends Exception> boolean batchAccept(
                    int batchSize, AbortingVisitor<? super List<Map.Entry<Cell, byte[]>>, K> visitor) throws K {
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
        };
    }

    public Iterator<Map.Entry<Cell, byte[]>> wrapIteratorWithBoundsChecking(
            TableReference tableRef,
            BatchColumnRangeSelection columnRangeSelection,
            byte[] row,
            Iterator<Map.Entry<Cell, byte[]>> iterator) {
        return new Iterator<Map.Entry<Cell, byte[]>>() {
            Map.Entry<Cell, byte[]> next = null;

            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                }

                if (iterator.hasNext()) {
                    next = iterator.next();
                    markRowColumnRangeRead(tableRef, row, columnRangeSelection, Collections.singletonList(next));
                    return true;
                }

                reachedEndOfColumnRange(tableRef, row, columnRangeSelection);
                return false;
            }

            @Override
            public Map.Entry<Cell, byte[]> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Map.Entry<Cell, byte[]> result = next;
                next = null;
                return result;
            }
        };
    }

    @Value.Immutable
    interface PartitionedTimestamps {
        long myCommitTimestamp();

        Set<Long> afterStart();

        Set<Long> beforeStart();

        OptionalLong splittingStartTimestamp();

        @Value.Derived
        default Map<Long, Long> myCommittedTransaction() {
            return splittingStartTimestamp().isPresent()
                    ? ImmutableMap.of(splittingStartTimestamp().getAsLong(), myCommitTimestamp())
                    : ImmutableMap.of();
        }
    }

    @Value.Immutable(prehash = true)
    interface GetSortedColumnsRequest {
        @Value.Parameter
        TableReference getTableRef();

        @Value.Parameter
        List<byte[]> getRows();

        @Value.Parameter
        BatchColumnRangeSelection getColumnRangeSelection();
    }
}
