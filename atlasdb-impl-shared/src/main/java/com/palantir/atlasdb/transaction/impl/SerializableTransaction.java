/**
 * Copyright 2015 Palantir Technologies
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
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
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionSerializableConflictException;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.collect.IterableUtils;
import com.palantir.common.collect.Maps2;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;
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
    private final static Logger log = LoggerFactory.getLogger(SerializableTransaction.class);

    final ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> readsByTable = Maps.newConcurrentMap();
    final ConcurrentMap<TableReference, ConcurrentMap<RangeRequest, byte[]>> rangeEndByTable = Maps.newConcurrentMap();
    final ConcurrentMap<TableReference, Set<Cell>> cellsRead = Maps.newConcurrentMap();
    final ConcurrentMap<TableReference, Set<RowRead>> rowsRead = Maps.newConcurrentMap();

    public SerializableTransaction(KeyValueService keyValueService,
                                   RemoteLockService lockService,
                                   TimestampService timestampService,
                                   TransactionService transactionService,
                                   Cleaner cleaner,
                                   Supplier<Long> startTimeStamp,
                                   ConflictDetectionManager conflictDetectionManager,
                                   SweepStrategyManager sweepStrategyManager,
                                   long immutableTimestamp,
                                   Iterable<LockRefreshToken> tokensValidForCommit,
                                   AtlasDbConstraintCheckingMode constraintCheckingMode,
                                   Long transactionTimeoutMillis,
                                   TransactionReadSentinelBehavior readSentinelBehavior,
                                   boolean allowHiddenTableAccess) {
        super(keyValueService,
              lockService,
              timestampService,
              transactionService,
              cleaner,
              startTimeStamp,
              conflictDetectionManager,
              sweepStrategyManager,
              immutableTimestamp,
              tokensValidForCommit,
              constraintCheckingMode,
              transactionTimeoutMillis,
              readSentinelBehavior,
              allowHiddenTableAccess);
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
    @Idempotent
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        Map<Cell, byte[]> ret = super.get(tableRef, cells);
        markCellsRead(tableRef, cells, ret);
        return ret;
    }

    @Override
    @Idempotent
    public BatchingVisitable<RowResult<byte[]>> getRange(final TableReference tableRef, final RangeRequest rangeRequest) {
        final BatchingVisitable<RowResult<byte[]>> ret = super.getRange(tableRef, rangeRequest);
        return wrapRange(tableRef, rangeRequest, ret);
    }

    @Override
    @Idempotent
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(final TableReference tableRef,
                                                             Iterable<RangeRequest> rangeRequests) {
        Iterable<BatchingVisitable<RowResult<byte[]>>> ret = super.getRanges(tableRef, rangeRequests);
        Iterable<Pair<RangeRequest, BatchingVisitable<RowResult<byte[]>>>> zip = IterableUtils.zip(rangeRequests, ret);
        return Iterables.transform(zip, new Function<Pair<RangeRequest, BatchingVisitable<RowResult<byte[]>>>, BatchingVisitable<RowResult<byte[]>>>() {
            @Override
            public BatchingVisitable<RowResult<byte[]>> apply(Pair<RangeRequest, BatchingVisitable<RowResult<byte[]>>> pair) {
                return wrapRange(tableRef, pair.lhSide, pair.rhSide);
            }
        });
    }

    private BatchingVisitable<RowResult<byte[]>> wrapRange(final TableReference tableRef,
                                                           final RangeRequest rangeRequest,
                                                           final BatchingVisitable<RowResult<byte[]>> ret) {
        return new BatchingVisitable<RowResult<byte[]>>() {
            @Override
            public <K extends Exception> boolean batchAccept(final int batchSize,
                                                             final AbortingVisitor<? super List<RowResult<byte[]>>, K> v)
                    throws K {
                boolean hitEnd = ret.batchAccept(batchSize, new AbortingVisitor<List<RowResult<byte[]>>, K>() {
                    @Override
                    public boolean visit(List<RowResult<byte[]>> items) throws K {
                        if (items.size() < batchSize) {
                            reachedEndOfRange(tableRef, rangeRequest);
                        }
                        markRangeRead(tableRef, rangeRequest, items);
                        return v.visit(items);
                    }
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
            ConcurrentNavigableMap<Cell, byte[]> newMap = new ConcurrentSkipListMap<Cell, byte[]>();
            readsByTable.putIfAbsent(table, newMap);
            reads = readsByTable.get(table);
        }
        return reads;
    }

    private void setRangeEnd(TableReference table, RangeRequest range, byte[] maxRow) {
        Validate.notNull(maxRow);
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

    boolean isSerializableTable(TableReference table) {
        return getConflictHandlerForTable(table) == ConflictHandler.SERIALIZABLE;
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
        result = transformGetsForTesting(result);
        getReadsForTable(table).putAll(result);
        Set<Cell> cellsForTable = cellsRead.get(table);
        if (cellsForTable == null) {
            cellsRead.putIfAbsent(table, Sets.<Cell>newConcurrentHashSet());
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
        setRangeEnd(table, range, result.get(result.size()-1).getRowName());
    }

    static class RowRead {
        final ImmutableList<byte[]> rows;
        final ColumnSelection cols;

        RowRead(Iterable<byte[]> rows, ColumnSelection cols) {
            this.rows = ImmutableList.copyOf(rows);
            this.cols = cols;
        }
    }

    private void markRowsRead(TableReference table, Iterable<byte[]> rows, ColumnSelection cols, Iterable<RowResult<byte[]>> result) {
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

    @Override
    @Idempotent
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        super.put(tableRef, values);
    }

    @Override
    protected void throwIfReadWriteConflictForSerializable(long commitTimestamp) {
        Transaction ro = getReadOnlyTransaction(commitTimestamp);
        verifyRanges(ro);
        verifyCells(ro);
        verifyRows(ro);
    }

    private void verifyRows(Transaction ro) {
        for (TableReference table : rowsRead.keySet()) {
            final ConcurrentNavigableMap<Cell, byte[]> readsForTable = getReadsForTable(table);
            Multimap<ColumnSelection, byte[]> map = Multimaps.newSortedSetMultimap(Maps.<ColumnSelection, Collection<byte[]>>newHashMap(), new Supplier<SortedSet<byte[]>>() {
                @Override
                public TreeSet<byte[]> get() {
                    return Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
                }
            });
            for (RowRead r : rowsRead.get(table)) {
                map.putAll(r.cols, r.rows);
            }
            for (final ColumnSelection cols : map.keySet()) {
                for (List<byte[]> batch : Iterables.partition(map.get(cols), 1000)) {
                    SortedMap<byte[], RowResult<byte[]>> currentRows = ro.getRows(table, batch, cols);
                    for (byte[] row : batch) {
                        RowResult<byte[]> currentRow = currentRows.get(row);
                        Map<Cell, byte[]> orignalReads = readsForTable.tailMap(Cells.createSmallestCellForRow(row), true).headMap(Cells.createLargestCellForRow(row), true);

                        // We want to filter out all our reads to just the set that matches our column selection.
                        orignalReads = Maps.filterKeys(orignalReads, new Predicate<Cell>() {
                            @Override
                            public boolean apply(Cell input) {
                                return cols.contains(input.getColumnName());
                            }
                        });

                        if (writesByTable.get(table) != null) {
                            // We don't want to verify any reads that we wrote to cause we will just read our own values.
                            // NB: We filter our write set out here because our normal SI checking handles this case to ensure the value hasn't changed.
                            orignalReads = Maps.filterKeys(orignalReads, Predicates.not(Predicates.in(writesByTable.get(table).keySet())));
                        }

                        if (currentRow == null && orignalReads.isEmpty()) {
                            continue;
                        }

                        if (currentRow == null) {
                            throw TransactionSerializableConflictException.create(table, getTimestamp(), System.currentTimeMillis() - timeCreated);
                        }

                        Map<Cell, byte[]> currentCells = Maps2.fromEntries(currentRow.getCells());
                        if (writesByTable.get(table) != null) {
                            // We don't want to verify any reads that we wrote to cause we will just read our own values.
                            // NB: We filter our write set out here because our normal SI checking handles this case to ensure the value hasn't changed.
                            currentCells = Maps.filterKeys(currentCells, Predicates.not(Predicates.in(writesByTable.get(table).keySet())));
                        }
                        if (!areMapsEqual(orignalReads, currentCells)) {
                            throw TransactionSerializableConflictException.create(table, getTimestamp(), System.currentTimeMillis() - timeCreated);
                        }
                    }
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

    private void verifyCells(Transaction ro) {
        for (TableReference table : cellsRead.keySet()) {
            final ConcurrentNavigableMap<Cell, byte[]> readsForTable = getReadsForTable(table);
            for (Iterable<Cell> batch : Iterables.partition(cellsRead.get(table), 1000)) {
                if (writesByTable.get(table) != null) {
                    // We don't want to verify any reads that we wrote to cause we will just read our own values.
                    // NB: If the value has changed between read and write, our normal SI checking handles this case
                    batch = Iterables.filter(batch, Predicates.not(Predicates.in(writesByTable.get(table).keySet())));
                }
                ImmutableSet<Cell> batchSet = ImmutableSet.copyOf(batch);
                Map<Cell, byte[]> currentBatch = ro.get(table, batchSet);
                ImmutableMap<Cell, byte[]> originalReads = Maps.toMap(Sets.intersection(batchSet, readsForTable.keySet()), Functions.forMap(readsForTable));
                if (!areMapsEqual(currentBatch, originalReads)) {
                    throw TransactionSerializableConflictException.create(table, getTimestamp(), System.currentTimeMillis() - timeCreated);
                }
            }
        }
    }

    private void verifyRanges(Transaction ro) {
        // verify each set of reads to ensure they are the same.
        for (TableReference table : rangeEndByTable.keySet()) {
            for (Entry<RangeRequest, byte[]> e : rangeEndByTable.get(table).entrySet()) {
                RangeRequest range = e.getKey();
                byte[] rangeEnd = e.getValue();
                if (rangeEnd.length != 0 && !RangeRequests.isTerminalRow(range.isReverse(), rangeEnd)) {
                    range = range.getBuilder().endRowExclusive(RangeRequests.getNextStartRow(range.isReverse(), rangeEnd)).build();
                }

                final ConcurrentNavigableMap<Cell, byte[]> writes = writesByTable.get(table);
                BatchingVisitableView<RowResult<byte[]>> bv = BatchingVisitableView.of(ro.getRange(table, range));
                NavigableMap<Cell, ByteBuffer> readsInRange = Maps.transformValues(getReadsInRange(table, e, range),
                        new Function<byte[], ByteBuffer>() {
                            @Override
                            public ByteBuffer apply(byte[] input) {
                                return ByteBuffer.wrap(input);
                            }
                        });
                boolean isEqual = bv.transformBatch(new Function<List<RowResult<byte[]>>, List<Entry<Cell, ByteBuffer>>>() {
                    @Override
                    public List<Entry<Cell, ByteBuffer>> apply(List<RowResult<byte[]>> input) {
                        List<Entry<Cell, ByteBuffer>> ret = Lists.newArrayList();
                        for (RowResult<byte[]> row : input) {
                            for (Entry<Cell, byte[]> cell : row.getCells()) {

                                // NB: We filter our write set out here because our normal SI checking handles this case to ensure the value hasn't changed.
                                if (writes == null || !writes.containsKey(cell.getKey())) {
                                    ret.add(Maps.immutableEntry(cell.getKey(), ByteBuffer.wrap(cell.getValue())));
                                }
                            }
                        }
                        return ret;
                    }
                }).isEqual(readsInRange.entrySet());
                if (!isEqual) {
                    throw TransactionSerializableConflictException.create(table, getTimestamp(), System.currentTimeMillis() - timeCreated);
                }
            }
        }
    }

    private NavigableMap<Cell, byte[]> getReadsInRange(TableReference table,
                                                       Entry<RangeRequest, byte[]> e,
                                                       RangeRequest range) {
        NavigableMap<Cell, byte[]> reads = getReadsForTable(table);
        if (range.getStartInclusive().length != 0) {
            reads = reads.tailMap(Cells.createSmallestCellForRow(range.getStartInclusive()), true);
        }
        if (range.getEndExclusive().length != 0) {
            reads = reads.headMap(Cells.createSmallestCellForRow(range.getEndExclusive()), false);
        }
        ConcurrentNavigableMap<Cell, byte[]> writes = writesByTable.get(table);
        if (writes != null) {
            reads = Maps.filterKeys(reads, Predicates.not(Predicates.in(writes.keySet())));
        }
        if (!range.getColumnNames().isEmpty()) {
            Predicate<Cell> columnInNames = Predicates.compose(Predicates.in(range.getColumnNames()), Cells.getColumnFunction());
            reads = Maps.filterKeys(reads, columnInNames);
        }
        return reads;
    }


    private Transaction getReadOnlyTransaction(final long commitTs) {
        return new SnapshotTransaction(
                keyValueService,
                lockService,
                timestampService,
                defaultTransactionService,
                NoOpCleaner.INSTANCE,
                Suppliers.ofInstance(commitTs + 1),
                ConflictDetectionManagers.withoutConflictDetection(keyValueService),
                sweepStrategyManager,
                immutableTimestamp,
                Collections.<LockRefreshToken>emptyList(),
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                transactionReadTimeoutMillis,
                getReadSentinelBehavior(),
                allowHiddenTableAccess) {
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
                        throw new TransactionSerializableConflictException("An uncommitted conflicting read was " +
                                "written after our start timestamp for table " + tableRef + ".  " +
                                "This case can cause deadlock and is very likely to be a read write conflict.");
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
}
