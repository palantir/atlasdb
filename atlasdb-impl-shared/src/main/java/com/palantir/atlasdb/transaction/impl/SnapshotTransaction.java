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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.AtlasDbPerformanceConstants;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.impl.RowResults;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.description.exceptions.AtlasDbConstraintException;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.atlasdb.transaction.api.ConstraintCheckingTransaction;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionCommitFailedException;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionConflictException.CellConflict;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionLockAcquisitionTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetrics;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbstractBatchingVisitable;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableFromIterable;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.ForwardingClosableIterator;
import com.palantir.common.collect.IteratorUtils;
import com.palantir.common.collect.MapEntries;
import com.palantir.common.streams.MoreStreams;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.tracing.CloseableTracer;
import com.palantir.util.AssertUtils;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * This implements snapshot isolation for transactions.
 * <p>
 * This object is thread safe and you may do reads and writes from multiple threads.
 * You may not continue reading or writing after {@link #commit()} or {@link #abort()}
 * is called.
 * <p>
 * Things to keep in mind when dealing with snapshot transactions:
 * 1. Transactions that do writes should be short lived.
 * 1a. Read only transactions can be long lived (within reason).
 * 2. Do not write too much data in one transaction (this relates back to #1)
 * 3. A row should be able to fit in memory without any trouble.  This includes
 *    all columns of the row.  If you are thinking about making your row bigger than like 10MB, you
 *    should think about breaking these up into different rows and using range scans.
 */
public class SnapshotTransaction extends AbstractTransaction implements ConstraintCheckingTransaction {
    private static final Logger log = LoggerFactory.getLogger(SnapshotTransaction.class);
    private static final Logger perfLogger = LoggerFactory.getLogger("dualschema.perf");
    private static final Logger constraintLogger = LoggerFactory.getLogger("dualschema.constraints");

    private static final int BATCH_SIZE_GET_FIRST_PAGE = 1000;

    private enum State {
        UNCOMMITTED,
        COMMITTED,
        COMMITTING,
        ABORTED,
        /**
         * Commit has failed during commit.
         */
        FAILED
    }

    protected final TimelockService timelockService;
    final KeyValueService keyValueService;
    final TransactionService defaultTransactionService;
    private final Cleaner cleaner;
    private final Supplier<Long> startTimestamp;
    protected final MetricsManager metricsManager;

    private final MultiTableSweepQueueWriter sweepQueue;

    protected final long immutableTimestamp;
    protected final Optional<LockToken> immutableTimestampLock;
    private final PreCommitCondition preCommitCondition;
    protected final long timeCreated = System.currentTimeMillis();

    protected final ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> writesByTable =
            Maps.newConcurrentMap();
    protected final ConflictDetectionManager conflictDetectionManager;
    private final AtomicLong byteCount = new AtomicLong();

    private final AtlasDbConstraintCheckingMode constraintCheckingMode;

    private final ConcurrentMap<TableReference, ConstraintCheckable> constraintsByTableName = Maps.newConcurrentMap();

    private final AtomicReference<State> state = new AtomicReference<>(State.UNCOMMITTED);
    private final AtomicLong numWriters = new AtomicLong();
    protected final SweepStrategyManager sweepStrategyManager;
    protected final Long transactionReadTimeoutMillis;
    private final TransactionReadSentinelBehavior readSentinelBehavior;
    private volatile long commitTsForScrubbing = TransactionConstants.FAILED_COMMIT_TS;
    protected final boolean allowHiddenTableAccess;
    protected final TimestampCache timestampValidationReadCache;
    protected final ExecutorService getRangesExecutor;
    protected final int defaultGetRangesConcurrency;
    private final Set<TableReference> involvedTables = Sets.newConcurrentHashSet();
    protected final ExecutorService deleteExecutor;
    private final Timer.Context transactionTimerContext;
    protected final TransactionOutcomeMetrics transactionOutcomeMetrics;
    protected final boolean validateLocksOnReads;
    protected final Supplier<TransactionConfig> transactionConfig;

    protected volatile boolean hasReads;

    /**
     * @param immutableTimestamp If we find a row written before the immutableTimestamp we don't need to
     *                           grab a read lock for it because we know that no writers exist.
     * @param preCommitCondition This check must pass for this transaction to commit.
     */
    /* package */ SnapshotTransaction(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
            TimelockService timelockService,
            TransactionService transactionService,
            Cleaner cleaner,
            Supplier<Long> startTimeStamp,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            long immutableTimestamp,
            Optional<LockToken> immutableTimestampLock,
            PreCommitCondition preCommitCondition,
            AtlasDbConstraintCheckingMode constraintCheckingMode,
            Long transactionTimeoutMillis,
            TransactionReadSentinelBehavior readSentinelBehavior,
            boolean allowHiddenTableAccess,
            TimestampCache timestampValidationReadCache,
            ExecutorService getRangesExecutor,
            int defaultGetRangesConcurrency,
            MultiTableSweepQueueWriter sweepQueue,
            ExecutorService deleteExecutor,
            boolean validateLocksOnReads,
            Supplier<TransactionConfig> transactionConfig) {
        this.metricsManager = metricsManager;
        this.transactionTimerContext = getTimer("transactionMillis").time();
        this.keyValueService = keyValueService;
        this.timelockService = timelockService;
        this.defaultTransactionService = transactionService;
        this.cleaner = cleaner;
        this.startTimestamp = startTimeStamp;
        this.conflictDetectionManager = conflictDetectionManager;
        this.sweepStrategyManager = sweepStrategyManager;
        this.immutableTimestamp = immutableTimestamp;
        this.immutableTimestampLock = immutableTimestampLock;
        this.preCommitCondition = preCommitCondition;
        this.constraintCheckingMode = constraintCheckingMode;
        this.transactionReadTimeoutMillis = transactionTimeoutMillis;
        this.readSentinelBehavior = readSentinelBehavior;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.timestampValidationReadCache = timestampValidationReadCache;
        this.getRangesExecutor = getRangesExecutor;
        this.defaultGetRangesConcurrency = defaultGetRangesConcurrency;
        this.sweepQueue = sweepQueue;
        this.deleteExecutor = deleteExecutor;
        this.hasReads = false;
        this.transactionOutcomeMetrics = TransactionOutcomeMetrics.create(metricsManager);
        this.validateLocksOnReads = validateLocksOnReads;
        this.transactionConfig = transactionConfig;
    }

    @Override
    public long getTimestamp() {
        return getStartTimestamp();
    }

    long getCommitTimestamp() {
        return commitTsForScrubbing;
    }

    @Override
    public TransactionReadSentinelBehavior getReadSentinelBehavior() {
        return readSentinelBehavior;
    }

    protected void checkGetPreconditions(TableReference tableRef) {
        markTableAsInvolvedInThisTransaction(tableRef);
        if (transactionReadTimeoutMillis != null
                && System.currentTimeMillis() - timeCreated > transactionReadTimeoutMillis) {
            throw new TransactionFailedRetriableException("Transaction timed out.");
        }
        Preconditions.checkArgument(allowHiddenTableAccess || !AtlasDbConstants.HIDDEN_TABLES.contains(tableRef));

        if (!(state.get() == State.UNCOMMITTED || state.get() == State.COMMITTING)) {
            throw new CommittedTransactionException();
        }
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(TableReference tableRef, Iterable<byte[]> rows,
                                                        ColumnSelection columnSelection) {
        Timer.Context timer = getTimer("getRows").time();
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return AbstractTransaction.EMPTY_SORTED_ROWS;
        }
        hasReads = true;
        ImmutableMap.Builder<Cell, byte[]> result = ImmutableSortedMap.naturalOrder();
        Map<Cell, Value> rawResults = Maps.newHashMap(
                keyValueService.getRows(tableRef, rows, columnSelection, getStartTimestamp()));
        SortedMap<Cell, byte[]> writes = writesByTable.get(tableRef);
        if (writes != null) {
            for (byte[] row : rows) {
                extractLocalWritesForRow(result, writes, row, columnSelection);
            }
        }

        // We don't need to do work postFiltering if we have a write locally.
        rawResults.keySet().removeAll(result.build().keySet());

        SortedMap<byte[], RowResult<byte[]>> results = filterRowResults(tableRef, rawResults, result);
        long getRowsMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());
        if (perfLogger.isDebugEnabled()) {
            perfLogger.debug("getRows({}, {} rows) found {} rows, took {} ms",
                    tableRef, Iterables.size(rows), results.size(), getRowsMillis);
        }
        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
        return results;
    }

    @Override
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection columnRangeSelection) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return ImmutableMap.of();
        }
        hasReads = true;
        Map<byte[], RowColumnRangeIterator> rawResults = keyValueService.getRowsColumnRange(tableRef, rows,
                columnRangeSelection, getStartTimestamp());
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> postFilteredResults =
                Maps.newHashMapWithExpectedSize(rawResults.size());
        for (Entry<byte[], RowColumnRangeIterator> e : rawResults.entrySet()) {
            byte[] row = e.getKey();
            RowColumnRangeIterator rawIterator = e.getValue();
            Iterator<Map.Entry<Cell, byte[]>> postFilteredIterator =
                    getPostFilteredColumns(tableRef, columnRangeSelection, row, rawIterator);
            postFilteredResults.put(row, BatchingVisitableFromIterable.create(postFilteredIterator));
        }
        return postFilteredResults;
    }

    @Override
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(TableReference tableRef,
                                                                Iterable<byte[]> rows,
                                                                ColumnRangeSelection columnRangeSelection,
                                                                int batchHint) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return Collections.emptyIterator();
        }
        hasReads = true;
        RowColumnRangeIterator rawResults =
                keyValueService.getRowsColumnRange(tableRef,
                                                   rows,
                                                   columnRangeSelection,
                                                   batchHint,
                                                   getStartTimestamp());
        if (!rawResults.hasNext()) {
            validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
        } // else the postFiltered iterator will check for each batch.

        Iterator<Map.Entry<byte[], RowColumnRangeIterator>> rawResultsByRow = partitionByRow(rawResults);
        Iterator<Iterator<Map.Entry<Cell, byte[]>>> postFiltered = Iterators.transform(rawResultsByRow, e -> {
            byte[] row = e.getKey();
            RowColumnRangeIterator rawIterator = e.getValue();
            BatchColumnRangeSelection batchColumnRangeSelection =
                    BatchColumnRangeSelection.create(columnRangeSelection, batchHint);
            return getPostFilteredColumns(tableRef, batchColumnRangeSelection, row, rawIterator);
        });

        return Iterators.concat(postFiltered);
    }

    private Iterator<Map.Entry<Cell, byte[]>> getPostFilteredColumns(
            TableReference tableRef,
            BatchColumnRangeSelection batchColumnRangeSelection,
            byte[] row,
            RowColumnRangeIterator rawIterator) {
        Iterator<Map.Entry<Cell, byte[]>> postFilterIterator =
                getRowColumnRangePostFiltered(tableRef, row, batchColumnRangeSelection, rawIterator);
        SortedMap<Cell, byte[]> localWrites = getLocalWritesForColumnRange(tableRef, batchColumnRangeSelection, row);
        Iterator<Map.Entry<Cell, byte[]>> localIterator = localWrites.entrySet().iterator();
        Iterator<Map.Entry<Cell, byte[]>> mergedIterator =
                IteratorUtils.mergeIterators(localIterator,
                        postFilterIterator,
                        Ordering.from(UnsignedBytes.lexicographicalComparator())
                                .onResultOf(entry -> entry.getKey().getColumnName()),
                        from -> from.getLhSide());

        return filterDeletedValues(mergedIterator, tableRef);
    }

    private Iterator<Map.Entry<Cell, byte[]>> filterDeletedValues(
            Iterator<Map.Entry<Cell, byte[]>> unfiltered,
            TableReference tableReference) {
        Meter emptyValueMeter = getMeter(AtlasDbMetricNames.CellFilterMetrics.EMPTY_VALUE, tableReference);
        return Iterators.filter(unfiltered, entry -> {
            if (entry.getValue().length == 0) {
                emptyValueMeter.mark();
                return false;
            }
            return true;
        });
    }

    private Iterator<Map.Entry<Cell, byte[]>> getRowColumnRangePostFiltered(
            TableReference tableRef,
            byte[] row,
            BatchColumnRangeSelection columnRangeSelection,
            RowColumnRangeIterator rawIterator) {
        ColumnRangeBatchProvider batchProvider = new ColumnRangeBatchProvider(
                keyValueService, tableRef, row, columnRangeSelection, getStartTimestamp());
        BatchSizeIncreasingIterator<Map.Entry<Cell, Value>> batchIterator = new BatchSizeIncreasingIterator<>(
                batchProvider, columnRangeSelection.getBatchHint(), ClosableIterators.wrap(rawIterator));
        Iterator<Iterator<Map.Entry<Cell, byte[]>>> postFilteredBatches =
                new AbstractIterator<Iterator<Map.Entry<Cell, byte[]>>>() {
            @Override
            protected Iterator<Map.Entry<Cell, byte[]>> computeNext() {
                ImmutableMap.Builder<Cell, Value> rawBuilder = ImmutableMap.builder();
                List<Map.Entry<Cell, Value>> batch = batchIterator.getBatch();
                for (Map.Entry<Cell, Value> result : batch) {
                    rawBuilder.put(result);
                }
                Map<Cell, Value> raw = rawBuilder.build();
                validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
                if (raw.isEmpty()) {
                    return endOfData();
                }
                ImmutableSortedMap.Builder<Cell, byte[]> post = ImmutableSortedMap.naturalOrder();
                getWithPostFiltering(tableRef, raw, post, Value.GET_VALUE);
                SortedMap<Cell, byte[]> postFiltered = post.build();
                batchIterator.markNumResultsNotDeleted(postFiltered.size());
                return postFiltered.entrySet().iterator();
            }
        };
        return Iterators.concat(postFilteredBatches);
    }

    /**
     * Partitions a {@link RowColumnRangeIterator} into contiguous blocks that share the same row name.
     * {@link KeyValueService#getRowsColumnRange(TableReference, Iterable, ColumnRangeSelection, int, long)} guarantees
     * that all columns for a single row are adjacent, so this method will return an {@link Iterator} with exactly one
     * entry per non-empty row.
     */
    private Iterator<Map.Entry<byte[], RowColumnRangeIterator>> partitionByRow(RowColumnRangeIterator rawResults) {
        PeekingIterator<Map.Entry<Cell, Value>> peekableRawResults = Iterators.peekingIterator(rawResults);
        return new AbstractIterator<Map.Entry<byte[], RowColumnRangeIterator>>() {
            byte[] prevRowName;

            @Override
            protected Map.Entry<byte[], RowColumnRangeIterator> computeNext() {
                finishConsumingPreviousRow(peekableRawResults);
                if (!peekableRawResults.hasNext()) {
                    return endOfData();
                }
                byte[] nextRowName = peekableRawResults.peek().getKey().getRowName();
                Iterator<Map.Entry<Cell, Value>> columnsIterator =
                        new AbstractIterator<Map.Entry<Cell, Value>>() {
                    @Override
                    protected Map.Entry<Cell, Value> computeNext() {
                        if (!peekableRawResults.hasNext()
                                || !Arrays.equals(peekableRawResults.peek().getKey().getRowName(), nextRowName)) {
                            return endOfData();
                        }
                        return peekableRawResults.next();
                    }
                };
                prevRowName = nextRowName;
                return Maps.immutableEntry(nextRowName, new LocalRowColumnRangeIterator(columnsIterator));
            }

            private void finishConsumingPreviousRow(PeekingIterator<Map.Entry<Cell, Value>> iter) {
                int numConsumed = 0;
                while (iter.hasNext() && Arrays.equals(iter.peek().getKey().getRowName(), prevRowName)) {
                    iter.next();
                    numConsumed++;
                }
                if (numConsumed > 0) {
                    log.warn("Not all columns for row {} were read. {} columns were discarded.",
                             UnsafeArg.of("row", Arrays.toString(prevRowName)),
                             SafeArg.of("numColumnsDiscarded", numConsumed));
                }
            }
        };
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRowsIgnoringLocalWrites(
            TableReference tableRef,
            Iterable<byte[]> rows) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return AbstractTransaction.EMPTY_SORTED_ROWS;
        }
        hasReads = true;

        Map<Cell, Value> rawResults = Maps.newHashMap(keyValueService.getRows(tableRef,
                rows,
                ColumnSelection.all(),
                getStartTimestamp()));

        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
        return filterRowResults(tableRef, rawResults, ImmutableMap.builderWithExpectedSize(rawResults.size()));
    }

    private SortedMap<byte[], RowResult<byte[]>> filterRowResults(TableReference tableRef,
                                                                  Map<Cell, Value> rawResults,
                                                                  ImmutableMap.Builder<Cell, byte[]> result) {
        getWithPostFiltering(tableRef, rawResults, result, Value.GET_VALUE);
        Map<Cell, byte[]> filterDeletedValues = removeEmptyColumns(result.build(), tableRef);
        return RowResults.viewOfSortedMap(Cells.breakCellsUpByRow(filterDeletedValues));
    }

    private Map<Cell, byte[]> removeEmptyColumns(Map<Cell, byte[]> unfiltered, TableReference tableReference) {
        Map<Cell, byte[]> filtered = Maps.filterValues(unfiltered, Predicates.not(Value::isTombstone));
        getMeter(AtlasDbMetricNames.CellFilterMetrics.EMPTY_VALUE, tableReference)
                .mark(unfiltered.size() - filtered.size());
        return filtered;
    }

    /**
     * This will add any local writes for this row to the result map.
     * <p>
     * If an empty value was written as a delete, this will also be included in the map.
     */
    private void extractLocalWritesForRow(
            @Output ImmutableMap.Builder<Cell, byte[]> result,
            SortedMap<Cell, byte[]> writes,
            byte[] row,
            ColumnSelection columnSelection) {
        Cell lowCell = Cells.createSmallestCellForRow(row);
        Iterator<Entry<Cell, byte[]>> it = writes.tailMap(lowCell).entrySet().iterator();
        while (it.hasNext()) {
            Entry<Cell, byte[]> entry = it.next();
            Cell cell = entry.getKey();
            if (!Arrays.equals(row, cell.getRowName())) {
                break;
            }
            if (columnSelection.allColumnsSelected()
                    || columnSelection.getSelectedColumns().contains(cell.getColumnName())) {
                result.put(cell, entry.getValue());
            }
        }
    }

    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        Timer.Context timer = getTimer("get").time();
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(cells)) {
            return ImmutableMap.of();
        }
        hasReads = true;

        Map<Cell, byte[]> result = Maps.newHashMap();
        SortedMap<Cell, byte[]> writes = writesByTable.get(tableRef);
        if (writes != null) {
            for (Cell cell : cells) {
                if (writes.containsKey(cell)) {
                    result.put(cell, writes.get(cell));
                }
            }
        }

        // We don't need to read any cells that were written locally.
        result.putAll(getFromKeyValueService(tableRef, Sets.difference(cells, result.keySet())));

        long getMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());
        if (perfLogger.isDebugEnabled()) {
            perfLogger.debug("get({}, {} cells) found {} cells (some possibly deleted), took {} ms",
                    tableRef, cells.size(), result.size(), getMillis);
        }
        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
        return removeEmptyColumns(result, tableRef);
    }

    @Override
    public Map<Cell, byte[]> getIgnoringLocalWrites(TableReference tableRef, Set<Cell> cells) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(cells)) {
            return ImmutableMap.of();
        }
        hasReads = true;

        Map<Cell, byte[]> result = getFromKeyValueService(tableRef, cells);
        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());

        return Maps.filterValues(result, Predicates.not(Value::isTombstone));
    }

    /**
     * This will load the given keys from the underlying key value service and apply postFiltering
     * so we have snapshot isolation.  If the value in the key value service is the empty array
     * this will be included here and needs to be filtered out.
     */
    private Map<Cell, byte[]> getFromKeyValueService(TableReference tableRef, Set<Cell> cells) {
        ImmutableMap.Builder<Cell, byte[]> result = ImmutableMap.builderWithExpectedSize(cells.size());
        Map<Cell, Long> toRead = Cells.constantValueMap(cells, getStartTimestamp());
        Map<Cell, Value> rawResults = keyValueService.get(tableRef, toRead);
        getWithPostFiltering(tableRef, rawResults, result, Value.GET_VALUE);
        return result.build();
    }

    private static byte[] getNextStartRowName(
            RangeRequest range,
            TokenBackedBasicResultsPage<RowResult<Value>, byte[]> prePostFilter) {
        if (!prePostFilter.moreResultsAvailable()) {
            return range.getEndExclusive();
        }
        return prePostFilter.getTokenForNextPage();
    }


    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(final TableReference tableRef,
                                                                    Iterable<RangeRequest> rangeRequests) {
        checkGetPreconditions(tableRef);

        if (perfLogger.isDebugEnabled()) {
            perfLogger.debug("Passed {} ranges to getRanges({}, {})",
                    Iterables.size(rangeRequests), tableRef, rangeRequests);
        }
        if (!Iterables.isEmpty(rangeRequests)) {
            hasReads = true;
        }

        return FluentIterable.from(Iterables.partition(rangeRequests, BATCH_SIZE_GET_FIRST_PAGE))
                .transformAndConcat(input -> {
                    Timer.Context timer = getTimer("processedRangeMillis").time();
                    Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> firstPages =
                            keyValueService.getFirstBatchForRanges(tableRef, input, getStartTimestamp());
                    validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());

                    SortedMap<Cell, byte[]> postFiltered = postFilterPages(
                            tableRef,
                            firstPages.values());

                    List<BatchingVisitable<RowResult<byte[]>>> ret = Lists.newArrayListWithCapacity(input.size());
                    for (RangeRequest rangeRequest : input) {
                        TokenBackedBasicResultsPage<RowResult<Value>, byte[]> prePostFilter =
                                firstPages.get(rangeRequest);
                        byte[] nextStartRowName = getNextStartRowName(
                                rangeRequest,
                                prePostFilter);
                        List<Entry<Cell, byte[]>> mergeIterators = getPostFilteredWithLocalWrites(
                                tableRef,
                                postFiltered,
                                rangeRequest,
                                prePostFilter.getResults(),
                                nextStartRowName);
                        ret.add(new AbstractBatchingVisitable<RowResult<byte[]>>() {
                            @Override
                            protected <K extends Exception> void batchAcceptSizeHint(
                                    int batchSizeHint,
                                    ConsistentVisitor<RowResult<byte[]>, K> visitor)
                                    throws K {
                                checkGetPreconditions(tableRef);
                                final Iterator<RowResult<byte[]>> rowResults = Cells.createRowView(mergeIterators);
                                while (rowResults.hasNext()) {
                                    if (!visitor.visit(ImmutableList.of(rowResults.next()))) {
                                        return;
                                    }
                                }
                                if ((nextStartRowName.length == 0) || !prePostFilter.moreResultsAvailable()) {
                                    return;
                                }
                                RangeRequest newRange = rangeRequest.getBuilder()
                                        .startRowInclusive(nextStartRowName)
                                        .build();
                                getRange(tableRef, newRange)
                                        .batchAccept(batchSizeHint, visitor);
                            }
                        });
                    }
                    long processedRangeMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());
                    log.trace("Processed {} range requests for {} in {}ms",
                            SafeArg.of("numRequests", input.size()),
                            LoggingArgs.tableRef(tableRef),
                            SafeArg.of("millis", processedRangeMillis));
                    return ret;
                });
    }

    @Override
    public <T> Stream<T> getRanges(
            final TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            int concurrencyLevel,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
        if (!Iterables.isEmpty(rangeRequests)) {
            hasReads = true;
        }
        Stream<Pair<RangeRequest, BatchingVisitable<RowResult<byte[]>>>> requestAndVisitables =
                StreamSupport.stream(rangeRequests.spliterator(), false)
                        .map(rangeRequest -> Pair.of(rangeRequest, getLazyRange(tableRef, rangeRequest)));

        if (concurrencyLevel == 1 || isSingleton(rangeRequests)) {
            return requestAndVisitables.map(pair -> visitableProcessor.apply(pair.getLeft(), pair.getRight()));
        }

        return MoreStreams.blockingStreamWithParallelism(
                requestAndVisitables,
                pair -> visitableProcessor.apply(pair.getLeft(), pair.getRight()),
                getRangesExecutor,
                concurrencyLevel);
    }

    @Override
    public <T> Stream<T> getRanges(
            final TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
        if (!Iterables.isEmpty(rangeRequests)) {
            hasReads = true;
        }
        return getRanges(tableRef, rangeRequests, defaultGetRangesConcurrency, visitableProcessor);
    }

    private static boolean isSingleton(Iterable<?> elements) {
        Iterator<?> it = elements.iterator();
        if (it.hasNext()) {
            it.next();
            return !it.hasNext();
        }
        return false;
    }

    @Override
    public Stream<BatchingVisitable<RowResult<byte[]>>> getRangesLazy(
            final TableReference tableRef, Iterable<RangeRequest> rangeRequests) {
        if (!Iterables.isEmpty(rangeRequests)) {
            hasReads = true;
        }
        return StreamSupport.stream(rangeRequests.spliterator(), false)
                .map(rangeRequest -> getLazyRange(tableRef, rangeRequest));
    }

    private BatchingVisitable<RowResult<byte[]>> getLazyRange(TableReference tableRef, RangeRequest rangeRequest) {
        return new AbstractBatchingVisitable<RowResult<byte[]>>() {
            @Override
            protected <K extends Exception> void batchAcceptSizeHint(
                    int batchSizeHint, ConsistentVisitor<RowResult<byte[]>, K> visitor) throws K {
                getRange(tableRef, rangeRequest).batchAccept(batchSizeHint, visitor);
            }
        };
    }

    private void validatePreCommitRequirementsOnReadIfNecessary(TableReference tableRef, long timestamp) {
        if (!isValidationNecessaryOnReads(tableRef)) {
            return;
        }
        throwIfPreCommitRequirementsNotMet(null, timestamp);
    }

    private boolean isValidationNecessaryOnReads(TableReference tableRef) {
        return validateLocksOnReads && requiresImmutableTimestampLocking(tableRef);
    }

    private boolean isValidationNecessaryOnCommit(TableReference tableRef) {
        return !validateLocksOnReads && requiresImmutableTimestampLocking(tableRef);
    }

    private boolean requiresImmutableTimestampLocking(TableReference tableRef) {
        return isThoroughlySwept(tableRef) || transactionConfig.get().lockImmutableTsOnReadOnlyTransactions();
    }

    private boolean isThoroughlySwept(TableReference tableRef) {
        return sweepStrategyManager.get().get(tableRef) == SweepStrategy.THOROUGH;
    }

    private List<Entry<Cell, byte[]>> getPostFilteredWithLocalWrites(final TableReference tableRef,
                                                                     final SortedMap<Cell, byte[]> postFiltered,
                                                                     final RangeRequest rangeRequest,
                                                                     List<RowResult<Value>> prePostFilter,
                                                                     final byte[] endRowExclusive) {
        Map<Cell, Value> prePostFilterCells = Cells.convertRowResultsToCells(prePostFilter);
        Collection<Entry<Cell, byte[]>> postFilteredCells = Collections2.filter(
                postFiltered.entrySet(),
                Predicates.compose(
                        Predicates.in(prePostFilterCells.keySet()),
                        MapEntries.getKeyFunction()));
        Collection<Entry<Cell, byte[]>> localWritesInRange = getLocalWritesForRange(
                tableRef,
                rangeRequest.getStartInclusive(),
                endRowExclusive).entrySet();
        return mergeInLocalWrites(
                tableRef,
                postFilteredCells.iterator(),
                localWritesInRange.iterator(),
                rangeRequest.isReverse());
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(final TableReference tableRef,
                                                         final RangeRequest range) {
        checkGetPreconditions(tableRef);
        if (range.isEmptyRange()) {
            return BatchingVisitables.emptyBatchingVisitable();
        }
        hasReads = true;

        return new AbstractBatchingVisitable<RowResult<byte[]>>() {
            @Override
            public <K extends Exception> void batchAcceptSizeHint(
                    int userRequestedSize,
                    ConsistentVisitor<RowResult<byte[]>, K> visitor)
                    throws K {
                ensureUncommitted();

                int requestSize = range.getBatchHint() != null ? range.getBatchHint() : userRequestedSize;
                int preFilterBatchSize = getRequestHintToKvStore(requestSize);

                Validate.isTrue(!range.isReverse(), "we currently do not support reverse ranges");
                getBatchingVisitableFromIterator(
                        tableRef,
                        range,
                        requestSize,
                        visitor,
                        preFilterBatchSize);
            }

        };
    }

    private <K extends Exception> boolean getBatchingVisitableFromIterator(
            TableReference tableRef,
            RangeRequest range,
            int userRequestedSize,
            AbortingVisitor<List<RowResult<byte[]>>, K> visitor,
            int preFilterBatchSize) throws K {
        ClosableIterator<RowResult<byte[]>> postFilterIterator =
                postFilterIterator(tableRef, range, preFilterBatchSize, Value.GET_VALUE);
        try {
            Iterator<RowResult<byte[]>> localWritesInRange = Cells.createRowView(
                    getLocalWritesForRange(tableRef, range.getStartInclusive(), range.getEndExclusive()).entrySet());
            Iterator<RowResult<byte[]>> mergeIterators =
                    mergeInLocalWritesRows(postFilterIterator, localWritesInRange, range.isReverse(), tableRef);
            return BatchingVisitableFromIterable.create(mergeIterators).batchAccept(userRequestedSize, visitor);
        } finally {
            postFilterIterator.close();
        }
    }

    protected static int getRequestHintToKvStore(int userRequestedSize) {
        if (userRequestedSize == 1) {
            // Handle 1 specially because the underlying store could have an optimization for 1
            return 1;
        }
        // TODO(carrino): tune the param here based on how likely we are to post filter
        // rows out and have deleted rows
        int preFilterBatchSize = userRequestedSize + ((userRequestedSize + 9) / 10);
        if (preFilterBatchSize > AtlasDbPerformanceConstants.MAX_BATCH_SIZE
                || preFilterBatchSize < 0) {
            preFilterBatchSize = AtlasDbPerformanceConstants.MAX_BATCH_SIZE;
        }
        return preFilterBatchSize;
    }

    private Iterator<RowResult<byte[]>> mergeInLocalWritesRows(
            Iterator<RowResult<byte[]>> postFilterIterator,
            Iterator<RowResult<byte[]>> localWritesInRange,
            boolean isReverse,
            TableReference tableReference) {
        Ordering<RowResult<byte[]>> ordering = RowResult.getOrderingByRowName();
        Iterator<RowResult<byte[]>> mergeIterators = IteratorUtils.mergeIterators(
                postFilterIterator, localWritesInRange,
                isReverse ? ordering.reverse() : ordering,
                from -> RowResults.merge(from.lhSide, from.rhSide)); // prefer local writes

        Iterator<RowResult<byte[]>> purgeDeleted = filterEmptyColumnsFromRows(mergeIterators, tableReference);
        return Iterators.filter(purgeDeleted, Predicates.not(RowResults.createIsEmptyPredicate()));
    }

    private Iterator<RowResult<byte[]>> filterEmptyColumnsFromRows(
            Iterator<RowResult<byte[]>> unfilteredRows, TableReference tableReference) {
        return Iterators.transform(unfilteredRows, unfilteredRow -> {
            SortedMap<byte[], byte[]> filteredColumns =
                    Maps.filterValues(unfilteredRow.getColumns(), Predicates.not(Value::isTombstone));
            getMeter(AtlasDbMetricNames.CellFilterMetrics.EMPTY_VALUE, tableReference)
                    .mark(unfilteredRow.getColumns().size() - filteredColumns.size());
            return RowResult.create(unfilteredRow.getRowName(), filteredColumns);
        });
    }

    private List<Entry<Cell, byte[]>> mergeInLocalWrites(
            TableReference tableRef,
            Iterator<Entry<Cell, byte[]>> postFilterIterator,
            Iterator<Entry<Cell, byte[]>> localWritesInRange,
            boolean isReverse) {
        Ordering<Entry<Cell, byte[]>> ordering = Ordering.natural().onResultOf(MapEntries.getKeyFunction());
        Iterator<Entry<Cell, byte[]>> mergeIterators = IteratorUtils.mergeIterators(
                postFilterIterator, localWritesInRange,
                isReverse ? ordering.reverse() : ordering,
                from -> from.rhSide); // always override their value with written values

        return postFilterEmptyValues(tableRef, mergeIterators);
    }

    private List<Entry<Cell, byte[]>> postFilterEmptyValues(
            TableReference tableRef,
            Iterator<Entry<Cell, byte[]>> mergeIterators) {
        List<Entry<Cell, byte[]>> mergedWritesWithoutEmptyValues = new ArrayList<>();
        Predicate<Entry<Cell, byte[]>> nonEmptyValuePredicate = Predicates.compose(Predicates.not(Value::isTombstone),
                MapEntries.getValueFunction());
        long numEmptyValues = 0;
        while (mergeIterators.hasNext()) {
            Entry<Cell, byte[]> next = mergeIterators.next();
            if (nonEmptyValuePredicate.apply(next)) {
                mergedWritesWithoutEmptyValues.add(next);
            } else {
                numEmptyValues++;
            }
        }
        getMeter(AtlasDbMetricNames.CellFilterMetrics.EMPTY_VALUE, tableRef).mark(numEmptyValues);
        return mergedWritesWithoutEmptyValues;
    }

    protected <T> ClosableIterator<RowResult<T>> postFilterIterator(
            TableReference tableRef,
            RangeRequest range,
            int preFilterBatchSize,
            Function<Value, T> transformer) {
        RowRangeBatchProvider batchProvider =
                new RowRangeBatchProvider(keyValueService, tableRef, range, getStartTimestamp());
        BatchSizeIncreasingIterator<RowResult<Value>> results =
                new BatchSizeIncreasingIterator<>(batchProvider, preFilterBatchSize, null);
        Iterator<Iterator<RowResult<T>>> batchedPostFiltered = new AbstractIterator<Iterator<RowResult<T>>>() {
            @Override
            protected Iterator<RowResult<T>> computeNext() {
                List<RowResult<Value>> batch = results.getBatch();
                validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
                if (batch.isEmpty()) {
                    return endOfData();
                }
                SortedMap<Cell, T> postFilter = postFilterRows(tableRef, batch, transformer);
                results.markNumResultsNotDeleted(Cells.getRows(postFilter.keySet()).size());
                return Cells.createRowView(postFilter.entrySet());
            }
        };

        final Iterator<RowResult<T>> rows = Iterators.concat(batchedPostFiltered);
        return new ForwardingClosableIterator<RowResult<T>>() {
            @Override
            protected ClosableIterator<RowResult<T>> delegate() {
                return ClosableIterators.wrap(rows);
            }

            @Override
            public void close() {
                results.close();
            }
        };
    }

    private ConcurrentNavigableMap<Cell, byte[]> getLocalWrites(TableReference tableRef) {
        return writesByTable.computeIfAbsent(tableRef, unused -> new ConcurrentSkipListMap<>());
    }

    /**
     * This includes deleted writes as zero length byte arrays, be sure to strip them out.
     */
    private SortedMap<Cell, byte[]> getLocalWritesForRange(TableReference tableRef, byte[] startRow, byte[] endRow) {
        SortedMap<Cell, byte[]> writes = getLocalWrites(tableRef);
        if (startRow.length != 0) {
            writes = writes.tailMap(Cells.createSmallestCellForRow(startRow));
        }
        if (endRow.length != 0) {
            writes = writes.headMap(Cells.createSmallestCellForRow(endRow));
        }
        return writes;
    }

    private SortedMap<Cell, byte[]> getLocalWritesForColumnRange(
            TableReference tableRef,
            BatchColumnRangeSelection columnRangeSelection,
            byte[] row) {
        SortedMap<Cell, byte[]> writes = getLocalWrites(tableRef);
        Cell startCell;
        if (columnRangeSelection.getStartCol().length != 0) {
            startCell = Cell.create(row, columnRangeSelection.getStartCol());
        } else {
            startCell = Cells.createSmallestCellForRow(row);
        }
        writes = writes.tailMap(startCell);
        if (RangeRequests.isLastRowName(row)) {
            return writes;
        }
        Cell endCell;
        if (columnRangeSelection.getEndCol().length != 0) {
            endCell = Cell.create(row, columnRangeSelection.getEndCol());
        } else {
            endCell = Cells.createSmallestCellForRow(RangeRequests.nextLexicographicName(row));
        }
        writes = writes.headMap(endCell);
        return writes;
    }

    private SortedMap<Cell, byte[]> postFilterPages(TableReference tableRef,
            Iterable<TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> rangeRows) {
        List<RowResult<Value>> results = Lists.newArrayList();
        for (TokenBackedBasicResultsPage<RowResult<Value>, byte[]> page : rangeRows) {
            results.addAll(page.getResults());
        }
        return postFilterRows(tableRef, results, Value.GET_VALUE);
    }

    private <T> SortedMap<Cell, T> postFilterRows(TableReference tableRef,
                                                  List<RowResult<Value>> rangeRows,
                                                  Function<Value, T> transformer) {
        ensureUncommitted();

        if (rangeRows.isEmpty()) {
            return ImmutableSortedMap.of();
        }

        Map<Cell, Value> rawResults = Maps.newHashMapWithExpectedSize(estimateSize(rangeRows));
        for (RowResult<Value> rowResult : rangeRows) {
            for (Map.Entry<byte[], Value> e : rowResult.getColumns().entrySet()) {
                rawResults.put(Cell.create(rowResult.getRowName(), e.getKey()), e.getValue());
            }
        }

        ImmutableSortedMap.Builder<Cell, T> postFilter = ImmutableSortedMap.naturalOrder();
        getWithPostFiltering(tableRef, rawResults, postFilter, transformer);
        return postFilter.build();
    }

    private int estimateSize(List<RowResult<Value>> rangeRows) {
        int estimatedSize = 0;
        for (RowResult<Value> rowResult : rangeRows) {
            estimatedSize += rowResult.getColumns().size();
        }
        return estimatedSize;
    }

    private <T> void getWithPostFiltering(TableReference tableRef,
                                          Map<Cell, Value> rawResults,
                                          @Output ImmutableMap.Builder<Cell, T> results,
                                          Function<Value, T> transformer) {
        long bytes = 0;
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            bytes += e.getValue().getContents().length + Cells.getApproxSizeOfCell(e.getKey());
        }
        if (bytes > TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES && log.isWarnEnabled()) {
            log.warn("A single get had quite a few bytes: {} for table {}. The number of results was {}. "
                    + "Enable debug logging for more information.",
                    SafeArg.of("numBytes", bytes),
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("numResults", rawResults.size()));
            if (log.isDebugEnabled()) {
                log.debug("The first 10 results of your request were {}.",
                        UnsafeArg.of("results", Iterables.limit(rawResults.entrySet(), 10)),
                        new RuntimeException("This exception and stack trace are provided for debugging purposes."));
            }
            getHistogram(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_TOO_MANY_BYTES_READ, tableRef).update(bytes);
        }

        getMeter(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_CELLS_READ, tableRef).mark(rawResults.size());

        if (AtlasDbConstants.HIDDEN_TABLES.contains(tableRef)) {
            Preconditions.checkState(allowHiddenTableAccess, "hidden tables cannot be read in this transaction");
            // hidden tables are used outside of the transaction protocol, and in general have invalid timestamps,
            // so do not apply post-filtering as post-filtering would rollback (actually delete) the data incorrectly
            // this case is hit when reading a hidden table from console
            for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
                results.put(e.getKey(), transformer.apply(e.getValue()));
            }
            return;
        }

        Map<Cell, Value> remainingResultsToPostfilter = rawResults;
        AtomicInteger resultCount = new AtomicInteger();
        while (!remainingResultsToPostfilter.isEmpty()) {
            remainingResultsToPostfilter = getWithPostFilteringInternal(
                    tableRef, remainingResultsToPostfilter, results, resultCount, transformer);
        }

        getMeter(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_CELLS_RETURNED, tableRef).mark(resultCount.get());
    }

    /**
     * A sentinel becomes orphaned if the table has been truncated between the time where the write occurred
     * and where it was truncated. In this case, there is a chance that we end up with a sentinel with no
     * valid AtlasDB cell covering it. In this case, we ignore it.
     */
    private Set<Cell> findOrphanedSweepSentinels(TableReference table, Map<Cell, Value> rawResults) {
        Set<Cell> sweepSentinels = Maps.filterValues(rawResults, SnapshotTransaction::isSweepSentinel).keySet();
        if (sweepSentinels.isEmpty()) {
            return Collections.emptySet();
        }
        Map<Cell, Long> atMaxTimestamp = keyValueService.getLatestTimestamps(
                table,
                Maps.asMap(sweepSentinels, x -> Long.MAX_VALUE));
        return Maps.filterValues(atMaxTimestamp, ts -> Value.INVALID_VALUE_TIMESTAMP == ts).keySet();
    }

    private static boolean isSweepSentinel(Value value) {
        return value.getTimestamp() == Value.INVALID_VALUE_TIMESTAMP;
    }

    /**
     * This will return all the keys that still need to be postFiltered.  It will output properly
     * postFiltered keys to the results output param.
     */
    private <T> Map<Cell, Value> getWithPostFilteringInternal(TableReference tableRef,
            Map<Cell, Value> rawResults,
            @Output ImmutableMap.Builder<Cell, T> results,
            @Output AtomicInteger count,
            Function<Value, T> transformer) {
        Set<Long> startTimestampsForValues = getStartTimestampsForValues(rawResults.values());
        Map<Long, Long> commitTimestamps = getCommitTimestamps(tableRef, startTimestampsForValues, true);
        Map<Cell, Long> keysToReload = Maps.newHashMapWithExpectedSize(0);
        Map<Cell, Long> keysToDelete = Maps.newHashMapWithExpectedSize(0);
        ImmutableSet.Builder<Cell> keysAddedBuilder = ImmutableSet.builder();

        Set<Cell> orphanedSentinels = findOrphanedSweepSentinels(tableRef, rawResults);
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            Cell key = e.getKey();
            Value value = e.getValue();

            if (isSweepSentinel(value)) {
                getMeter(AtlasDbMetricNames.CellFilterMetrics.INVALID_START_TS, tableRef).mark();

                // This means that this transaction started too long ago. When we do garbage collection,
                // we clean up old values, and this transaction started at a timestamp before the garbage collection.
                switch (getReadSentinelBehavior()) {
                    case IGNORE:
                        break;
                    case THROW_EXCEPTION:
                        if (!orphanedSentinels.contains(key)) {
                            throw new TransactionFailedRetriableException("Tried to read a value that has been "
                                    + "deleted. This can be caused by hard delete transactions using the type "
                                    + TransactionType.AGGRESSIVE_HARD_DELETE
                                    + ". It can also be caused by transactions taking too long, or"
                                    + " its locks expired. Retrying it should work.");

                        }
                        break;
                    default:
                        throw new IllegalStateException("Invalid read sentinel behavior " + getReadSentinelBehavior());
                }
            } else {
                Long theirCommitTimestamp = commitTimestamps.get(value.getTimestamp());
                if (theirCommitTimestamp == null || theirCommitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
                    keysToReload.put(key, value.getTimestamp());
                    if (shouldDeleteAndRollback()) {
                        // This is from a failed transaction so we can roll it back and then reload it.
                        keysToDelete.put(key, value.getTimestamp());
                        getMeter(AtlasDbMetricNames.CellFilterMetrics.INVALID_COMMIT_TS, tableRef).mark();
                    }
                } else if (theirCommitTimestamp > getStartTimestamp()) {
                    // The value's commit timestamp is after our start timestamp.
                    // This means the value is from a transaction which committed
                    // after our transaction began. We need to try reading at an
                    // earlier timestamp.
                    keysToReload.put(key, value.getTimestamp());
                    getMeter(AtlasDbMetricNames.CellFilterMetrics.COMMIT_TS_GREATER_THAN_TRANSACTION_TS, tableRef)
                            .mark();
                } else {
                    // The value has a commit timestamp less than our start timestamp, and is visible and valid.
                    if (value.getContents().length != 0) {
                        results.put(key, transformer.apply(value));
                        keysAddedBuilder.add(key);
                    }
                }
            }
        }
        Set<Cell> keysAddedToResults = keysAddedBuilder.build();
        count.addAndGet(keysAddedToResults.size());

        if (!keysToDelete.isEmpty()) {
            // if we can't roll back the failed transactions, we should just try again
            if (!rollbackFailedTransactions(tableRef, keysToDelete, commitTimestamps, defaultTransactionService)) {
                return getRemainingResults(rawResults, keysAddedToResults);
            }
        }

        if (!keysToReload.isEmpty()) {
            Map<Cell, Value> nextRawResults = keyValueService.get(tableRef, keysToReload);
            validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
            return getRemainingResults(nextRawResults, keysAddedToResults);
        } else {
            return ImmutableMap.of();
        }
    }

    private Map<Cell, Value> getRemainingResults(Map<Cell, Value> rawResults, Set<Cell> keysAddedToResults) {
        Map<Cell, Value> remainingResults = Maps.newHashMap(rawResults);
        remainingResults.keySet().removeAll(keysAddedToResults);
        return remainingResults;
    }

    /**
     * This is protected to allow for different post filter behavior.
     */
    protected boolean shouldDeleteAndRollback() {
        Validate.notNull(timelockService, "if we don't have a valid lock server we can't roll back transactions");
        return true;
    }

    @Override
    public final void delete(TableReference tableRef, Set<Cell> cells) {
        putInternal(tableRef, Cells.constantValueMap(cells, PtBytes.EMPTY_BYTE_ARRAY));
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        ensureNoEmptyValues(values);
        putInternal(tableRef, values);
    }

    public void putInternal(TableReference tableRef, Map<Cell, byte[]> values) {
        Preconditions.checkArgument(!AtlasDbConstants.HIDDEN_TABLES.contains(tableRef));
        markTableAsInvolvedInThisTransaction(tableRef);

        if (values.isEmpty()) {
            return;
        }

        numWriters.incrementAndGet();
        try {
            // We need to check the status after incrementing writers to ensure that we fail if we are committing.
            ensureUncommitted();

            ConcurrentNavigableMap<Cell, byte[]> writes = getLocalWrites(tableRef);

            putWritesAndLogIfTooLarge(values, writes);
        } finally {
            numWriters.decrementAndGet();
        }
    }

    private void ensureNoEmptyValues(Map<Cell, byte[]> values) {
        for (Entry<Cell, byte[]> cellEntry : values.entrySet()) {
            if ((cellEntry.getValue() == null) || (cellEntry.getValue().length == 0)) {
                throw new IllegalArgumentException(
                        "AtlasDB does not currently support inserting null or empty (zero-byte) values.");
            }
        }
    }

    private void putWritesAndLogIfTooLarge(Map<Cell, byte[]> values, SortedMap<Cell, byte[]> writes) {
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            byte[] val = MoreObjects.firstNonNull(e.getValue(), PtBytes.EMPTY_BYTE_ARRAY);
            Cell cell = e.getKey();
            if (writes.put(cell, val) == null) {
                long toAdd = val.length + Cells.getApproxSizeOfCell(cell);
                long newVal = byteCount.addAndGet(toAdd);
                if (newVal >= TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES
                        && newVal - toAdd < TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES) {
                    log.warn("A single transaction has put quite a few bytes: {}. "
                            + "Enable debug logging for more information",
                            SafeArg.of("numBytes", newVal));
                    if (log.isDebugEnabled()) {
                        log.debug("This exception and stack trace are provided for debugging purposes.",
                                new RuntimeException());
                    }
                }
            }
        }
    }

    @Override
    public void abort() {
        if (state.get() == State.ABORTED) {
            return;
        }
        while (true) {
            ensureUncommitted();
            if (state.compareAndSet(State.UNCOMMITTED, State.ABORTED)) {
                if (hasWrites()) {
                    throwIfPreCommitRequirementsNotMet(null, getStartTimestamp());
                }
                transactionOutcomeMetrics.markAbort();
                return;
            }
        }
    }

    @Override
    public boolean isAborted() {
        return state.get() == State.ABORTED;
    }

    @Override
    public boolean isUncommitted() {
        return state.get() == State.UNCOMMITTED;
    }

    private void ensureUncommitted() {
        if (!isUncommitted()) {
            throw new CommittedTransactionException();
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    /// Committing
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public void commit() {
        commit(defaultTransactionService);
    }

    @Override
    public void commit(TransactionService transactionService) {
        if (state.get() == State.COMMITTED) {
            return;
        }
        if (state.get() == State.FAILED) {
            throw new IllegalStateException("this transaction has already failed");
        }
        while (true) {
            ensureUncommitted();
            if (state.compareAndSet(State.UNCOMMITTED, State.COMMITTING)) {
                break;
            }
        }

        // This must be done BEFORE we commit (otherwise if the system goes down after
        // we commit but before we queue cells for scrubbing, then we will lose track of
        // which cells we need to scrub)
        if (getTransactionType() == TransactionType.AGGRESSIVE_HARD_DELETE
                || getTransactionType() == TransactionType.HARD_DELETE) {
            cleaner.queueCellsForScrubbing(getCellsToQueueForScrubbing(), getStartTimestamp());
        }

        boolean success = false;
        try {
            if (numWriters.get() > 0) {
                // After we set state to committing we need to make sure no one is still writing.
                throw new IllegalStateException("Cannot commit while other threads are still calling put.");
            }

            checkConstraints();
            commitWrites(transactionService);
            if (perfLogger.isDebugEnabled()) {
                long transactionMillis = TimeUnit.NANOSECONDS.toMillis(transactionTimerContext.stop());
                perfLogger.debug("Committed transaction {} in {}ms",
                        getStartTimestamp(),
                        transactionMillis);
            }
            success = true;
        } finally {
            // Once we are in state committing, we need to try/finally to set the state to a terminal state.
            if (success) {
                state.set(State.COMMITTED);
                transactionOutcomeMetrics.markSuccessfulCommit();
            } else {
                state.set(State.FAILED);
                transactionOutcomeMetrics.markFailedCommit();
            }
        }
    }

    private void checkConstraints() {
        List<String> violations = Lists.newArrayList();
        for (Map.Entry<TableReference, ConstraintCheckable> entry : constraintsByTableName.entrySet()) {
            SortedMap<Cell, byte[]> sortedMap = writesByTable.get(entry.getKey());
            if (sortedMap != null) {
                violations.addAll(entry.getValue().findConstraintFailures(sortedMap, this, constraintCheckingMode));
            }
        }
        if (!violations.isEmpty()) {
            if (constraintCheckingMode.shouldThrowException()) {
                throw new AtlasDbConstraintException(violations);
            } else {
                constraintLogger.error("Constraint failure on commit.",
                        new AtlasDbConstraintException(violations));
            }
        }
    }

    private void commitWrites(TransactionService transactionService) {
        if (!hasWrites()) {
            if (hasReads()) {
                // verify any pre-commit conditions on the transaction
                preCommitCondition.throwIfConditionInvalid(getStartTimestamp());

                // if there are no writes, we must still make sure the immutable timestamp lock is still valid,
                // to ensure that sweep hasn't thoroughly deleted cells we tried to read
                if (validationNecessaryForInvolvedTablesOnCommit()) {
                    throwIfImmutableTsOrCommitLocksExpired(null);
                }
                return;
            }
            return;
        }

        timedAndTraced("commitStage", () -> {
            // Acquire row locks and a lock on the start timestamp row in the transactions table.
            // This must happen before conflict checking, otherwise we could complete the checks and then have someone
            // else write underneath us before we proceed (thus missing a write/write conflict).
            LockToken commitLocksToken = timedAndTraced("commitAcquireLocks", this::acquireLocksForCommit);
            try {
                // Conflict checking. We can actually do this later without compromising correctness, but there is no
                // reason to postpone this check - we waste resources writing unnecessarily if these are going to fail.
                timedAndTraced("commitCheckingForConflicts",
                        () -> throwIfConflictOnCommit(commitLocksToken, transactionService));

                // Write to the targeted sweep queue. We must do this before writing to the key value service -
                // otherwise we may have hanging values that targeted sweep won't know about.
                timedAndTraced("writingToSweepQueue", () -> sweepQueue.enqueue(writesByTable, getStartTimestamp()));

                // Write to the key value service. We must do this before getting the commit timestamp - otherwise
                // we risk another transaction starting at a timestamp after our commit timestamp not seeing our writes.
                timedAndTraced("commitWrite", () -> keyValueService.multiPut(writesByTable, getStartTimestamp()));

                // Now that all writes are done, get the commit timestamp
                // We must do this before we check that our locks are still valid to ensure that other transactions that
                // will hold these locks are sure to have start timestamps after our commit timestamp.
                long commitTimestamp = timedAndTraced("getCommitTimestamp", timelockService::getFreshTimestamp);
                commitTsForScrubbing = commitTimestamp;

                // Punch on commit so that if hard delete is the only thing happening on a system,
                // we won't block forever waiting for the unreadable timestamp to advance past the
                // scrub timestamp (same as the hard delete transaction's start timestamp).
                // May not need to be here specifically, but this is a very cheap operation - scheduling another thread
                // might well cost more.
                timedAndTraced("microsForPunch", () -> cleaner.punch(commitTimestamp));

                // Serializable transactions need to check their reads haven't changed, by reading again at
                // commitTs + 1. This must happen before the lock check for thorough tables, because the lock check
                // verifies the immutable timestamp hasn't moved forward - thorough sweep might sweep a conflict out
                // from underneath us.
                timedAndTraced("readWriteConflictCheck",
                        () -> throwIfReadWriteConflictForSerializable(commitTimestamp));

                // Verify that our locks and pre-commit conditions are still valid before we actually commit;
                // this throwIfPreCommitRequirementsNotMet is required by the transaction protocol for correctness.
                timedAndTraced("preCommitLockCheck", () -> throwIfImmutableTsOrCommitLocksExpired(commitLocksToken));
                timedAndTraced("userPreCommitCondition", () -> throwIfPreCommitConditionInvalid(commitTimestamp));

                timedAndTraced("commitPutCommitTs",
                        () -> putCommitTimestamp(commitTimestamp, commitLocksToken, transactionService));

                long microsSinceCreation = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis() - timeCreated);
                getTimer("commitTotalTimeSinceTxCreation").update(microsSinceCreation, TimeUnit.MICROSECONDS);
                getHistogram(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_BYTES_WRITTEN).update(byteCount.get());
            } finally {
                timedAndTraced("postCommitUnlock",
                        () -> timelockService.tryUnlock(ImmutableSet.of(commitLocksToken)));
            }
        });
    }

    private void timedAndTraced(String timerName, Runnable runnable) {
        try (Timer.Context timer = getTimer(timerName).time();
                CloseableTracer tracer = CloseableTracer.startSpan(timerName)) {
            runnable.run();
        }
    }

    private <T> T timedAndTraced(String timerName, Supplier<T> supplier) {
        try (Timer.Context timer = getTimer(timerName).time();
                CloseableTracer tracer = CloseableTracer.startSpan(timerName)) {
            return supplier.get();
        }
    }

    protected void throwIfReadWriteConflictForSerializable(long commitTimestamp) {
        // This is for overriding to get serializable transactions
    }

    private boolean hasWrites() {
        return writesByTable.values().stream().anyMatch(writesForTable -> !writesForTable.isEmpty());
    }

    protected boolean hasReads() {
        return hasReads;
    }

    protected ConflictHandler getConflictHandlerForTable(TableReference tableRef) {
        return Preconditions.checkNotNull(conflictDetectionManager.get(tableRef),
            "Not a valid table for this transaction. Make sure this table name exists or has a valid namespace: %s",
            tableRef);
    }

    private String getExpiredLocksErrorString(@Nullable LockToken commitLocksToken,
                                              Set<LockToken> expiredLocks) {
        return "The following immutable timestamp lock was required: " + immutableTimestampLock
            + "; the following commit locks were required: " + commitLocksToken
            + "; the following locks are no longer valid: " + expiredLocks;
    }

    private void throwIfPreCommitRequirementsNotMet(@Nullable LockToken commitLocksToken, long timestamp) {
        throwIfImmutableTsOrCommitLocksExpired(commitLocksToken);
        throwIfPreCommitConditionInvalid(timestamp);
    }

    private void throwIfPreCommitConditionInvalid(long timestamp) {
        try {
            preCommitCondition.throwIfConditionInvalid(timestamp);
        } catch (TransactionFailedException ex) {
            transactionOutcomeMetrics.markPreCommitCheckFailed();
            throw ex;
        }
    }

    private void throwIfImmutableTsOrCommitLocksExpired(@Nullable LockToken commitLocksToken) {
        Set<LockToken> expiredLocks = refreshCommitAndImmutableTsLocks(commitLocksToken);
        if (!expiredLocks.isEmpty()) {
            final String baseMsg = "Locks acquired as part of the transaction protocol are no longer valid. ";
            String expiredLocksErrorString = getExpiredLocksErrorString(commitLocksToken, expiredLocks);
            TransactionLockTimeoutException ex = new TransactionLockTimeoutException(baseMsg + expiredLocksErrorString);
            log.error(baseMsg + "{}", expiredLocksErrorString, ex);
            transactionOutcomeMetrics.markLocksExpired();
            throw ex;
        }
    }

    /**
     * Refreshes external and commit locks.
     * @return set of locks that could not be refreshed
     */
    private Set<LockToken> refreshCommitAndImmutableTsLocks(@Nullable LockToken commitLocksToken) {
        Set<LockToken> toRefresh = Sets.newHashSet();
        if (commitLocksToken != null) {
            toRefresh.add(commitLocksToken);
        }
        immutableTimestampLock.ifPresent(toRefresh::add);

        if (toRefresh.isEmpty()) {
            return ImmutableSet.of();
        }

        return Sets.difference(toRefresh, timelockService.refreshLockLeases(toRefresh)).immutableCopy();
    }

    /**
     * Make sure we have all the rows we are checking already locked before calling this.
     */
    protected void throwIfConflictOnCommit(LockToken commitLocksToken, TransactionService transactionService)
            throws TransactionConflictException {
        for (Entry<TableReference, ConcurrentNavigableMap<Cell, byte[]>> write : writesByTable.entrySet()) {
            ConflictHandler conflictHandler = getConflictHandlerForTable(write.getKey());
            throwIfWriteAlreadyCommitted(
                    write.getKey(),
                    write.getValue(),
                    conflictHandler,
                    commitLocksToken,
                    transactionService);
        }
    }

    protected void throwIfWriteAlreadyCommitted(TableReference tableRef,
                                                Map<Cell, byte[]> writes,
                                                ConflictHandler conflictHandler,
                                                LockToken commitLocksToken,
                                                TransactionService transactionService)
            throws TransactionConflictException {
        if (writes.isEmpty() || !conflictHandler.checkWriteWriteConflicts()) {
            return;
        }
        Set<CellConflict> spanningWrites = Sets.newHashSet();
        Set<CellConflict> dominatingWrites = Sets.newHashSet();
        Map<Cell, Long> keysToLoad = Maps.asMap(writes.keySet(), Functions.constant(Long.MAX_VALUE));
        while (!keysToLoad.isEmpty()) {
            keysToLoad = detectWriteAlreadyCommittedInternal(
                    tableRef,
                    keysToLoad,
                    spanningWrites,
                    dominatingWrites,
                    transactionService);
        }

        if (conflictHandler == ConflictHandler.RETRY_ON_VALUE_CHANGED) {
            throwIfValueChangedConflict(tableRef, writes, spanningWrites, dominatingWrites, commitLocksToken);
        } else {
            if (!spanningWrites.isEmpty() || !dominatingWrites.isEmpty()) {
                transactionOutcomeMetrics.markWriteWriteConflict(tableRef);
                throw TransactionConflictException.create(tableRef, getStartTimestamp(), spanningWrites,
                        dominatingWrites, System.currentTimeMillis() - timeCreated);
            }
        }
    }

    /**
     * This will throw if we have a value changed conflict.  This means that either we changed the
     * value and anyone did a write after our start timestamp, or we just touched the value (put the
     * same value as before) and a changed value was written after our start time.
     */
    private void throwIfValueChangedConflict(TableReference table,
                                             Map<Cell, byte[]> writes,
                                             Set<CellConflict> spanningWrites,
                                             Set<CellConflict> dominatingWrites,
                                             LockToken commitLocksToken) {
        Map<Cell, CellConflict> cellToConflict = Maps.newHashMap();
        Map<Cell, Long> cellToTs = Maps.newHashMap();
        for (CellConflict c : Sets.union(spanningWrites, dominatingWrites)) {
            cellToConflict.put(c.getCell(), c);
            cellToTs.put(c.getCell(), c.getTheirStart() + 1);
        }

        Map<Cell, byte[]> oldValues = getIgnoringLocalWrites(table, cellToTs.keySet());
        Map<Cell, Value> conflictingValues = keyValueService.get(table, cellToTs);

        Set<Cell> conflictingCells = Sets.newHashSet();
        for (Entry<Cell, Long> cellEntry : cellToTs.entrySet()) {
            Cell cell = cellEntry.getKey();
            if (!writes.containsKey(cell)) {
                Validate.isTrue(false, "Missing write for cell: %s for table %s", cellToConflict.get(cell), table);
            }
            if (!conflictingValues.containsKey(cell)) {
                // This error case could happen if our locks expired.
                throwIfPreCommitRequirementsNotMet(commitLocksToken, getStartTimestamp());
                Validate.isTrue(false, "Missing conflicting value for cell: %s for table %s", cellToConflict.get(cell),
                        table);
            }
            if (conflictingValues.get(cell).getTimestamp() != (cellEntry.getValue() - 1)) {
                // This error case could happen if our locks expired.
                throwIfPreCommitRequirementsNotMet(commitLocksToken, getStartTimestamp());
                Validate.isTrue(false, "Wrong timestamp for cell in table %s Expected: %s Actual: %s", table,
                        cellToConflict.get(cell),
                        conflictingValues.get(cell));
            }
            @Nullable byte[] oldVal = oldValues.get(cell);
            byte[] writeVal = writes.get(cell);
            byte[] conflictingVal = conflictingValues.get(cell).getContents();
            if (!Transactions.cellValuesEqual(oldVal, writeVal)
                    || !Arrays.equals(writeVal, conflictingVal)) {
                conflictingCells.add(cell);
            } else if (log.isInfoEnabled()) {
                log.info("Another transaction committed to the same cell before us but their value was the same."
                        + " Cell: {} Table: {}",
                        UnsafeArg.of("cell", cell),
                        LoggingArgs.tableRef(table));
            }
        }
        if (conflictingCells.isEmpty()) {
            return;
        }
        Predicate<CellConflict> conflicting = Predicates.compose(
                Predicates.in(conflictingCells),
                CellConflict.getCellFunction());
        transactionOutcomeMetrics.markWriteWriteConflict(table);
        throw TransactionConflictException.create(table,
                getStartTimestamp(),
                Sets.filter(spanningWrites, conflicting),
                Sets.filter(dominatingWrites, conflicting),
                System.currentTimeMillis() - timeCreated);
    }

    /**
     * This will return the set of keys that need to be retried.  It will output any conflicts
     * it finds into the output params.
     */
    protected Map<Cell, Long> detectWriteAlreadyCommittedInternal(TableReference tableRef,
                                                                  Map<Cell, Long> keysToLoad,
                                                                  @Output Set<CellConflict> spanningWrites,
                                                                  @Output Set<CellConflict> dominatingWrites,
                                                                  TransactionService transactionService) {
        Map<Cell, Long> rawResults = keyValueService.getLatestTimestamps(tableRef, keysToLoad);
        Map<Long, Long> commitTimestamps = getCommitTimestamps(tableRef, rawResults.values(), false);
        Map<Cell, Long> keysToDelete = Maps.newHashMapWithExpectedSize(0);

        for (Map.Entry<Cell, Long> e : rawResults.entrySet()) {
            Cell key = e.getKey();
            long theirStartTimestamp = e.getValue();
            AssertUtils.assertAndLog(log, theirStartTimestamp != getStartTimestamp(),
                    "Timestamp reuse is bad:%d", getStartTimestamp());

            Long theirCommitTimestamp = commitTimestamps.get(theirStartTimestamp);
            if (theirCommitTimestamp == null
                    || theirCommitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
                // The value has no commit timestamp or was explicitly rolled back.
                // This means the value is garbage from a transaction which didn't commit.
                keysToDelete.put(key, theirStartTimestamp);
                continue;
            }

            AssertUtils.assertAndLog(log, theirCommitTimestamp != getStartTimestamp(),
                    "Timestamp reuse is bad:%d", getStartTimestamp());
            if (theirStartTimestamp > getStartTimestamp()) {
                dominatingWrites.add(Cells.createConflictWithMetadata(
                        keyValueService,
                        tableRef,
                        key,
                        theirStartTimestamp,
                        theirCommitTimestamp));
            } else if (theirCommitTimestamp > getStartTimestamp()) {
                spanningWrites.add(Cells.createConflictWithMetadata(
                        keyValueService,
                        tableRef,
                        key,
                        theirStartTimestamp,
                        theirCommitTimestamp));
            }
        }

        if (!keysToDelete.isEmpty()) {
            if (!rollbackFailedTransactions(tableRef, keysToDelete, commitTimestamps, transactionService)) {
                // If we can't roll back the failed transactions, we should just try again.
                return keysToLoad;
            }
        }

        // Once we successfully rollback and delete these cells we need to reload them.
        return keysToDelete;
    }

    /**
     * This will attempt to rollback the passed transactions.  If all are rolled back correctly this
     * method will also delete the values for the transactions that have been rolled back.
     * @return false if we cannot roll back the failed transactions because someone beat us to it
     */
    private boolean rollbackFailedTransactions(
            TableReference tableRef,
            Map<Cell, Long> keysToDelete,
            Map<Long, Long> commitTimestamps,
            TransactionService transactionService) {
        for (long startTs : Sets.newHashSet(keysToDelete.values())) {
            if (commitTimestamps.get(startTs) == null) {
                log.warn("Rolling back transaction: {}", SafeArg.of("startTs", startTs));
                if (!rollbackOtherTransaction(startTs, transactionService)) {
                    return false;
                }
            } else {
                Validate.isTrue(commitTimestamps.get(startTs) == TransactionConstants.FAILED_COMMIT_TS);
            }
        }

        try {
            deleteExecutor.submit(() -> deleteCells(tableRef, keysToDelete));
        } catch (RejectedExecutionException rejected) {
            log.info("Could not delete keys {} for table {}, because the delete executor's queue was full."
                    + " Sweep should eventually clean these values.",
                    UnsafeArg.of("keysToDelete", keysToDelete),
                    LoggingArgs.tableRef(tableRef),
                    rejected);
        }
        return true;
    }

    private void deleteCells(TableReference tableRef, Map<Cell, Long> keysToDelete) {
        try {
            log.debug("For table: {} we are deleting values of an uncommitted transaction: {}",
                    LoggingArgs.tableRef(tableRef),
                    UnsafeArg.of("keysToDelete", keysToDelete));
            keyValueService.delete(tableRef, Multimaps.forMap(keysToDelete));
        } catch (RuntimeException e) {
            final String msg = "This isn't a bug but it should be infrequent if all nodes of your KV service are"
                    + " running. Delete has stronger consistency semantics than read/write and must talk to all nodes"
                    + " instead of just talking to a quorum of nodes. "
                    + "Failed to delete keys for table: {} from an uncommitted transaction; "
                    + " sweep should eventually clean these values.";
            if (log.isDebugEnabled()) {
                log.warn(msg + " The keys that failed to be deleted during rollback were {}",
                        LoggingArgs.tableRef(tableRef),
                        UnsafeArg.of("keysToDelete", keysToDelete));
            } else {
                log.warn(msg, LoggingArgs.tableRef(tableRef), e);
            }
        }
    }

    /**
     * Rollback a someone else's transaction.
     * @return true if the other transaction was rolled back
     */
    private boolean rollbackOtherTransaction(long startTs, TransactionService transactionService) {
        try {
            transactionService.putUnlessExists(startTs, TransactionConstants.FAILED_COMMIT_TS);
            transactionOutcomeMetrics.markRollbackOtherTransaction();
            return true;
        } catch (KeyAlreadyExistsException e) {
            log.info("This isn't a bug but it should be very infrequent. Two transactions tried to roll back someone"
                    + " else's request with start: {}",
                    SafeArg.of("startTs", startTs),
                    new TransactionFailedRetriableException(
                            "Two transactions tried to roll back someone else's request with start: " + startTs, e));
            return false;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    /// Locking
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This method should acquire any locks needed to do proper concurrency control at commit time.
     */
    protected LockToken acquireLocksForCommit() {
        Set<LockDescriptor> lockDescriptors = getLocksForWrites();

        LockRequest request = LockRequest.of(lockDescriptors, transactionConfig.get().getLockAcquireTimeoutMillis());
        LockResponse lockResponse = timelockService.lock(request);
        if (!lockResponse.wasSuccessful()) {
            log.error("Timed out waiting while acquiring commit locks. Timeout was {} ms. "
                            + "First ten required locks were {}.",
                    SafeArg.of("acquireTimeoutMs", transactionConfig.get().getLockAcquireTimeoutMillis()),
                    UnsafeArg.of("firstTenLockDescriptors", Iterables.limit(lockDescriptors, 10)));
            throw new TransactionLockAcquisitionTimeoutException("Timed out while acquiring commit locks.");
        }
        return lockResponse.getToken();
    }

    protected Set<LockDescriptor> getLocksForWrites() {
        Set<LockDescriptor> result = Sets.newHashSet();
        for (TableReference tableRef : writesByTable.keySet()) {
            ConflictHandler conflictHandler = getConflictHandlerForTable(tableRef);
            if (conflictHandler.lockCellsForConflicts()) {
                for (Cell cell : getLocalWrites(tableRef).keySet()) {
                    result.add(
                            AtlasCellLockDescriptor.of(
                                    tableRef.getQualifiedName(),
                                    cell.getRowName(),
                                    cell.getColumnName()));
                }
            }

            if (conflictHandler.lockRowsForConflicts()) {
                Cell lastCell = null;
                for (Cell cell : getLocalWrites(tableRef).keySet()) {
                    if (lastCell == null || !Arrays.equals(lastCell.getRowName(), cell.getRowName())) {
                        result.add(
                                AtlasRowLockDescriptor.of(tableRef.getQualifiedName(), cell.getRowName()));
                    }
                    lastCell = cell;
                }
            }
        }
        result.add(
                AtlasRowLockDescriptor.of(
                        TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                        TransactionConstants.getValueForTimestamp(getStartTimestamp())));
        return result;
    }

    /**
     * We will block here until the passed transactions have released their lock.  This means that
     * the committing transaction is either complete or it has failed and we are allowed to roll
     * it back.
     */
    private void waitForCommitToComplete(Iterable<Long> startTimestamps) {
        Set<LockDescriptor> lockDescriptors = Sets.newHashSet();
        for (long start : startTimestamps) {
            if (start < immutableTimestamp) {
                // We don't need to block in this case because this transaction is already complete
                continue;
            }
            lockDescriptors.add(
                    AtlasRowLockDescriptor.of(
                            TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                            TransactionConstants.getValueForTimestamp(start)));
        }

        if (lockDescriptors.isEmpty()) {
            return;
        }

        waitFor(lockDescriptors);
    }

    private void waitFor(Set<LockDescriptor> lockDescriptors) {
        WaitForLocksRequest request = WaitForLocksRequest.of(lockDescriptors,
                transactionConfig.get().getLockAcquireTimeoutMillis());
        WaitForLocksResponse response = timelockService.waitForLocks(request);
        if (!response.wasSuccessful()) {
            log.error("Timed out waiting for commits to complete. Timeout was {} ms. First ten locks were {}.",
                    SafeArg.of("requestId", request.getRequestId()),
                    SafeArg.of("acquireTimeoutMs", transactionConfig.get().getLockAcquireTimeoutMillis()),
                    UnsafeArg.of("firstTenLockDescriptors", Iterables.limit(lockDescriptors, 10)));
            throw new TransactionLockAcquisitionTimeoutException("Timed out waiting for commits to complete.");
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    /// Commit timestamp management
    ///////////////////////////////////////////////////////////////////////////

    private Set<Long> getStartTimestampsForValues(Iterable<Value> values) {
        Set<Long> results = Sets.newHashSet();
        for (Value v : values) {
            results.add(v.getTimestamp());
        }
        return results;
    }

    /**
     * Returns a map from start timestamp to commit timestamp.  If a start timestamp wasn't
     * committed, then it will be missing from the map.  This method will block until the
     * transactions for these start timestamps are complete.
     */
    protected Map<Long, Long> getCommitTimestamps(@Nullable TableReference tableRef,
                                                  Iterable<Long> startTimestamps,
                                                  boolean waitForCommitterToComplete) {
        if (Iterables.isEmpty(startTimestamps)) {
            return ImmutableMap.of();
        }
        Map<Long, Long> result = Maps.newHashMap();
        Set<Long> gets = Sets.newHashSet();
        for (long startTs : startTimestamps) {
            Long cached = timestampValidationReadCache.getCommitTimestampIfPresent(startTs);
            if (cached != null) {
                result.put(startTs, cached);
            } else {
                gets.add(startTs);
            }
        }

        if (gets.isEmpty()) {
            return result;
        }

        // Before we do the reads, we need to make sure the committer is done writing.
        if (waitForCommitterToComplete) {
            Timer.Context timer = getTimer("waitForCommitTsMillis").time();
            waitForCommitToComplete(startTimestamps);
            long waitForCommitTsMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());

            if (tableRef != null) {
                perfLogger.debug("Waited {} ms to get commit timestamps for table {}.",
                        SafeArg.of("commitTsMillis", waitForCommitTsMillis),
                        LoggingArgs.tableRef(tableRef));
            } else {
                perfLogger.debug("Waited {} ms to get commit timestamps",
                        SafeArg.of("commitTsMillis", waitForCommitTsMillis));
            }
        }

        if (tableRef != null) {
            log.trace("Getting commit timestamps for {} start timestamps in response to read from table {}",
                    SafeArg.of("numTimestamps", gets.size()),
                    LoggingArgs.tableRef(tableRef));
        } else {
            log.trace("Getting commit timestamps for {} start timestamps",
                    SafeArg.of("numTimestamps", gets.size()));
        }

        if (gets.size() > transactionConfig.get().getThresholdForLoggingLargeNumberOfTransactionLookups()) {
            log.info(
                    "Looking up a large number of transactions ({}) for table {}",
                    SafeArg.of("numberOfTransactionIds", gets.size()),
                    tableRef == null
                            ? SafeArg.of("tableRef", "no_table")
                            : LoggingArgs.tableRef(tableRef)
            );
        }

        getHistogram(AtlasDbMetricNames.NUMBER_OF_TRANSACTIONS_READ_FROM_DB, tableRef).update(gets.size());

        Map<Long, Long> rawResults = loadCommitTimestamps(gets);

        for (Map.Entry<Long, Long> e : rawResults.entrySet()) {
            if (e.getValue() != null) {
                long startTs = e.getKey();
                long commitTs = e.getValue();
                result.put(startTs, commitTs);
                timestampValidationReadCache.putAlreadyCommittedTransaction(startTs, commitTs);
            }
        }
        return result;
    }

    private Map<Long, Long> loadCommitTimestamps(Set<Long> startTimestamps) {
        // distinguish between a single timestamp and a batch, for more granular metrics
        if (startTimestamps.size() == 1) {
            long singleTs = startTimestamps.iterator().next();
            Long commitTsOrNull = defaultTransactionService.get(singleTs);
            return commitTsOrNull == null ? ImmutableMap.of() : ImmutableMap.of(singleTs, commitTsOrNull);
        } else {
            return defaultTransactionService.get(startTimestamps);
        }
    }

    /**
     * This will attempt to put the commitTimestamp into the DB.
     *
     * @throws TransactionLockTimeoutException If our locks timed out while trying to commit.
     * @throws TransactionCommitFailedException failed when committing in a way that isn't retriable
     */
    private void putCommitTimestamp(
            long commitTimestamp,
            LockToken locksToken,
            TransactionService transactionService)
            throws TransactionFailedException {
        Validate.isTrue(commitTimestamp > getStartTimestamp(), "commitTs must be greater than startTs");
        try {
            transactionService.putUnlessExists(getStartTimestamp(), commitTimestamp);
        } catch (KeyAlreadyExistsException e) {
            handleKeyAlreadyExistsException(commitTimestamp, e, locksToken);
        } catch (Exception e) {
            TransactionCommitFailedException commitFailedEx = new TransactionCommitFailedException(
                    "This transaction failed writing the commit timestamp. "
                    + "It might have been committed, but it may not have.", e);
            log.error("failed to commit an atlasdb transaction", commitFailedEx);
            transactionOutcomeMetrics.markPutUnlessExistsFailed();
            throw commitFailedEx;
        }
    }

    private void handleKeyAlreadyExistsException(
            long commitTs,
            KeyAlreadyExistsException ex,
            LockToken commitLocksToken) {
        try {
            if (wasCommitSuccessful(commitTs)) {
                // We did actually commit successfully.  This case could happen if the impl
                // for putUnlessExists did a retry and we had committed already
                return;
            }
            Set<LockToken> expiredLocks = refreshCommitAndImmutableTsLocks(commitLocksToken);
            if (!expiredLocks.isEmpty()) {
                transactionOutcomeMetrics.markLocksExpired();
                throw new TransactionLockTimeoutException("Our commit was already rolled back at commit time"
                        + " because our locks timed out. startTs: " + getStartTimestamp() + ".  "
                        + getExpiredLocksErrorString(commitLocksToken, expiredLocks), ex);
            } else {
                log.warn("Possible bug: Someone rolled back our transaction but"
                        + " our locks were still valid; Atlas code is not allowed to do this, though others can.",
                        SafeArg.of("immutableTimestampLock", immutableTimestampLock),
                        SafeArg.of("commitLocksToken", commitLocksToken));
            }
        } catch (TransactionFailedException e1) {
            throw e1;
        } catch (Exception e1) {
            log.error("Failed to determine if we can retry this transaction. startTs: {}",
                    SafeArg.of("startTs", getStartTimestamp()),
                    e1);
        }
        String msg = "Our commit was already rolled back at commit time."
                + " Locking should prevent this from happening, but our locks may have timed out."
                + " startTs: " + getStartTimestamp();
        throw new TransactionCommitFailedException(msg, ex);
    }

    private boolean wasCommitSuccessful(long commitTs) throws Exception {
        Map<Long, Long> commitTimestamps = getCommitTimestamps(null, Collections.singleton(getStartTimestamp()), false);
        long storedCommit = commitTimestamps.get(getStartTimestamp());
        if (storedCommit != commitTs && storedCommit != TransactionConstants.FAILED_COMMIT_TS) {
            Validate.isTrue(false, "Commit value is wrong. startTs %s  commitTs: %s", getStartTimestamp(), commitTs);
        }
        return storedCommit == commitTs;
    }

    @Override
    public void useTable(TableReference tableRef, ConstraintCheckable table) {
        constraintsByTableName.put(tableRef, table);
    }

    /** The similarly-named-and-intentioned useTable method is only called on writes.
     *  This one is more comprehensive and covers read paths as well
     * (necessary because we wish to get the sweep strategies of tables in read-only transactions)
     */
    private void markTableAsInvolvedInThisTransaction(TableReference tableRef) {
        involvedTables.add(tableRef);
    }

    private boolean validationNecessaryForInvolvedTablesOnCommit() {
        return involvedTables.stream().anyMatch(this::isValidationNecessaryOnCommit);
    }

    private long getStartTimestamp() {
        return startTimestamp.get();
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return keyValueService;
    }

    private Multimap<Cell, TableReference> getCellsToQueueForScrubbing() {
        return getCellsToScrubByCell(State.COMMITTING);
    }

    Multimap<TableReference, Cell> getCellsToScrubImmediately() {
        return getCellsToScrubByTable(State.COMMITTED);
    }

    private Multimap<Cell, TableReference> getCellsToScrubByCell(State expectedState) {
        Multimap<Cell, TableReference> cellToTableName = HashMultimap.create();
        State actualState = state.get();
        if (expectedState == actualState) {
            for (Entry<TableReference, ConcurrentNavigableMap<Cell, byte[]>> entry : writesByTable.entrySet()) {
                TableReference table = entry.getKey();
                Set<Cell> cells = entry.getValue().keySet();
                for (Cell c : cells) {
                    cellToTableName.put(c, table);
                }
            }
        } else {
            AssertUtils.assertAndLog(log, false, "Expected state: " + expectedState + "; actual state: " + actualState);
        }
        return cellToTableName;
    }


    private Multimap<TableReference, Cell> getCellsToScrubByTable(State expectedState) {
        Multimap<TableReference, Cell> tableRefToCells = HashMultimap.create();
        State actualState = state.get();
        if (expectedState == actualState) {
            for (Entry<TableReference, ConcurrentNavigableMap<Cell, byte[]>> entry : writesByTable.entrySet()) {
                TableReference table = entry.getKey();
                Set<Cell> cells = entry.getValue().keySet();
                tableRefToCells.putAll(table, cells);
            }
        } else {
            AssertUtils.assertAndLog(log, false, "Expected state: " + expectedState + "; actual state: " + actualState);
        }
        return tableRefToCells;
    }

    private Timer getTimer(String name) {
        return metricsManager.registerOrGetTimer(SnapshotTransaction.class, name);
    }

    private Histogram getHistogram(String name) {
        return metricsManager.registerOrGetHistogram(SnapshotTransaction.class, name);
    }

    private Histogram getHistogram(String name, TableReference tableRef) {
        return metricsManager.registerOrGetTaggedHistogram(
                SnapshotTransaction.class,
                name,
                metricsManager.getTableNameTagFor(tableRef));
    }

    private Meter getMeter(String name, TableReference tableRef) {
        return metricsManager.registerOrGetTaggedMeter(
                SnapshotTransaction.class,
                name,
                metricsManager.getTableNameTagFor(tableRef));
    }
}
