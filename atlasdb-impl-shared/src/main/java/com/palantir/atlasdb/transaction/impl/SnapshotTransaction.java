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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
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
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
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
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.table.description.exceptions.AtlasDbConstraintException;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.atlasdb.transaction.api.ConstraintCheckingTransaction;
import com.palantir.atlasdb.transaction.api.TransactionCommitFailedException;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionConflictException.CellConflict;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionLockAcquisitionTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
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
import com.palantir.common.collect.IterableUtils;
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
    private static final MetricsManager metricsManager = new MetricsManager();

    protected final long immutableTimestamp;
    protected final Optional<LockToken> immutableTimestampLock;
    private final AdvisoryLockPreCommitCheck advisoryLockCheck;
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
    protected final Stopwatch transactionTimer = Stopwatch.createStarted();
    protected final TimestampCache timestampValidationReadCache;
    protected final long lockAcquireTimeoutMs;
    protected final ExecutorService getRangesExecutor;
    protected final int defaultGetRangesConcurrency;

    private final Timer.Context transactionTimerContext = getTimer("transactionMillis").time();

    /**
     * @param immutableTimestamp If we find a row written before the immutableTimestamp we don't need to
     *                           grab a read lock for it because we know that no writers exist.
     * @param advisoryLockCheck This check must pass for this transaction to commit.  If these locks have
     *                          expired then the commit will fail.
     */
    /* package */ SnapshotTransaction(KeyValueService keyValueService,
                               TimelockService timelockService,
                               TransactionService transactionService,
                               Cleaner cleaner,
                               Supplier<Long> startTimeStamp,
                               ConflictDetectionManager conflictDetectionManager,
                               SweepStrategyManager sweepStrategyManager,
                               long immutableTimestamp,
                               Optional<LockToken> immutableTimestampLock,
                               AdvisoryLockPreCommitCheck advisoryLockCheck,
                               AtlasDbConstraintCheckingMode constraintCheckingMode,
                               Long transactionTimeoutMillis,
                               TransactionReadSentinelBehavior readSentinelBehavior,
                               boolean allowHiddenTableAccess,
                               TimestampCache timestampValidationReadCache,
                               long lockAcquireTimeoutMs,
                               ExecutorService getRangesExecutor,
                               int defaultGetRangesConcurrency) {
        this.keyValueService = keyValueService;
        this.timelockService = timelockService;
        this.defaultTransactionService = transactionService;
        this.cleaner = cleaner;
        this.startTimestamp = startTimeStamp;
        this.conflictDetectionManager = conflictDetectionManager;
        this.sweepStrategyManager = sweepStrategyManager;
        this.immutableTimestamp = immutableTimestamp;
        this.immutableTimestampLock = immutableTimestampLock;
        this.advisoryLockCheck = advisoryLockCheck;
        this.constraintCheckingMode = constraintCheckingMode;
        this.transactionReadTimeoutMillis = transactionTimeoutMillis;
        this.readSentinelBehavior = readSentinelBehavior;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.timestampValidationReadCache = timestampValidationReadCache;
        this.lockAcquireTimeoutMs = lockAcquireTimeoutMs;
        this.getRangesExecutor = getRangesExecutor;
        this.defaultGetRangesConcurrency = defaultGetRangesConcurrency;
    }

    // TEST ONLY
    @VisibleForTesting
    SnapshotTransaction(KeyValueService keyValueService,
                        TimelockService timelockService,
                        TransactionService transactionService,
                        Cleaner cleaner,
                        long startTimeStamp,
                        ConflictDetectionManager conflictDetectionManager,
                        AtlasDbConstraintCheckingMode constraintCheckingMode,
                        TransactionReadSentinelBehavior readSentinelBehavior,
                        TimestampCache timestampValidationReadCache,
                        ExecutorService getRangesExecutor,
                        int defaultGetRangesConcurrency) {
        this.keyValueService = keyValueService;
        this.timelockService = timelockService;
        this.defaultTransactionService = transactionService;
        this.cleaner = cleaner;
        this.startTimestamp = Suppliers.ofInstance(startTimeStamp);
        this.conflictDetectionManager = conflictDetectionManager;
        this.sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);
        this.immutableTimestamp = 0;
        this.immutableTimestampLock = Optional.empty();
        this.advisoryLockCheck = AdvisoryLockPreCommitCheck.NO_OP;
        this.constraintCheckingMode = constraintCheckingMode;
        this.transactionReadTimeoutMillis = null;
        this.readSentinelBehavior = readSentinelBehavior;
        this.allowHiddenTableAccess = false;
        this.timestampValidationReadCache = timestampValidationReadCache;
        this.lockAcquireTimeoutMs = AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS;
        this.getRangesExecutor = getRangesExecutor;
        this.defaultGetRangesConcurrency = defaultGetRangesConcurrency;
    }

    protected SnapshotTransaction(KeyValueService keyValueService,
                                  TransactionService transactionService,
                                  TimelockService timelockService,
                                  long startTimeStamp,
                                  AtlasDbConstraintCheckingMode constraintCheckingMode,
                                  TransactionReadSentinelBehavior readSentinelBehavior,
                                  boolean allowHiddenTableAccess,
                                  TimestampCache timestampValidationReadCache,
                                  long lockAcquireTimeoutMs,
                                  ExecutorService getRangesExecutor,
                                  int defaultGetRangesConcurrency) {
        this.keyValueService = keyValueService;
        this.defaultTransactionService = transactionService;
        this.cleaner = NoOpCleaner.INSTANCE;
        this.timelockService = timelockService;
        this.startTimestamp = Suppliers.ofInstance(startTimeStamp);
        this.conflictDetectionManager = ConflictDetectionManagers.createWithNoConflictDetection();
        this.sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);
        this.immutableTimestamp = startTimeStamp;
        this.immutableTimestampLock = Optional.empty();
        this.advisoryLockCheck = AdvisoryLockPreCommitCheck.NO_OP;
        this.constraintCheckingMode = constraintCheckingMode;
        this.transactionReadTimeoutMillis = null;
        this.readSentinelBehavior = readSentinelBehavior;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.timestampValidationReadCache = timestampValidationReadCache;
        this.lockAcquireTimeoutMs = lockAcquireTimeoutMs;
        this.getRangesExecutor = getRangesExecutor;
        this.defaultGetRangesConcurrency = defaultGetRangesConcurrency;
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

    public Stopwatch getTrasactionTimer() {
        return transactionTimer;
    }

    protected void checkGetPreconditions(TableReference tableRef) {
        if (transactionReadTimeoutMillis != null
                && System.currentTimeMillis() - timeCreated > transactionReadTimeoutMillis) {
            throw new TransactionFailedRetriableException("Transaction timed out.");
        }
        Preconditions.checkArgument(allowHiddenTableAccess || !AtlasDbConstants.hiddenTables.contains(tableRef));
        Preconditions.checkState(state.get() == State.UNCOMMITTED || state.get() == State.COMMITTING,
                "Transaction must be uncommitted.");
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(TableReference tableRef, Iterable<byte[]> rows,
                                                        ColumnSelection columnSelection) {
        Timer.Context timer = getTimer("getRows").time();
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return AbstractTransaction.EMPTY_SORTED_ROWS;
        }
        Map<Cell, byte[]> result = Maps.newHashMap();
        Map<Cell, Value> rawResults = Maps.newHashMap(
                keyValueService.getRows(tableRef, rows, columnSelection, getStartTimestamp()));
        SortedMap<Cell, byte[]> writes = writesByTable.get(tableRef);
        if (writes != null) {
            for (byte[] row : rows) {
                extractLocalWritesForRow(result, writes, row);
            }
        }

        // We don't need to do work postFiltering if we have a write locally.
        rawResults.keySet().removeAll(result.keySet());

        SortedMap<byte[], RowResult<byte[]>> results = filterRowResults(tableRef, rawResults, result);
        long getRowsMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());
        if (perfLogger.isDebugEnabled()) {
            perfLogger.debug("getRows({}, {} rows) found {} rows, took {} ms",
                    tableRef, Iterables.size(rows), results.size(), getRowsMillis);
        }
        validateExternalAndCommitLocksIfNecessary(tableRef);
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
        RowColumnRangeIterator rawResults =
                keyValueService.getRowsColumnRange(tableRef,
                                                   rows,
                                                   columnRangeSelection,
                                                   batchHint,
                                                   getStartTimestamp());
        if (!rawResults.hasNext()) {
            validateExternalAndCommitLocksIfNecessary(tableRef);
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
        // Filter empty columns.
        return Iterators.filter(mergedIterator, entry -> entry.getValue().length > 0);
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
                validateExternalAndCommitLocksIfNecessary(tableRef);
                if (raw.isEmpty()) {
                    return endOfData();
                }
                Map<Cell, byte[]> post = new LinkedHashMap<>();
                getWithPostFiltering(tableRef, raw, post, Value.GET_VALUE);
                batchIterator.markNumResultsNotDeleted(post.keySet().size());
                return post.entrySet().iterator();
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
                             Arrays.toString(prevRowName),
                             numConsumed);
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

        Map<Cell, Value> rawResults = Maps.newHashMap(keyValueService.getRows(tableRef,
                rows,
                ColumnSelection.all(),
                getStartTimestamp()));

        validateExternalAndCommitLocksIfNecessary(tableRef);
        return filterRowResults(tableRef, rawResults, Maps.newHashMap());
    }

    private SortedMap<byte[], RowResult<byte[]>> filterRowResults(TableReference tableRef,
                                                                  Map<Cell, Value> rawResults,
                                                                  Map<Cell, byte[]> result) {
        getWithPostFiltering(tableRef, rawResults, result, Value.GET_VALUE);
        Map<Cell, byte[]> filterDeletedValues = Maps.filterValues(result, Predicates.not(Value.IS_EMPTY));
        return RowResults.viewOfSortedMap(Cells.breakCellsUpByRow(filterDeletedValues));
    }

    /**
     * This will add any local writes for this row to the result map.
     * <p>
     * If an empty value was written as a delete, this will also be included in the map.
     */
    private void extractLocalWritesForRow(@Output Map<Cell, byte[]> result,
            SortedMap<Cell, byte[]> writes, byte[] row) {
        Cell lowCell = Cells.createSmallestCellForRow(row);
        Iterator<Entry<Cell, byte[]>> it = writes.tailMap(lowCell).entrySet().iterator();
        while (it.hasNext()) {
            Entry<Cell, byte[]> entry = it.next();
            Cell cell = entry.getKey();
            if (!Arrays.equals(row, cell.getRowName())) {
                break;
            }
            result.put(cell, entry.getValue());
        }
    }

    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        Timer.Context timer = getTimer("get").time();
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(cells)) {
            return ImmutableMap.of();
        }

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
        validateExternalAndCommitLocksIfNecessary(tableRef);
        return Maps.filterValues(result, Predicates.not(Value.IS_EMPTY));
    }

    @Override
    public Map<Cell, byte[]> getIgnoringLocalWrites(TableReference tableRef, Set<Cell> cells) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(cells)) {
            return ImmutableMap.of();
        }

        Map<Cell, byte[]> result = getFromKeyValueService(tableRef, cells);
        validateExternalAndCommitLocksIfNecessary(tableRef);

        return Maps.filterValues(result, Predicates.not(Value.IS_EMPTY));
    }

    /**
     * This will load the given keys from the underlying key value service and apply postFiltering
     * so we have snapshot isolation.  If the value in the key value service is the empty array
     * this will be included here and needs to be filtered out.
     */
    private Map<Cell, byte[]> getFromKeyValueService(TableReference tableRef, Set<Cell> cells) {
        Map<Cell, byte[]> result = Maps.newHashMap();
        Map<Cell, Long> toRead = Cells.constantValueMap(cells, getStartTimestamp());
        Map<Cell, Value> rawResults = keyValueService.get(tableRef, toRead);
        getWithPostFiltering(tableRef, rawResults, result, Value.GET_VALUE);
        return result;
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

        return FluentIterable.from(Iterables.partition(rangeRequests, BATCH_SIZE_GET_FIRST_PAGE))
                .transformAndConcat(input -> {
                    Timer.Context timer = getTimer("processedRangeMillis").time();
                    Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> firstPages =
                            keyValueService.getFirstBatchForRanges(tableRef, input, getStartTimestamp());
                    validateExternalAndCommitLocksIfNecessary(tableRef);

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
                            input.size(), tableRef, processedRangeMillis);
                    return ret;
                });
    }

    @Override
    public <T> Stream<T> getRanges(
            final TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            int concurrencyLevel,
            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> visitableProcessor) {
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

    private void validateExternalAndCommitLocksIfNecessary(TableReference tableRef) {
        if (!isValidationNecessary(tableRef)) {
            return;
        }
        throwIfPreCommitRequirementsNotMet(null);
    }

    private boolean isValidationNecessary(TableReference tableRef) {
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

        return new AbstractBatchingVisitable<RowResult<byte[]>>() {
            @Override
            public <K extends Exception> void batchAcceptSizeHint(
                    int userRequestedSize,
                    ConsistentVisitor<RowResult<byte[]>, K> visitor)
                    throws K {
                Preconditions.checkState(state.get() == State.UNCOMMITTED,
                        "Transaction must be uncommitted.");

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
                    mergeInLocalWritesRows(postFilterIterator, localWritesInRange, range.isReverse());
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

    private static Iterator<RowResult<byte[]>> mergeInLocalWritesRows(
            Iterator<RowResult<byte[]>> postFilterIterator,
            Iterator<RowResult<byte[]>> localWritesInRange,
            boolean isReverse) {
        Ordering<RowResult<byte[]>> ordering = RowResult.getOrderingByRowName();
        Iterator<RowResult<byte[]>> mergeIterators = IteratorUtils.mergeIterators(
                postFilterIterator, localWritesInRange,
                isReverse ? ordering.reverse() : ordering,
                from -> RowResults.merge(from.lhSide, from.rhSide)); // prefer local writes
        return RowResults.filterDeletedColumnsAndEmptyRows(mergeIterators);
    }

    private static List<Entry<Cell, byte[]>> mergeInLocalWrites(
            Iterator<Entry<Cell, byte[]>> postFilterIterator,
            Iterator<Entry<Cell, byte[]>> localWritesInRange,
            boolean isReverse) {
        Ordering<Entry<Cell, byte[]>> ordering = Ordering.natural().onResultOf(MapEntries.getKeyFunction());
        Iterator<Entry<Cell, byte[]>> mergeIterators = IteratorUtils.mergeIterators(
                postFilterIterator, localWritesInRange,
                isReverse ? ordering.reverse() : ordering,
                from -> from.rhSide); // always override their value with written values

        return postFilterEmptyValues(mergeIterators);
    }

    private static List<Entry<Cell, byte[]>> postFilterEmptyValues(
            Iterator<Entry<Cell, byte[]>> mergeIterators) {
        List<Entry<Cell, byte[]>> mergedWritesWithoutEmptyValues = new ArrayList<>();
        Predicate<Entry<Cell, byte[]>> nonEmptyValuePredicate = Predicates.compose(Predicates.not(Value.IS_EMPTY),
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
        getMeter(AtlasDbMetricNames.CellFilterMetrics.EMPTY_VALUE).mark(numEmptyValues);
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
                validateExternalAndCommitLocksIfNecessary(tableRef);
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
        ConcurrentNavigableMap<Cell, byte[]> writes = writesByTable.get(tableRef);
        if (writes == null) {
            writes = new ConcurrentSkipListMap<Cell, byte[]>();
            ConcurrentNavigableMap<Cell, byte[]> previous = writesByTable.putIfAbsent(tableRef, writes);
            if (previous != null) {
                writes = previous;
            }
        }
        return writes;
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
        Preconditions.checkState(state.get() == State.UNCOMMITTED, "Transaction must be uncommitted.");

        if (rangeRows.isEmpty()) {
            return ImmutableSortedMap.of();
        }

        Map<Cell, Value> rawResults = Maps.newHashMapWithExpectedSize(estimateSize(rangeRows));
        for (RowResult<Value> rowResult : rangeRows) {
            for (Map.Entry<byte[], Value> e : rowResult.getColumns().entrySet()) {
                rawResults.put(Cell.create(rowResult.getRowName(), e.getKey()), e.getValue());
            }
        }

        SortedMap<Cell, T> postFilter = Maps.newTreeMap();
        getWithPostFiltering(tableRef, rawResults, postFilter, transformer);
        return postFilter;
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
                                          @Output Map<Cell, T> results,
                                          Function<Value, T> transformer) {
        long bytes = 0;
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            bytes += e.getValue().getContents().length + Cells.getApproxSizeOfCell(e.getKey());
        }
        if (bytes > TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES && log.isWarnEnabled()) {
            log.warn("A single get had quite a few bytes: {} for table {}. The number of results was {}. "
                    + "Enable debug logging for more information.",
                    bytes, tableRef.getQualifiedName(), rawResults.size());
            if (log.isDebugEnabled()) {
                log.debug("The first 10 results of your request were {}.", Iterables.limit(rawResults.entrySet(), 10),
                        new RuntimeException("This exception and stack trace are provided for debugging purposes."));
            }
            getHistogram(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_TOO_MANY_BYTES_READ).update(bytes);
        }
        // TODO(hsaraogi): add table names as a tag
        getMeter(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_CELLS_READ).mark(rawResults.size());

        if (AtlasDbConstants.hiddenTables.contains(tableRef)) {
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
        while (!remainingResultsToPostfilter.isEmpty()) {
            remainingResultsToPostfilter = getWithPostFilteringInternal(
                    tableRef, remainingResultsToPostfilter, results, transformer);
        }

        getMeter(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_CELLS_RETURNED).mark(results.size());
    }

    /**
     * This will return all the keys that still need to be postFiltered.  It will output properly
     * postFiltered keys to the results output param.
     */
    private <T> Map<Cell, Value> getWithPostFilteringInternal(TableReference tableRef,
            Map<Cell, Value> rawResults,
            @Output Map<Cell, T> results,
            Function<Value, T> transformer) {
        Set<Long> startTimestampsForValues = getStartTimestampsForValues(rawResults.values());
        Map<Long, Long> commitTimestamps = getCommitTimestamps(tableRef, startTimestampsForValues, true);
        Map<Cell, Long> keysToReload = Maps.newHashMapWithExpectedSize(0);
        Map<Cell, Long> keysToDelete = Maps.newHashMapWithExpectedSize(0);
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            Cell key = e.getKey();
            Value value = e.getValue();

            if (value.getTimestamp() == Value.INVALID_VALUE_TIMESTAMP) {
                getMeter(AtlasDbMetricNames.CellFilterMetrics.INVALID_START_TS).mark();
                // This means that this transaction started too long ago. When we do garbage collection,
                // we clean up old values, and this transaction started at a timestamp before the garbage collection.
                switch (getReadSentinelBehavior()) {
                    case IGNORE:
                        break;
                    case THROW_EXCEPTION:
                        throw new TransactionFailedRetriableException("Tried to read a value that has been deleted. "
                                + " This can be caused by hard delete transactions using the type "
                                + TransactionType.AGGRESSIVE_HARD_DELETE
                                + ". It can also be caused by transactions taking too long, or"
                                + " its locks expired. Retrying it should work.");
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
                        getMeter(AtlasDbMetricNames.CellFilterMetrics.INVALID_COMMIT_TS).mark();
                    }
                } else if (theirCommitTimestamp > getStartTimestamp()) {
                    // The value's commit timestamp is after our start timestamp.
                    // This means the value is from a transaction which committed
                    // after our transaction began. We need to try reading at an
                    // earlier timestamp.
                    keysToReload.put(key, value.getTimestamp());
                    getMeter(AtlasDbMetricNames.CellFilterMetrics.COMMIT_TS_GREATER_THAN_TRANSACTION_TS).mark();
                } else {
                    // The value has a commit timestamp less than our start timestamp, and is visible and valid.
                    if (value.getContents().length != 0) {
                        results.put(key, transformer.apply(value));
                    }
                }
            }
        }

        if (!keysToDelete.isEmpty()) {
            // if we can't roll back the failed transactions, we should just try again
            if (!rollbackFailedTransactions(tableRef, keysToDelete, commitTimestamps, defaultTransactionService)) {
                return rawResults;
            }
        }

        if (!keysToReload.isEmpty()) {
            Map<Cell, Value> nextRawResults = keyValueService.get(tableRef, keysToReload);
            validateExternalAndCommitLocksIfNecessary(tableRef);
            return nextRawResults;
        } else {
            return ImmutableMap.of();
        }
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
        put(tableRef, Cells.constantValueMap(cells, PtBytes.EMPTY_BYTE_ARRAY), Cell.INVALID_TTL, Cell.INVALID_TTL_TYPE);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        ensureNoEmptyValues(values);
        put(tableRef, values, Cell.INVALID_TTL, Cell.INVALID_TTL_TYPE);
    }

    private void put(TableReference tableRef, Map<Cell, byte[]> values, long ttlDuration, TimeUnit ttlUnit) {
        Preconditions.checkArgument(!AtlasDbConstants.hiddenTables.contains(tableRef));

        if (values.isEmpty()) {
            return;
        }

        // todo (clockfort) also check if valid table for TTL
        Map<Cell, byte[]> valuesToWrite = ttlDuration != Cell.INVALID_TTL && ttlUnit != Cell.INVALID_TTL_TYPE
                ? createExpiringValues(values, ttlDuration, ttlUnit)
                : values;

        numWriters.incrementAndGet();
        try {
            // We need to check the status after incrementing writers to ensure that we fail if we are committing.
            Preconditions.checkState(state.get() == State.UNCOMMITTED, "Transaction must be uncommitted.");

            ConcurrentNavigableMap<Cell, byte[]> writes = getLocalWrites(tableRef);

            putWritesAndLogIfTooLarge(valuesToWrite, writes);
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

    private Map<Cell, byte[]> createExpiringValues(Map<Cell, byte[]> values,
                                                   long ttlDuration,
                                                   TimeUnit ttlUnit) {
        Map<Cell, byte[]> expiringValues = Maps.newHashMapWithExpectedSize(values.size());
        for (Entry<Cell, byte[]> cellEntry : values.entrySet()) {
            Cell expiringCell = Cell.create(
                    cellEntry.getKey().getRowName(),
                    cellEntry.getKey().getColumnName(),
                    ttlDuration, ttlUnit);
            expiringValues.put(expiringCell, cellEntry.getValue());
        }
        return expiringValues;
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
                            + "Enable debug logging for more information", newVal);
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
            Preconditions.checkState(state.get() == State.UNCOMMITTED, "Transaction must be uncommitted.");
            if (state.compareAndSet(State.UNCOMMITTED, State.ABORTED)) {
                if (hasWrites()) {
                    throwIfPreCommitRequirementsNotMet(null);
                }
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
            Preconditions.checkState(state.get() == State.UNCOMMITTED, "Transaction must be uncommitted.");
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
            state.set(success ? State.COMMITTED : State.FAILED);
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
            // if there are no writes, we must still make sure the immutable timestamp lock is still valid,
            // to ensure that sweep hasn't thoroughly deleted cells we tried to read
            // TODO(nziebart): we actually only need to do this if we've read from tables with THOROUGH sweep strategy
            throwIfImmutableTsOrCommitLocksExpired(null);
            return;
        }

        Timer.Context acquireLocksTimer = getTimer("commitAcquireLocks").time();
        LockToken commitLocksToken = acquireLocksForCommit();
        long millisForLocks = TimeUnit.NANOSECONDS.toMillis(acquireLocksTimer.stop());
        try {
            Timer.Context conflictsTimer = getTimer("commitCheckingForConflicts").time();
            throwIfConflictOnCommit(commitLocksToken, transactionService);
            long millisCheckingForConflicts = TimeUnit.NANOSECONDS.toMillis(conflictsTimer.stop());
            Timer.Context writesTimer = getTimer("commitWrite").time();
            keyValueService.multiPut(writesByTable, getStartTimestamp());
            long millisForWrites = TimeUnit.NANOSECONDS.toMillis(writesTimer.stop());

            // Now that all writes are done, get the commit timestamp
            // We must do this before we check that our locks are still valid to ensure that
            // other transactions that will hold these locks are sure to have start
            // timestamps after our commit timestamp.
            long commitTimestamp = timelockService.getFreshTimestamp();
            commitTsForScrubbing = commitTimestamp;

            // punch on commit so that if hard delete is the only thing happening on a system,
            // we won't block forever waiting for the unreadable timestamp to advance past the
            // scrub timestamp (same as the hard delete transaction's start timestamp)
            Timer.Context punchTimer = getTimer("millisForPunch").time();
            cleaner.punch(commitTimestamp);
            long millisForPunch = TimeUnit.NANOSECONDS.toMillis(punchTimer.stop());

            throwIfReadWriteConflictForSerializable(commitTimestamp);

            // Verify that our locks are still valid before we actually commit;
            // this throwIfLocksExpired is required by the transaction protocol for correctness
            throwIfPreCommitRequirementsNotMet(commitLocksToken);

            Timer.Context commitTsTimer = getTimer("commitPutCommitTs").time();
            putCommitTimestamp(commitTimestamp, commitLocksToken, transactionService);
            long millisForCommitTs = TimeUnit.NANOSECONDS.toMillis(commitTsTimer.stop());

            Set<LockToken> expiredLocks = refreshCommitAndImmutableTsLocks(commitLocksToken);
            if (!expiredLocks.isEmpty()) {
                final String baseMsg = "This isn't a bug but it should happen very infrequently. "
                        + "Required locks are no longer valid but we have already committed successfully. ";
                String expiredLocksErrorString = getExpiredLocksErrorString(commitLocksToken, expiredLocks);
                log.error(baseMsg + "{}", expiredLocksErrorString,
                        new TransactionFailedRetriableException(baseMsg + expiredLocksErrorString));
            }
            long millisSinceCreation = System.currentTimeMillis() - timeCreated;
            getTimer("commitTotalTimeSinceTxCreation").update(millisSinceCreation, TimeUnit.MILLISECONDS);
            getHistogram(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_BYTES_WRITTEN).update(byteCount.get());
            if (perfLogger.isDebugEnabled()) {
                perfLogger.debug("Committed {} bytes with locks, start ts {}, commit ts {}, "
                        + "acquiring locks took {} ms, checking for conflicts took {} ms, "
                        + "writing took {} ms, punch took {} ms, putCommitTs took {} ms, "
                        + "total time since tx creation {} ms, tables: {}.",
                        byteCount.get(), getStartTimestamp(),
                        commitTimestamp, millisForLocks, millisCheckingForConflicts, millisForWrites,
                        millisForPunch, millisForCommitTs, millisSinceCreation, writesByTable.keySet());
            }
        } finally {
            timelockService.unlock(ImmutableSet.of(commitLocksToken));
        }
    }

    protected void throwIfReadWriteConflictForSerializable(long commitTimestamp) {
        // This is for overriding to get serializable transactions
    }

    private boolean hasWrites() {
        boolean hasWrites = false;
        for (SortedMap<?, ?> map : writesByTable.values()) {
            if (!map.isEmpty()) {
                hasWrites = true;
                break;
            }
        }
        return hasWrites;
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

    private void throwIfPreCommitRequirementsNotMet(@Nullable LockToken commitLocksToken) {
        throwIfImmutableTsOrCommitLocksExpired(commitLocksToken);
        advisoryLockCheck.throwIfLocksExpired();
    }

    private void throwIfImmutableTsOrCommitLocksExpired(@Nullable LockToken commitLocksToken) {
        Set<LockToken> expiredLocks = refreshCommitAndImmutableTsLocks(commitLocksToken);
        if (!expiredLocks.isEmpty()) {
            final String baseMsg = "Required locks are no longer valid. ";
            String expiredLocksErrorString = getExpiredLocksErrorString(commitLocksToken, expiredLocks);
            TransactionLockTimeoutException ex = new TransactionLockTimeoutException(baseMsg + expiredLocksErrorString);
            log.error(baseMsg + "{}", expiredLocksErrorString, ex);
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
        if (writes.isEmpty() || conflictHandler == ConflictHandler.IGNORE_ALL) {
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
        } else if (conflictHandler == ConflictHandler.RETRY_ON_WRITE_WRITE
                || conflictHandler == ConflictHandler.RETRY_ON_WRITE_WRITE_CELL
                || conflictHandler == ConflictHandler.SERIALIZABLE) {
            if (!spanningWrites.isEmpty() || !dominatingWrites.isEmpty()) {
                getTransactionConflictsMeter().mark();
                throw TransactionConflictException.create(tableRef, getStartTimestamp(), spanningWrites,
                        dominatingWrites, System.currentTimeMillis() - timeCreated);
            }
        } else {
            throw new IllegalArgumentException("Unknown conflictHandler type: " + conflictHandler);
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
                Validate.isTrue(false, "Missing write for cell: " + cellToConflict.get(cell)
                        + " for table " + table);
            }
            if (!conflictingValues.containsKey(cell)) {
                // This error case could happen if our locks expired.
                throwIfPreCommitRequirementsNotMet(commitLocksToken);
                Validate.isTrue(false, "Missing conflicting value for cell: " + cellToConflict.get(cell)
                        + " for table " + table);
            }
            if (conflictingValues.get(cell).getTimestamp() != (cellEntry.getValue() - 1)) {
                // This error case could happen if our locks expired.
                throwIfPreCommitRequirementsNotMet(commitLocksToken);
                Validate.isTrue(false, "Wrong timestamp for cell in table " + table
                        + " Expected: " + cellToConflict.get(cell)
                        + " Actual: " + conflictingValues.get(cell));
            }
            @Nullable byte[] oldVal = oldValues.get(cell);
            byte[] writeVal = writes.get(cell);
            byte[] conflictingVal = conflictingValues.get(cell).getContents();
            if (!Transactions.cellValuesEqual(oldVal, writeVal)
                    || !Arrays.equals(writeVal, conflictingVal)) {
                conflictingCells.add(cell);
            } else if (log.isInfoEnabled()) {
                log.info("Another transaction committed to the same cell before us but their value was the same."
                        + " Cell: {}"
                        + " Table: {}",
                        cell, table);
            }
        }
        if (conflictingCells.isEmpty()) {
            return;
        }
        Predicate<CellConflict> conflicting = Predicates.compose(
                Predicates.in(conflictingCells),
                CellConflict.getCellFunction());
        getTransactionConflictsMeter().mark();
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
                log.warn("Rolling back transaction: {}", startTs);
                if (!rollbackOtherTransaction(startTs, transactionService)) {
                    return false;
                }
            } else {
                Validate.isTrue(commitTimestamps.get(startTs) == TransactionConstants.FAILED_COMMIT_TS);
            }
        }

        try {
            log.debug("For table: {} we are deleting values of an uncommitted transaction: {}", tableRef, keysToDelete);
            keyValueService.delete(tableRef, Multimaps.forMap(keysToDelete));
        } catch (RuntimeException e) {
            final String msg = "This isn't a bug but it should be infrequent if all nodes of your KV service are"
                    + " running. Delete has stronger consistency semantics than read/write and must talk to all nodes"
                    + " instead of just talking to a quorum of nodes. "
                    + "Failed to delete keys for table: {} from an uncommitted transaction; "
                    + " sweep should eventually clean this when it processes this table.";
            if (log.isDebugEnabled()) {
                log.warn(msg + " The keys that failed to be deleted during rollback were {}", tableRef, keysToDelete);
            } else {
                log.warn(msg, tableRef, e);
            }
        }

        return true;
    }

    /**
     * Rollback a someone else's transaction.
     * @return true if the other transaction was rolled back
     */
    private boolean rollbackOtherTransaction(long startTs, TransactionService transactionService) {
        try {
            transactionService.putUnlessExists(startTs, TransactionConstants.FAILED_COMMIT_TS);
            return true;
        } catch (KeyAlreadyExistsException e) {
            String msg = "Two transactions tried to roll back someone else's request with start: " + startTs;
            log.error("This isn't a bug but it should be very infrequent. {}", msg,
                    new TransactionFailedRetriableException(msg, e));
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

        LockRequest request = LockRequest.of(lockDescriptors, lockAcquireTimeoutMs);
        LockResponse lockResponse = timelockService.lock(request);
        if (!lockResponse.wasSuccessful()) {
            log.error("Timed out waiting while acquiring commit locks. Request id was {}. Timeout was {} ms. "
                            + "First ten required locks were {}.",
                    SafeArg.of("requestId", request.getRequestId()),
                    SafeArg.of("acquireTimeoutMs", lockAcquireTimeoutMs),
                    UnsafeArg.of("firstTenLockDescriptors", Iterables.limit(lockDescriptors, 10)));
            throw new TransactionLockAcquisitionTimeoutException("Timed out while acquiring commit locks.");
        }
        return lockResponse.getToken();
    }

    protected Set<LockDescriptor> getLocksForWrites() {
        Set<LockDescriptor> result = Sets.newHashSet();
        Iterable<TableReference> allTables = IterableUtils.append(
                writesByTable.keySet(),
                TransactionConstants.TRANSACTION_TABLE);
        for (TableReference tableRef : allTables) {
            if (tableRef.equals(TransactionConstants.TRANSACTION_TABLE)) {
                result.add(
                        AtlasRowLockDescriptor.of(
                                TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                                TransactionConstants.getValueForTimestamp(getStartTimestamp())));
                continue;
            }
            ConflictHandler conflictHandler = getConflictHandlerForTable(tableRef);
            if (conflictHandler == ConflictHandler.RETRY_ON_WRITE_WRITE_CELL) {
                for (Cell cell : getLocalWrites(tableRef).keySet()) {
                    result.add(
                            AtlasCellLockDescriptor.of(
                                    tableRef.getQualifiedName(),
                                    cell.getRowName(),
                                    cell.getColumnName()));
                }
            } else if (conflictHandler != ConflictHandler.IGNORE_ALL) {
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
        return result;
    }

    /**
     * We will block here until the passed transactions have released their lock.  This means that
     * the committing transaction is either complete or it has failed and we are allowed to roll
     * it back.
     */
    private void waitForCommitToComplete(Iterable<Long> startTimestamps) {
        boolean isEmpty = true;
        Set<LockDescriptor> lockDescriptors = Sets.newHashSet();
        for (long start : startTimestamps) {
            if (start < immutableTimestamp) {
                // We don't need to block in this case because this transaction is already complete
                continue;
            }
            isEmpty = false;
            lockDescriptors.add(
                    AtlasRowLockDescriptor.of(
                            TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                            TransactionConstants.getValueForTimestamp(start)));
        }

        if (isEmpty) {
            return;
        }

        WaitForLocksRequest request = WaitForLocksRequest.of(lockDescriptors, lockAcquireTimeoutMs);
        WaitForLocksResponse response = timelockService.waitForLocks(request);
        if (!response.wasSuccessful()) {
            log.error("Timed out waiting for commits to complete. Timeout was {} ms. First ten locks were {}.",
                    SafeArg.of("requestId", request.getRequestId()),
                    SafeArg.of("acquireTimeoutMs", lockAcquireTimeoutMs),
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
            perfLogger.debug("Waited {} ms to get commit timestamps for table {}.",
                    waitForCommitTsMillis, tableRef);
        }

        log.trace("Getting commit timestamps for {} start timestamps in response to read from table {}",
                gets.size(), tableRef);
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
                throw new TransactionLockTimeoutException("Our commit was already rolled back at commit time"
                        + " because our locks timed out. startTs: " + getStartTimestamp() + ".  "
                        + getExpiredLocksErrorString(commitLocksToken, expiredLocks), ex);
            } else {
                AssertUtils.assertAndLog(log, false, "BUG: Someone tried to roll back our transaction but"
                        + " our locks were still valid; this is not allowed."
                        + " Held immutable timestamp lock: " + immutableTimestampLock
                        + "; held commit locks: " + commitLocksToken);
            }
        } catch (TransactionFailedException e1) {
            throw e1;
        } catch (Exception e1) {
            log.error("Failed to determine if we can retry this transaction. startTs: {}", getStartTimestamp(), e1);
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
            Validate.isTrue(false, "Commit value is wrong. startTs " + getStartTimestamp() + "  commitTs: " + commitTs);
        }
        return storedCommit == commitTs;
    }

    @Override
    public void useTable(TableReference tableRef, ConstraintCheckable table) {
        constraintsByTableName.put(tableRef, table);
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

    private Meter getTransactionConflictsMeter() {
        return getMeter("SnapshotTransactionConflict");
    }

    private static Meter getMeter(String name) {
        // TODO(hsaraogi): add table names as a tag
        return metricsManager.registerOrGetMeter(SnapshotTransaction.class, name);
    }

}


