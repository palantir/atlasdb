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

import static java.util.stream.Collectors.toList;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.AtlasDbPerformanceConstants;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.AsyncKeyValueService;
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
import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCache;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.impl.RowResults;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy;
import com.palantir.atlasdb.table.description.exceptions.AtlasDbConstraintException;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.atlasdb.transaction.api.ConstraintCheckingTransaction;
import com.palantir.atlasdb.transaction.api.GetRangesQuery;
import com.palantir.atlasdb.transaction.api.ImmutableGetRangesQuery;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionCommitFailedException;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionConflictException.CellConflict;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionLockAcquisitionTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.metrics.TableLevelMetricsController;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetrics;
import com.palantir.atlasdb.transaction.service.AsyncTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.annotation.Idempotent;
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
import com.palantir.common.streams.KeyedStream;
import com.palantir.common.streams.MoreStreams;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.ImmutableLockRequest;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.tracing.CloseableTracer;
import com.palantir.util.AssertUtils;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implements snapshot isolation for transactions.
 * <p>
 * This object is thread safe and you may do reads and writes from multiple threads. You may not continue reading or
 * writing after {@link #commit()} or {@link #abort()} is called.
 * <p>
 * Things to keep in mind when dealing with snapshot transactions: 1. Transactions that do writes should be short lived.
 * 1a. Read only transactions can be long lived (within reason). 2. Do not write too much data in one transaction (this
 * relates back to #1) 3. A row should be able to fit in memory without any trouble.  This includes all columns of the
 * row.  If you are thinking about making your row bigger than like 10MB, you should think about breaking these up into
 * different rows and using range scans.
 */
public class SnapshotTransaction extends AbstractTransaction implements ConstraintCheckingTransaction {
    private static final Logger log = LoggerFactory.getLogger(SnapshotTransaction.class);
    private static final Logger perfLogger = LoggerFactory.getLogger("dualschema.perf");
    private static final Logger constraintLogger = LoggerFactory.getLogger("dualschema.constraints");

    private static final int BATCH_SIZE_GET_FIRST_PAGE = 1000;

    @VisibleForTesting
    static final int MIN_BATCH_SIZE_FOR_DISTRIBUTED_LOAD = 100;

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
    protected final LockWatchManagerInternal lockWatchManager;
    final KeyValueService keyValueService;
    final AsyncKeyValueService immediateKeyValueService;
    final TransactionService defaultTransactionService;
    private final AsyncTransactionService immediateTransactionService;
    private final Cleaner cleaner;
    private final Supplier<Long> startTimestamp;
    protected final MetricsManager metricsManager;
    protected final ConflictTracer conflictTracer;

    protected final MultiTableSweepQueueWriter sweepQueue;

    protected final long immutableTimestamp;
    protected final Optional<LockToken> immutableTimestampLock;
    private final PreCommitCondition preCommitCondition;
    protected final long timeCreated = System.currentTimeMillis();

    protected final ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> writesByTable =
            new ConcurrentHashMap<>();
    protected final TransactionConflictDetectionManager conflictDetectionManager;
    private final AtomicLong byteCount = new AtomicLong();

    private final AtlasDbConstraintCheckingMode constraintCheckingMode;

    private final ConcurrentMap<TableReference, ConstraintCheckable> constraintsByTableName = new ConcurrentHashMap<>();

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
    private final Set<TableReference> involvedTables = ConcurrentHashMap.newKeySet();
    protected final ExecutorService deleteExecutor;
    private final Timer.Context transactionTimerContext;
    protected final TransactionOutcomeMetrics transactionOutcomeMetrics;
    protected final boolean validateLocksOnReads;
    protected final Supplier<TransactionConfig> transactionConfig;
    protected final TableLevelMetricsController tableLevelMetricsController;
    protected final SuccessCallbackManager successCallbackManager = new SuccessCallbackManager();

    protected volatile boolean hasReads;

    /**
     * @param immutableTimestamp If we find a row written before the immutableTimestamp we don't need to grab a read
     *                           lock for it because we know that no writers exist.
     * @param preCommitCondition This check must pass for this transaction to commit.
     */
    /* package */ SnapshotTransaction(
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
            Supplier<TransactionConfig> transactionConfig,
            ConflictTracer conflictTracer,
            TableLevelMetricsController tableLevelMetricsController) {
        this.metricsManager = metricsManager;
        this.lockWatchManager = lockWatchManager;
        this.conflictTracer = conflictTracer;
        this.transactionTimerContext = getTimer("transactionMillis").time();
        this.keyValueService = keyValueService;
        this.immediateKeyValueService = KeyValueServices.synchronousAsAsyncKeyValueService(keyValueService);
        this.timelockService = timelockService;
        this.defaultTransactionService = transactionService;
        this.immediateTransactionService = TransactionServices.synchronousAsAsyncTransactionService(transactionService);
        this.cleaner = cleaner;
        this.startTimestamp = startTimeStamp;
        this.conflictDetectionManager = new TransactionConflictDetectionManager(conflictDetectionManager);
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
        this.tableLevelMetricsController = tableLevelMetricsController;
    }

    protected TransactionScopedCache getCache() {
        return lockWatchManager.getOrCreateTransactionScopedCache(getTimestamp());
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
    public void disableReadWriteConflictChecking(TableReference tableRef) {
        conflictDetectionManager.disableReadWriteConflict(tableRef);
    }

    @Override
    public void markTableInvolved(TableReference tableRef) {
        // Not setting hasReads on purpose.
        checkGetPreconditions(tableRef);
    }

    @Override
    public NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection) {
        Timer.Context timer = getTimer("getRows").time();
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return AbstractTransaction.EMPTY_SORTED_ROWS;
        }
        hasReads = true;
        ImmutableSortedMap.Builder<Cell, byte[]> result = ImmutableSortedMap.naturalOrder();
        Map<Cell, Value> rawResults =
                new HashMap<>(keyValueService.getRows(tableRef, rows, columnSelection, getStartTimestamp()));
        NavigableMap<Cell, byte[]> writes = writesByTable.get(tableRef);
        if (writes != null) {
            for (byte[] row : rows) {
                extractLocalWritesForRow(result, writes, row, columnSelection);
            }
        }

        // We don't need to do work postFiltering if we have a write locally.
        rawResults.keySet().removeAll(result.build().keySet());

        NavigableMap<byte[], RowResult<byte[]>> results = filterRowResults(tableRef, rawResults, result);
        long getRowsMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());
        if (perfLogger.isDebugEnabled()) {
            perfLogger.debug(
                    "getRows({}, {} rows) found {} rows, took {} ms",
                    tableRef,
                    Iterables.size(rows),
                    results.size(),
                    getRowsMillis);
        }
        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
        return results;
    }

    @Override
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        return KeyedStream.stream(getRowsColumnRangeIterator(tableRef, rows, columnRangeSelection))
                .map((row, iterator) -> BatchingVisitableFromIterable.create(iterator))
                .collectTo(() -> new TreeMap<>(UnsignedBytes.lexicographicalComparator()));
    }

    @Override
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return Collections.emptyIterator();
        }
        Iterable<byte[]> stableRows = ImmutableList.copyOf(rows);
        hasReads = true;
        RowColumnRangeIterator rawResults = keyValueService.getRowsColumnRange(
                tableRef, stableRows, columnRangeSelection, batchHint, getStartTimestamp());
        if (!rawResults.hasNext()) {
            validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
        } // else the postFiltered iterator will check for each batch.

        BatchColumnRangeSelection batchColumnRangeSelection =
                BatchColumnRangeSelection.create(columnRangeSelection, batchHint);
        return getPostFilteredColumns(tableRef, batchColumnRangeSelection, rawResults);
    }

    @Override
    public Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> getRowsColumnRangeIterator(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return ImmutableMap.of();
        }
        hasReads = true;
        Map<byte[], RowColumnRangeIterator> rawResults =
                keyValueService.getRowsColumnRange(tableRef, rows, columnRangeSelection, getStartTimestamp());
        ImmutableSortedMap.Builder<byte[], Iterator<Map.Entry<Cell, byte[]>>> postFilteredResultsBuilder =
                ImmutableSortedMap.orderedBy(PtBytes.BYTES_COMPARATOR);
        for (Map.Entry<byte[], RowColumnRangeIterator> e : rawResults.entrySet()) {
            byte[] row = e.getKey();
            RowColumnRangeIterator rawIterator = e.getValue();
            Iterator<Map.Entry<Cell, byte[]>> postFilteredIterator =
                    getPostFilteredColumns(tableRef, columnRangeSelection, row, rawIterator);
            postFilteredResultsBuilder.put(row, postFilteredIterator);
        }
        SortedMap<byte[], Iterator<Map.Entry<Cell, byte[]>>> postFilteredResults = postFilteredResultsBuilder.build();
        // validate requirements here as the first batch for each of the above iterators will not check
        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
        return postFilteredResults;
    }

    @Override
    public Iterator<Map.Entry<Cell, byte[]>> getSortedColumns(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection batchColumnRangeSelection) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return Collections.emptyIterator();
        }
        Iterable<byte[]> distinctRows = getDistinctRows(rows);

        hasReads = true;
        int batchSize = getPerRowBatchSize(batchColumnRangeSelection, Iterables.size(distinctRows));
        BatchColumnRangeSelection perBatchSelection = BatchColumnRangeSelection.create(
                batchColumnRangeSelection.getStartCol(), batchColumnRangeSelection.getEndCol(), batchSize);

        Map<byte[], RowColumnRangeIterator> rawResults =
                keyValueService.getRowsColumnRange(tableRef, distinctRows, perBatchSelection, getStartTimestamp());

        return getPostFilteredSortedColumns(tableRef, batchColumnRangeSelection, distinctRows, rawResults);
    }

    private Iterator<Map.Entry<Cell, byte[]>> getPostFilteredSortedColumns(
            TableReference tableRef,
            BatchColumnRangeSelection batchColumnRangeSelection,
            Iterable<byte[]> distinctRows,
            Map<byte[], RowColumnRangeIterator> rawResults) {
        Comparator<Cell> cellComparator = columnOrderThenPreserveInputRowOrder(distinctRows);

        Iterator<Map.Entry<Cell, Value>> postFilterIterator = getRowColumnRangePostFilteredWithoutSorting(
                tableRef,
                mergeByComparator(rawResults.values(), cellComparator),
                batchColumnRangeSelection.getBatchHint(),
                cellComparator);
        Iterator<Map.Entry<Cell, byte[]>> remoteWrites = Iterators.transform(
                postFilterIterator,
                entry -> Maps.immutableEntry(entry.getKey(), entry.getValue().getContents()));
        Iterator<Map.Entry<Cell, byte[]>> localWrites =
                getSortedColumnsLocalWrites(tableRef, distinctRows, batchColumnRangeSelection, cellComparator);
        Iterator<Map.Entry<Cell, byte[]>> merged = mergeLocalAndRemoteWrites(localWrites, remoteWrites, cellComparator);

        return filterDeletedValues(merged, tableRef);
    }

    private static Iterator<Map.Entry<Cell, byte[]>> mergeLocalAndRemoteWrites(
            Iterator<Map.Entry<Cell, byte[]>> localWrites,
            Iterator<Map.Entry<Cell, byte[]>> remoteWrites,
            Comparator<Cell> cellComparator) {
        // always override remote values with locally written values
        return IteratorUtils.mergeIterators(
                localWrites,
                remoteWrites,
                Comparator.comparing(Map.Entry::getKey, cellComparator),
                com.palantir.util.Pair::getLhSide);
    }

    private Iterator<Map.Entry<Cell, byte[]>> getSortedColumnsLocalWrites(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection columns,
            Comparator<Cell> cellComparator) {
        return mergeByComparator(
                Iterables.transform(rows, row -> getLocalWritesForColumnRange(tableRef, columns, row)
                        .entrySet()
                        .iterator()),
                cellComparator);
    }

    /**
     * Provides comparator to sort cells by columns (sorted lexicographically on byte ordering) and then in the order
     * of input rows.
     * */
    @VisibleForTesting
    static Comparator<Cell> columnOrderThenPreserveInputRowOrder(Iterable<byte[]> rows) {
        return Cell.COLUMN_COMPARATOR.thenComparing(
                (Cell cell) -> ByteBuffer.wrap(cell.getRowName()),
                Ordering.explicit(Streams.stream(rows)
                        .map(ByteBuffer::wrap)
                        .distinct()
                        .collect(ImmutableList.toImmutableList())));
    }

    protected static <V> Iterator<Map.Entry<Cell, V>> mergeByComparator(
            Iterable<? extends Iterator<Map.Entry<Cell, V>>> iterators, Comparator<Cell> cellComparator) {
        Comparator<Map.Entry<Cell, V>> comp = Comparator.comparing(Map.Entry::getKey, cellComparator);
        return Iterators.mergeSorted(iterators, comp);
    }

    /**
     * If the batch hint is small, ask for at least that many from each of the input rows to avoid the
     * possibility of needing a second batch of fetching.
     * If the batch hint is large, split batch size across rows to avoid loading too much data, while accepting that
     * second fetches may be needed to get everyone their data.
     * */
    private static int getPerRowBatchSize(BatchColumnRangeSelection columnRangeSelection, int distinctRowCount) {
        return Math.max(
                Math.min(MIN_BATCH_SIZE_FOR_DISTRIBUTED_LOAD, columnRangeSelection.getBatchHint()),
                columnRangeSelection.getBatchHint() / distinctRowCount);
    }

    protected List<byte[]> getDistinctRows(Iterable<byte[]> inputRows) {
        return Streams.stream(inputRows)
                .map(ByteBuffer::wrap)
                .distinct()
                .map(ByteBuffer::array)
                .collect(toList());
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
                mergeLocalAndRemoteWrites(localIterator, postFilterIterator, Cell.COLUMN_COMPARATOR);
        return filterDeletedValues(mergedIterator, tableRef);
    }

    private Iterator<Map.Entry<Cell, byte[]>> getPostFilteredColumns(
            TableReference tableRef,
            BatchColumnRangeSelection batchColumnRangeSelection,
            RowColumnRangeIterator rawIterator) {
        Iterator<Map.Entry<Cell, Value>> postFilterIterator =
                getRowColumnRangePostFiltered(tableRef, rawIterator, batchColumnRangeSelection.getBatchHint());
        Iterator<Map.Entry<byte[], RowColumnRangeIterator>> rawResultsByRow = partitionByRow(postFilterIterator);
        Iterator<Map.Entry<Cell, byte[]>> merged = Iterators.concat(Iterators.transform(rawResultsByRow, row -> {
            SortedMap<Cell, byte[]> localWrites =
                    getLocalWritesForColumnRange(tableRef, batchColumnRangeSelection, row.getKey());
            Iterator<Map.Entry<Cell, byte[]>> remoteIterator = Iterators.transform(
                    row.getValue(),
                    entry ->
                            Maps.immutableEntry(entry.getKey(), entry.getValue().getContents()));
            Iterator<Map.Entry<Cell, byte[]>> localIterator =
                    localWrites.entrySet().iterator();
            return mergeLocalAndRemoteWrites(localIterator, remoteIterator, Cell.COLUMN_COMPARATOR);
        }));

        return filterDeletedValues(merged, tableRef);
    }

    private Iterator<Map.Entry<Cell, byte[]>> filterDeletedValues(
            Iterator<Map.Entry<Cell, byte[]>> unfiltered, TableReference tableReference) {
        Counter emptyValueCounter = getCounter(AtlasDbMetricNames.CellFilterMetrics.EMPTY_VALUE, tableReference);
        return Iterators.filter(unfiltered, entry -> {
            if (entry.getValue().length == 0) {
                emptyValueCounter.inc();
                return false;
            }
            return true;
        });
    }

    private Iterator<Map.Entry<Cell, Value>> getRowColumnRangePostFiltered(
            TableReference tableRef, RowColumnRangeIterator iterator, int batchHint) {
        return Iterators.concat(Iterators.transform(Iterators.partition(iterator, batchHint), batch -> {
            Map<Cell, Value> raw = validateBatch(tableRef, batch);
            if (raw.isEmpty()) {
                return Collections.emptyIterator();
            }
            SortedMap<Cell, Value> postFiltered = ImmutableSortedMap.copyOf(
                    getWithPostFilteringSync(tableRef, raw, x -> x), preserveInputRowOrder(batch));
            return postFiltered.entrySet().iterator();
        }));
    }

    private Iterator<Map.Entry<Cell, byte[]>> getRowColumnRangePostFiltered(
            TableReference tableRef,
            byte[] row,
            BatchColumnRangeSelection columnRangeSelection,
            RowColumnRangeIterator rawIterator) {
        ColumnRangeBatchProvider batchProvider =
                new ColumnRangeBatchProvider(keyValueService, tableRef, row, columnRangeSelection, getStartTimestamp());
        return GetRowsColumnRangeIterator.iterator(
                batchProvider,
                rawIterator,
                columnRangeSelection,
                () -> validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp()),
                raw -> getWithPostFilteringSync(tableRef, raw, Value.GET_VALUE));
    }

    private Iterator<Map.Entry<Cell, Value>> getRowColumnRangePostFilteredWithoutSorting(
            TableReference tableRef,
            Iterator<Map.Entry<Cell, Value>> iterator,
            int batchHint,
            Comparator<Cell> cellComparator) {
        return Iterators.concat(Iterators.transform(Iterators.partition(iterator, batchHint), batch -> {
            Map<Cell, Value> raw = validateBatch(tableRef, batch);
            if (raw.isEmpty()) {
                return Collections.emptyIterator();
            }

            SortedMap<Cell, Value> postFiltered =
                    ImmutableSortedMap.copyOf(getWithPostFilteringSync(tableRef, raw, x -> x), cellComparator);
            return postFiltered.entrySet().iterator();
        }));
    }

    private Map<Cell, Value> validateBatch(TableReference tableRef, List<Map.Entry<Cell, Value>> batch) {
        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
        return ImmutableMap.copyOf(batch);
    }

    private Comparator<Cell> preserveInputRowOrder(List<Map.Entry<Cell, Value>> inputEntries) {
        // N.B. This batch could be spread across multiple rows, and those rows might extend into other
        // batches. We are given cells for a row grouped together, so easiest way to ensure they stay together
        // is to preserve the original row order.
        return Comparator.comparing(
                        (Cell cell) -> ByteBuffer.wrap(cell.getRowName()),
                        Ordering.explicit(inputEntries.stream()
                                .map(Map.Entry::getKey)
                                .map(Cell::getRowName)
                                .map(ByteBuffer::wrap)
                                .distinct()
                                .collect(ImmutableList.toImmutableList())))
                .thenComparing(Cell::getColumnName, PtBytes.BYTES_COMPARATOR);
    }

    /**
     * Partitions a {@link RowColumnRangeIterator} into contiguous blocks that share the same row name. {@link
     * KeyValueService#getRowsColumnRange(TableReference, Iterable, ColumnRangeSelection, int, long)} guarantees that
     * all columns for a single row are adjacent, so this method will return an {@link Iterator} with exactly one entry
     * per non-empty row.
     */
    private Iterator<Map.Entry<byte[], RowColumnRangeIterator>> partitionByRow(
            Iterator<Map.Entry<Cell, Value>> rawResults) {
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
                Iterator<Map.Entry<Cell, Value>> columnsIterator = new AbstractIterator<Map.Entry<Cell, Value>>() {
                    @Override
                    protected Map.Entry<Cell, Value> computeNext() {
                        if (!peekableRawResults.hasNext()
                                || !Arrays.equals(
                                        peekableRawResults.peek().getKey().getRowName(), nextRowName)) {
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
                    log.warn(
                            "Not all columns for row {} were read. {} columns were discarded.",
                            UnsafeArg.of("row", Arrays.toString(prevRowName)),
                            SafeArg.of("numColumnsDiscarded", numConsumed));
                }
            }
        };
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRowsIgnoringLocalWrites(
            TableReference tableRef, Iterable<byte[]> rows) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return AbstractTransaction.EMPTY_SORTED_ROWS;
        }
        hasReads = true;

        Map<Cell, Value> rawResults =
                new HashMap<>(keyValueService.getRows(tableRef, rows, ColumnSelection.all(), getStartTimestamp()));

        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
        return filterRowResults(tableRef, rawResults, ImmutableMap.builderWithExpectedSize(rawResults.size()));
    }

    private NavigableMap<byte[], RowResult<byte[]>> filterRowResults(
            TableReference tableRef, Map<Cell, Value> rawResults, ImmutableMap.Builder<Cell, byte[]> resultCollector) {
        ImmutableMap<Cell, byte[]> collected = resultCollector
                .putAll(getWithPostFilteringSync(tableRef, rawResults, Value.GET_VALUE))
                .build();
        Map<Cell, byte[]> filterDeletedValues = removeEmptyColumns(collected, tableRef);
        return RowResults.viewOfSortedMap(Cells.breakCellsUpByRow(filterDeletedValues));
    }

    private Map<Cell, byte[]> removeEmptyColumns(Map<Cell, byte[]> unfiltered, TableReference tableReference) {
        Map<Cell, byte[]> filtered = Maps.filterValues(unfiltered, Predicates.not(Value::isTombstone));
        getCounter(AtlasDbMetricNames.CellFilterMetrics.EMPTY_VALUE, tableReference)
                .inc(unfiltered.size() - filtered.size());
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
        Iterator<Map.Entry<Cell, byte[]>> it =
                writes.tailMap(lowCell).entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Cell, byte[]> entry = it.next();
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
    @Idempotent
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        return getCache()
                .get(
                        tableRef,
                        cells,
                        (table, uncached) -> getInternal(
                                "get", tableRef, uncached, immediateKeyValueService, immediateTransactionService));
    }

    @Override
    @Idempotent
    public ListenableFuture<Map<Cell, byte[]>> getAsync(TableReference tableRef, Set<Cell> cells) {
        return getCache()
                .getAsync(
                        tableRef,
                        cells,
                        (table, uncached) -> getInternal(
                                "getAsync", tableRef, uncached, keyValueService, defaultTransactionService));
    }

    private ListenableFuture<Map<Cell, byte[]>> getInternal(
            String operationName,
            TableReference tableRef,
            Set<Cell> cells,
            AsyncKeyValueService asyncKeyValueService,
            AsyncTransactionService asyncTransactionService) {
        Timer.Context timer = getTimer(operationName).time();
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(cells)) {
            return Futures.immediateFuture(ImmutableMap.of());
        }
        hasReads = true;

        Map<Cell, byte[]> result = new HashMap<>();
        SortedMap<Cell, byte[]> writes = writesByTable.get(tableRef);
        if (writes != null) {
            for (Cell cell : cells) {
                if (writes.containsKey(cell)) {
                    result.put(cell, writes.get(cell));
                }
            }
        }

        // We don't need to read any cells that were written locally.
        return Futures.transform(
                getFromKeyValueService(
                        tableRef,
                        Sets.difference(cells, result.keySet()),
                        asyncKeyValueService,
                        asyncTransactionService),
                fromKeyValueService -> {
                    result.putAll(fromKeyValueService);

                    long getMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());
                    if (perfLogger.isDebugEnabled()) {
                        perfLogger.debug(
                                "Snapshot transaction get cells (some possibly deleted)",
                                LoggingArgs.tableRef(tableRef),
                                SafeArg.of("numberOfCells", cells.size()),
                                SafeArg.of("numberOfCellsRetrieved", result.size()),
                                SafeArg.of("getOperation", operationName),
                                SafeArg.of("durationMillis", getMillis));
                    }
                    validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
                    return removeEmptyColumns(result, tableRef);
                },
                MoreExecutors.directExecutor());
    }

    @Override
    public Map<Cell, byte[]> getIgnoringLocalWrites(TableReference tableRef, Set<Cell> cells) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(cells)) {
            return ImmutableMap.of();
        }
        hasReads = true;

        ListenableFuture<Map<Cell, byte[]>> result =
                getFromKeyValueService(tableRef, cells, immediateKeyValueService, immediateTransactionService);
        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());

        return Maps.filterValues(Futures.getUnchecked(result), Predicates.not(Value::isTombstone));
    }

    /**
     * This will load the given keys from the underlying key value service and apply postFiltering so we have snapshot
     * isolation.  If the value in the key value service is the empty array this will be included here and needs to be
     * filtered out.
     */
    private ListenableFuture<Map<Cell, byte[]>> getFromKeyValueService(
            TableReference tableRef,
            Set<Cell> cells,
            AsyncKeyValueService asyncKeyValueService,
            AsyncTransactionService asyncTransactionService) {
        Map<Cell, Long> toRead = Cells.constantValueMap(cells, getStartTimestamp());
        ListenableFuture<Collection<Map.Entry<Cell, byte[]>>> postFilteredResults = Futures.transformAsync(
                asyncKeyValueService.getAsync(tableRef, toRead),
                rawResults -> getWithPostFilteringAsync(
                        tableRef, rawResults, Value.GET_VALUE, asyncKeyValueService, asyncTransactionService),
                MoreExecutors.directExecutor());

        return Futures.transform(postFilteredResults, ImmutableMap::copyOf, MoreExecutors.directExecutor());
    }

    private static byte[] getNextStartRowName(
            RangeRequest range, TokenBackedBasicResultsPage<RowResult<Value>, byte[]> prePostFilter) {
        if (!prePostFilter.moreResultsAvailable()) {
            return range.getEndExclusive();
        }
        return prePostFilter.getTokenForNextPage();
    }

    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(
            final TableReference tableRef, Iterable<RangeRequest> rangeRequests) {
        checkGetPreconditions(tableRef);

        if (perfLogger.isDebugEnabled()) {
            perfLogger.debug(
                    "Passed {} ranges to getRanges({}, {})", Iterables.size(rangeRequests), tableRef, rangeRequests);
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

                    SortedMap<Cell, byte[]> postFiltered = postFilterPages(tableRef, firstPages.values());

                    List<BatchingVisitable<RowResult<byte[]>>> ret = new ArrayList<>(input.size());
                    for (RangeRequest rangeRequest : input) {
                        TokenBackedBasicResultsPage<RowResult<Value>, byte[]> prePostFilter =
                                firstPages.get(rangeRequest);
                        byte[] nextStartRowName = getNextStartRowName(rangeRequest, prePostFilter);
                        List<Map.Entry<Cell, byte[]>> mergeIterators = getPostFilteredWithLocalWrites(
                                tableRef, postFiltered, rangeRequest, prePostFilter.getResults(), nextStartRowName);
                        ret.add(new AbstractBatchingVisitable<RowResult<byte[]>>() {
                            @Override
                            protected <K extends Exception> void batchAcceptSizeHint(
                                    int batchSizeHint, ConsistentVisitor<RowResult<byte[]>, K> visitor) throws K {
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
                                RangeRequest newRange = rangeRequest
                                        .getBuilder()
                                        .startRowInclusive(nextStartRowName)
                                        .build();
                                getRange(tableRef, newRange).batchAccept(batchSizeHint, visitor);
                            }
                        });
                    }
                    long processedRangeMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());
                    log.trace(
                            "Processed {} range requests for {} in {}ms",
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
        return getRanges(ImmutableGetRangesQuery.<T>builder()
                .tableRef(tableRef)
                .rangeRequests(rangeRequests)
                .concurrencyLevel(concurrencyLevel)
                .visitableProcessor(visitableProcessor)
                .build());
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

    @Override
    public <T> Stream<T> getRanges(GetRangesQuery<T> query) {
        if (!Iterables.isEmpty(query.rangeRequests())) {
            hasReads = true;
        }
        Stream<Pair<RangeRequest, BatchingVisitable<RowResult<byte[]>>>> requestAndVisitables = StreamSupport.stream(
                        query.rangeRequests().spliterator(), false)
                .map(rangeRequest -> Pair.of(
                        rangeRequest,
                        getLazyRange(
                                query.tableRef(), query.rangeRequestOptimizer().apply(rangeRequest))));

        BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, T> processor = query.visitableProcessor();
        int concurrencyLevel = query.concurrencyLevel().orElse(defaultGetRangesConcurrency);

        if (concurrencyLevel == 1 || isSingleton(query.rangeRequests())) {
            return requestAndVisitables.map(pair -> processor.apply(pair.getLeft(), pair.getRight()));
        }

        return MoreStreams.blockingStreamWithParallelism(
                requestAndVisitables,
                pair -> processor.apply(pair.getLeft(), pair.getRight()),
                getRangesExecutor,
                concurrencyLevel);
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

    private boolean requiresImmutableTimestampLocking(TableReference tableRef) {
        return sweepStrategyManager.get(tableRef).mustCheckImmutableLockAfterReads()
                || transactionConfig.get().lockImmutableTsOnReadOnlyTransactions();
    }

    private List<Map.Entry<Cell, byte[]>> getPostFilteredWithLocalWrites(
            final TableReference tableRef,
            final SortedMap<Cell, byte[]> postFiltered,
            final RangeRequest rangeRequest,
            List<RowResult<Value>> prePostFilter,
            final byte[] endRowExclusive) {
        Map<Cell, Value> prePostFilterCells = Cells.convertRowResultsToCells(prePostFilter);
        Collection<Map.Entry<Cell, byte[]>> postFilteredCells = Collections2.filter(
                postFiltered.entrySet(),
                Predicates.compose(Predicates.in(prePostFilterCells.keySet()), MapEntries.getKeyFunction()));
        Collection<Map.Entry<Cell, byte[]>> localWritesInRange = getLocalWritesForRange(
                        tableRef, rangeRequest.getStartInclusive(), endRowExclusive)
                .entrySet();
        return mergeInLocalWrites(
                tableRef, postFilteredCells.iterator(), localWritesInRange.iterator(), rangeRequest.isReverse());
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(final TableReference tableRef, final RangeRequest range) {
        checkGetPreconditions(tableRef);
        if (range.isEmptyRange()) {
            return BatchingVisitables.emptyBatchingVisitable();
        }
        hasReads = true;

        return new AbstractBatchingVisitable<RowResult<byte[]>>() {
            @Override
            public <K extends Exception> void batchAcceptSizeHint(
                    int userRequestedSize, ConsistentVisitor<RowResult<byte[]>, K> visitor) throws K {
                ensureUncommitted();

                int requestSize = range.getBatchHint() != null ? range.getBatchHint() : userRequestedSize;
                int preFilterBatchSize = getRequestHintToKvStore(requestSize);

                Preconditions.checkArgument(!range.isReverse(), "we currently do not support reverse ranges");
                getBatchingVisitableFromIterator(tableRef, range, requestSize, visitor, preFilterBatchSize);
            }
        };
    }

    private <K extends Exception> boolean getBatchingVisitableFromIterator(
            TableReference tableRef,
            RangeRequest range,
            int userRequestedSize,
            AbortingVisitor<List<RowResult<byte[]>>, K> visitor,
            int preFilterBatchSize)
            throws K {
        ClosableIterator<RowResult<byte[]>> postFilterIterator =
                postFilterIterator(tableRef, range, preFilterBatchSize, Value.GET_VALUE);
        try {
            Iterator<RowResult<byte[]>> localWritesInRange = Cells.createRowView(
                    getLocalWritesForRange(tableRef, range.getStartInclusive(), range.getEndExclusive())
                            .entrySet());
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
        if (preFilterBatchSize > AtlasDbPerformanceConstants.MAX_BATCH_SIZE || preFilterBatchSize < 0) {
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
                postFilterIterator,
                localWritesInRange,
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
            getCounter(AtlasDbMetricNames.CellFilterMetrics.EMPTY_VALUE, tableReference)
                    .inc(unfilteredRow.getColumns().size() - filteredColumns.size());
            return RowResult.create(unfilteredRow.getRowName(), filteredColumns);
        });
    }

    private List<Map.Entry<Cell, byte[]>> mergeInLocalWrites(
            TableReference tableRef,
            Iterator<Map.Entry<Cell, byte[]>> postFilterIterator,
            Iterator<Map.Entry<Cell, byte[]>> localWritesInRange,
            boolean isReverse) {
        Comparator<Cell> cellComparator = isReverse ? Comparator.reverseOrder() : Comparator.naturalOrder();
        Iterator<Map.Entry<Cell, byte[]>> mergeIterators =
                mergeLocalAndRemoteWrites(localWritesInRange, postFilterIterator, cellComparator);
        return postFilterEmptyValues(tableRef, mergeIterators);
    }

    private List<Map.Entry<Cell, byte[]>> postFilterEmptyValues(
            TableReference tableRef, Iterator<Map.Entry<Cell, byte[]>> mergeIterators) {
        List<Map.Entry<Cell, byte[]>> mergedWritesWithoutEmptyValues = new ArrayList<>();
        Predicate<Map.Entry<Cell, byte[]>> nonEmptyValuePredicate =
                Predicates.compose(Predicates.not(Value::isTombstone), MapEntries.getValueFunction());
        long numEmptyValues = 0;
        while (mergeIterators.hasNext()) {
            Map.Entry<Cell, byte[]> next = mergeIterators.next();
            if (nonEmptyValuePredicate.apply(next)) {
                mergedWritesWithoutEmptyValues.add(next);
            } else {
                numEmptyValues++;
            }
        }
        getCounter(AtlasDbMetricNames.CellFilterMetrics.EMPTY_VALUE, tableRef).inc(numEmptyValues);
        return mergedWritesWithoutEmptyValues;
    }

    protected <T> ClosableIterator<RowResult<T>> postFilterIterator(
            TableReference tableRef, RangeRequest range, int preFilterBatchSize, Function<Value, T> transformer) {
        RowRangeBatchProvider batchProvider =
                new RowRangeBatchProvider(keyValueService, tableRef, range, getStartTimestamp());
        BatchSizeIncreasingIterator<RowResult<Value>> results =
                new BatchSizeIncreasingIterator<>(batchProvider, preFilterBatchSize, null);
        Iterator<Iterator<RowResult<T>>> batchedPostFiltered = new AbstractIterator<Iterator<RowResult<T>>>() {
            @Override
            protected Iterator<RowResult<T>> computeNext() {
                List<RowResult<Value>> batch = results.getBatch().batch();
                validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
                if (batch.isEmpty()) {
                    return endOfData();
                }
                SortedMap<Cell, T> postFilter = postFilterRows(tableRef, batch, transformer);
                results.markNumResultsNotDeleted(
                        Cells.getRows(postFilter.keySet()).size());
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
            TableReference tableRef, BatchColumnRangeSelection columnRangeSelection, byte[] row) {
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

    private SortedMap<Cell, byte[]> postFilterPages(
            TableReference tableRef, Iterable<TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> rangeRows) {
        List<RowResult<Value>> results = new ArrayList<>();
        for (TokenBackedBasicResultsPage<RowResult<Value>, byte[]> page : rangeRows) {
            results.addAll(page.getResults());
        }
        return postFilterRows(tableRef, results, Value.GET_VALUE);
    }

    private <T> SortedMap<Cell, T> postFilterRows(
            TableReference tableRef, List<RowResult<Value>> rangeRows, Function<Value, T> transformer) {
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

        return ImmutableSortedMap.copyOf(getWithPostFilteringSync(tableRef, rawResults, transformer));
    }

    private int estimateSize(List<RowResult<Value>> rangeRows) {
        int estimatedSize = 0;
        for (RowResult<Value> rowResult : rangeRows) {
            estimatedSize += rowResult.getColumns().size();
        }
        return estimatedSize;
    }

    private <T> Collection<Map.Entry<Cell, T>> getWithPostFilteringSync(
            TableReference tableRef, Map<Cell, Value> rawResults, Function<Value, T> transformer) {
        return AtlasFutures.getUnchecked(getWithPostFilteringAsync(
                tableRef, rawResults, transformer, immediateKeyValueService, immediateTransactionService));
    }

    private <T> ListenableFuture<Collection<Map.Entry<Cell, T>>> getWithPostFilteringAsync(
            TableReference tableRef,
            Map<Cell, Value> rawResults,
            Function<Value, T> transformer,
            AsyncKeyValueService asyncKeyValueService,
            AsyncTransactionService asyncTransactionService) {
        long bytes = 0;
        for (Map.Entry<Cell, Value> entry : rawResults.entrySet()) {
            bytes += entry.getValue().getContents().length + Cells.getApproxSizeOfCell(entry.getKey());
        }
        if (bytes > TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES && log.isWarnEnabled()) {
            log.warn(
                    "A single get had quite a few bytes: {} for table {}. The number of results was {}. "
                            + "Enable debug logging for more information.",
                    SafeArg.of("numBytes", bytes),
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("numResults", rawResults.size()));
            if (log.isDebugEnabled()) {
                log.debug(
                        "The first 10 results of your request were {}.",
                        UnsafeArg.of("results", Iterables.limit(rawResults.entrySet(), 10)),
                        new SafeRuntimeException("This exception and stack trace are provided for debugging purposes"));
            }
            getHistogram(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_TOO_MANY_BYTES_READ, tableRef)
                    .update(bytes);
        }

        getCounter(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_CELLS_READ, tableRef).inc(rawResults.size());

        // LinkedList is chosen for fast append operation since we just add to this collection.
        Collection<Map.Entry<Cell, T>> resultsAccumulator = new LinkedList<>();

        if (AtlasDbConstants.HIDDEN_TABLES.contains(tableRef)) {
            Preconditions.checkState(allowHiddenTableAccess, "hidden tables cannot be read in this transaction");
            // hidden tables are used outside of the transaction protocol, and in general have invalid timestamps,
            // so do not apply post-filtering as post-filtering would rollback (actually delete) the data incorrectly
            // this case is hit when reading a hidden table from console
            for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
                resultsAccumulator.add(Maps.immutableEntry(e.getKey(), transformer.apply(e.getValue())));
            }
            return Futures.immediateFuture(resultsAccumulator);
        }

        return Futures.transformAsync(
                Futures.immediateFuture(rawResults),
                remainingResultsToPostFilter -> getWithPostFilteringIterate(
                        tableRef,
                        remainingResultsToPostFilter,
                        resultsAccumulator,
                        transformer,
                        asyncKeyValueService,
                        asyncTransactionService),
                MoreExecutors.directExecutor());
    }

    private <T> ListenableFuture<Collection<Map.Entry<Cell, T>>> getWithPostFilteringIterate(
            TableReference tableReference,
            Map<Cell, Value> remainingResultsToPostFilter,
            Collection<Map.Entry<Cell, T>> resultsAccumulator,
            Function<Value, T> transformer,
            AsyncKeyValueService asyncKeyValueService,
            AsyncTransactionService asyncTransactionService) {
        if (remainingResultsToPostFilter.isEmpty()) {
            getCounter(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_CELLS_RETURNED, tableReference)
                    .inc(resultsAccumulator.size());
            return Futures.immediateFuture(resultsAccumulator);
        }

        return Futures.transformAsync(
                getWithPostFilteringInternal(
                        tableReference,
                        remainingResultsToPostFilter,
                        resultsAccumulator,
                        transformer,
                        asyncKeyValueService,
                        asyncTransactionService),
                remaining -> getWithPostFilteringIterate(
                        tableReference,
                        remaining,
                        resultsAccumulator,
                        transformer,
                        asyncKeyValueService,
                        asyncTransactionService),
                MoreExecutors.directExecutor());
    }

    /**
     * A sentinel becomes orphaned if the table has been truncated between the time where the write occurred and where
     * it was truncated. In this case, there is a chance that we end up with a sentinel with no valid AtlasDB cell
     * covering it. In this case, we ignore it.
     */
    private Set<Cell> findOrphanedSweepSentinels(TableReference table, Map<Cell, Value> rawResults) {
        Set<Cell> sweepSentinels = Maps.filterValues(rawResults, SnapshotTransaction::isSweepSentinel)
                .keySet();
        if (sweepSentinels.isEmpty()) {
            return Collections.emptySet();
        }

        // for each sentinel, start at long max. Then iterate down with each found uncommitted value.
        // if committed value seen, stop: the sentinel is not orphaned
        // if we get back -1, the sentinel is orphaned
        Map<Cell, Long> timestampCandidates = new HashMap<>(
                keyValueService.getLatestTimestamps(table, Maps.asMap(sweepSentinels, x -> Long.MAX_VALUE)));
        Set<Cell> actualOrphanedSentinels = new HashSet<>();

        while (!timestampCandidates.isEmpty()) {
            Map<SentinelType, Map<Cell, Long>> sentinelTypeToTimestamps = timestampCandidates.entrySet().stream()
                    .collect(Collectors.groupingBy(
                            entry -> entry.getValue() == Value.INVALID_VALUE_TIMESTAMP
                                    ? SentinelType.DEFINITE_ORPHANED
                                    : SentinelType.INDETERMINATE,
                            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

            Map<Cell, Long> definiteOrphans = sentinelTypeToTimestamps.get(SentinelType.DEFINITE_ORPHANED);
            if (definiteOrphans != null) {
                actualOrphanedSentinels.addAll(definiteOrphans.keySet());
            }

            Map<Cell, Long> cellsToQuery = sentinelTypeToTimestamps.get(SentinelType.INDETERMINATE);
            if (cellsToQuery == null) {
                break;
            }
            Set<Long> committedStartTimestamps = KeyedStream.stream(
                            defaultTransactionService.get(cellsToQuery.values()))
                    .filter(Objects::nonNull)
                    .keys()
                    .collect(Collectors.toSet());

            Map<Cell, Long> nextTimestampCandidates = KeyedStream.stream(cellsToQuery)
                    .filter(cellStartTimestamp -> !committedStartTimestamps.contains(cellStartTimestamp))
                    .collectToMap();
            timestampCandidates = keyValueService.getLatestTimestamps(table, nextTimestampCandidates);
        }

        deleteOrphanedSentinelsAsync(table, actualOrphanedSentinels);

        return actualOrphanedSentinels;
    }

    private void deleteOrphanedSentinelsAsync(TableReference table, Set<Cell> actualOrphanedSentinels) {
        sweepQueue.getSweepStrategy(table).ifPresent(strategy -> {
            if (strategy == SweeperStrategy.THOROUGH) {
                SetMultimap<Cell, Long> sentinels = KeyedStream.of(actualOrphanedSentinels)
                        .map(_ignore -> Value.INVALID_VALUE_TIMESTAMP)
                        .collectToSetMultimap();
                try {
                    deleteExecutor.execute(() -> keyValueService.delete(table, sentinels));
                } catch (Throwable th) {
                    // best effort
                }
            }
        });
    }

    private static boolean isSweepSentinel(Value value) {
        return value.getTimestamp() == Value.INVALID_VALUE_TIMESTAMP;
    }

    /**
     * This will return all the key-value pairs that still need to be postFiltered.  It will output properly post
     * filtered keys to the {@code resultsCollector} output param.
     */
    private <T> ListenableFuture<Map<Cell, Value>> getWithPostFilteringInternal(
            TableReference tableRef,
            Map<Cell, Value> rawResults,
            @Output Collection<Map.Entry<Cell, T>> resultsCollector,
            Function<Value, T> transformer,
            AsyncKeyValueService asyncKeyValueService,
            AsyncTransactionService asyncTransactionService) {
        Set<Cell> orphanedSentinels = findOrphanedSweepSentinels(tableRef, rawResults);
        Set<Long> valuesStartTimestamps = getStartTimestampsForValues(rawResults.values());

        return Futures.transformAsync(
                getCommitTimestamps(tableRef, valuesStartTimestamps, true, asyncTransactionService),
                commitTimestamps -> collectCellsToPostFilter(
                        tableRef,
                        rawResults,
                        resultsCollector,
                        transformer,
                        asyncKeyValueService,
                        orphanedSentinels,
                        commitTimestamps),
                MoreExecutors.directExecutor());
    }

    private <T> ListenableFuture<Map<Cell, Value>> collectCellsToPostFilter(
            TableReference tableRef,
            Map<Cell, Value> rawResults,
            @Output Collection<Map.Entry<Cell, T>> resultsCollector,
            Function<Value, T> transformer,
            AsyncKeyValueService asyncKeyValueService,
            Set<Cell> orphanedSentinels,
            Map<Long, Long> commitTimestamps) {
        Map<Cell, Long> keysToReload = Maps.newHashMapWithExpectedSize(0);
        Map<Cell, Long> keysToDelete = Maps.newHashMapWithExpectedSize(0);
        ImmutableSet.Builder<Cell> keysAddedBuilder = ImmutableSet.builder();

        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            Cell key = e.getKey();
            Value value = e.getValue();

            if (isSweepSentinel(value)) {
                getCounter(AtlasDbMetricNames.CellFilterMetrics.INVALID_START_TS, tableRef)
                        .inc();

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
                        getCounter(AtlasDbMetricNames.CellFilterMetrics.INVALID_COMMIT_TS, tableRef)
                                .inc();
                    }
                } else if (theirCommitTimestamp > getStartTimestamp()) {
                    // The value's commit timestamp is after our start timestamp.
                    // This means the value is from a transaction which committed
                    // after our transaction began. We need to try reading at an
                    // earlier timestamp.
                    keysToReload.put(key, value.getTimestamp());
                    getCounter(AtlasDbMetricNames.CellFilterMetrics.COMMIT_TS_GREATER_THAN_TRANSACTION_TS, tableRef)
                            .inc();
                } else {
                    // The value has a commit timestamp less than our start timestamp, and is visible and valid.
                    if (value.getContents().length != 0) {
                        resultsCollector.add(Maps.immutableEntry(key, transformer.apply(value)));
                        keysAddedBuilder.add(key);
                    }
                }
            }
        }
        Set<Cell> keysAddedToResults = keysAddedBuilder.build();

        if (!keysToDelete.isEmpty()) {
            // if we can't roll back the failed transactions, we should just try again
            if (!rollbackFailedTransactions(tableRef, keysToDelete, commitTimestamps, defaultTransactionService)) {
                return Futures.immediateFuture(getRemainingResults(rawResults, keysAddedToResults));
            }
        }

        if (!keysToReload.isEmpty()) {
            return Futures.transform(
                    asyncKeyValueService.getAsync(tableRef, keysToReload),
                    nextRawResults -> {
                        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp());
                        return getRemainingResults(nextRawResults, keysAddedToResults);
                    },
                    MoreExecutors.directExecutor());
        }
        return Futures.immediateFuture(ImmutableMap.of());
    }

    private Map<Cell, Value> getRemainingResults(Map<Cell, Value> rawResults, Set<Cell> keysAddedToResults) {
        Map<Cell, Value> remainingResults = new HashMap<>(rawResults);
        remainingResults.keySet().removeAll(keysAddedToResults);
        return remainingResults;
    }

    /**
     * This is protected to allow for different post filter behavior.
     */
    protected boolean shouldDeleteAndRollback() {
        Preconditions.checkNotNull(
                timelockService, "if we don't have a valid lock server we can't roll back transactions");
        return true;
    }

    @Override
    public final void delete(TableReference tableRef, Set<Cell> cells) {
        putInternal(tableRef, Cells.constantValueMap(cells, PtBytes.EMPTY_BYTE_ARRAY));
        getCache().delete(tableRef, cells);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        ensureNoEmptyValues(values);
        putInternal(tableRef, values);
        getCache().write(tableRef, values);
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
        for (Map.Entry<Cell, byte[]> cellEntry : values.entrySet()) {
            if ((cellEntry.getValue() == null) || (cellEntry.getValue().length == 0)) {
                throw new SafeIllegalArgumentException(
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
                    log.warn(
                            "A single transaction has put quite a few bytes: {}. "
                                    + "Enable debug logging for more information",
                            SafeArg.of("numBytes", newVal));
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "This exception and stack trace are provided for debugging purposes.",
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

    /**
     * Returns true iff the transaction is known to have successfully committed.
     *
     * Be careful when using this method! A transaction that the client thinks has failed could actually have
     * committed as far as the key-value service is concerned.
     */
    private boolean isDefinitivelyCommitted() {
        return state.get() == State.COMMITTED;
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
            throw new SafeIllegalStateException("this transaction has already failed");
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
                throw new SafeIllegalStateException("Cannot commit while other threads are still calling put.");
            }

            checkConstraints();
            commitWrites(transactionService);
            if (perfLogger.isDebugEnabled()) {
                long transactionMillis = TimeUnit.NANOSECONDS.toMillis(transactionTimerContext.stop());
                perfLogger.debug("Committed transaction {} in {}ms", getStartTimestamp(), transactionMillis);
            }
            success = true;
        } finally {
            // Once we are in state committing, we need to try/finally to set the state to a terminal state.
            if (success) {
                state.set(State.COMMITTED);
                transactionOutcomeMetrics.markSuccessfulCommit();
                successCallbackManager.runCallbacks();
            } else {
                state.set(State.FAILED);
                transactionOutcomeMetrics.markFailedCommit();
            }
        }
    }

    private void checkConstraints() {
        List<String> violations = new ArrayList<>();
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
                constraintLogger.error("Constraint failure on commit.", new AtlasDbConstraintException(violations));
            }
        }
    }

    private void commitWrites(TransactionService transactionService) {
        if (!hasWrites()) {
            if (hasReads() || hasAnyInvolvedTables()) {
                // verify any pre-commit conditions on the transaction
                preCommitCondition.throwIfConditionInvalid(getStartTimestamp());

                // if there are no writes, we must still make sure the immutable timestamp lock is still valid,
                // to ensure that sweep hasn't thoroughly deleted cells we tried to read
                if (validationNecessaryForInvolvedTablesOnCommit()) {
                    throwIfImmutableTsOrCommitLocksExpired(null);
                }

                // if the cache has been used, we must work out which values can be flushed to the central cache by
                // obtaining a commit update, which is obtained via the get commit timestamp request.
                if (cache.get().hasUpdates()) {
                    timedAndTraced(
                            "getCommitTimestamp",
                            () -> timelockService.getCommitTimestamp(
                                    getStartTimestamp(), LockToken.of(UUID.randomUUID())));
                }
                return;
            }
            return;
        }

        timedAndTraced("commitStage", () -> {
            // Acquire row locks and a lock on the start timestamp row in the transactions table.
            // This must happen before conflict checking, otherwise we could complete the checks and then have someone
            // else write underneath us before we proceed (thus missing a write/write conflict).
            // Timing still useful to distinguish bad lock percentiles from user-generated lock requests.
            LockToken commitLocksToken = timedAndTraced("commitAcquireLocks", this::acquireLocksForCommit);
            try {
                // Conflict checking. We can actually do this later without compromising correctness, but there is no
                // reason to postpone this check - we waste resources writing unnecessarily if these are going to fail.
                timedAndTraced(
                        "commitCheckingForConflicts",
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
                // Timing is still useful, as this may perform operations pertaining to lock watches.
                long commitTimestamp = timedAndTraced(
                        "getCommitTimestamp",
                        () -> timelockService.getCommitTimestamp(getStartTimestamp(), commitLocksToken));
                commitTsForScrubbing = commitTimestamp;

                // Punch on commit so that if hard delete is the only thing happening on a system,
                // we won't block forever waiting for the unreadable timestamp to advance past the
                // scrub timestamp (same as the hard delete transaction's start timestamp).
                // May not need to be here specifically, but this is a very cheap operation - scheduling another thread
                // might well cost more.
                // Not timed as this is generally an asynchronous operation.
                traced("microsForPunch", () -> cleaner.punch(commitTimestamp));

                // Serializable transactions need to check their reads haven't changed, by reading again at
                // commitTs + 1. This must happen before the lock check for thorough tables, because the lock check
                // verifies the immutable timestamp hasn't moved forward - thorough sweep might sweep a conflict out
                // from underneath us.
                timedAndTraced(
                        "readWriteConflictCheck", () -> throwIfReadWriteConflictForSerializable(commitTimestamp));

                // Verify that our locks and pre-commit conditions are still valid before we actually commit;
                // this throwIfPreCommitRequirementsNotMet is required by the transaction protocol for correctness.
                // We check the pre-commit conditions first since they may operate similarly to read write conflict
                // handling - we should check lock validity last to ensure that sweep hasn't affected the checks.
                timedAndTraced("userPreCommitCondition", () -> throwIfPreCommitConditionInvalid(commitTimestamp));

                // Not timed, because this just calls ConjureTimelockServiceBlocking.refreshLockLeases, and that is
                // timed.
                traced("preCommitLockCheck", () -> throwIfImmutableTsOrCommitLocksExpired(commitLocksToken));

                // Not timed, because this just calls TransactionService.putUnlessExists, and that is timed.
                traced(
                        "commitPutCommitTs",
                        () -> putCommitTimestamp(commitTimestamp, commitLocksToken, transactionService));

                long microsSinceCreation = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis() - timeCreated);
                getTimer("commitTotalTimeSinceTxCreation").update(microsSinceCreation, TimeUnit.MICROSECONDS);
                getHistogram(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_BYTES_WRITTEN)
                        .update(byteCount.get());
            } finally {
                // Not timed because tryUnlock() is an asynchronous operation.
                traced("postCommitUnlock", () -> timelockService.tryUnlock(ImmutableSet.of(commitLocksToken)));
            }
        });
    }

    private void traced(String spanName, Runnable runnable) {
        try (CloseableTracer tracer = CloseableTracer.startSpan(spanName)) {
            runnable.run();
        }
    }

    private void timedAndTraced(String timerName, Runnable runnable) {
        try (Timer.Context timer = getTimer(timerName).time()) {
            traced(timerName, runnable);
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
        return com.google.common.base.Preconditions.checkNotNull(
                conflictDetectionManager.get(tableRef),
                "Not a valid table for this transaction. Make sure this table name exists or has a valid namespace: %s",
                tableRef);
    }

    private String getExpiredLocksErrorString(@Nullable LockToken commitLocksToken, Set<LockToken> expiredLocks) {
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
            log.warn(baseMsg + "{}", expiredLocksErrorString, ex);
            transactionOutcomeMetrics.markLocksExpired();
            throw ex;
        }
    }

    /**
     * Refreshes external and commit locks.
     *
     * @return set of locks that could not be refreshed
     */
    private Set<LockToken> refreshCommitAndImmutableTsLocks(@Nullable LockToken commitLocksToken) {
        Set<LockToken> toRefresh = new HashSet<>();
        if (commitLocksToken != null) {
            toRefresh.add(commitLocksToken);
        }
        immutableTimestampLock.ifPresent(toRefresh::add);

        if (toRefresh.isEmpty()) {
            return ImmutableSet.of();
        }

        return Sets.difference(toRefresh, timelockService.refreshLockLeases(toRefresh))
                .immutableCopy();
    }

    /**
     * Make sure we have all the rows we are checking already locked before calling this.
     */
    protected void throwIfConflictOnCommit(LockToken commitLocksToken, TransactionService transactionService)
            throws TransactionConflictException {
        for (Map.Entry<TableReference, ConcurrentNavigableMap<Cell, byte[]>> write : writesByTable.entrySet()) {
            ConflictHandler conflictHandler = getConflictHandlerForTable(write.getKey());
            throwIfWriteAlreadyCommitted(
                    write.getKey(), write.getValue(), conflictHandler, commitLocksToken, transactionService);
        }
    }

    protected void throwIfWriteAlreadyCommitted(
            TableReference tableRef,
            Map<Cell, byte[]> writes,
            ConflictHandler conflictHandler,
            LockToken commitLocksToken,
            TransactionService transactionService)
            throws TransactionConflictException {
        if (writes.isEmpty() || !conflictHandler.checkWriteWriteConflicts()) {
            return;
        }
        Set<CellConflict> spanningWrites = new HashSet<>();
        Set<CellConflict> dominatingWrites = new HashSet<>();
        Map<Cell, Long> keysToLoad = Maps.asMap(writes.keySet(), Functions.constant(Long.MAX_VALUE));
        while (!keysToLoad.isEmpty()) {
            keysToLoad = detectWriteAlreadyCommittedInternal(
                    tableRef, keysToLoad, spanningWrites, dominatingWrites, transactionService);
        }

        if (conflictHandler == ConflictHandler.RETRY_ON_VALUE_CHANGED) {
            throwIfValueChangedConflict(tableRef, writes, spanningWrites, dominatingWrites, commitLocksToken);
        } else {
            if (!spanningWrites.isEmpty() || !dominatingWrites.isEmpty()) {
                transactionOutcomeMetrics.markWriteWriteConflict(tableRef);
                throw TransactionConflictException.create(
                        tableRef,
                        getStartTimestamp(),
                        spanningWrites,
                        dominatingWrites,
                        System.currentTimeMillis() - timeCreated);
            }
        }
    }

    /**
     * This will throw if we have a value changed conflict.  This means that either we changed the value and anyone did
     * a write after our start timestamp, or we just touched the value (put the same value as before) and a changed
     * value was written after our start time.
     */
    private void throwIfValueChangedConflict(
            TableReference table,
            Map<Cell, byte[]> writes,
            Set<CellConflict> spanningWrites,
            Set<CellConflict> dominatingWrites,
            LockToken commitLocksToken) {
        Map<Cell, CellConflict> cellToConflict = new HashMap<>();
        Map<Cell, Long> cellToTs = new HashMap<>();
        for (CellConflict c : Sets.union(spanningWrites, dominatingWrites)) {
            cellToConflict.put(c.getCell(), c);
            cellToTs.put(c.getCell(), c.getTheirStart() + 1);
        }

        Map<Cell, byte[]> oldValues = getIgnoringLocalWrites(table, cellToTs.keySet());
        Map<Cell, Value> conflictingValues = keyValueService.get(table, cellToTs);

        Set<Cell> conflictingCells = new HashSet<>();
        for (Map.Entry<Cell, Long> cellEntry : cellToTs.entrySet()) {
            Cell cell = cellEntry.getKey();
            if (!writes.containsKey(cell)) {
                Validate.isTrue(false, "Missing write for cell: %s for table %s", cellToConflict.get(cell), table);
            }
            if (!conflictingValues.containsKey(cell)) {
                // This error case could happen if our locks expired.
                throwIfPreCommitRequirementsNotMet(commitLocksToken, getStartTimestamp());
                Validate.isTrue(
                        false, "Missing conflicting value for cell: %s for table %s", cellToConflict.get(cell), table);
            }
            if (conflictingValues.get(cell).getTimestamp() != (cellEntry.getValue() - 1)) {
                // This error case could happen if our locks expired.
                throwIfPreCommitRequirementsNotMet(commitLocksToken, getStartTimestamp());
                Validate.isTrue(
                        false,
                        "Wrong timestamp for cell in table %s Expected: %s Actual: %s",
                        table,
                        cellToConflict.get(cell),
                        conflictingValues.get(cell));
            }
            @Nullable byte[] oldVal = oldValues.get(cell);
            byte[] writeVal = writes.get(cell);
            byte[] conflictingVal = conflictingValues.get(cell).getContents();
            if (!Transactions.cellValuesEqual(oldVal, writeVal) || !Arrays.equals(writeVal, conflictingVal)) {
                conflictingCells.add(cell);
            } else if (log.isInfoEnabled()) {
                log.info(
                        "Another transaction committed to the same cell before us but their value was the same."
                                + " Cell: {} Table: {}",
                        UnsafeArg.of("cell", cell),
                        LoggingArgs.tableRef(table));
            }
        }
        if (conflictingCells.isEmpty()) {
            return;
        }
        Predicate<CellConflict> conflicting =
                Predicates.compose(Predicates.in(conflictingCells), CellConflict.getCellFunction());
        transactionOutcomeMetrics.markWriteWriteConflict(table);
        throw TransactionConflictException.create(
                table,
                getStartTimestamp(),
                Sets.filter(spanningWrites, conflicting),
                Sets.filter(dominatingWrites, conflicting),
                System.currentTimeMillis() - timeCreated);
    }

    /**
     * This will return the set of keys that need to be retried.  It will output any conflicts it finds into the output
     * params.
     */
    protected Map<Cell, Long> detectWriteAlreadyCommittedInternal(
            TableReference tableRef,
            Map<Cell, Long> keysToLoad,
            @Output Set<CellConflict> spanningWrites,
            @Output Set<CellConflict> dominatingWrites,
            TransactionService transactionService) {
        Map<Cell, Long> rawResults = keyValueService.getLatestTimestamps(tableRef, keysToLoad);
        Map<Long, Long> commitTimestamps = getCommitTimestampsSync(tableRef, rawResults.values(), false);

        // TODO(fdesouza): Remove this once PDS-95791 is resolved.
        conflictTracer.collect(getStartTimestamp(), keysToLoad, rawResults, commitTimestamps);

        Map<Cell, Long> keysToDelete = Maps.newHashMapWithExpectedSize(0);

        for (Map.Entry<Cell, Long> e : rawResults.entrySet()) {
            Cell key = e.getKey();
            long theirStartTimestamp = e.getValue();
            AssertUtils.assertAndLog(
                    log, theirStartTimestamp != getStartTimestamp(), "Timestamp reuse is bad:%d", getStartTimestamp());

            Long theirCommitTimestamp = commitTimestamps.get(theirStartTimestamp);
            if (theirCommitTimestamp == null || theirCommitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
                // The value has no commit timestamp or was explicitly rolled back.
                // This means the value is garbage from a transaction which didn't commit.
                keysToDelete.put(key, theirStartTimestamp);
                continue;
            }

            AssertUtils.assertAndLog(
                    log, theirCommitTimestamp != getStartTimestamp(), "Timestamp reuse is bad:%d", getStartTimestamp());
            if (theirStartTimestamp > getStartTimestamp()) {
                dominatingWrites.add(Cells.createConflictWithMetadata(
                        keyValueService, tableRef, key, theirStartTimestamp, theirCommitTimestamp));
            } else if (theirCommitTimestamp > getStartTimestamp()) {
                spanningWrites.add(Cells.createConflictWithMetadata(
                        keyValueService, tableRef, key, theirStartTimestamp, theirCommitTimestamp));
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
     * This will attempt to rollback the passed transactions.  If all are rolled back correctly this method will also
     * delete the values for the transactions that have been rolled back.
     *
     * @return false if we cannot roll back the failed transactions because someone beat us to it
     */
    private boolean rollbackFailedTransactions(
            TableReference tableRef,
            Map<Cell, Long> keysToDelete,
            Map<Long, Long> commitTimestamps,
            TransactionService transactionService) {
        for (long startTs : new HashSet<>(keysToDelete.values())) {
            if (commitTimestamps.get(startTs) == null) {
                log.warn("Rolling back transaction: {}", SafeArg.of("startTs", startTs));
                if (!rollbackOtherTransaction(startTs, transactionService)) {
                    return false;
                }
            } else {
                Preconditions.checkArgument(commitTimestamps.get(startTs) == TransactionConstants.FAILED_COMMIT_TS);
            }
        }

        try {
            deleteExecutor.execute(() -> deleteCells(keyValueService, tableRef, keysToDelete));
        } catch (RejectedExecutionException rejected) {
            log.info(
                    "Could not delete keys {} for table {}, because the delete executor's queue was full."
                            + " Sweep should eventually clean these values.",
                    UnsafeArg.of("keysToDelete", keysToDelete),
                    LoggingArgs.tableRef(tableRef),
                    rejected);
        }
        return true;
    }

    /**
     * This method is made static so it loses reference to the SnapshotTransaction reference when passed to
     * deleteExecutor::execute in a lambda reducing its retained memory size.
     */
    private static void deleteCells(
            KeyValueService keyValueService, TableReference tableRef, Map<Cell, Long> keysToDelete) {
        try {
            log.debug(
                    "For table: {} we are deleting values of an uncommitted transaction: {}",
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
                log.warn(
                        msg + " The keys that failed to be deleted during rollback were {}",
                        LoggingArgs.tableRef(tableRef),
                        UnsafeArg.of("keysToDelete", keysToDelete));
            } else {
                log.warn(msg, LoggingArgs.tableRef(tableRef), e);
            }
        }
    }

    /**
     * Rollback a someone else's transaction.
     *
     * @return true if the other transaction was rolled back
     */
    private boolean rollbackOtherTransaction(long startTs, TransactionService transactionService) {
        try {
            transactionService.putUnlessExists(startTs, TransactionConstants.FAILED_COMMIT_TS);
            transactionOutcomeMetrics.markRollbackOtherTransaction();
            return true;
        } catch (KeyAlreadyExistsException e) {
            log.debug(
                    "This isn't a bug but it should be very infrequent. Two transactions tried to roll back someone"
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
        TransactionConfig currentTransactionConfig = transactionConfig.get();

        // TODO(fdesouza): Revert this once PDS-95791 is resolved.
        long lockAcquireTimeoutMillis = currentTransactionConfig.getLockAcquireTimeoutMillis();
        LockRequest request = ImmutableLockRequest.of(
                lockDescriptors,
                lockAcquireTimeoutMillis,
                Optional.ofNullable(getStartTimestampAsClientDescription(currentTransactionConfig)));
        LockResponse lockResponse = timelockService.lock(request);
        if (!lockResponse.wasSuccessful()) {
            log.error(
                    "Timed out waiting while acquiring commit locks. Timeout was {} ms. "
                            + "First ten required locks were {}.",
                    SafeArg.of("acquireTimeoutMs", lockAcquireTimeoutMillis),
                    UnsafeArg.of("firstTenLockDescriptors", Iterables.limit(lockDescriptors, 10)));
            throw new TransactionLockAcquisitionTimeoutException("Timed out while acquiring commit locks.");
        }
        return lockResponse.getToken();
    }

    protected Set<LockDescriptor> getLocksForWrites() {
        Set<LockDescriptor> result = new HashSet<>();
        for (TableReference tableRef : writesByTable.keySet()) {
            ConflictHandler conflictHandler = getConflictHandlerForTable(tableRef);
            if (conflictHandler.lockCellsForConflicts()) {
                for (Cell cell : getLocalWrites(tableRef).keySet()) {
                    result.add(AtlasCellLockDescriptor.of(
                            tableRef.getQualifiedName(), cell.getRowName(), cell.getColumnName()));
                }
            }

            if (conflictHandler.lockRowsForConflicts()) {
                Cell lastCell = null;
                for (Cell cell : getLocalWrites(tableRef).keySet()) {
                    if (lastCell == null || !Arrays.equals(lastCell.getRowName(), cell.getRowName())) {
                        result.add(AtlasRowLockDescriptor.of(tableRef.getQualifiedName(), cell.getRowName()));
                    }
                    lastCell = cell;
                }
            }
        }
        result.add(AtlasRowLockDescriptor.of(
                TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                TransactionConstants.getValueForTimestamp(getStartTimestamp())));
        return result;
    }

    /**
     * We will block here until the passed transactions have released their lock.  This means that the committing
     * transaction is either complete or it has failed and we are allowed to roll it back.
     */
    private void waitForCommitToComplete(Iterable<Long> startTimestamps) {
        Set<LockDescriptor> lockDescriptors = new HashSet<>();
        for (long start : startTimestamps) {
            if (start < immutableTimestamp) {
                // We don't need to block in this case because this transaction is already complete
                continue;
            }
            lockDescriptors.add(AtlasRowLockDescriptor.of(
                    TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                    TransactionConstants.getValueForTimestamp(start)));
        }

        if (lockDescriptors.isEmpty()) {
            return;
        }

        waitFor(lockDescriptors);
    }

    private void waitFor(Set<LockDescriptor> lockDescriptors) {
        TransactionConfig currentTransactionConfig = transactionConfig.get();
        String startTimestampAsDescription = getStartTimestampAsClientDescription(currentTransactionConfig);

        // TODO(fdesouza): Revert this once PDS-95791 is resolved.
        long lockAcquireTimeoutMillis = currentTransactionConfig.getLockAcquireTimeoutMillis();
        WaitForLocksRequest request =
                WaitForLocksRequest.of(lockDescriptors, lockAcquireTimeoutMillis, startTimestampAsDescription);
        WaitForLocksResponse response = timelockService.waitForLocks(request);
        if (!response.wasSuccessful()) {
            log.error(
                    "Timed out waiting for commits to complete. Timeout was {} ms. First ten locks were {}.",
                    SafeArg.of("requestId", request.getRequestId()),
                    SafeArg.of("acquireTimeoutMs", lockAcquireTimeoutMillis),
                    UnsafeArg.of("firstTenLockDescriptors", Iterables.limit(lockDescriptors, 10)));
            throw new TransactionLockAcquisitionTimeoutException("Timed out waiting for commits to complete.");
        }
    }

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     *
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    @Nullable
    private String getStartTimestampAsClientDescription(TransactionConfig currentTransactionConfig) {
        return currentTransactionConfig.attachStartTimestampToLockRequestDescriptions()
                ? Long.toString(getStartTimestamp())
                : null;
    }

    ///////////////////////////////////////////////////////////////////////////
    /// Commit timestamp management
    ///////////////////////////////////////////////////////////////////////////

    private Set<Long> getStartTimestampsForValues(Iterable<Value> values) {
        Set<Long> results = new HashSet<>();
        for (Value v : values) {
            results.add(v.getTimestamp());
        }
        return results;
    }

    private Map<Long, Long> getCommitTimestampsSync(
            @Nullable TableReference tableRef, Iterable<Long> startTimestamps, boolean waitForCommitterToComplete) {
        return AtlasFutures.getUnchecked(getCommitTimestamps(
                tableRef, startTimestamps, waitForCommitterToComplete, immediateTransactionService));
    }

    /**
     * Returns a map from start timestamp to commit timestamp.  If a start timestamp wasn't committed, then it will be
     * missing from the map.  This method will block until the transactions for these start timestamps are complete.
     */
    protected ListenableFuture<Map<Long, Long>> getCommitTimestamps(
            @Nullable TableReference tableRef,
            Iterable<Long> startTimestamps,
            boolean shouldWaitForCommitterToComplete,
            AsyncTransactionService asyncTransactionService) {
        if (Iterables.isEmpty(startTimestamps)) {
            return Futures.immediateFuture(ImmutableMap.of());
        }
        Map<Long, Long> startToCommitTimestamps = new HashMap<>();
        Set<Long> gets = new HashSet<>();
        for (Long startTs : startTimestamps) {
            Long cached = timestampValidationReadCache.getCommitTimestampIfPresent(startTs);
            if (cached != null) {
                startToCommitTimestamps.put(startTs, cached);
            } else {
                gets.add(startTs);
            }
        }

        if (gets.isEmpty()) {
            return Futures.immediateFuture(startToCommitTimestamps);
        }

        // Before we do the reads, we need to make sure the committer is done writing.
        if (shouldWaitForCommitterToComplete) {
            waitForCommitterToComplete(tableRef, startTimestamps);
        }

        traceGetCommitTimestamps(tableRef, gets);

        if (gets.size() > transactionConfig.get().getThresholdForLoggingLargeNumberOfTransactionLookups()) {
            logLargeNumberOfTransactions(tableRef, gets);
        }

        return Futures.transform(
                loadCommitTimestamps(asyncTransactionService, gets),
                rawResults -> {
                    for (Map.Entry<Long, Long> e : rawResults.entrySet()) {
                        if (e.getValue() != null) {
                            Long startTs = e.getKey();
                            Long commitTs = e.getValue();
                            startToCommitTimestamps.put(startTs, commitTs);
                            timestampValidationReadCache.putAlreadyCommittedTransaction(startTs, commitTs);
                        }
                    }
                    return startToCommitTimestamps;
                },
                MoreExecutors.directExecutor());
    }

    private void waitForCommitterToComplete(@Nullable TableReference tableRef, Iterable<Long> startTimestamps) {
        Timer.Context timer = getTimer("waitForCommitTsMillis").time();
        waitForCommitToComplete(startTimestamps);
        long waitForCommitTsMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());

        if (tableRef != null) {
            perfLogger.debug(
                    "Waited to get commit timestamps when reading from a known table.",
                    SafeArg.of("commitTsMillis", waitForCommitTsMillis),
                    LoggingArgs.tableRef(tableRef));
        } else {
            perfLogger.debug("Waited to get commit timestamps.", SafeArg.of("commitTsMillis", waitForCommitTsMillis));
        }
    }

    private void traceGetCommitTimestamps(@Nullable TableReference tableRef, Set<Long> gets) {
        if (tableRef != null) {
            log.trace(
                    "Getting commit timestamps for a read while reading table.",
                    SafeArg.of("numTimestamps", gets.size()),
                    LoggingArgs.tableRef(tableRef));
            return;
        }

        log.trace("Getting commit timestamps.", SafeArg.of("numTimestamps", gets.size()));
    }

    private void logLargeNumberOfTransactions(@Nullable TableReference tableRef, Set<Long> gets) {
        log.info(
                "Looking up a large number of transactions.",
                SafeArg.of("numberOfTransactionIds", gets.size()),
                tableRef == null ? SafeArg.of("tableRef", "no_table") : LoggingArgs.tableRef(tableRef));
    }

    private static ListenableFuture<Map<Long, Long>> loadCommitTimestamps(
            AsyncTransactionService asyncTransactionService, Set<Long> startTimestamps) {
        // distinguish between a single timestamp and a batch, for more granular metrics
        if (startTimestamps.size() == 1) {
            Long singleTs = startTimestamps.iterator().next();
            return Futures.transform(
                    asyncTransactionService.getAsync(singleTs),
                    commitTsOrNull ->
                            commitTsOrNull == null ? ImmutableMap.of() : ImmutableMap.of(singleTs, commitTsOrNull),
                    MoreExecutors.directExecutor());
        } else {
            return asyncTransactionService.getAsync(startTimestamps);
        }
    }

    /**
     * This will attempt to put the commitTimestamp into the DB.
     *
     * @throws TransactionLockTimeoutException  If our locks timed out while trying to commit.
     * @throws TransactionCommitFailedException failed when committing in a way that isn't retriable
     */
    private void putCommitTimestamp(long commitTimestamp, LockToken locksToken, TransactionService transactionService)
            throws TransactionFailedException {
        Preconditions.checkArgument(commitTimestamp > getStartTimestamp(), "commitTs must be greater than startTs");
        try {
            transactionService.putUnlessExists(getStartTimestamp(), commitTimestamp);
        } catch (KeyAlreadyExistsException e) {
            handleKeyAlreadyExistsException(commitTimestamp, e, locksToken);
        } catch (Exception e) {
            TransactionCommitFailedException commitFailedEx = new TransactionCommitFailedException(
                    "This transaction failed writing the commit timestamp. "
                            + "It might have been committed, but it may not have.",
                    e);
            log.error("failed to commit an atlasdb transaction", commitFailedEx);
            transactionOutcomeMetrics.markPutUnlessExistsFailed();
            throw commitFailedEx;
        }
    }

    private void handleKeyAlreadyExistsException(
            long commitTs, KeyAlreadyExistsException ex, LockToken commitLocksToken) {
        try {
            if (wasCommitSuccessful(commitTs)) {
                // We did actually commit successfully.  This case could happen if the impl
                // for putUnlessExists did a retry and we had committed already
                return;
            }
            Set<LockToken> expiredLocks = refreshCommitAndImmutableTsLocks(commitLocksToken);
            if (!expiredLocks.isEmpty()) {
                transactionOutcomeMetrics.markLocksExpired();
                throw new TransactionLockTimeoutException(
                        "Our commit was already rolled back at commit time"
                                + " because our locks timed out. startTs: " + getStartTimestamp() + ".  "
                                + getExpiredLocksErrorString(commitLocksToken, expiredLocks),
                        ex);
            } else {
                log.info(
                        "This transaction has been rolled back by someone else, even though we believe we still hold "
                                + "the locks. This is not expected to occur frequently.",
                        immutableTimestampLock
                                .map(token -> token.toSafeArg("immutableTimestampLock"))
                                .orElseGet(() -> SafeArg.of("immutableTimestampLock", null)),
                        commitLocksToken.toSafeArg("commitLocksToken"));
            }
        } catch (TransactionFailedException e1) {
            throw e1;
        } catch (Exception e1) {
            log.error(
                    "Failed to determine if we can retry this transaction. startTs: {}",
                    SafeArg.of("startTs", getStartTimestamp()),
                    e1);
        }
        String msg = "Our commit was already rolled back at commit time."
                + " Locking should prevent this from happening, but our locks may have timed out."
                + " startTs: " + getStartTimestamp();
        throw new TransactionCommitFailedException(msg, ex);
    }

    private boolean wasCommitSuccessful(long commitTs) throws Exception {
        Map<Long, Long> commitTimestamps =
                getCommitTimestampsSync(null, Collections.singleton(getStartTimestamp()), false);
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

    @Override
    public void onSuccess(Runnable callback) {
        Preconditions.checkNotNull(callback, "Callback cannot be null");
        successCallbackManager.registerCallback(callback);
    }

    /**
     * The similarly-named-and-intentioned useTable method is only called on writes. This one is more comprehensive and
     * covers read paths as well (necessary because we wish to get the sweep strategies of tables in read-only
     * transactions)
     * <p>
     * A table can be involved in a transaction, even if there were no reads done on it, see #markTableInvolved.
     */
    private void markTableAsInvolvedInThisTransaction(TableReference tableRef) {
        involvedTables.add(tableRef);
    }

    private boolean hasAnyInvolvedTables() {
        return !involvedTables.isEmpty();
    }

    private boolean validationNecessaryForInvolvedTablesOnCommit() {
        boolean anyTableRequiresImmutableTimestampLocking =
                involvedTables.stream().anyMatch(this::requiresImmutableTimestampLocking);
        boolean needsToValidate = !validateLocksOnReads || !hasReads();
        return anyTableRequiresImmutableTimestampLocking && needsToValidate;
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
            for (Map.Entry<TableReference, ConcurrentNavigableMap<Cell, byte[]>> entry : writesByTable.entrySet()) {
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
            for (Map.Entry<TableReference, ConcurrentNavigableMap<Cell, byte[]>> entry : writesByTable.entrySet()) {
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
                SnapshotTransaction.class, name, metricsManager.getTableNameTagFor(tableRef));
    }

    private Counter getCounter(String name, TableReference tableRef) {
        return tableLevelMetricsController.createAndRegisterCounter(SnapshotTransaction.class, name, tableRef);
    }

    private enum SentinelType {
        DEFINITE_ORPHANED,
        INDETERMINATE;
    }

    private final class SuccessCallbackManager {
        private final List<Runnable> callbacks = new CopyOnWriteArrayList<>();

        public void registerCallback(Runnable runnable) {
            ensureUncommitted();
            callbacks.add(runnable);
        }

        public void runCallbacks() {
            Preconditions.checkState(
                    isDefinitivelyCommitted(),
                    "Callbacks must not be run if it is not known that the transaction has definitively committed! "
                            + "This is likely a bug in AtlasDB transaction code.");
            callbacks.forEach(Runnable::run);
        }
    }
}
