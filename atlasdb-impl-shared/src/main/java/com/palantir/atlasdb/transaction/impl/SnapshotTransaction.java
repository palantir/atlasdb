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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
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
import com.google.common.collect.ImmutableSortedMap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbPerformanceConstants;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
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
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbstractBatchingVisitable;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableFromIterable;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.ForwardingClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.IterableUtils;
import com.palantir.common.collect.IteratorUtils;
import com.palantir.common.collect.MapEntries;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;
import com.palantir.util.AssertUtils;
import com.palantir.util.DistributedCacheMgrCache;
import com.palantir.util.Pair;
import com.palantir.util.SoftCache;
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
    private static final int BATCH_SIZE_GET_FIRST_PAGE = 1000;
    private final static Logger log = LoggerFactory.getLogger(SnapshotTransaction.class);
    private static final Logger perfLogger = LoggerFactory.getLogger("dualschema.perf");
    private static final Logger constraintLogger = LoggerFactory.getLogger("dualschema.constraints");

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
    protected final TimestampService timestampService;
    final KeyValueService keyValueService;
    protected final RemoteLockService lockService;
    final TransactionService defaultTransactionService;
    private final Cleaner cleaner;
    private final Supplier<Long> startTimestamp;

    protected final long immutableTimestamp;
    protected final ImmutableSet<LockRefreshToken> externalLocksTokens;

    protected final long timeCreated = System.currentTimeMillis();

    protected final ConcurrentMap<String, ConcurrentNavigableMap<Cell, byte[]>> writesByTable = Maps.newConcurrentMap();
    private final ConflictDetectionManager conflictDetectionManager;
    private final DistributedCacheMgrCache<Long, Long> cachedCommitTimes = new SoftCache<Long, Long>();
    private final AtomicLong byteCount = new AtomicLong();

    private final AtlasDbConstraintCheckingMode constraintCheckingMode;

    private final ConcurrentMap<String, ConstraintCheckable> constraintsByTableName = Maps.newConcurrentMap();

    private final AtomicReference<State> state = new AtomicReference<State>(State.UNCOMMITTED);
    private final AtomicLong numWriters = new AtomicLong();
    protected final SweepStrategyManager sweepStrategyManager;
    protected final Long transactionReadTimeoutMillis;
    private final TransactionReadSentinelBehavior readSentinelBehavior;
    private volatile long commitTsForScrubbing = TransactionConstants.FAILED_COMMIT_TS;
    protected final boolean allowHiddenTableAccess;
    protected final Stopwatch transactionTimer = Stopwatch.createStarted();

    /**
     * @param keyValueService
     * @param lockService
     * @param timestampService
     * @param startTimeStamp
     * @param immutableTimestamp If we find a row written before the immutableTimestamp we don't need to
     *                           grab a read lock for it because we know that no writers exist.
     * @param tokensValidForCommit These tokens need to be valid with {@link #lockService} for this transaction
     *                             to commit.  If these locks have expired then the commit will fail.
     * @param transactionTimeoutMillis
     */
    public SnapshotTransaction(KeyValueService keyValueService,
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
        this.keyValueService = keyValueService;
        this.timestampService = timestampService;
        this.defaultTransactionService = transactionService;
        this.cleaner = cleaner;
        this.lockService = lockService;
        this.startTimestamp = startTimeStamp;
        this.conflictDetectionManager = conflictDetectionManager;
        this.sweepStrategyManager = sweepStrategyManager;
        this.immutableTimestamp = immutableTimestamp;
        this.externalLocksTokens = ImmutableSet.copyOf(tokensValidForCommit);
        this.constraintCheckingMode = constraintCheckingMode;
        this.transactionReadTimeoutMillis = transactionTimeoutMillis;
        this.readSentinelBehavior = readSentinelBehavior;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
    }

    // TEST ONLY
    SnapshotTransaction(KeyValueService keyValueService,
                        RemoteLockService lockService,
                        TimestampService timestampService,
                        TransactionService transactionService,
                        Cleaner cleaner,
                        long startTimeStamp,
                        Map<String, ConflictHandler> tablesToWriteWrite,
                        AtlasDbConstraintCheckingMode constraintCheckingMode,
                        TransactionReadSentinelBehavior readSentinelBehavior) {
        this.keyValueService = keyValueService;
        this.timestampService = timestampService;
        this.defaultTransactionService = transactionService;
        this.cleaner = cleaner;
        this.lockService = lockService;
        this.startTimestamp = Suppliers.ofInstance(startTimeStamp);
        this.conflictDetectionManager = ConflictDetectionManagers.fromMap(tablesToWriteWrite);
        this.sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);
        this.immutableTimestamp = 0;
        this.externalLocksTokens = ImmutableSet.of();
        this.constraintCheckingMode = constraintCheckingMode;
        this.transactionReadTimeoutMillis = null;
        this.readSentinelBehavior = readSentinelBehavior;
        this.allowHiddenTableAccess = false;
    }

    @Deprecated
    public static SnapshotTransaction createReadOnly(KeyValueService keyValueService,
                                                     TransactionService transactionService,
                                                     RemoteLockService lockService,
                                                     long startTimeStamp,
                                                     AtlasDbConstraintCheckingMode constraintCheckingEnabled) {
        return new SnapshotTransaction(
                keyValueService,
                transactionService,
                lockService,
                startTimeStamp,
                constraintCheckingEnabled,
                TransactionReadSentinelBehavior.THROW_EXCEPTION);
    }

    /**
     * Used for read only transactions and subclasses that are read only and
     * bypass aspects of the transaction protocol.
     */
    protected SnapshotTransaction(KeyValueService keyValueService,
                                  TransactionService transactionService,
                                  RemoteLockService lockService,
                                  long startTimeStamp,
                                  AtlasDbConstraintCheckingMode constraintCheckingMode,
                                  TransactionReadSentinelBehavior readSentinelBehavior) {
        this(keyValueService, transactionService, lockService, startTimeStamp, constraintCheckingMode, readSentinelBehavior, false);
    }

    protected SnapshotTransaction(KeyValueService keyValueService,
                                  TransactionService transactionService,
                                  RemoteLockService lockService,
                                  long startTimeStamp,
                                  AtlasDbConstraintCheckingMode constraintCheckingMode,
                                  TransactionReadSentinelBehavior readSentinelBehavior,
                                  boolean allowHiddenTableAccess) {
        this.keyValueService = keyValueService;
        this.defaultTransactionService = transactionService;
        this.cleaner = NoOpCleaner.INSTANCE;
        this.lockService = lockService;
        this.startTimestamp = Suppliers.ofInstance(startTimeStamp);
        this.conflictDetectionManager = ConflictDetectionManagers.withoutConflictDetection(keyValueService);
        this.sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);
        this.timestampService = null;
        this.immutableTimestamp = startTimeStamp;
        this.externalLocksTokens = ImmutableSet.of();
        this.constraintCheckingMode = constraintCheckingMode;
        this.transactionReadTimeoutMillis = null;
        this.readSentinelBehavior = readSentinelBehavior;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
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

    protected void checkGetPreconditions(String tableName) {
        if (transactionReadTimeoutMillis != null && System.currentTimeMillis() - timeCreated > transactionReadTimeoutMillis) {
            throw new TransactionFailedRetriableException("Transaction timed out.");
        }
        Preconditions.checkArgument(allowHiddenTableAccess || !AtlasDbConstants.hiddenTables.contains(tableName));
        Preconditions.checkState(state.get() == State.UNCOMMITTED || state.get() == State.COMMITTING,
                "Transaction must be uncommitted.");
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRows(String tableName, Iterable<byte[]> rows,
                                                        ColumnSelection columnSelection) {
        Stopwatch watch = Stopwatch.createStarted();
        checkGetPreconditions(tableName);
        if (Iterables.isEmpty(rows)) {
            return AbstractTransaction.EMPTY_SORTED_ROWS;
        }
        Map<Cell, byte[]> result = Maps.newHashMap();
        Map<Cell, Value> rawResults = Maps.newHashMap(
                keyValueService.getRows(tableName, rows, columnSelection, getStartTimestamp()));
        SortedMap<Cell, byte[]> writes = writesByTable.get(tableName);
        if (writes != null) {
            for (byte[] row : rows) {
                extractLocalWritesForRow(result, writes, row);
            }
        }

        // We don't need to do work postfiltering if we have a write locally.
        rawResults.keySet().removeAll(result.keySet());

        SortedMap<byte[], RowResult<byte[]>> results = filterRowResults(tableName, rawResults, result);
        if (perfLogger.isDebugEnabled()) {
            perfLogger.debug("getRows({}, {} rows) found {} rows, took {} ms",
                    tableName, Iterables.size(rows), results.size(), watch.elapsed(TimeUnit.MILLISECONDS));
        }
        validateExternalAndCommitLocksIfNecessary(tableName);
        return results;
    }

    @Override
    public SortedMap<byte[], RowResult<byte[]>> getRowsIgnoringLocalWrites(String tableName, Iterable<byte[]> rows) {
        checkGetPreconditions(tableName);
        if (Iterables.isEmpty(rows)) {
            return AbstractTransaction.EMPTY_SORTED_ROWS;
        }

        Map<Cell, Value> rawResults = Maps.newHashMap(keyValueService.getRows(tableName,
                rows,
                ColumnSelection.all(),
                getStartTimestamp()));

        return filterRowResults(tableName, rawResults, Maps.<Cell, byte[]>newHashMap());
    }

    private SortedMap<byte[], RowResult<byte[]>> filterRowResults(String tableName,
                                                                  Map<Cell, Value> rawResults,
                                                                  Map<Cell, byte[]> result) {
        getWithPostfiltering(tableName, rawResults, result, Value.GET_VALUE);
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
            Entry<Cell, byte[]> e = it.next();
            Cell cell = e.getKey();
            if (!Arrays.equals(row, cell.getRowName())) {
                break;
            }
            result.put(cell, e.getValue());
        }
    }

    @Override
    public Map<Cell, byte[]> get(String tableName, Set<Cell> cells) {
        Stopwatch watch = Stopwatch.createStarted();
        checkGetPreconditions(tableName);
        if (Iterables.isEmpty(cells)) { return ImmutableMap.of(); }

        Map<Cell, byte[]> result = Maps.newHashMap();
        SortedMap<Cell, byte[]> writes = writesByTable.get(tableName);
        if (writes != null) {
            for (Cell cell : cells) {
                if (writes.containsKey(cell)) {
                    result.put(cell, writes.get(cell));
                }
            }
        }

        // We don't need to read any cells that were written locally.
        result.putAll(getFromKeyValueService(tableName, Sets.difference(cells, result.keySet())));

        if (perfLogger.isDebugEnabled()) {
            perfLogger.debug("get({}, {} cells) found {} cells (some possibly deleted), took {} ms",
                    tableName, cells.size(), result.size(), watch.elapsed(TimeUnit.MILLISECONDS));
        }
        validateExternalAndCommitLocksIfNecessary(tableName);
        return Maps.filterValues(result, Predicates.not(Value.IS_EMPTY));
    }

    @Override
    public Map<Cell, byte[]> getIgnoringLocalWrites(String tableName, Set<Cell> cells) {
        checkGetPreconditions(tableName);
        if (Iterables.isEmpty(cells)) { return ImmutableMap.of(); }

        Map<Cell, byte[]> result = getFromKeyValueService(tableName, cells);

        return Maps.filterValues(result, Predicates.not(Value.IS_EMPTY));
    }

    /**
     * This will load the given keys from the underlying key value service and apply postfiltering
     * so we have snapshot isolation.  If the value in the key value service is the empty array
     * this will be included here and needs to be filtered out.
     */
    private Map<Cell, byte[]> getFromKeyValueService(String tableName, Set<Cell> cells) {
        Map<Cell, byte[]> result = Maps.newHashMap();
        Map<Cell, Long> toRead = Cells.constantValueMap(cells, getStartTimestamp());
        Map<Cell, Value> rawResults = keyValueService.get(tableName, toRead);
        getWithPostfiltering(tableName, rawResults, result, Value.GET_VALUE);
        return result;
    }

    private static byte[] getNextStartRowName(RangeRequest range, TokenBackedBasicResultsPage<RowResult<Value>, byte[]> prePostFilter) {
        if (!prePostFilter.moreResultsAvailable()) {
            return range.getEndExclusive();
        }
        return prePostFilter.getTokenForNextPage();
    }


    @Override
    public Iterable<BatchingVisitable<RowResult<byte[]>>> getRanges(final String tableName,
                                                                    Iterable<RangeRequest> rangeRequests) {
        checkGetPreconditions(tableName);

        if (perfLogger.isDebugEnabled()) {
            perfLogger.debug("Passed {} ranges to getRanges({}, {})",
                    Iterables.size(rangeRequests), tableName, rangeRequests);
        }

        return FluentIterable.from(Iterables.partition(rangeRequests, BATCH_SIZE_GET_FIRST_PAGE))
                .transformAndConcat(new Function<List<RangeRequest>, List<BatchingVisitable<RowResult<byte[]>>>>() {
                    @Override
                    public List<BatchingVisitable<RowResult<byte[]>>> apply(List<RangeRequest> input) {
                        Stopwatch timer = Stopwatch.createStarted();
                        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> firstPages = keyValueService.getFirstBatchForRanges(
                                tableName,
                                input,
                                getStartTimestamp());
                        validateExternalAndCommitLocksIfNecessary(tableName);

                        final SortedMap<Cell, byte[]> postFiltered = postFilterPages(
                                tableName,
                                firstPages.values());

                        List<BatchingVisitable<RowResult<byte[]>>> ret = Lists.newArrayListWithCapacity(input.size());
                        for (final RangeRequest rangeRequest : input) {
                            TokenBackedBasicResultsPage<RowResult<Value>, byte[]> prePostFilter = firstPages.get(rangeRequest);
                            final byte[] nextStartRowName = getNextStartRowName(
                                    rangeRequest,
                                    prePostFilter);
                            final List<Entry<Cell, byte[]>> mergeIterators = getPostfilteredWithLocalWrites(
                                    tableName,
                                    postFiltered,
                                    rangeRequest,
                                    prePostFilter.getResults(),
                                    nextStartRowName);
                            ret.add(new AbstractBatchingVisitable<RowResult<byte[]>>() {
                                @Override
                                protected <K extends Exception> void batchAcceptSizeHint(int batchSizeHint,
                                                                                         ConsistentVisitor<RowResult<byte[]>, K> v)
                                                                                                 throws K {
                                    checkGetPreconditions(tableName);
                                    final Iterator<RowResult<byte[]>> rowResults = Cells.createRowView(mergeIterators);
                                    while (rowResults.hasNext()) {
                                        if (!v.visit(ImmutableList.of(rowResults.next()))) {
                                            return;
                                        }
                                    }
                                    if (nextStartRowName.length == 0) {
                                        return;
                                    }
                                    RangeRequest newRange = rangeRequest.getBuilder()
                                            .startRowInclusive(nextStartRowName)
                                            .build();
                                    getRange(tableName, newRange)
                                            .batchAccept(batchSizeHint, v);
                                }
                            });
                        }
                        log.info("Processed {} range requests for {} in {}ms",
                                input.size(), tableName, timer.elapsed(TimeUnit.MILLISECONDS));
                        return ret;
                    }

                });
    }

    private void validateExternalAndCommitLocksIfNecessary(String tableName) {
        if (!isValidationNecessary(tableName)) {
            return;
        }
        throwIfExternalAndCommitLocksNotValid(null);
    }

    private boolean isValidationNecessary(String tableName) {
        return sweepStrategyManager.get().get(tableName) == SweepStrategy.THOROUGH;
    }

    private List<Entry<Cell, byte[]>> getPostfilteredWithLocalWrites(final String tableName,
                                                                     final SortedMap<Cell, byte[]> postFiltered,
                                                                     final RangeRequest rangeRequest,
                                                                     List<RowResult<Value>> prePostFilter,
                                                                     final byte[] endRowExclusive) {
        Map<Cell, Value> prePostFilterCells = Cells.convertRowResultsToCells(prePostFilter);
        Collection<Entry<Cell, byte[]>> postFilteredCells = Collections2.filter(
                postFiltered.entrySet(),
                Predicates.compose(
                        Predicates.in(prePostFilterCells.keySet()),
                        MapEntries.<Cell, byte[]> getKeyFunction()));
        Collection<Entry<Cell, byte[]>> localWritesInRange = getLocalWritesForRange(
                tableName,
                rangeRequest.getStartInclusive(),
                endRowExclusive).entrySet();
        return ImmutableList.copyOf(mergeInLocalWrites(
                postFilteredCells.iterator(),
                localWritesInRange.iterator(),
                rangeRequest.isReverse()));
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(final String tableName,
                                                         final RangeRequest range) {
        checkGetPreconditions(tableName);
        if (range.isEmptyRange()) {
            return BatchingVisitables.emptyBatchingVisitable();
        }

        return new AbstractBatchingVisitable<RowResult<byte[]>>() {
            @Override
            public <K extends Exception> void batchAcceptSizeHint(int userRequestedSize,
                                                                  ConsistentVisitor<RowResult<byte[]>, K> v)
                    throws K {
                Preconditions.checkState(state.get() == State.UNCOMMITTED,
                        "Transaction must be uncommitted.");
                if (range.getBatchHint() != null) {
                    userRequestedSize = range.getBatchHint();
                }

                int preFilterBatchSize = getRequestHintToKvStore(userRequestedSize);

                Validate.isTrue(!range.isReverse(), "we currently do not support reverse ranges");
                getBatchingVisitableFromIterator(
                        tableName,
                        range,
                        userRequestedSize,
                        v,
                        preFilterBatchSize);
            }

        };
    }

    private <K extends Exception> boolean getBatchingVisitableFromIterator(final String tableName,
                                                                           RangeRequest range,
                                                                           int userRequestedSize,
                                                                           AbortingVisitor<List<RowResult<byte[]>>, K> v,
                                                                           int preFilterBatchSize) throws K {
        ClosableIterator<RowResult<byte[]>> postFilterIterator =
                postFilterIterator(tableName, range, preFilterBatchSize, Value.GET_VALUE);
        try {
            Iterator<RowResult<byte[]>> localWritesInRange =
                    Cells.createRowView(getLocalWritesForRange(tableName, range.getStartInclusive(), range.getEndExclusive()).entrySet());
            Iterator<RowResult<byte[]>> mergeIterators =
                    mergeInLocalWritesRows(postFilterIterator, localWritesInRange, range.isReverse());
            return BatchingVisitableFromIterable.create(mergeIterators).batchAccept(userRequestedSize, v);
        } finally {
            postFilterIterator.close();
        }
    }

    protected static int getRequestHintToKvStore(int userRequestedSize) {
        if (userRequestedSize == 1) {
            // Handle 1 specially because the underlying store could have an optimization for 1
            return 1;
        }
        //TODO: carrino: tune the param here based on how likely we are to post filter
        // rows out and have deleted rows
        int preFilterBatchSize = userRequestedSize + ((userRequestedSize+9)/10);
        if (preFilterBatchSize > AtlasDbPerformanceConstants.MAX_BATCH_SIZE
                || preFilterBatchSize < 0) {
            preFilterBatchSize = AtlasDbPerformanceConstants.MAX_BATCH_SIZE;
        }
        return preFilterBatchSize;
    }

    private static Iterator<RowResult<byte[]>> mergeInLocalWritesRows(Iterator<RowResult<byte[]>> postFilterIterator,
                                                                           Iterator<RowResult<byte[]>> localWritesInRange,
                                                                           boolean isReverse) {
        Ordering<RowResult<byte[]>> ordering = RowResult.<byte[]>getOrderingByRowName();
        Iterator<RowResult<byte[]>> mergeIterators = IteratorUtils.mergeIterators(
            postFilterIterator, localWritesInRange,
            isReverse ? ordering.reverse() : ordering,
            new Function<Pair<RowResult<byte[]>, RowResult<byte[]>>, RowResult<byte[]>>() {
                @Override
                public RowResult<byte[]> apply(Pair<RowResult<byte[]>,RowResult<byte[]>> from) {
                    // prefer local writes
                    return RowResults.merge(from.lhSide, from.rhSide);
                }
            });
        return RowResults.filterDeletedColumnsAndEmptyRows(mergeIterators);
    }

    private static Iterator<Entry<Cell, byte[]>> mergeInLocalWrites(Iterator<Entry<Cell, byte[]>> postFilterIterator,
                                                                         Iterator<Entry<Cell, byte[]>> localWritesInRange,
                                                                         boolean isReverse) {
        Ordering<Entry<Cell, byte[]>> ordering = Ordering.natural().onResultOf(MapEntries.<Cell, byte[]>getKeyFunction());
        Iterator<Entry<Cell, byte[]>> mergeIterators = IteratorUtils.mergeIterators(
                postFilterIterator, localWritesInRange,
                isReverse ? ordering.reverse() : ordering,
                new Function<Pair<Entry<Cell, byte[]>, Entry<Cell, byte[]>>, Entry<Cell, byte[]>>() {
                    @Override
                    public Map.Entry<Cell, byte[]> apply(Pair<Map.Entry<Cell, byte[]>, Map.Entry<Cell, byte[]>> from) {
                        // always override their value with written values
                        return from.rhSide;
                    }
                });
        return Iterators.filter(mergeIterators,
            Predicates.compose(Predicates.not(Value.IS_EMPTY), MapEntries.<Cell, byte[]>getValueFunction()));
    }

    protected <T> ClosableIterator<RowResult<T>> postFilterIterator(final String tableName,
                                                                    RangeRequest range,
                                                                    int preFilterBatchSize,
                                                                    final Function<Value, T> transformer) {
        final BatchSizeIncreasingRangeIterator results = new BatchSizeIncreasingRangeIterator(tableName, range, preFilterBatchSize);
        Iterator<Iterator<RowResult<T>>> batchedPostfiltered = new AbstractIterator<Iterator<RowResult<T>>>() {
            @Override
            protected Iterator<RowResult<T>> computeNext() {
                List<RowResult<Value>> batch = results.getBatch();
                if (batch.isEmpty()) {
                    return endOfData();
                }
                SortedMap<Cell, T> postFilter = postFilterRows(tableName, batch, transformer);
                results.markNumRowsNotDeleted(Cells.getRows(postFilter.keySet()).size());
                return Cells.createRowView(postFilter.entrySet());
            }
        };

        final Iterator<RowResult<T>> rows = Iterators.concat(batchedPostfiltered);
        return new ForwardingClosableIterator<RowResult<T>>() {
            @Override
            protected ClosableIterator<RowResult<T>> delegate() {
                return ClosableIterators.wrap(rows);
            }

            @Override
            public void close() {
                if (results != null) {
                    results.close();
                }
            }
        };
    }

    private class BatchSizeIncreasingRangeIterator {
        final String tableName;
        final RangeRequest range;
        final int originalBatchSize;

        long numReturned = 0;
        long numNotDeleted = 0;

        ClosableIterator<RowResult<Value>> results = null;
        int lastBatchSize;
        byte[] lastRow = null;

        public BatchSizeIncreasingRangeIterator(String tableName,
                                                RangeRequest range,
                                                int originalBatchSize) {
            Validate.isTrue(originalBatchSize > 0);
            this.tableName = tableName;
            this.range = range;
            this.originalBatchSize = originalBatchSize;
        }

        public void markNumRowsNotDeleted(int rowsInBatch) {
            numNotDeleted += rowsInBatch;
            AssertUtils.assertAndLog(numNotDeleted <= numReturned, "NotDeleted is bigger than the number of rows we returned.");
        }

        int getBestBatchSize() {
            if (numReturned == 0) {
                return originalBatchSize;
            }
            final long batchSize;
            if (numNotDeleted == 0) {
                // If everything we've seen has been deleted, we should be aggressive about getting more rows.
                batchSize = numReturned*4;
            } else {
                batchSize = (long)Math.ceil(originalBatchSize * (numReturned / (double)numNotDeleted));
            }
            return (int)Math.min(batchSize, AtlasDbPerformanceConstants.MAX_BATCH_SIZE);
        }

        private void updateResultsIfNeeded() {
            if (results == null) {
                results = keyValueService.getRange(tableName, range.withBatchHint(originalBatchSize), getStartTimestamp());
                lastBatchSize = originalBatchSize;
                return;
            }

            Validate.isTrue(lastRow != null);

            // If the last row we got was the maximal row, then we are done.
            if (RangeRequests.isTerminalRow(range.isReverse(), lastRow)) {
                results = ClosableIterators.wrap(ImmutableList.<RowResult<Value>>of().iterator());
                return;
            }

            int bestBatchSize = getBestBatchSize();
            // Only close and throw away our old iterator if the batch size has changed by a factor of 2 or more.
            if (bestBatchSize >= lastBatchSize*2 || bestBatchSize <= lastBatchSize/2) {
                RangeRequest.Builder newRange = range.getBuilder();
                newRange.startRowInclusive(RangeRequests.getNextStartRow(range.isReverse(), lastRow));
                newRange.batchHint(bestBatchSize);
                results.close();
                results = keyValueService.getRange(tableName, newRange.build(), getStartTimestamp());
                lastBatchSize = bestBatchSize;
            }
        }

        public List<RowResult<Value>> getBatch() {
            updateResultsIfNeeded();
            Validate.isTrue(lastBatchSize > 0);
            ImmutableList<RowResult<Value>> list = ImmutableList.copyOf(Iterators.limit(results, lastBatchSize));
            numReturned += list.size();
            if (!list.isEmpty()) {
                lastRow = list.get(list.size()-1).getRowName();
            }
            return list;
        }

        public void close() {
            if (results != null) {
                results.close();
            }
        }

    }

    private ConcurrentNavigableMap<Cell, byte[]> getLocalWrites(String tableName) {
        ConcurrentNavigableMap<Cell, byte[]> writes = writesByTable.get(tableName);
        if (writes == null) {
            writes = new ConcurrentSkipListMap<Cell, byte[]>();
            ConcurrentNavigableMap<Cell, byte[]> previous = writesByTable.putIfAbsent(tableName, writes);
            if (previous != null) {
                writes = previous;
            }
        }
        return writes;
    }

    /**
     * This includes deleted writes as zero length byte arrays, be sure to strip them out.
     */
    private SortedMap<Cell, byte[]> getLocalWritesForRange(String tableName, byte[] startRow, byte[] endRow) {
        SortedMap<Cell, byte[]> writes = getLocalWrites(tableName);
        if (startRow.length != 0) {
            writes = writes.tailMap(Cells.createSmallestCellForRow(startRow));
        }
        if (endRow.length != 0) {
            writes = writes.headMap(Cells.createSmallestCellForRow(endRow));
        }
        return writes;
    }

    private SortedMap<Cell, byte[]> postFilterPages(String tableName,
            Iterable<TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> rangeRows) {
        List<RowResult<Value>> results = Lists.newArrayList();
        for (TokenBackedBasicResultsPage<RowResult<Value>, byte[]> page : rangeRows) {
            results.addAll(page.getResults());
        }
        return postFilterRows(tableName, results, Value.GET_VALUE);
    }

    private <T> SortedMap<Cell, T> postFilterRows(String tableName,
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
        getWithPostfiltering(tableName, rawResults, postFilter, transformer);
        return postFilter;
    }

    private int estimateSize(List<RowResult<Value>> rangeRows) {
        int estimatedSize = 0;
        for (RowResult<Value> rowResult : rangeRows) {
            estimatedSize += rowResult.getColumns().size();
        }
        return estimatedSize;
    }

    private <T> void getWithPostfiltering(String tableName,
                                          Map<Cell, Value> rawResults,
                                          @Output Map<Cell, T> results,
                                          Function<Value, T> transformer) {
        long bytes = 0;
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            bytes += e.getValue().getContents().length + Cells.getApproxSizeOfCell(e.getKey());
        }
        if (bytes > TransactionConstants.ERROR_LEVEL_FOR_QUEUED_BYTES && !AtlasDbConstants.TABLES_KNOWN_TO_BE_POORLY_DESIGNED.contains(tableName)) {
            log.error("A single get had a lot of bytes: " + bytes + " for table " + tableName + ". "
                    + "The number of results was " + rawResults.size() + ". "
                    + "The first 10 results were " + Iterables.limit(rawResults.entrySet(), 10) + ". "
                    + "This can potentially cause out-of-memory errors.",
                    new RuntimeException("This exception and stack trace are provided for debugging purposes."));
        } else if (bytes > TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES && log.isWarnEnabled()) {
            log.warn("A single get had quite a few bytes: " + bytes + " for table " + tableName + ". "
                    + "The number of results was " + rawResults.size() + ". "
                    + "The first 10 results were " + Iterables.limit(rawResults.entrySet(), 10) + ". ",
                    new RuntimeException("This exception and stack trace are provided for debugging purposes."));
        }

        if (isTempTable(tableName) || (AtlasDbConstants.SKIP_POSTFILTER_TABLES.contains(tableName) && allowHiddenTableAccess)) {
            // If we are reading from a temp table, we can just bypass postfiltering
            // or skip postfiltering if reading the transaction or namespace table from atlasdb shell
            for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
                results.put(e.getKey(), transformer.apply(e.getValue()));
            }
            return;
        }
        while (!rawResults.isEmpty()) {
            rawResults = getWithPostfilteringInternal(tableName, rawResults, results, transformer);
        }
    }

    /**
     * This will return all the keys that still need to be postfiltered.  It will output properly
     * postfiltered keys to the results output param.
     */
    private <T> Map<Cell, Value> getWithPostfilteringInternal(String tableName,
                                                              Map<Cell, Value> rawResults,
                                                              @Output Map<Cell, T> results,
                                                              Function<Value, T> transformer) {
        Set<Long> startTimestampsForValues = getStartTimestampsForValues(rawResults.values());
        Map<Long, Long> commitTimestamps = getCommitTimestamps(tableName, startTimestampsForValues, true);
        Map<Cell, Long> keysToReload = Maps.newHashMapWithExpectedSize(0);
        Map<Cell, Long> keysToDelete = Maps.newHashMapWithExpectedSize(0);
        for (Map.Entry<Cell, Value> e :  rawResults.entrySet()) {
            Cell key = e.getKey();
            Value value = e.getValue();

            if (value.getTimestamp() == Value.INVALID_VALUE_TIMESTAMP) {
                // This means that this transaction started too long ago. When we do garbage collection,
                // we clean up old values, and this transaction started at a timestamp before the garbage collection.
                switch (getReadSentinelBehavior()) {
                    case IGNORE:
                        break;
                    case THROW_EXCEPTION:
                        throw new TransactionFailedRetriableException("Tried to read a value that has been deleted. " +
                                " This can be caused by hard delete transactions using the type " +
                                TransactionType.AGGRESSIVE_HARD_DELETE +
                                ". It can also be caused by transactions taking too long, or" +
                                " its locks expired. Retrying it should work.");
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
                    }
                } else if (theirCommitTimestamp > getStartTimestamp()) {
                    // The value's commit timestamp is after our start timestamp.
                    // This means the value is from a transaction which committed
                    // after our transaction began. We need to try reading at an
                    // earlier timestamp.
                    keysToReload.put(key, value.getTimestamp());
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
            if (!rollbackFailedTransactions(tableName, keysToDelete, commitTimestamps, defaultTransactionService)) {
                return rawResults;
            }
        }

        if (!keysToReload.isEmpty()) {
            Map<Cell, Value> nextRawResults = keyValueService.get(tableName, keysToReload);
            return nextRawResults;
        } else {
            return ImmutableMap.of();
        }
    }

    /**
     * This is protected to allow for different post filter behavior.
     */
    protected boolean shouldDeleteAndRollback() {
        Validate.notNull(lockService, "if we don't have a valid lock server we can't roll back transactions");
        return true;
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values) {
        put(tableName, values, Cell.INVALID_TTL, Cell.INVALID_TTL_TYPE);
    }

    public void put(String tableName, Map<Cell, byte[]> values, long ttlDuration, TimeUnit ttlUnit) {
        Preconditions.checkArgument(!AtlasDbConstants.hiddenTables.contains(tableName));
        // todo (clockfort) also check if valid table for TTL
        if (ttlDuration != Cell.INVALID_TTL && ttlUnit != Cell.INVALID_TTL_TYPE) {
            values = createExpiringValues(values, ttlDuration, ttlUnit);
        }

        if (!validConflictDetection(tableName)) {
            conflictDetectionManager.recompute();
            Preconditions.checkArgument(validConflictDetection(tableName),
                    "Not a valid table for this transaction.  Make sure this table name has a namespace: " + tableName);
        }
        Validate.isTrue(isTempTable(tableName) || getAllTempTables().isEmpty(),
                "Temp tables may only be used by read only transactions.");
        if (values.isEmpty()) {
            return;
        }

        numWriters.incrementAndGet();
        try {
            // We need to check the status after incrementing writers to ensure that we fail if we are committing.
            Preconditions.checkState(state.get() == State.UNCOMMITTED, "Transaction must be uncommitted.");

            ConcurrentNavigableMap<Cell, byte[]> writes = getLocalWrites(tableName);

            if (isTempTable(tableName)) {
                putTempTableWrites(tableName, values, writes);
            } else {
                putWritesAndLogIfTooLarge(values, writes);
            }
        } finally {
            numWriters.decrementAndGet();
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

    private boolean validConflictDetection(String tableName) {
        if (isTempTable(tableName)) {
            return true;
        }
        return conflictDetectionManager.isEmptyOrContainsTable(tableName);
    }

    private void putWritesAndLogIfTooLarge(Map<Cell, byte[]> values, SortedMap<Cell, byte[]> writes) {
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            byte[] val = e.getValue();
            if (val == null) {
                val = PtBytes.EMPTY_BYTE_ARRAY;
            }
            Cell cell = e.getKey();
            if (writes.put(cell, val) == null) {
                long toAdd = val.length + Cells.getApproxSizeOfCell(cell);
                long newVal = byteCount.addAndGet(toAdd);
                if (newVal >= TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES
                        && newVal - toAdd < TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES) {
                    log.warn("A single transaction has put quite a few bytes: " + newVal,
                            new RuntimeException("This exception and stack trace are provided for debugging purposes."));
                }
                if (newVal >= TransactionConstants.ERROR_LEVEL_FOR_QUEUED_BYTES
                        && newVal - toAdd < TransactionConstants.ERROR_LEVEL_FOR_QUEUED_BYTES) {
                    log.warn("A single transaction has put too many bytes: " + newVal + ". This can potentially cause" +
                            "out-of-memory errors.",
                            new RuntimeException("This exception and stack trace are provided for debugging purposes."));
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
                dropTempTables();
                if (hasWrites()) {
                    throwIfExternalAndCommitLocksNotValid(null);
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
        if (getTransactionType() == TransactionType.AGGRESSIVE_HARD_DELETE ||
                getTransactionType() == TransactionType.HARD_DELETE) {
            cleaner.queueCellsForScrubbing(getCellsToQueueForScrubbing(), getStartTimestamp());
        }

        boolean success = false;
        try {
            if (numWriters.get() > 0) {
                // After we set state to committing we need to make sure no one is still writing.
                throw new IllegalStateException("Cannot commit while other threads are still calling put.");
            }

            if (!getAllTempTables().isEmpty()) {
                dropTempTables();
                Validate.isTrue(getAllTempTables().containsAll(writesByTable.keySet()),
                        "Temp tables may only be used by read only transactions.");
            } else {
                checkConstraints();
                commitWrites(transactionService);
            }
            perfLogger.debug("Commited transaction {} in {}ms",
                    getStartTimestamp(),
                    getTrasactionTimer().elapsed(TimeUnit.MILLISECONDS));
            success = true;
        } finally {
            // Once we are in state committing, we need to try/finally to set the state to a terminal state.
            state.set(success ? State.COMMITTED : State.FAILED);
        }
    }

    private void checkConstraints() {
        List<String> violations = Lists.newArrayList();
        for (Map.Entry<String, ConstraintCheckable> entry : constraintsByTableName.entrySet()) {
            SortedMap<Cell, byte[]> sortedMap = writesByTable.get(entry.getKey());
            if (sortedMap != null) {
                violations.addAll(entry.getValue().findConstraintFailures(sortedMap, this, constraintCheckingMode));
            }
        }
        if (!violations.isEmpty()) {
            if(constraintCheckingMode.shouldThrowException()) {
                throw new AtlasDbConstraintException(violations);
            } else {
                constraintLogger.error("Constraint failure on commit.",
                        new AtlasDbConstraintException(violations));
            }
        }
    }

    private void commitWrites(TransactionService transactionService) {
        if (!hasWrites()) {
            return;
        }
        Stopwatch watch = Stopwatch.createStarted();
        LockRefreshToken commitLocksToken = acquireLocksForCommit();
        long millisForLocks = watch.elapsed(TimeUnit.MILLISECONDS);
        try {
            watch.reset().start();
            throwIfConflictOnCommit(commitLocksToken, transactionService);
            long millisCheckingForConflicts = watch.elapsed(TimeUnit.MILLISECONDS);

            watch.reset().start();
            keyValueService.multiPut(writesByTable, getStartTimestamp());
            long millisForWrites = watch.elapsed(TimeUnit.MILLISECONDS);

            // Now that all writes are done, get the commit timestamp
            // We must do this before we check that our locks are still valid to ensure that
            // other transactions that will hold these locks are sure to have start
            // timestamps after our commit timestamp.
            long commitTimestamp = timestampService.getFreshTimestamp();
            commitTsForScrubbing = commitTimestamp;

            // punch on commit so that if hard delete is the only thing happening on a system,
            // we won't block forever waiting for the unreadable timestamp to advance past the
            // scrub timestamp (same as the hard delete transaction's start timestamp)
            watch.reset().start();
            cleaner.punch(commitTimestamp);
            long millisForPunch = watch.elapsed(TimeUnit.MILLISECONDS);

            throwIfReadWriteConflictForSerializable(commitTimestamp);

            // Verify that our locks are still valid before we actually commit;
            // this check is required by the transaction protocol for correctness
            throwIfExternalAndCommitLocksNotValid(commitLocksToken);

            watch.reset().start();
            putCommitTimestamp(commitTimestamp, commitLocksToken, transactionService);
            long millisForCommitTs = watch.elapsed(TimeUnit.MILLISECONDS);

            Set<LockRefreshToken> expiredLocks = refreshExternalAndCommitLocks(commitLocksToken);
            if (!expiredLocks.isEmpty()) {
                String errorMessage =
                    "This isn't a bug but it should happen very infrequently.  Required locks are no longer" +
                    " valid but we have already committed successfully.  " + getExpiredLocksErrorString(commitLocksToken, expiredLocks);
                log.error(errorMessage, new TransactionFailedRetriableException(errorMessage));
            }
            long millisSinceCreation = System.currentTimeMillis() - timeCreated;
            if (perfLogger.isDebugEnabled()) {
                perfLogger.debug("Committed {} bytes with locks, start ts {}, commit ts {}, " +
                        "acquiring locks took {} ms, checking for conflicts took {} ms, " +
                        "writing took {} ms, punch took {} ms, putCommitTs took {} ms, " +
                        "total time since tx creation {} ms, tables: {}.",
                        byteCount.get(), getStartTimestamp(),
                        commitTimestamp, millisForLocks, millisCheckingForConflicts, millisForWrites,
                        millisForPunch, millisForCommitTs, millisSinceCreation, writesByTable.keySet());
            }
        } finally {
            lockService.unlock(commitLocksToken);
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

    protected ConflictHandler getConflictHandlerForTable(String tableName) {
        Map<String, ConflictHandler> tableToConflictHandler = conflictDetectionManager.get();
        if (tableToConflictHandler.isEmpty()) {
            return ConflictHandler.RETRY_ON_WRITE_WRITE;
        }
        return tableToConflictHandler.get(tableName);
    }

    private String getExpiredLocksErrorString(@Nullable LockRefreshToken commitLocksToken,
                                              Set<LockRefreshToken> expiredLocks) {
        return "The following external locks were required: " + externalLocksTokens +
            "; the following commit locks were required: " + commitLocksToken +
            "; the following locks are no longer valid: " + expiredLocks;
    }

    private void throwIfExternalAndCommitLocksNotValid(@Nullable LockRefreshToken commitLocksToken) {
        Set<LockRefreshToken> expiredLocks = refreshExternalAndCommitLocks(commitLocksToken);
        if (!expiredLocks.isEmpty()) {
            String errorMessage =
                "Required locks are no longer valid.  " + getExpiredLocksErrorString(commitLocksToken, expiredLocks);
            TransactionLockTimeoutException e = new TransactionLockTimeoutException(errorMessage);
            log.error(errorMessage, e);
            throw e;
       }
    }

    /**
     * @param commitLocksToken
     * @return set of locks that could not be refreshed
     */
    private Set<LockRefreshToken> refreshExternalAndCommitLocks(@Nullable LockRefreshToken commitLocksToken) {
        ImmutableSet<LockRefreshToken> toRefresh;
        if (commitLocksToken == null) {
            toRefresh = externalLocksTokens;
        } else {
            toRefresh = ImmutableSet.<LockRefreshToken>builder()
                    .addAll(externalLocksTokens)
                    .add(commitLocksToken).build();
        }
        if (toRefresh.isEmpty()) {
            return ImmutableSet.of();
        }

        return Sets.difference(toRefresh, lockService.refreshLockRefreshTokens(toRefresh)).immutableCopy();
    }

    /**
     * Make sure we have all the rows we are checking already locked before calling this.
     */
    protected void throwIfConflictOnCommit(LockRefreshToken commitLocksToken, TransactionService transactionService) throws TransactionConflictException {
        for (Entry<String, ConcurrentNavigableMap<Cell, byte[]>> write : writesByTable.entrySet()) {
            ConflictHandler conflictHandler = getConflictHandlerForTable(write.getKey());
            throwIfWriteAlreadyCommitted(write.getKey(), write.getValue(), conflictHandler, commitLocksToken, transactionService);
        }
    }

    protected void throwIfWriteAlreadyCommitted(String tableName,
                                                Map<Cell, byte[]> writes,
                                                ConflictHandler conflictHandler,
                                                LockRefreshToken commitLocksToken,
                                                TransactionService transactionService)
            throws TransactionConflictException {
        if (writes.isEmpty() || conflictHandler == ConflictHandler.IGNORE_ALL) {
            return;
        }
        Set<CellConflict> spanningWrites = Sets.newHashSet();
        Set<CellConflict> dominatingWrites = Sets.newHashSet();
        Map<Cell, Long> keysToLoad = Maps.asMap(writes.keySet(), Functions.constant(Long.MAX_VALUE));
        while (!keysToLoad.isEmpty()) {
            keysToLoad = detectWriteAlreadyCommittedInternal(tableName, keysToLoad, spanningWrites, dominatingWrites, transactionService);
        }

        if (conflictHandler == ConflictHandler.RETRY_ON_VALUE_CHANGED) {
            throwIfValueChangedConflict(tableName, writes, spanningWrites, dominatingWrites, commitLocksToken);
        } else if (conflictHandler == ConflictHandler.RETRY_ON_WRITE_WRITE
                || conflictHandler == ConflictHandler.RETRY_ON_WRITE_WRITE_CELL
                || conflictHandler == ConflictHandler.SERIALIZABLE) {
            if (!spanningWrites.isEmpty() || !dominatingWrites.isEmpty()) {
                throw TransactionConflictException.create(tableName, getStartTimestamp(), spanningWrites,
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
    private void throwIfValueChangedConflict(String table,
                                             Map<Cell, byte[]> writes,
                                             Set<CellConflict> spanningWrites,
                                             Set<CellConflict> dominatingWrites,
                                             LockRefreshToken commitLocksToken) {
        Map<Cell, CellConflict> cellToConflict = Maps.newHashMap();
        Map<Cell, Long> cellToTs = Maps.newHashMap();
        for (CellConflict c : Sets.union(spanningWrites, dominatingWrites)) {
            cellToConflict.put(c.cell, c);
            cellToTs.put(c.cell, c.theirStart + 1);
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
                throwIfExternalAndCommitLocksNotValid(commitLocksToken);
                Validate.isTrue(false, "Missing conflicting value for cell: " + cellToConflict.get(cell)
                        + " for table " + table);
            }
            if (conflictingValues.get(cell).getTimestamp() != (cellEntry.getValue() - 1)) {
                // This error case could happen if our locks expired.
                throwIfExternalAndCommitLocksNotValid(commitLocksToken);
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
                log.info("Another transaction committed to the same cell before us but " +
                        "their value was the same. " + "Cell: "  + cell + " Table: " + table);
            }
        }
        if (conflictingCells.isEmpty()) {
            return;
        }
        Predicate<CellConflict> conflicting = Predicates.compose(Predicates.in(conflictingCells), CellConflict.getCellFunction());
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
    protected Map<Cell, Long> detectWriteAlreadyCommittedInternal(String tableName,
                                                                  Map<Cell, Long> keysToLoad,
                                                                  @Output Set<CellConflict> spanningWrites,
                                                                  @Output Set<CellConflict> dominatingWrites,
                                                                  TransactionService transactionService) {
        Map<Cell, Long> rawResults = keyValueService.getLatestTimestamps(tableName, keysToLoad);
        Map<Long, Long> commitTimestamps = getCommitTimestamps(tableName, rawResults.values(), false);
        Map<Cell, Long> keysToDelete = Maps.newHashMapWithExpectedSize(0);

        for (Map.Entry<Cell, Long> e : rawResults.entrySet()) {
            Cell key = e.getKey();
            long theirStartTimestamp = e.getValue();
            AssertUtils.assertAndLog(theirStartTimestamp != getStartTimestamp(),
                    "Timestamp reuse is bad:%d", getStartTimestamp());

            Long theirCommitTimestamp = commitTimestamps.get(theirStartTimestamp);
            if (theirCommitTimestamp == null
                    || theirCommitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
                // The value has no commit timestamp or was explicitly rolled back.
                // This means the value is garbage from a transaction which didn't commit.
                keysToDelete.put(key, theirStartTimestamp);
                continue;
            }

            AssertUtils.assertAndLog(theirCommitTimestamp != getStartTimestamp(),
                    "Timestamp reuse is bad:%d", getStartTimestamp());
            if (theirStartTimestamp > getStartTimestamp()) {
                dominatingWrites.add(Cells.createConflictWithMetadata(
                        keyValueService,
                        tableName,
                        key,
                        theirStartTimestamp,
                        theirCommitTimestamp));
            } else if (theirCommitTimestamp > getStartTimestamp()) {
                spanningWrites.add(Cells.createConflictWithMetadata(
                        keyValueService,
                        tableName,
                        key,
                        theirStartTimestamp,
                        theirCommitTimestamp));
            }
        }

        if (!keysToDelete.isEmpty()) {
            if (!rollbackFailedTransactions(tableName, keysToDelete, commitTimestamps, transactionService)) {
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
     * @return false if we cannot roll back the failed transactions because someone beat us to it.
     */
    private boolean rollbackFailedTransactions(String tableName,
            Map<Cell, Long> keysToDelete,  Map<Long, Long> commitTimestamps, TransactionService transactionService) {
        for (long startTs : Sets.newHashSet(keysToDelete.values())) {
            if (commitTimestamps.get(startTs) == null) {
                log.warn("Rolling back transaction: " + startTs);
                if (!rollbackOtherTransaction(startTs, transactionService)) {
                    return false;
                }
            } else {
                Validate.isTrue(commitTimestamps.get(startTs) == TransactionConstants.FAILED_COMMIT_TS);
            }
        }

        try {
            log.warn("For table: " + tableName + " we are deleting values of an uncommitted transaction: " + keysToDelete);
            keyValueService.delete(tableName, Multimaps.forMap(keysToDelete));
        } catch (RuntimeException e) {
            String msg = "This isn't a bug but it should be infrequent if all nodes of your KV service are running. "
                    + "Delete has stronger consistency semantics than read/write and must talk to all nodes "
                    + "instead of just talking to a quorum of nodes. "
                    + "Failed to delete keys for table" + tableName
                    + " from an uncommitted transaction: " + keysToDelete;
            log.error(msg, e);
        }


        return true;
    }

    /**
     * @return true if the other transaction was rolled back
     */
    private boolean rollbackOtherTransaction(long startTs, TransactionService transactionService) {
        try {
            transactionService.putUnlessExists(startTs, TransactionConstants.FAILED_COMMIT_TS);
            return true;
        } catch (KeyAlreadyExistsException e) {
                String msg = "Two transactions tried to roll back someone else's request with start: " + startTs;
                log.error("This isn't a bug but it should be very infrequent. " + msg, new TransactionFailedRetriableException(msg, e));
                return false;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    /// Locking
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This method should acquire any locks needed to do proper concurrency control at commit time.
     */
    protected LockRefreshToken acquireLocksForCommit() {
        SortedMap<LockDescriptor, LockMode> lockMap = getLocksForWrites();
        try {
            return lockService.lockAnonymously(LockRequest.builder(lockMap).build());
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    protected ImmutableSortedMap<LockDescriptor, LockMode> getLocksForWrites() {
        Builder<LockDescriptor, LockMode> builder = ImmutableSortedMap.naturalOrder();
        Iterable<String> allTables = IterableUtils.append(writesByTable.keySet(), TransactionConstants.TRANSACTION_TABLE);
        for (String tableName : allTables) {
            if (tableName.equals(TransactionConstants.TRANSACTION_TABLE)) {
                builder.put(AtlasRowLockDescriptor.of(TransactionConstants.TRANSACTION_TABLE, TransactionConstants.getValueForTimestamp(getStartTimestamp())), LockMode.WRITE);
                continue;
            }
            ConflictHandler conflictHandler = getConflictHandlerForTable(tableName);
            if (conflictHandler == ConflictHandler.RETRY_ON_WRITE_WRITE_CELL) {
                for (Cell cell : getLocalWrites(tableName).keySet()) {
                    builder.put(AtlasCellLockDescriptor.of(tableName, cell.getRowName(), cell.getColumnName()), LockMode.WRITE);
                }
            } else if (conflictHandler != ConflictHandler.IGNORE_ALL) {
                Cell lastCell = null;
                for (Cell cell : getLocalWrites(tableName).keySet()) {
                    if (lastCell == null || !Arrays.equals(lastCell.getRowName(), cell.getRowName())) {
                        builder.put(AtlasRowLockDescriptor.of(tableName, cell.getRowName()), LockMode.WRITE);
                    }
                    lastCell = cell;
                }
            }
        }
        return builder.build();
    }

    /**
     * We will block here until the passed transactions have released their lock.  This means that
     * the committing transaction is either complete or it has failed and we are allowed to roll
     * it back.
     */
    private void waitForCommitToComplete(Iterable<Long> startTimestamps) {
        boolean isEmpty = true;
        Builder<LockDescriptor, LockMode> builder = ImmutableSortedMap.naturalOrder();
        for (long start : startTimestamps) {
            if (start < immutableTimestamp) {
                // We don't need to block in this case because this transaction is already complete
                continue;
            }
            isEmpty = false;
            builder.put(AtlasRowLockDescriptor.of(TransactionConstants.TRANSACTION_TABLE, TransactionConstants.getValueForTimestamp(start)), LockMode.READ);
        }

        if (isEmpty) {
            return;
        }

        // TODO: This can have better performance if we have a blockAndReturn method in lock server
        // However lock server blocking is an issue if we fill up all our requests
        try {
            lockService.lockAnonymously(LockRequest.builder(builder.build()).lockAndRelease().build());
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
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
    protected Map<Long, Long> getCommitTimestamps(@Nullable String tableName,
                                                  Iterable<Long> startTimestamps,
                                                  boolean waitForCommitterToComplete) {
        if (Iterables.isEmpty(startTimestamps)) {
            return ImmutableMap.of();
        }
        Map<Long, Long> result = Maps.newHashMap();
        Set<Long> gets = Sets.newHashSet();
        for (long startTS : startTimestamps) {
            Long cached = cachedCommitTimes.get(startTS);
            if (cached != null) {
                result.put(startTS, cached);
            } else {
                gets.add(startTS);
            }
        }

        if (gets.isEmpty()) {
            return result;
        }

        // Before we do the reads, we need to make sure the committer is done writing.
        if (waitForCommitterToComplete) {
            Stopwatch watch = Stopwatch.createStarted();
            waitForCommitToComplete(startTimestamps);
            perfLogger.debug("Waited {} ms to get commit timestamps for table {}.",
                    watch.elapsed(TimeUnit.MILLISECONDS), tableName);
        }

        Map<Long, Long> rawResults = defaultTransactionService.get(gets);
        for (Map.Entry<Long, Long> e : rawResults.entrySet()) {
            if (e.getValue() != null) {
                long startTS = e.getKey();
                long commitTS = e.getValue();
                result.put(startTS, commitTS);
                cachedCommitTimes.put(startTS, commitTS);
            }
        }
        return result;
    }

    /**
     * This will attempt to put the commitTimestamp into the DB.
     *
     * @throws TransactionLockTimeoutException If our locks timed out while trying to commit.
     * @throws TransactionCommitFailedException failed when committing in a way that isn't retriable
     */
    private void putCommitTimestamp(long commitTimestamp, LockRefreshToken locksToken, TransactionService transactionService) throws TransactionFailedException {
        Validate.isTrue(commitTimestamp > getStartTimestamp(), "commitTs must be greater than startTs");
        try {
            transactionService.putUnlessExists(getStartTimestamp(), commitTimestamp);
        } catch (KeyAlreadyExistsException e) {
            handleKeyAlreadyExistsException(commitTimestamp, e, locksToken);
        } catch (Exception e) {
            TransactionCommitFailedException commitFailedEx = new TransactionCommitFailedException(
                    "This transaction failed writing the commit timestamp. " +
                    "It might have been committed, but it may not have.", e);
            log.error("failed to commit an atlasdb transaction", commitFailedEx);
            throw commitFailedEx;
        }
    }

    private void handleKeyAlreadyExistsException(long commitTs, KeyAlreadyExistsException e, LockRefreshToken commitLocksToken) {
        try {
            if (wasCommitSuccessful(commitTs)) {
                // We did actually commit successfully.  This case could happen if the impl
                // for putUnlessExists did a retry and we had committed already
                return;
            }
            Set<LockRefreshToken> expiredLocks = refreshExternalAndCommitLocks(commitLocksToken);
            if (!expiredLocks.isEmpty()) {
                throw new TransactionLockTimeoutException("Our commit was already rolled back at commit time " +
                        "because our locks timed out.  startTs: " + getStartTimestamp() + ".  " +
                        getExpiredLocksErrorString(commitLocksToken, expiredLocks), e);
            } else {
                AssertUtils.assertAndLog(false,
                        "BUG: Someone tried to roll back our transaction but our locks were still valid; this is not allowed." +
                        " Held external locks: " + externalLocksTokens + "; held commit locks: " + commitLocksToken);
            }
        } catch (TransactionFailedException e1) {
            throw e1;
        } catch (Exception e1) {
            log.error("Failed to determine if we can retry this transaction. startTs: " + getStartTimestamp(), e1);
        }
        String msg = "Our commit was already rolled back at commit time.  " +
                "Locking should prevent this from happening, but our locks may have timed out.  " +
                "startTs: " + getStartTimestamp();
        throw new TransactionCommitFailedException(msg, e);
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
    public void useTable(String tableName, ConstraintCheckable table) {
        constraintsByTableName.put(tableName, table);
    }

    private long getStartTimestamp() {
        return startTimestamp.get();
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return keyValueService;
    }

    private Multimap<String, Cell> getCellsToQueueForScrubbing() {
        return getCellsToScrub(State.COMMITTING);
    }

    Multimap<String, Cell> getCellsToScrubImmediately() {
        return getCellsToScrub(State.COMMITTED);
    }

    private Multimap<String, Cell> getCellsToScrub(State expectedState) {
        Multimap<String, Cell> tableNameToCell = HashMultimap.create();
        State actualState = state.get();
        if (expectedState == actualState) {
            for (Entry<String, ConcurrentNavigableMap<Cell, byte[]>> entry : writesByTable.entrySet()) {
                String table = entry.getKey();
                Set<Cell> cells = entry.getValue().keySet();
                tableNameToCell.putAll(table, cells);
            }
        } else {
            AssertUtils.assertAndLog(false, "Expected state: " + expectedState + "; actual state: " + actualState);
        }
        return tableNameToCell;
    }
}


