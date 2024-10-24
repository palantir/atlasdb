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

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
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
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.RestrictedApi;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.AtlasDbPerformanceConstants;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cell.api.DataKeyValueService;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.common.api.annotations.ReviewedRestrictedApiUsage;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.futures.AtlasFutures;
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
import com.palantir.atlasdb.keyvalue.api.watch.LocksAndMetadata;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.impl.RowResults;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.description.exceptions.AtlasDbConstraintException;
import com.palantir.atlasdb.tracing.TraceStatistics;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.CommitTimestampLoader;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.atlasdb.transaction.api.ConstraintCheckingTransaction;
import com.palantir.atlasdb.transaction.api.DeleteExecutor;
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
import com.palantir.atlasdb.transaction.api.ValueAndChangeMetadata;
import com.palantir.atlasdb.transaction.api.exceptions.MoreCellsPresentThanExpectedException;
import com.palantir.atlasdb.transaction.api.exceptions.SafeTransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.expectations.ExpectationsData;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableExpectationsData;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionCommitLockInfo;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionWriteMetadataInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionCommitLockInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionWriteMetadataInfo;
import com.palantir.atlasdb.transaction.api.metrics.KeyValueSnapshotMetricRecorder;
import com.palantir.atlasdb.transaction.api.precommit.PreCommitRequirementValidator;
import com.palantir.atlasdb.transaction.api.precommit.ReadSnapshotValidator;
import com.palantir.atlasdb.transaction.api.precommit.ReadSnapshotValidator.ValidationState;
import com.palantir.atlasdb.transaction.api.snapshot.ImmutableTransactionContext;
import com.palantir.atlasdb.transaction.api.snapshot.KeyValueSnapshotReader;
import com.palantir.atlasdb.transaction.api.snapshot.KeyValueSnapshotReaderManager;
import com.palantir.atlasdb.transaction.expectations.ExpectationsMetrics;
import com.palantir.atlasdb.transaction.impl.TransactionLocksManager.SummarizedLockCheckResult;
import com.palantir.atlasdb.transaction.impl.expectations.CellCountValidator;
import com.palantir.atlasdb.transaction.impl.expectations.TrackingDataKeyValueService;
import com.palantir.atlasdb.transaction.impl.expectations.TrackingDataKeyValueServiceImpl;
import com.palantir.atlasdb.transaction.impl.metrics.DefaultKeyValueSnapshotMetricRecorder;
import com.palantir.atlasdb.transaction.impl.metrics.SnapshotTransactionMetricFactory;
import com.palantir.atlasdb.transaction.impl.metrics.TableLevelMetricsController;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionMetrics;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetrics;
import com.palantir.atlasdb.transaction.impl.precommit.DefaultLockValidityChecker;
import com.palantir.atlasdb.transaction.impl.precommit.DefaultPreCommitRequirementValidator;
import com.palantir.atlasdb.transaction.impl.precommit.DefaultReadSnapshotValidator;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
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
import com.palantir.common.collect.IteratorUtils;
import com.palantir.common.collect.MapEntries;
import com.palantir.common.streams.KeyedStream;
import com.palantir.common.streams.MoreStreams;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeNullPointerException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tracing.CloseableTracer;
import com.palantir.util.AssertUtils;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import com.palantir.util.result.Result;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;

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
public class SnapshotTransaction extends AbstractTransaction
        implements ConstraintCheckingTransaction, CallbackAwareTransaction {
    private static final SafeLogger log = SafeLoggerFactory.get(SnapshotTransaction.class);
    private static final SafeLogger perfLogger = SafeLoggerFactory.get("dualschema.perf");
    private static final SafeLogger transactionLengthLogger = SafeLoggerFactory.get("txn.length");
    private static final SafeLogger constraintLogger = SafeLoggerFactory.get("dualschema.constraints");

    private static final int BATCH_SIZE_GET_FIRST_PAGE = 1000;
    private static final long TXN_LENGTH_THRESHOLD = Duration.ofMinutes(30).toMillis();

    // TODO (jkong): Remove once this class is no longer responsible for post-filtering
    public static final int MAX_POST_FILTERING_ITERATIONS = 200;

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
    final TrackingDataKeyValueService dataKeyValueService;
    final TransactionService defaultTransactionService;
    private final AsyncTransactionService immediateTransactionService;
    private final Cleaner cleaner;
    private final LongSupplier startTimestamp;
    protected final MetricsManager metricsManager;
    protected final ConflictTracer conflictTracer;

    protected final MultiTableSweepQueueWriter sweepQueue;

    protected final long immutableTimestamp;
    protected final long timeCreated = System.currentTimeMillis();

    protected final LocalWriteBuffer localWriteBuffer = new LocalWriteBuffer();

    protected final TransactionConflictDetectionManager conflictDetectionManager;

    private final AtlasDbConstraintCheckingMode constraintCheckingMode;

    private final ConcurrentMap<TableReference, ConstraintCheckable> constraintsByTableName = new ConcurrentHashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.UNCOMMITTED);
    private final AtomicLong numWriters = new AtomicLong();
    protected final SweepStrategyManager sweepStrategyManager;
    protected final Long transactionReadTimeoutMillis;
    private final TransactionReadSentinelBehavior readSentinelBehavior;
    private volatile long commitTsForScrubbing = TransactionConstants.FAILED_COMMIT_TS;
    protected final boolean allowHiddenTableAccess;
    protected final ExecutorService getRangesExecutor;
    protected final int defaultGetRangesConcurrency;
    private final Set<TableReference> involvedTables = ConcurrentHashMap.newKeySet();
    protected final DeleteExecutor deleteExecutor;
    private final Timer.Context transactionTimerContext;
    protected final TransactionOutcomeMetrics transactionOutcomeMetrics;
    protected volatile boolean validateLocksOnReads;
    protected final Supplier<TransactionConfig> transactionConfig;
    protected final TableLevelMetricsController tableLevelMetricsController;
    protected final SuccessCallbackManager successCallbackManager = new SuccessCallbackManager();
    protected final CommitTimestampLoader commitTimestampLoader;
    private final ExpectationsMetrics expectationsDataCollectionMetrics;
    private volatile long cellCommitLocksRequested = 0L;
    private volatile long rowCommitLocksRequested = 0L;
    private volatile long cellChangeMetadataSent = 0L;
    private volatile long rowChangeMetadataSent = 0L;

    protected volatile boolean hasReads;

    // On thoroughly swept tables, we might require a immutable timestamp lock check if we have performed a
    // non-exhaustive read.
    protected volatile boolean hasPossiblyUnvalidatedReads;

    protected final TimestampCache timestampCache;

    protected final TransactionKnowledgeComponents knowledge;
    private final TransactionLocksManager transactionLocksManager;
    private final PreCommitRequirementValidator preCommitRequirementValidator;
    private final ReadSnapshotValidator readSnapshotValidator;
    private final ThreadSafeCloser closer = new ThreadSafeCloser();

    private final SnapshotTransactionMetricFactory snapshotTransactionMetricFactory;

    private final KeyValueSnapshotMetricRecorder snapshotEventRecorder;
    private final ReadSentinelHandler readSentinelHandler;
    private final KeyValueSnapshotReader keyValueSnapshotReader;
    protected final KeyValueSnapshotReaderManager keyValueSnapshotReaderManager;

    /**
     * @param immutableTimestamp If we find a row written before the immutableTimestamp we don't need to grab a read
     * lock for it because we know that no writers exist.
     * @param preCommitCondition This check must pass for this transaction to commit.
     */
    /* package */ SnapshotTransaction(
            MetricsManager metricsManager,
            DataKeyValueService delegateDataKeyValueService,
            TimelockService timelockService,
            LockWatchManagerInternal lockWatchManager,
            TransactionService transactionService,
            Cleaner cleaner,
            LongSupplier startTimestamp,
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
            DeleteExecutor deleteExecutor,
            boolean validateLocksOnReads,
            Supplier<TransactionConfig> transactionConfig,
            ConflictTracer conflictTracer,
            TableLevelMetricsController tableLevelMetricsController,
            TransactionKnowledgeComponents knowledge,
            CommitTimestampLoader commitTimestampLoader,
            KeyValueSnapshotReaderManager keyValueSnapshotReaderManager) {
        this.metricsManager = metricsManager;
        this.lockWatchManager = lockWatchManager;
        this.conflictTracer = conflictTracer;
        this.dataKeyValueService = new TrackingDataKeyValueServiceImpl(delegateDataKeyValueService);
        this.timelockService = timelockService;
        this.defaultTransactionService = transactionService;
        this.immediateTransactionService = TransactionServices.synchronousAsAsyncTransactionService(transactionService);
        this.cleaner = cleaner;
        this.startTimestamp = startTimestamp;
        this.conflictDetectionManager = new TransactionConflictDetectionManager(conflictDetectionManager);
        this.sweepStrategyManager = sweepStrategyManager;
        this.immutableTimestamp = immutableTimestamp;
        this.constraintCheckingMode = constraintCheckingMode;
        this.transactionReadTimeoutMillis = transactionTimeoutMillis;
        this.readSentinelBehavior = readSentinelBehavior;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.getRangesExecutor = getRangesExecutor;
        this.defaultGetRangesConcurrency = defaultGetRangesConcurrency;
        this.sweepQueue = sweepQueue;
        this.deleteExecutor = deleteExecutor;
        this.hasReads = false;
        this.hasPossiblyUnvalidatedReads = false;
        this.validateLocksOnReads = validateLocksOnReads;
        this.transactionConfig = transactionConfig;
        this.tableLevelMetricsController = tableLevelMetricsController;
        this.timestampCache = timestampValidationReadCache;
        this.knowledge = knowledge;
        this.snapshotTransactionMetricFactory =
                new SnapshotTransactionMetricFactory(metricsManager, tableLevelMetricsController);
        this.transactionTimerContext =
                snapshotTransactionMetricFactory.getTimer("transactionMillis").time();

        TransactionMetrics transactionMetrics = TransactionMetrics.of(metricsManager.getTaggedRegistry());

        // TODO (jkong): Move to use dependency injection!
        this.snapshotEventRecorder =
                new DefaultKeyValueSnapshotMetricRecorder(snapshotTransactionMetricFactory, transactionMetrics);
        this.transactionOutcomeMetrics =
                TransactionOutcomeMetrics.create(transactionMetrics, metricsManager.getTaggedRegistry());
        this.expectationsDataCollectionMetrics = ExpectationsMetrics.of(metricsManager.getTaggedRegistry());
        this.transactionLocksManager = new TransactionLocksManager(
                immutableTimestampLock, new DefaultLockValidityChecker(timelockService), timelockService::tryUnlock);
        closer.register(transactionLocksManager);
        this.preCommitRequirementValidator = new DefaultPreCommitRequirementValidator(
                preCommitCondition, transactionOutcomeMetrics, transactionLocksManager);
        this.readSnapshotValidator = new DefaultReadSnapshotValidator(
                preCommitRequirementValidator, validateLocksOnReads, sweepStrategyManager, transactionConfig);
        this.commitTimestampLoader = commitTimestampLoader;
        this.readSentinelHandler = new ReadSentinelHandler(
                dataKeyValueService,
                transactionService,
                readSentinelBehavior,
                new DefaultOrphanedSentinelDeleter(sweepStrategyManager::get, deleteExecutor));
        this.keyValueSnapshotReaderManager = keyValueSnapshotReaderManager;
        this.keyValueSnapshotReader = getDefaultKeyValueSnapshotReader();
    }

    private KeyValueSnapshotReader getDefaultKeyValueSnapshotReader() {
        return keyValueSnapshotReaderManager.createKeyValueSnapshotReader(ImmutableTransactionContext.builder()
                .startTimestampSupplier(startTimestamp)
                .transactionReadSentinelBehavior(readSentinelBehavior)
                .commitTimestampLoader(commitTimestampLoader)
                .preCommitRequirementValidator(preCommitRequirementValidator)
                .readSnapshotValidator(readSnapshotValidator)
                .keyValueSnapshotMetricRecorder(snapshotEventRecorder)
                .build());
    }

    protected TransactionScopedCache getCache() {
        return lockWatchManager.getTransactionScopedCache(getTimestamp());
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

        ensureStillRunning();
    }

    @Override
    public void disableReadWriteConflictChecking(TableReference tableRef) {
        conflictDetectionManager.disableReadWriteConflict(tableRef);
    }

    @RestrictedApi(
            explanation = "This API is only meant to be used by AtlasDb proxies that want to make use of the "
                    + "performance improvement that are achievable by avoiding immutable timestamp lock check on reads "
                    + "and delaying them to commit time. When validation on reads is disabled, it is possible for a "
                    + "transaction to read values that were thoroughly swept and the transaction would not fail until "
                    + "validation is done at commit commit time. Disabling validation on reads in situations when a "
                    + "transaction can potentially have side effects outside the transaction scope (e.g. remote call "
                    + "to another service) can cause correctness issues. The API is restricted as misuses of it can "
                    + "cause correctness issues.",
            link = "https://github.com/palantir/atlasdb/pull/7111",
            allowedOnPath = ".*/src/test/.*",
            allowlistAnnotations = {ReviewedRestrictedApiUsage.class})
    @Override
    public void disableValidatingLocksOnReads() {
        this.validateLocksOnReads = false;
        this.readSnapshotValidator.disableValidatingLocksOnReads();
    }

    @Override
    public void markTableInvolved(TableReference tableRef) {
        // Not setting hasReads on purpose.
        checkGetPreconditions(tableRef);
    }

    @Override
    public NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection) {
        if (columnSelection.allColumnsSelected()) {
            return getRowsInternal(tableRef, rows, columnSelection);
        }
        return getCache()
                .getRows(
                        tableRef,
                        rows,
                        columnSelection,
                        cells -> AtlasFutures.getUnchecked(getInternal("getRows", tableRef, cells, cells.size())),
                        unCachedRows -> getRowsInternal(tableRef, unCachedRows, columnSelection));
    }

    private NavigableMap<byte[], RowResult<byte[]>> getRowsInternal(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection) {
        Timer.Context timer =
                snapshotTransactionMetricFactory.getTimer("getRows").time();
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return AbstractTransaction.EMPTY_SORTED_ROWS;
        }
        hasReads = true;
        NavigableMap<Cell, byte[]> writes = localWriteBuffer.getLocalWrites().get(tableRef);
        Map<Cell, byte[]> writesForRows = extractLocalWritesForRows(writes, rows, columnSelection);

        NavigableMap<byte[], RowResult<byte[]>> results =
                keyValueSnapshotReader.getRows(tableRef, rows, columnSelection, writesForRows);
        long getRowsMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());
        if (perfLogger.isDebugEnabled()) {
            perfLogger.debug(
                    "getRows({}, {} rows) found {} rows, took {} ms",
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("numRows", Iterables.size(rows)),
                    SafeArg.of("resultSize", results.size()),
                    SafeArg.of("timeTakenMillis", getRowsMillis));
        }

        /* can't skip lock check as we don't know how many cells to expect for the column selection */
        validatePreCommitRequirementsOnNonExhaustiveReadIfNecessary(tableRef, getStartTimestamp());
        return results;
    }

    @Override
    public Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        return KeyedStream.stream(getRowsColumnRangeIterator(tableRef, rows, columnRangeSelection))
                .map(BatchingVisitableFromIterable::create)
                .map(this::scopeToTransaction)
                .collectTo(() -> new TreeMap<>(UnsignedBytes.lexicographicalComparator()));
    }

    @Override
    public Iterator<Map.Entry<Cell, byte[]>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return Collections.emptyIterator();
        }
        ImmutableList<byte[]> stableRows = ImmutableList.copyOf(rows);
        hasReads = true;
        RowColumnRangeIterator rawResults = dataKeyValueService.getRowsColumnRange(
                tableRef, stableRows, columnRangeSelection, batchHint, getStartTimestamp());
        if (!rawResults.hasNext()) {
            // we can't skip checks for range scans
            validatePreCommitRequirementsOnNonExhaustiveReadIfNecessary(tableRef, getStartTimestamp());
        } // else the postFiltered iterator will check for each batch.

        BatchColumnRangeSelection batchColumnRangeSelection =
                BatchColumnRangeSelection.create(columnRangeSelection, batchHint);
        return scopeToTransaction(getPostFilteredColumns(tableRef, batchColumnRangeSelection, stableRows, rawResults));
    }

    @Override
    @SuppressWarnings("MustBeClosedChecker") // Sadly we can't close properly here without an ABI break :/
    public Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> getRowsColumnRangeIterator(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(rows)) {
            return ImmutableMap.of();
        }
        hasReads = true;
        ImmutableSortedMap<byte[], RowColumnRangeIterator> rawResults = ImmutableSortedMap.copyOf(
                dataKeyValueService.getRowsColumnRange(tableRef, rows, columnRangeSelection, getStartTimestamp()),
                PtBytes.BYTES_COMPARATOR);
        ImmutableSortedMap<byte[], Iterator<Map.Entry<Cell, byte[]>>> postFilteredResults = Streams.stream(rows)
                .collect(ImmutableSortedMap.toImmutableSortedMap(PtBytes.BYTES_COMPARATOR, row -> row, row -> {
                    // explicitly not using Optional due to allocation perf overhead
                    Iterator<Map.Entry<Cell, byte[]>> entryIterator;
                    RowColumnRangeIterator rawIterator = rawResults.get(row);
                    if (rawIterator != null) {
                        entryIterator = closer.register(
                                getPostFilteredColumns(tableRef, columnRangeSelection, row, rawIterator));
                    } else {
                        entryIterator = ClosableIterators.wrapWithEmptyClose(
                                getLocalWritesForColumnRange(tableRef, columnRangeSelection, row)
                                        .entrySet()
                                        .iterator());
                    }
                    return scopeToTransaction(entryIterator);
                }));

        // validate requirements here as the first batch for each of the above iterators will not check
        // we can't skip checks on range scans
        validatePreCommitRequirementsOnNonExhaustiveReadIfNecessary(tableRef, getStartTimestamp());
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
                dataKeyValueService.getRowsColumnRange(tableRef, distinctRows, perBatchSelection, getStartTimestamp());

        return scopeToTransaction(
                getPostFilteredSortedColumns(tableRef, batchColumnRangeSelection, distinctRows, rawResults));
    }

    private ClosableIterator<Map.Entry<Cell, byte[]>> getPostFilteredSortedColumns(
            TableReference tableRef,
            BatchColumnRangeSelection batchColumnRangeSelection,
            Iterable<byte[]> distinctRows,
            Map<byte[], RowColumnRangeIterator> rawResults) {
        Comparator<Cell> cellComparator = columnOrderThenPreserveInputRowOrder(distinctRows);

        Iterator<Entry<Cell, Value>> mergeSortedIterator = mergeByComparator(rawResults.values(), cellComparator);
        if (!mergeSortedIterator.hasNext()) {
            // we can't skip checks on range scans
            validatePreCommitRequirementsOnNonExhaustiveReadIfNecessary(tableRef, getStartTimestamp());
        } // else the postFiltered iterator will check for each batch.

        Iterator<Map.Entry<Cell, Value>> postFilterIterator = getRowColumnRangePostFilteredWithoutSorting(
                tableRef, mergeSortedIterator, batchColumnRangeSelection.getBatchHint(), cellComparator);
        Iterator<Map.Entry<Cell, byte[]>> remoteWrites = Iterators.transform(
                postFilterIterator,
                entry -> Maps.immutableEntry(entry.getKey(), entry.getValue().getContents()));
        Iterator<Map.Entry<Cell, byte[]>> localWrites =
                getSortedColumnsLocalWrites(tableRef, distinctRows, batchColumnRangeSelection, cellComparator);
        ClosableIterator<Map.Entry<Cell, byte[]>> merged = mergeLocalAndRemoteWrites(
                localWrites, ClosableIterators.wrapWithEmptyClose(remoteWrites), cellComparator);

        return ClosableIterators.wrap(filterDeletedValues(merged, tableRef), merged);
    }

    private static ClosableIterator<Map.Entry<Cell, byte[]>> mergeLocalAndRemoteWrites(
            Iterator<Map.Entry<Cell, byte[]>> localWrites,
            ClosableIterator<Map.Entry<Cell, byte[]>> remoteWrites,
            Comparator<Cell> cellComparator) {
        // always override remote values with locally written values
        return ClosableIterators.wrap(
                IteratorUtils.mergeIterators(
                        localWrites,
                        remoteWrites,
                        Comparator.comparing(Map.Entry::getKey, cellComparator),
                        com.palantir.util.Pair::getLhSide),
                remoteWrites);
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
     */
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
        return Iterators.mergeSorted(iterators, Map.Entry.comparingByKey(cellComparator));
    }

    /**
     * If the batch hint is small, ask for at least that many from each of the input rows to avoid the
     * possibility of needing a second batch of fetching.
     * If the batch hint is large, split batch size across rows to avoid loading too much data, while accepting that
     * second fetches may be needed to get everyone their data.
     */
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

    @MustBeClosed
    private ClosableIterator<Map.Entry<Cell, byte[]>> getPostFilteredColumns(
            TableReference tableRef,
            BatchColumnRangeSelection batchColumnRangeSelection,
            byte[] row,
            RowColumnRangeIterator rawIterator) {
        // This isn't ideal (any exception in the method means close won't be called), but we can't practically
        // guarantee that
        @SuppressWarnings("MustBeClosedChecker")
        ClosableIterator<Map.Entry<Cell, byte[]>> postFilterIterator =
                getRowColumnRangePostFiltered(tableRef, row, batchColumnRangeSelection, rawIterator);

        SortedMap<Cell, byte[]> localWrites = getLocalWritesForColumnRange(tableRef, batchColumnRangeSelection, row);
        Iterator<Map.Entry<Cell, byte[]>> localIterator = localWrites.entrySet().iterator();
        ClosableIterator<Map.Entry<Cell, byte[]>> mergedIterator =
                mergeLocalAndRemoteWrites(localIterator, postFilterIterator, Cell.COLUMN_COMPARATOR);
        return ClosableIterators.wrap(filterDeletedValues(mergedIterator, tableRef), mergedIterator);
    }

    private Iterator<Map.Entry<Cell, byte[]>> getPostFilteredColumns(
            TableReference tableRef,
            BatchColumnRangeSelection batchColumnRangeSelection,
            List<byte[]> expectedRows,
            RowColumnRangeIterator rawIterator) {
        Iterator<Map.Entry<Cell, Value>> postFilterIterator =
                getRowColumnRangePostFiltered(tableRef, rawIterator, batchColumnRangeSelection.getBatchHint());
        Iterator<Map.Entry<byte[], RowColumnRangeIterator>> rawResultsByRow =
                partitionByRow(postFilterIterator, expectedRows.iterator());
        Iterator<Map.Entry<Cell, byte[]>> merged = Iterators.concat(Iterators.transform(rawResultsByRow, row -> {
            SortedMap<Cell, byte[]> localWrites =
                    getLocalWritesForColumnRange(tableRef, batchColumnRangeSelection, row.getKey());
            Iterator<Map.Entry<Cell, byte[]>> remoteIterator = Iterators.transform(
                    row.getValue(),
                    entry ->
                            Maps.immutableEntry(entry.getKey(), entry.getValue().getContents()));
            Iterator<Map.Entry<Cell, byte[]>> localIterator =
                    localWrites.entrySet().iterator();
            return mergeLocalAndRemoteWrites(
                    localIterator, ClosableIterators.wrapWithEmptyClose(remoteIterator), Cell.COLUMN_COMPARATOR);
        }));

        return filterDeletedValues(merged, tableRef);
    }

    private Iterator<Map.Entry<Cell, byte[]>> filterDeletedValues(
            Iterator<Map.Entry<Cell, byte[]>> unfiltered, TableReference tableReference) {
        return Iterators.filter(unfiltered, entry -> {
            if (entry.getValue().length == 0) {
                snapshotEventRecorder.recordFilteredEmptyValues(tableReference, 1);
                TraceStatistics.incEmptyValues(1);
                return false;
            }
            return true;
        });
    }

    private Iterator<Map.Entry<Cell, Value>> getRowColumnRangePostFiltered(
            TableReference tableRef, RowColumnRangeIterator iterator, int batchHint) {
        return Iterators.concat(Iterators.transform(Iterators.partition(iterator, batchHint), batch -> {
            Map<Cell, Value> raw = validateBatch(tableRef, batch, false /* can't skip lock checks on range scans */);
            if (raw.isEmpty()) {
                return Collections.emptyIterator();
            }
            SortedMap<Cell, Value> postFiltered = ImmutableSortedMap.copyOf(
                    getWithPostFilteringSync(tableRef, raw, x -> x), preserveInputRowOrder(batch));
            return postFiltered.entrySet().iterator();
        }));
    }

    @MustBeClosed
    private ClosableIterator<Map.Entry<Cell, byte[]>> getRowColumnRangePostFiltered(
            TableReference tableRef,
            byte[] row,
            BatchColumnRangeSelection columnRangeSelection,
            RowColumnRangeIterator rawIterator) {
        ColumnRangeBatchProvider batchProvider = new ColumnRangeBatchProvider(
                dataKeyValueService, tableRef, row, columnRangeSelection, getStartTimestamp());
        return GetRowsColumnRangeIterator.iterator(
                batchProvider,
                rawIterator,
                columnRangeSelection,
                () -> {
                    // we can't skip lock checks on range scans
                    validatePreCommitRequirementsOnNonExhaustiveReadIfNecessary(tableRef, getStartTimestamp());
                },
                raw -> getWithPostFilteringSync(tableRef, raw, Value.GET_VALUE));
    }

    private Iterator<Map.Entry<Cell, Value>> getRowColumnRangePostFilteredWithoutSorting(
            TableReference tableRef,
            Iterator<Map.Entry<Cell, Value>> iterator,
            int batchHint,
            Comparator<Cell> cellComparator) {
        return Iterators.concat(Iterators.transform(Iterators.partition(iterator, batchHint), batch -> {
            Map<Cell, Value> raw = validateBatch(tableRef, batch, false /* can't skip lock check on range scans */);
            if (raw.isEmpty()) {
                return Collections.emptyIterator();
            }

            SortedMap<Cell, Value> postFiltered =
                    ImmutableSortedMap.copyOf(getWithPostFilteringSync(tableRef, raw, x -> x), cellComparator);
            return postFiltered.entrySet().iterator();
        }));
    }

    private Map<Cell, Value> validateBatch(
            TableReference tableRef, List<Map.Entry<Cell, Value>> batch, boolean allPossibleCellsReadAndPresent) {
        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp(), allPossibleCellsReadAndPresent);
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
            Iterator<Map.Entry<Cell, Value>> rawResults, Iterator<byte[]> expectedRows) {
        PeekingIterator<Map.Entry<Cell, Value>> peekableRawResults = Iterators.peekingIterator(rawResults);
        return new AbstractIterator<>() {
            byte[] prevRowName;

            @Override
            protected Map.Entry<byte[], RowColumnRangeIterator> computeNext() {
                finishConsumingPreviousRow(peekableRawResults);
                if (!expectedRows.hasNext()) {
                    if (peekableRawResults.hasNext()) {
                        throw new SafeIllegalStateException(
                                "Iterators are not consistent: there is some data returned by getRowsColumnRange()"
                                        + " even when we expect no more rows. This is likely an AtlasDB product bug.",
                                UnsafeArg.of("nextRawResult", peekableRawResults.peek()));
                    }
                    return endOfData();
                }
                byte[] nextExpectedRow = expectedRows.next();

                if (!peekableRawResults.hasNext()) {
                    return Maps.immutableEntry(nextExpectedRow, EmptyRowColumnRangeIterator.INSTANCE);
                }
                byte[] nextRowName = peekableRawResults.peek().getKey().getRowName();
                if (!Arrays.equals(nextRowName, nextExpectedRow)) {
                    return Maps.immutableEntry(nextExpectedRow, EmptyRowColumnRangeIterator.INSTANCE);
                }

                Iterator<Map.Entry<Cell, Value>> columnsIterator = new AbstractIterator<>() {
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

    private enum EmptyRowColumnRangeIterator implements RowColumnRangeIterator {
        INSTANCE;

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Map.Entry<Cell, Value> next() {
            throw new NoSuchElementException();
        }
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
                new HashMap<>(dataKeyValueService.getRows(tableRef, rows, ColumnSelection.all(), getStartTimestamp()));

        // can't skip lock check as we don't know how many cells to expect for the column selection
        validatePreCommitRequirementsOnNonExhaustiveReadIfNecessary(tableRef, getStartTimestamp());

        Map<Cell, byte[]> results = Maps.newHashMapWithExpectedSize(rawResults.size());
        getWithPostFilteringSync(tableRef, rawResults, Value.GET_VALUE).forEach(e -> {
            results.put(e.getKey(), e.getValue());
        });

        removeEmptyColumns(tableRef, results);

        return RowResults.viewOfSortedMap(Cells.breakCellsUpByRow(results));
    }

    private void removeEmptyColumns(TableReference tableReference, Map<Cell, byte[]> results) {
        int unfilteredSize = results.size();

        results.values().removeIf(Value::isTombstone);

        int emptyValues = unfilteredSize - results.size();
        snapshotEventRecorder.recordFilteredEmptyValues(tableReference, emptyValues);
        TraceStatistics.incEmptyValues(emptyValues);
    }

    /**
     * This will return any local writes for these rows.
     * <p>
     * If an empty value was written as a delete, this will also be included in the map.
     */
    private Map<Cell, byte[]> extractLocalWritesForRows(
            SortedMap<Cell, byte[]> writes, Iterable<byte[]> rows, ColumnSelection columnSelection) {
        if (writes == null || writes.isEmpty()) {
            return Map.of();
        }

        Map<Cell, byte[]> results = new HashMap<>();

        for (byte[] row : rows) {
            Cell lowCell = Cells.createSmallestCellForRow(row);
            for (Map.Entry<Cell, byte[]> entry : writes.tailMap(lowCell).entrySet()) {
                Cell cell = entry.getKey();
                if (!Arrays.equals(row, cell.getRowName())) {
                    break;
                }
                if (columnSelection.allColumnsSelected()
                        || columnSelection.getSelectedColumns().contains(cell.getColumnName())) {
                    results.put(cell, entry.getValue());
                }
            }
        }

        return Collections.unmodifiableMap(results);
    }

    @Override
    @Idempotent
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        return getCache().get(tableRef, cells, uncached -> getInternal("get", tableRef, uncached, uncached.size()));
    }

    @Override
    public Result<Map<Cell, byte[]>, MoreCellsPresentThanExpectedException> getWithExpectedNumberOfCells(
            TableReference tableRef, Set<Cell> cells, long expectedNumberOfPresentCells) {
        try {
            return Result.ok(getCache().getWithCachedRef(tableRef, cells, cacheLookupResult -> {
                long cachedCellsWithNonEmptyValue = CellCountValidator.validateCacheAndGetNonEmptyValuesCount(
                        expectedNumberOfPresentCells, cacheLookupResult.cacheHits());
                long numberOfCellsExpectingValuePostCache = expectedNumberOfPresentCells - cachedCellsWithNonEmptyValue;

                return getInternal(
                        "getWithExpectedNumberOfCells",
                        tableRef,
                        cacheLookupResult.missedCells(),
                        numberOfCellsExpectingValuePostCache);
            }));
        } catch (MoreCellsPresentThanExpectedException e) {
            return Result.err(e);
        }
    }

    @Override
    @Idempotent
    public ListenableFuture<Map<Cell, byte[]>> getAsync(TableReference tableRef, Set<Cell> cells) {
        return scopeToTransaction(getCache()
                .getAsync(tableRef, cells, uncached -> getInternal("getAsync", tableRef, uncached, uncached.size())));
    }

    @VisibleForTesting
    ListenableFuture<Map<Cell, byte[]>> getInternal(
            String operationName, TableReference tableRef, Set<Cell> cells, long numberOfExpectedPresentCells) {
        Timer.Context timer =
                snapshotTransactionMetricFactory.getTimer(operationName).time();
        checkGetPreconditions(tableRef);
        if (Iterables.isEmpty(cells)) {
            return Futures.immediateFuture(ImmutableMap.of());
        }
        hasReads = true;

        Map<Cell, byte[]> results = new HashMap<>();
        Map<Cell, byte[]> writes = localWriteBuffer.getLocalWrites().get(tableRef);
        long numberOfNonDeleteLocalWrites = 0;
        if (writes != null && !writes.isEmpty()) {
            for (Cell cell : cells) {
                byte[] value = writes.get(cell);
                if (value != null) {
                    results.put(cell, value);
                    if (value != PtBytes.EMPTY_BYTE_ARRAY) {
                        numberOfNonDeleteLocalWrites++;
                    }
                }
            }
        }

        // We don't need to read any cells that were written locally.
        long expectedNumberOfPresentCellsToFetch = numberOfExpectedPresentCells - numberOfNonDeleteLocalWrites;
        Set<Cell> cellsToFetch = Sets.difference(cells, results.keySet());
        ListenableFuture<Map<Cell, byte[]>> initialResults = keyValueSnapshotReader.getAsync(tableRef, cellsToFetch);
        return Futures.transform(
                initialResults,
                fromKeyValueService -> {
                    results.putAll(fromKeyValueService);

                    long getMillis = TimeUnit.NANOSECONDS.toMillis(timer.stop());
                    if (perfLogger.isDebugEnabled()) {
                        perfLogger.debug(
                                "Snapshot transaction get cells (some possibly deleted)",
                                LoggingArgs.tableRef(tableRef),
                                SafeArg.of("numberOfCells", cells.size()),
                                SafeArg.of("numberOfCellsRetrieved", results.size()),
                                SafeArg.of("getOperation", operationName),
                                SafeArg.of("durationMillis", getMillis));
                    }

                    CellCountValidator.validateFetchedLessOrEqualToExpected(
                            expectedNumberOfPresentCellsToFetch, fromKeyValueService);
                    boolean allPossibleCellsReadAndPresent =
                            fromKeyValueService.size() == expectedNumberOfPresentCellsToFetch;
                    validatePreCommitRequirementsOnReadIfNecessary(
                            tableRef, getStartTimestamp(), allPossibleCellsReadAndPresent);
                    removeEmptyColumns(tableRef, results);

                    return Collections.unmodifiableMap(results);
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

        Map<Cell, byte[]> unfiltered = AtlasFutures.getUnchecked(keyValueSnapshotReader.getAsync(tableRef, cells));
        Map<Cell, byte[]> filtered = Maps.filterValues(unfiltered, Predicates.not(Value::isTombstone));

        TraceStatistics.incEmptyValues(unfiltered.size() - filtered.size());

        boolean allPossibleCellsReadAndPresent = unfiltered.size() == cells.size();
        validatePreCommitRequirementsOnReadIfNecessary(tableRef, getStartTimestamp(), allPossibleCellsReadAndPresent);

        return filtered;
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
                    "Passed {} ranges to getRanges({}, {})",
                    SafeArg.of("numRanges", Iterables.size(rangeRequests)),
                    LoggingArgs.tableRef(tableRef),
                    UnsafeArg.of("rangeRequests", rangeRequests));
        }
        if (!Iterables.isEmpty(rangeRequests)) {
            hasReads = true;
        }

        return FluentIterable.from(Iterables.partition(rangeRequests, BATCH_SIZE_GET_FIRST_PAGE))
                .transformAndConcat(input -> {
                    Timer.Context timer = snapshotTransactionMetricFactory
                            .getTimer("processedRangeMillis")
                            .time();
                    Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> firstPages =
                            dataKeyValueService.getFirstBatchForRanges(tableRef, input, getStartTimestamp());
                    // can't skip lock check for range scans
                    validatePreCommitRequirementsOnNonExhaustiveReadIfNecessary(tableRef, getStartTimestamp());

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

    private void validatePreCommitRequirementsOnNonExhaustiveReadIfNecessary(TableReference tableRef, long timestamp) {
        validatePreCommitRequirementsOnReadIfNecessary(tableRef, timestamp, false);
    }

    /*
    We don't have any guarantees that reads and commit will be executed in a single threaded manner, so we could in
    theory return wrong results if we perform async reads while trying to commit. But our protocol doesn't aim to
    support such case, and we're OK with having undefined behaviour in these occasions.

    Example of such case:
       - We're reading a thoroughly swept table with no configuration for immediate validation on reads.
       - An async range scan is performed, but hasn't started executing yet.
       - We start commit round on the main thread
       - Commit doesn't check for immutable timestamp lock because
           hasReadsThatRequireImmutableTimestampLockValidationAtCommitRound is false
       - We finish the range scan on the second thread, but validation is skipped due to the configuration and it sets
           hasReadsThatRequireImmutableTimestampLockValidationAtCommitRound to true, but that is never checked due to
            the commit round having already happened.
       - We then risk having returned inconsistent values in this transaction, since we could have lost the immutableT
            lock, but never checked it and values could've been swept under us.
    */
    private void validatePreCommitRequirementsOnReadIfNecessary(
            TableReference tableRef, long timestamp, boolean allPossibleCellsReadAndPresent) {
        ValidationState validationState = readSnapshotValidator.throwIfPreCommitRequirementsNotMetOnRead(
                tableRef, timestamp, allPossibleCellsReadAndPresent);
        if (validationState == ValidationState.NOT_COMPLETELY_VALIDATED) {
            hasPossiblyUnvalidatedReads = true;
        }
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
                        tableRef, rangeRequest.getStartInclusive(), endRowExclusive, rangeRequest.getColumnNames())
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
        try (ClosableIterator<RowResult<byte[]>> postFilterIterator =
                postFilterIterator(tableRef, range, preFilterBatchSize, Value.GET_VALUE)) {
            Iterator<RowResult<byte[]>> localWritesInRange = Cells.createRowView(getLocalWritesForRange(
                            tableRef, range.getStartInclusive(), range.getEndExclusive(), range.getColumnNames())
                    .entrySet());
            Iterator<RowResult<byte[]>> mergeIterators =
                    mergeInLocalWritesRows(postFilterIterator, localWritesInRange, range.isReverse(), tableRef);
            return BatchingVisitableFromIterable.create(mergeIterators).batchAccept(userRequestedSize, visitor);
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
            // compute filtered size without traversing lazily transformed map `size()` as that allocates entries
            long filteredCount = unfilteredRow.getColumns().values().stream()
                    .filter(Predicates.not(Value::isTombstone))
                    .count();

            long emptyValues = unfilteredRow.getColumns().size() - filteredCount;
            snapshotEventRecorder.recordFilteredEmptyValues(tableReference, emptyValues);
            TraceStatistics.incEmptyValues(emptyValues);

            return RowResult.create(unfilteredRow.getRowName(), filteredColumns);
        });
    }

    private List<Map.Entry<Cell, byte[]>> mergeInLocalWrites(
            TableReference tableRef,
            Iterator<Map.Entry<Cell, byte[]>> postFilterIterator,
            Iterator<Map.Entry<Cell, byte[]>> localWritesInRange,
            boolean isReverse) {
        Comparator<Cell> cellComparator = isReverse ? Comparator.reverseOrder() : Comparator.naturalOrder();
        Iterator<Map.Entry<Cell, byte[]>> mergeIterators = mergeLocalAndRemoteWrites(
                localWritesInRange, ClosableIterators.wrapWithEmptyClose(postFilterIterator), cellComparator);
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
        snapshotEventRecorder.recordFilteredEmptyValues(tableRef, numEmptyValues);
        TraceStatistics.incEmptyValues(numEmptyValues);

        return mergedWritesWithoutEmptyValues;
    }

    @MustBeClosed
    protected <T> ClosableIterator<RowResult<T>> postFilterIterator(
            TableReference tableRef, RangeRequest range, int preFilterBatchSize, Function<Value, T> transformer) {
        RowRangeBatchProvider batchProvider =
                new RowRangeBatchProvider(dataKeyValueService, tableRef, range, getStartTimestamp());
        BatchSizeIncreasingIterator<RowResult<Value>> results =
                new BatchSizeIncreasingIterator<>(batchProvider, preFilterBatchSize, null);
        Iterator<Iterator<RowResult<T>>> batchedPostFiltered = new AbstractIterator<Iterator<RowResult<T>>>() {
            @Override
            protected Iterator<RowResult<T>> computeNext() {
                List<RowResult<Value>> batch = results.getBatch().batch();
                if (batch.isEmpty()) {
                    return endOfData();
                }
                SortedMap<Cell, T> postFilter = postFilterRows(tableRef, batch, transformer);

                // can't skip lock checks for range scans
                validatePreCommitRequirementsOnNonExhaustiveReadIfNecessary(tableRef, getStartTimestamp());
                results.markNumResultsNotDeleted(
                        Cells.getRows(postFilter.keySet()).size());
                return Cells.createRowView(postFilter.entrySet());
            }
        };

        final Iterator<RowResult<T>> rows = Iterators.concat(batchedPostFiltered);

        return ClosableIterators.wrap(rows, results);
    }

    /**
     * This includes deleted writes as zero length byte arrays, be sure to strip them out.
     * <p>
     * For the selectedColumns parameter, empty set means all columns. This is unfortunate, but follows the semantics of
     * {@link RangeRequest}.
     */
    private SortedMap<Cell, byte[]> getLocalWritesForRange(
            TableReference tableRef, byte[] startRow, byte[] endRow, SortedSet<byte[]> selectedColumns) {
        SortedMap<Cell, byte[]> writes = localWriteBuffer.getLocalWritesForTable(tableRef);
        if (startRow.length != 0) {
            writes = writes.tailMap(Cells.createSmallestCellForRow(startRow));
        }
        if (endRow.length != 0) {
            writes = writes.headMap(Cells.createSmallestCellForRow(endRow));
        }
        if (!selectedColumns.isEmpty()) {
            writes = Maps.filterKeys(writes, cell -> selectedColumns.contains(cell.getColumnName()));
        }
        return writes;
    }

    private SortedMap<Cell, byte[]> getLocalWritesForColumnRange(
            TableReference tableRef, BatchColumnRangeSelection columnRangeSelection, byte[] row) {
        SortedMap<Cell, byte[]> writes = localWriteBuffer.getLocalWritesForTable(tableRef);
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
                tableRef, rawResults, transformer, dataKeyValueService, immediateTransactionService));
    }

    private <T> ListenableFuture<Collection<Map.Entry<Cell, T>>> getWithPostFilteringAsync(
            TableReference tableRef,
            Map<Cell, Value> rawResults,
            Function<Value, T> transformer,
            DataKeyValueService asyncKeyValueService,
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
            snapshotEventRecorder.recordManyBytesReadForTable(tableRef, bytes);
        }

        snapshotEventRecorder.recordCellsRead(tableRef, rawResults.size());

        if (AtlasDbConstants.HIDDEN_TABLES.contains(tableRef)) {
            Preconditions.checkState(allowHiddenTableAccess, "hidden tables cannot be read in this transaction");
            // hidden tables are used outside of the transaction protocol, and in general have invalid timestamps,
            // so do not apply post-filtering as post-filtering would rollback (actually delete) the data incorrectly
            // this case is hit when reading a hidden table from console
            return Futures.immediateFuture(ImmutableList.copyOf(
                    Maps.transformValues(rawResults, transformer::apply).entrySet()));
        }

        Collection<Map.Entry<Cell, T>> resultsAccumulator = new ArrayList<>();
        return Futures.transformAsync(
                Futures.immediateFuture(rawResults),
                resultsToPostFilter -> getWithPostFilteringIterate(
                        tableRef,
                        resultsToPostFilter,
                        resultsAccumulator,
                        transformer,
                        asyncKeyValueService,
                        asyncTransactionService),
                MoreExecutors.directExecutor());
    }

    private <T> ListenableFuture<Collection<Map.Entry<Cell, T>>> getWithPostFilteringIterate(
            TableReference tableReference,
            Map<Cell, Value> resultsToPostFilter,
            Collection<Map.Entry<Cell, T>> resultsAccumulator,
            Function<Value, T> transformer,
            DataKeyValueService asyncKeyValueService,
            AsyncTransactionService asyncTransactionService) {
        return Futures.transformAsync(
                Futures.immediateFuture(resultsToPostFilter),
                results -> {
                    int iterations = 0;
                    Map<Cell, Value> remainingResultsToPostFilter = results;
                    while (!remainingResultsToPostFilter.isEmpty()) {
                        remainingResultsToPostFilter = AtlasFutures.getUnchecked(getWithPostFilteringInternal(
                                tableReference,
                                remainingResultsToPostFilter,
                                resultsAccumulator,
                                transformer,
                                asyncKeyValueService,
                                asyncTransactionService));
                        Preconditions.checkState(
                                ++iterations < MAX_POST_FILTERING_ITERATIONS,
                                "Unable to filter cells to find correct result after "
                                        + "reaching max iterations. This is likely due to aborted cells lying around,"
                                        + " or in the very rare case, could be due to transactions which constantly "
                                        + "conflict but never commit. These values will be cleaned up eventually, but"
                                        + " if the issue persists, ensure that sweep is caught up.",
                                LoggingArgs.tableRef(tableReference),
                                SafeArg.of("maxIterations", MAX_POST_FILTERING_ITERATIONS));
                    }
                    snapshotEventRecorder.recordCellsReturned(tableReference, resultsAccumulator.size());

                    return Futures.immediateFuture(resultsAccumulator);
                },
                MoreExecutors.directExecutor());
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
            DataKeyValueService asyncKeyValueService,
            AsyncTransactionService asyncTransactionService) {
        Set<Cell> orphanedSentinels =
                readSentinelHandler.findAndMarkOrphanedSweepSentinelsForDeletion(tableRef, rawResults);
        LongSet valuesStartTimestamps = getStartTimestampsForValues(rawResults.values());

        return Futures.transformAsync(
                commitTimestampLoader.getCommitTimestamps(tableRef, valuesStartTimestamps),
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
            DataKeyValueService asyncKeyValueService,
            Set<Cell> orphanedSentinels,
            LongLongMap commitTimestamps) {
        Map<Cell, Long> keysToReload = Maps.newHashMapWithExpectedSize(0);
        Map<Cell, Long> keysToDelete = Maps.newHashMapWithExpectedSize(0);
        Set<Cell> keysAdded = new HashSet<>();

        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            Cell key = e.getKey();
            Value value = e.getValue();

            if (ReadSentinelHandler.isSweepSentinel(value)) {
                snapshotEventRecorder.recordFilteredSweepSentinel(tableRef);
                if (!orphanedSentinels.contains(key)) {
                    readSentinelHandler.handleReadSentinel();
                }
            } else {
                long theirCommitTimestamp =
                        commitTimestamps.getIfAbsent(value.getTimestamp(), TransactionConstants.FAILED_COMMIT_TS);
                if (theirCommitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
                    keysToReload.put(key, value.getTimestamp());
                    if (shouldDeleteAndRollback()) {
                        // This is from a failed transaction so we can roll it back and then reload it.
                        keysToDelete.put(key, value.getTimestamp());
                        snapshotEventRecorder.recordFilteredUncommittedTransaction(tableRef);
                    }
                } else if (theirCommitTimestamp > getStartTimestamp()) {
                    // The value's commit timestamp is after our start timestamp.
                    // This means the value is from a transaction which committed
                    // after our transaction began. We need to try reading at an
                    // earlier timestamp.
                    keysToReload.put(key, value.getTimestamp());
                    snapshotEventRecorder.recordFilteredTransactionCommittingAfterOurStart(tableRef);
                } else {
                    // The value has a commit timestamp less than our start timestamp, and is visible and valid.
                    if (!value.isEmpty()) {
                        resultsCollector.add(Maps.immutableEntry(key, transformer.apply(value)));
                        keysAdded.add(key);
                    }
                }
            }
        }
        Set<Cell> keysAddedToResults = Collections.unmodifiableSet(keysAdded);

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
                        boolean allPossibleCellsReadAndPresent = nextRawResults.size() == keysToReload.size();
                        validatePreCommitRequirementsOnReadIfNecessary(
                                tableRef, getStartTimestamp(), allPossibleCellsReadAndPresent);
                        return getRemainingResults(nextRawResults, keysAddedToResults);
                    },
                    MoreExecutors.directExecutor());
        }
        return Futures.immediateFuture(ImmutableMap.of());
    }

    private Map<Cell, Value> getRemainingResults(Map<Cell, Value> rawResults, Set<Cell> keysAddedToResults) {
        return Maps.filterKeys(rawResults, cell -> !keysAddedToResults.contains(cell));
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
        deleteWithMetadataInternal(tableRef, cells, ImmutableMap.of());
    }

    @Override
    public void deleteWithMetadata(TableReference tableRef, Map<Cell, ChangeMetadata> cellsWithMetadata) {
        deleteWithMetadataInternal(tableRef, cellsWithMetadata.keySet(), cellsWithMetadata);
    }

    private void deleteWithMetadataInternal(
            TableReference tableRef, Set<Cell> cells, Map<Cell, ChangeMetadata> metadata) {
        getCache().delete(tableRef, cells);
        writeToLocalBuffer(tableRef, Cells.constantValueMap(cells, PtBytes.EMPTY_BYTE_ARRAY), metadata);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values) {
        putWithMetadataInternal(tableRef, values, ImmutableMap.of());
    }

    @Override
    public void putWithMetadata(TableReference tableRef, Map<Cell, ValueAndChangeMetadata> valuesAndMetadata) {
        putWithMetadataInternal(
                tableRef,
                Maps.transformValues(valuesAndMetadata, ValueAndChangeMetadata::value),
                Maps.transformValues(valuesAndMetadata, ValueAndChangeMetadata::metadata));
    }

    private void putWithMetadataInternal(
            TableReference tableRef, Map<Cell, byte[]> values, Map<Cell, ChangeMetadata> metadata) {
        ensureNoEmptyValues(values);
        getCache().write(tableRef, values);
        writeToLocalBuffer(tableRef, values, metadata);
    }

    private void writeToLocalBuffer(
            TableReference tableRef, Map<Cell, byte[]> values, Map<Cell, ChangeMetadata> metadata) {
        Preconditions.checkArgument(!AtlasDbConstants.HIDDEN_TABLES.contains(tableRef));
        markTableAsInvolvedInThisTransaction(tableRef);

        if (values.isEmpty()) {
            return;
        }

        numWriters.incrementAndGet();
        try {
            // We need to check the status after incrementing writers to ensure that we fail if we are committing.
            ensureUncommitted();

            localWriteBuffer.putLocalWritesAndMetadata(tableRef, values, metadata);
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

    @Override
    public void abort() {
        try {
            if (state.get() == State.ABORTED) {
                return;
            }
            while (true) {
                ensureUncommitted();
                if (state.compareAndSet(State.UNCOMMITTED, State.ABORTED)) {
                    if (hasWrites()) {
                        preCommitRequirementValidator.throwIfPreCommitRequirementsNotMet(getStartTimestamp());
                    }
                    transactionOutcomeMetrics.markAbort();
                    if (transactionLengthLogger.isDebugEnabled()) {
                        long transactionMillis = TimeUnit.NANOSECONDS.toMillis(transactionTimerContext.stop());

                        if (transactionMillis > TXN_LENGTH_THRESHOLD) {
                            transactionLengthLogger.debug(
                                    "Aborted transaction {} in {} ms",
                                    SafeArg.of("startTimestamp", getStartTimestamp()),
                                    SafeArg.of("transactionLengthMillis", transactionMillis),
                                    SafeArg.of("transactionDuration", Duration.ofMillis(transactionMillis)));
                        }
                    }
                    return;
                }
            }
        } finally {
            close();
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

    private void ensureStillRunning() {
        if (!isStillRunning()) {
            throw new CommittedTransactionException();
        }
    }

    private boolean isStillRunning() {
        State stateNow = state.get();
        return stateNow == State.UNCOMMITTED || stateNow == State.COMMITTING;
    }

    /**
     * Returns true iff the transaction is known to have successfully committed.
     * <p>
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
        try {
            commitWithoutCallbacks(transactionService);
        } finally {
            close();
        }
    }

    private void commitWithoutCallbacks(TransactionService transactionService) {
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
                perfLogger.debug(
                        "Committed transaction {} in {}ms",
                        SafeArg.of("startTimestamp", getStartTimestamp()),
                        SafeArg.of("transactionTimeMillis", transactionMillis));
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
        if (localWriteBuffer.getLocalWrites().isEmpty()) {
            // avoid work in cases where constraints do not apply (e.g. read only transactions)
            return;
        }

        List<String> violations = new ArrayList<>();
        for (Map.Entry<TableReference, ConstraintCheckable> entry : constraintsByTableName.entrySet()) {
            Map<Cell, byte[]> writes = localWriteBuffer.getLocalWrites().get(entry.getKey());
            if (writes != null && !writes.isEmpty()) {
                List<String> failures = entry.getValue().findConstraintFailures(writes, this, constraintCheckingMode);
                if (!failures.isEmpty()) {
                    violations.addAll(failures);
                }
            }
        }

        if (!violations.isEmpty()) {
            AtlasDbConstraintException error = new AtlasDbConstraintException(violations);
            if (constraintCheckingMode.shouldThrowException()) {
                throw error;
            } else {
                constraintLogger.error("Constraint failure on commit.", error);
            }
        }
    }

    private void commitWrites(TransactionService transactionService) {
        if (!hasWrites()) {
            if (hasReads() || hasAnyInvolvedTables()) {
                // verify any pre-commit conditions on the transaction
                preCommitRequirementValidator.throwIfPreCommitConditionInvalid(getStartTimestamp());

                // if there are no writes, we must still make sure the immutable timestamp lock is still valid,
                // to ensure that sweep hasn't thoroughly deleted cells we tried to read
                if (validationNecessaryForInvolvedTablesOnCommit()) {
                    preCommitRequirementValidator.throwIfImmutableTsOrCommitLocksExpired();
                }
            }
            snapshotTransactionMetricFactory
                    .getTimer("nonWriteCommitTotalTimeSinceTxCreation")
                    .update(Duration.of(transactionTimerContext.stop(), ChronoUnit.NANOS));
            return;
        }

        timedAndTraced("commitStage", () -> {
            // Acquire row locks and a lock on the start timestamp row in the transactions table.
            // This must happen before conflict checking, otherwise we could complete the checks and then have someone
            // else write underneath us before we proceed (thus missing a write/write conflict).
            // Timing still useful to distinguish bad lock percentiles from user-generated lock requests.
            LockToken commitLocksToken = transactionLocksManager.registerCommitLockOnly(
                    timedAndTraced("commitAcquireLocks", this::acquireLocksForCommit));

            // Conflict checking. We can actually do this later without compromising correctness, but there is no
            // reason to postpone this check - we waste resources writing unnecessarily if these are going to fail.
            timedAndTraced("commitCheckingForConflicts", () -> throwIfConflictOnCommit(transactionService));

            // Before doing any remote writes, we mark that the transaction is in progress. Until this point, all
            // writes are buffered in memory.
            timedAndTraced(
                    "markingTransactionInProgress", () -> transactionService.markInProgress(getStartTimestamp()));

            // Freeze the writes that we will commit. It is possible for writes to be added to the write buffer past
            // this point (if they had passed the #ensureUncommitted check before committing started), but they will
            // be discarded silently. This sounds scary, but this is not a regression from previous behaviour
            // TODO(mdaudali): We should explicitly freeze the write buffer to better surface lost writes in this
            // edge case.
            Map<TableReference, ? extends Map<Cell, byte[]>> writes = localWriteBuffer.getLocalWrites();

            // Write to the targeted sweep queue. We must do this before writing to the key value service -
            // otherwise we may have hanging values that targeted sweep won't know about.
            timedAndTraced("writingToSweepQueue", () -> sweepQueue.enqueue(writes, getStartTimestamp()));

            // Introduced for txn4 - Prevents sweep from making progress beyond immutableTs before entries were
            // put into the sweep queue. This ensures that sweep must process writes to the sweep queue done by
            // this transaction before making progress.
            traced("postSweepEnqueueLockCheck", preCommitRequirementValidator::throwIfImmutableTsOrCommitLocksExpired);

            // Write to the key value service. We must do this before getting the commit timestamp - otherwise
            // we risk another transaction starting at a timestamp after our commit timestamp not seeing our writes.
            timedAndTraced("commitWrite", () -> dataKeyValueService.multiPut(writes, getStartTimestamp()));

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

            // Check the data key-value-service is still the source of truth at the commit timestamp.
            // This can take place anytime before the actual commit (the putUnlessExists). However, situations
            // in which the data key-value-service is invalid may also affect subsequent checks, and we
            // would prefer for these to be flagged explicitly as such.
            // Timed; this may in some implementations end up requiring external RPCs or database calls.
            timedAndTraced("dataKvsValidityCheck", () -> throwIfDataKeyValueServiceNoLongerValid(commitTimestamp));

            // Serializable transactions need to check their reads haven't changed, by reading again at
            // commitTs + 1. This must happen before the lock check for thorough tables, because the lock check
            // verifies the immutable timestamp hasn't moved forward - thorough sweep might sweep a conflict out
            // from underneath us.
            timedAndTraced("readWriteConflictCheck", () -> throwIfReadWriteConflictForSerializable(commitTimestamp));

            // Verify that our locks and pre-commit conditions are still valid before we actually commit;
            // this throwIfPreCommitRequirementsNotMet is required by the transaction protocol for correctness.
            // We check the pre-commit conditions first since they may operate similarly to read write conflict
            // handling - we should check lock validity last to ensure that sweep hasn't affected the checks.
            timedAndTraced(
                    "userPreCommitCondition",
                    () -> preCommitRequirementValidator.throwIfPreCommitConditionInvalidAtCommitOnWriteTransaction(
                            writes, commitTimestamp));

            // Not timed, because this just calls ConjureTimelockServiceBlocking.refreshLockLeases, and that is
            // timed.
            traced("preCommitLockCheck", preCommitRequirementValidator::throwIfImmutableTsOrCommitLocksExpired);

            // Not timed, because this just calls TransactionService.putUnlessExists, and that is timed.
            traced("commitPutCommitTs", () -> putCommitTimestamp(commitTimestamp, transactionService));

            long microsSinceCreation = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis() - timeCreated);
            snapshotTransactionMetricFactory
                    .getTimer("commitTotalTimeSinceTxCreation")
                    .update(Duration.of(microsSinceCreation, ChronoUnit.MICROS));
            snapshotTransactionMetricFactory
                    .getHistogram(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_BYTES_WRITTEN)
                    .update(localWriteBuffer.getValuesByteCount());
        });
    }

    private void throwIfDataKeyValueServiceNoLongerValid(long commitTimestamp) {
        if (!dataKeyValueService.isValid(commitTimestamp)) {
            throw new SafeTransactionFailedRetriableException(
                    "Transaction key value service is no longer valid",
                    SafeArg.of("startTimestamp", getStartTimestamp()),
                    SafeArg.of("commitTimestamp", commitTimestamp));
        }
    }

    private void traced(String spanName, Runnable runnable) {
        try (CloseableTracer tracer = CloseableTracer.startSpan(spanName)) {
            runnable.run();
        }
    }

    private void timedAndTraced(String timerName, Runnable runnable) {
        try (Timer.Context timer =
                snapshotTransactionMetricFactory.getTimer(timerName).time()) {
            traced(timerName, runnable);
        }
    }

    private <T> T timedAndTraced(String timerName, Supplier<T> supplier) {
        try (Timer.Context timer =
                        snapshotTransactionMetricFactory.getTimer(timerName).time();
                CloseableTracer tracer = CloseableTracer.startSpan(timerName)) {
            return supplier.get();
        }
    }

    protected void throwIfReadWriteConflictForSerializable(long commitTimestamp) {
        // This is for overriding to get serializable transactions
    }

    private boolean hasWrites() {
        return !localWriteBuffer.getLocalWrites().isEmpty()
                && localWriteBuffer.getLocalWrites().values().stream()
                        .anyMatch(writesForTable -> !writesForTable.isEmpty());
    }

    protected boolean hasReads() {
        return hasReads;
    }

    protected ConflictHandler getConflictHandlerForTable(TableReference tableRef) {
        return conflictDetectionManager
                .get(tableRef)
                .orElseThrow(() -> new SafeNullPointerException(
                        "Not a valid table for this transaction. Make sure this table name exists or has a valid "
                                + "namespace.",
                        LoggingArgs.tableRef(tableRef)));
    }

    /**
     * Make sure we have all the rows we are checking already locked before calling this.
     */
    protected void throwIfConflictOnCommit(TransactionService transactionService) throws TransactionConflictException {
        for (Map.Entry<TableReference, ConcurrentNavigableMap<Cell, byte[]>> write :
                localWriteBuffer.getLocalWrites().entrySet()) {
            ConflictHandler conflictHandler = getConflictHandlerForTable(write.getKey());
            throwIfWriteAlreadyCommitted(write.getKey(), write.getValue(), conflictHandler, transactionService);
        }
    }

    protected void throwIfWriteAlreadyCommitted(
            TableReference tableRef,
            Map<Cell, byte[]> writes,
            ConflictHandler conflictHandler,
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
            throwIfValueChangedConflict(tableRef, writes, spanningWrites, dominatingWrites);
        } else {
            if (!spanningWrites.isEmpty() || !dominatingWrites.isEmpty()) {
                transactionOutcomeMetrics.markWriteWriteConflict(tableRef);
                throw TransactionConflictException.create(
                        tableRef,
                        getStartTimestamp(),
                        spanningWrites,
                        dominatingWrites,
                        System.currentTimeMillis() - timeCreated,
                        List.of(LoggingArgs.tableRef(tableRef)));
            }
        }
    }

    /**
     * This will throw if we have a value changed conflict.  This means that either we changed the value and anyone did
     * a write after our start timestamp, or we just touched the value (put the same value as before) and a changed
     * value was written after our start time.
     */
    private void throwIfValueChangedConflict(
            TableReference tableRef,
            Map<Cell, byte[]> writes,
            Set<CellConflict> spanningWrites,
            Set<CellConflict> dominatingWrites) {
        Map<Cell, CellConflict> cellToConflict = new HashMap<>();
        Map<Cell, Long> cellToTs = new HashMap<>();
        for (CellConflict c : Sets.union(spanningWrites, dominatingWrites)) {
            cellToConflict.put(c.getCell(), c);
            cellToTs.put(c.getCell(), c.getTheirStart() + 1);
        }

        Map<Cell, byte[]> oldValues = getIgnoringLocalWrites(tableRef, cellToTs.keySet());
        Map<Cell, Value> conflictingValues =
                AtlasFutures.getUnchecked(dataKeyValueService.getAsync(tableRef, cellToTs));

        Set<Cell> conflictingCells = new HashSet<>();
        for (Map.Entry<Cell, Long> cellEntry : cellToTs.entrySet()) {
            Cell cell = cellEntry.getKey();
            if (!writes.containsKey(cell)) {
                Validate.isTrue(false, "Missing write for cell: %s for table %s", cellToConflict.get(cell), tableRef);
            }
            if (!conflictingValues.containsKey(cell)) {
                // This error case could happen if our locks expired.
                preCommitRequirementValidator.throwIfPreCommitRequirementsNotMet(getStartTimestamp());
                Validate.isTrue(
                        false,
                        "Missing conflicting value for cell: %s for table %s",
                        cellToConflict.get(cell),
                        tableRef);
            }
            if (conflictingValues.get(cell).getTimestamp() != (cellEntry.getValue() - 1)) {
                // This error case could happen if our locks expired.
                preCommitRequirementValidator.throwIfPreCommitRequirementsNotMet(getStartTimestamp());
                Validate.isTrue(
                        false,
                        "Wrong timestamp for cell in table %s Expected: %s Actual: %s",
                        tableRef,
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
                        LoggingArgs.tableRef(tableRef));
            }
        }
        if (conflictingCells.isEmpty()) {
            return;
        }
        Predicate<CellConflict> conflicting =
                Predicates.compose(Predicates.in(conflictingCells), CellConflict.getCellFunction());
        transactionOutcomeMetrics.markWriteWriteConflict(tableRef);
        throw TransactionConflictException.create(
                tableRef,
                getStartTimestamp(),
                Sets.filter(spanningWrites, conflicting),
                Sets.filter(dominatingWrites, conflicting),
                System.currentTimeMillis() - timeCreated,
                List.of(LoggingArgs.tableRef(tableRef)));
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
        long startTs = getStartTimestamp();
        Map<Cell, Long> rawResults = dataKeyValueService.getLatestTimestamps(tableRef, keysToLoad);
        LongLongMap commitTimestamps =
                AtlasFutures.getUnchecked(commitTimestampLoader.getCommitTimestampsNonBlockingForValidation(
                        tableRef, LongLists.immutable.ofAll(rawResults.values())));

        // TODO(fdesouza): Remove this once PDS-95791 is resolved.
        conflictTracer.collect(startTs, keysToLoad, rawResults, commitTimestamps);

        Map<Cell, Long> keysToDelete = Maps.newHashMapWithExpectedSize(0);

        for (Map.Entry<Cell, Long> e : rawResults.entrySet()) {
            Cell key = e.getKey();
            long theirStartTimestamp = e.getValue();
            if (theirStartTimestamp == startTs) {
                AssertUtils.assertAndLog(log, false, "Timestamp reuse is bad:" + startTs);
            }

            long theirCommitTimestamp =
                    commitTimestamps.getIfAbsent(theirStartTimestamp, TransactionConstants.FAILED_COMMIT_TS);
            if (theirCommitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
                // The value has no commit timestamp or was explicitly rolled back.
                // This means the value is garbage from a transaction which didn't commit.
                keysToDelete.put(key, theirStartTimestamp);
                continue;
            } else if (theirCommitTimestamp == startTs) {
                AssertUtils.assertAndLog(log, false, "Timestamp reuse is bad:" + startTs);
            }

            if (theirStartTimestamp > startTs) {
                dominatingWrites.add(Cells.createConflict(key, theirStartTimestamp, theirCommitTimestamp));
            } else if (theirCommitTimestamp > startTs) {
                spanningWrites.add(Cells.createConflict(key, theirStartTimestamp, theirCommitTimestamp));
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
            LongLongMap commitTimestamps,
            TransactionService transactionService) {
        ImmutableLongSet timestamps = LongSets.immutable.ofAll(keysToDelete.values());
        boolean allRolledBack = timestamps.allSatisfy(startTs -> {
            if (commitTimestamps.containsKey(startTs)) {
                long commitTs = commitTimestamps.get(startTs);
                if (commitTs != TransactionConstants.FAILED_COMMIT_TS) {
                    throw new SafeIllegalArgumentException(
                            "Cannot rollback already committed transaction",
                            SafeArg.of("startTs", startTs),
                            SafeArg.of("commitTs", commitTs));
                }
                return true;
            }
            log.warn("Rolling back transaction: {}", SafeArg.of("startTs", startTs));
            return rollbackOtherTransaction(startTs, transactionService);
        });

        if (allRolledBack) {
            deleteExecutor.scheduleForDeletion(tableRef, keysToDelete);
        }
        return allRolledBack;
    }

    /**
     * Rollback a someone else's transaction.
     *
     * @return true if the other transaction was rolled back
     */
    private boolean rollbackOtherTransaction(long startTs, TransactionService transactionService) {
        try {
            transactionService.putUnlessExists(startTs, TransactionConstants.FAILED_COMMIT_TS);
            snapshotEventRecorder.recordRolledBackOtherTransaction();
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
    /// Close
    ///////////////////////////////////////////////////////////////////////////

    private void close() {
        try {
            // Run close() to release locks before running success callbacks, since success callbacks might
            // start a new transaction and attempt to grab the same locks as the current transaction.
            closer.close();
        } finally {
            if (isDefinitivelyCommitted()) {
                successCallbackManager.runCallbacks();
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    /// Locking
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This method should acquire any locks needed to do proper concurrency control at commit time.
     */
    protected LockToken acquireLocksForCommit() {
        LocksAndMetadata locksAndMetadata = getLocksAndMetadataForWrites();
        Set<LockDescriptor> lockDescriptors = locksAndMetadata.lockDescriptors();
        TransactionConfig currentTransactionConfig = transactionConfig.get();

        // TODO(fdesouza): Revert this once PDS-95791 is resolved.
        long lockAcquireTimeoutMillis = currentTransactionConfig.getLockAcquireTimeoutMillis();
        LockRequest request = LockRequest.of(lockDescriptors, lockAcquireTimeoutMillis, locksAndMetadata.metadata());

        RuntimeException stackTraceSnapshot = new SafeRuntimeException("I exist to show you the stack trace");
        LockResponse lockResponse = timelockService.lock(
                request,
                ClientLockingOptions.builder()
                        .maximumLockTenure(
                                currentTransactionConfig.commitLockTenure().toJavaDuration())
                        .tenureExpirationCallback(() -> logCommitLockTenureExceeded(
                                lockDescriptors, currentTransactionConfig, stackTraceSnapshot))
                        .build());
        if (!lockResponse.wasSuccessful()) {
            transactionOutcomeMetrics.markCommitLockAcquisitionFailed();
            log.error(
                    "Timed out waiting while acquiring commit locks. Timeout was {} ms. "
                            + "First ten required locks were {}.",
                    SafeArg.of("acquireTimeoutMs", lockAcquireTimeoutMillis),
                    SafeArg.of("numberOfDescriptors", lockDescriptors.size()),
                    UnsafeArg.of("firstTenLockDescriptors", Iterables.limit(lockDescriptors, 10)));
            throw new TransactionLockAcquisitionTimeoutException("Timed out while acquiring commit locks.");
        }
        return lockResponse.getToken();
    }

    private void logCommitLockTenureExceeded(
            Set<LockDescriptor> lockDescriptors,
            TransactionConfig currentTransactionConfig,
            RuntimeException stackTraceSnapshot) {
        log.warn(
                "This transaction held on to its commit locks for longer than its tenure, which is"
                        + " suspicious. In the interest of liveness we will unlock this lock and allow"
                        + " other transactions to proceed.",
                SafeArg.of("commitLockTenure", currentTransactionConfig.commitLockTenure()),
                UnsafeArg.of("firstTenLockDescriptors", Iterables.limit(lockDescriptors, 10)),
                stackTraceSnapshot);
    }

    protected LocksAndMetadata getLocksAndMetadataForWrites() {
        ImmutableSet.Builder<LockDescriptor> lockDescriptorSetBuilder = ImmutableSet.builder();
        ImmutableMap.Builder<LockDescriptor, ChangeMetadata> lockDescriptorToChangeMetadataBuilder =
                ImmutableMap.builder();
        long cellLockCount = 0L;
        long rowLockCount = 0L;
        long cellChangeMetadataCount = 0L;
        long rowChangeMetadataCount = 0L;
        for (TableReference tableRef : localWriteBuffer.getLocalWrites().keySet()) {
            ConflictHandler conflictHandler = getConflictHandlerForTable(tableRef);
            if (conflictHandler.lockCellsForConflicts()) {
                LockAndChangeMetadataCount counts =
                        collectCellLocks(lockDescriptorSetBuilder, lockDescriptorToChangeMetadataBuilder, tableRef);
                cellLockCount += counts.lockCount();
                cellChangeMetadataCount += counts.changeMetadataCount();
            }
            if (conflictHandler.lockRowsForConflicts()) {
                LockAndChangeMetadataCount counts =
                        collectRowLocks(lockDescriptorSetBuilder, lockDescriptorToChangeMetadataBuilder, tableRef);
                rowLockCount += counts.lockCount();
                rowChangeMetadataCount += counts.changeMetadataCount();
            }
        }
        lockDescriptorSetBuilder.add(AtlasRowLockDescriptor.of(
                TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                TransactionConstants.getValueForTimestamp(getStartTimestamp())));

        cellCommitLocksRequested = cellLockCount;
        rowCommitLocksRequested = rowLockCount + 1;
        cellChangeMetadataSent = cellChangeMetadataCount;
        rowChangeMetadataSent = rowChangeMetadataCount;

        Map<LockDescriptor, ChangeMetadata> lockDescriptorToChangeMetadata =
                lockDescriptorToChangeMetadataBuilder.buildOrThrow();
        return LocksAndMetadata.of(
                lockDescriptorSetBuilder.build(),
                // For now, lock request metadata only consists of change metadata. If it is absent, we can save
                // computation by not doing an index encoding
                lockDescriptorToChangeMetadata.isEmpty()
                        ? Optional.empty()
                        : Optional.of(LockRequestMetadata.of(lockDescriptorToChangeMetadata)));
    }

    private LockAndChangeMetadataCount collectCellLocks(
            ImmutableSet.Builder<LockDescriptor> lockDescriptorSetBuilder,
            ImmutableMap.Builder<LockDescriptor, ChangeMetadata> lockDescriptorToChangeMetadataBuilder,
            TableReference tableRef) {
        long lockCount = 0;
        long changeMetadataCount = 0;
        Map<Cell, ChangeMetadata> changeMetadataForWrites = localWriteBuffer.getChangeMetadataForTable(tableRef);
        for (Cell cell : localWriteBuffer.getLocalWritesForTable(tableRef).keySet()) {
            LockDescriptor lockDescriptor =
                    AtlasCellLockDescriptor.of(tableRef.getQualifiedName(), cell.getRowName(), cell.getColumnName());
            lockDescriptorSetBuilder.add(lockDescriptor);
            lockCount++;
            if (changeMetadataForWrites.containsKey(cell)) {
                lockDescriptorToChangeMetadataBuilder.put(lockDescriptor, changeMetadataForWrites.get(cell));
                changeMetadataCount++;
            }
        }
        return ImmutableLockAndChangeMetadataCount.of(lockCount, changeMetadataCount);
    }

    private LockAndChangeMetadataCount collectRowLocks(
            ImmutableSet.Builder<LockDescriptor> lockDescriptorSetBuilder,
            ImmutableMap.Builder<LockDescriptor, ChangeMetadata> lockDescriptorToChangeMetadataBuilder,
            TableReference tableRef) {
        long lockCount = 0;
        long changeMetadataCount = 0;
        Map<Cell, ChangeMetadata> changeMetadataForWrites = localWriteBuffer.getChangeMetadataForTable(tableRef);
        Optional<byte[]> lastRow = Optional.empty();
        Optional<byte[]> lastRowWithMetadata = Optional.empty();
        Optional<LockDescriptor> currentRowDescriptor = Optional.empty();
        for (Cell cell : localWriteBuffer.getLocalWritesForTable(tableRef).keySet()) {
            if (lastRow.isEmpty() || !isSameRow(lastRow.get(), cell.getRowName())) {
                // We are looking at the first cell of a new row
                LockDescriptor rowLock = AtlasRowLockDescriptor.of(tableRef.getQualifiedName(), cell.getRowName());
                lockDescriptorSetBuilder.add(rowLock);
                currentRowDescriptor = Optional.of(rowLock);
                lastRow = Optional.of(cell.getRowName());
                lockCount++;
            }
            if (changeMetadataForWrites.containsKey(cell)) {
                if (lastRowWithMetadata.isPresent() && isSameRow(lastRowWithMetadata.get(), cell.getRowName())) {
                    throw new SafeIllegalStateException(
                            "Two different cells in the same row have metadata and we create locks on row level.",
                            LoggingArgs.tableRef(tableRef),
                            UnsafeArg.of("rowName", cell.getRowName()),
                            UnsafeArg.of("newMetadata", changeMetadataForWrites.get(cell)));
                }
                // At this point, currentRowDescriptor will always contain the row of the current cell
                lockDescriptorToChangeMetadataBuilder.put(
                        currentRowDescriptor.orElseThrow(), changeMetadataForWrites.get(cell));
                lastRowWithMetadata = Optional.of(cell.getRowName());
                changeMetadataCount++;
            }
        }
        return ImmutableLockAndChangeMetadataCount.of(lockCount, changeMetadataCount);
    }

    private boolean isSameRow(byte[] rowA, byte[] rowB) {
        return rowA != null && rowB != null && Arrays.equals(rowA, rowB);
    }

    ///////////////////////////////////////////////////////////////////////////
    /// Commit timestamp management
    ///////////////////////////////////////////////////////////////////////////

    private LongSet getStartTimestampsForValues(Iterable<Value> values) {
        return LongSets.immutable.withAll(Streams.stream(values).mapToLong(Value::getTimestamp));
    }

    /**
     * This will attempt to put the commitTimestamp into the DB.
     *
     * @throws TransactionLockTimeoutException If our locks timed out while trying to commit.
     * @throws TransactionCommitFailedException failed when committing in a way that isn't retriable
     */
    private void putCommitTimestamp(long commitTimestamp, TransactionService transactionService)
            throws TransactionFailedException {
        Preconditions.checkArgument(commitTimestamp > getStartTimestamp(), "commitTs must be greater than startTs");
        try {
            transactionService.putUnlessExists(getStartTimestamp(), commitTimestamp);
        } catch (KeyAlreadyExistsException e) {
            handleKeyAlreadyExistsException(commitTimestamp, e);
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

    private void handleKeyAlreadyExistsException(long commitTs, KeyAlreadyExistsException ex) {
        try {
            if (wasCommitSuccessful(commitTs)) {
                // We did actually commit successfully.  This case could happen if the impl
                // for putUnlessExists did a retry and we had committed already
                return;
            }

            SummarizedLockCheckResult lockCheckResult =
                    transactionLocksManager.getExpiredImmutableTimestampAndCommitLocksWithFullSummary();
            if (lockCheckResult.expiredLocks().isPresent()) {
                transactionOutcomeMetrics.markLocksExpired();
                throw new TransactionLockTimeoutException(
                        "Our commit was already rolled back at commit time"
                                + " because our locks timed out. startTs: " + getStartTimestamp() + ".  "
                                + lockCheckResult.expiredLocks().get().errorDescription(),
                        ex);
            } else {
                log.info(
                        "This transaction has been rolled back by someone else, even though we believe we still hold "
                                + "the locks. This is not expected to occur frequently.",
                        lockCheckResult
                                .immutableTimestampLock()
                                .map(token -> token.toSafeArg("immutableTimestampLock"))
                                .orElseGet(() -> SafeArg.of("immutableTimestampLock", null)),
                        SafeArg.of(
                                "allLockTokens",
                                lockCheckResult.allLockTokens().stream()
                                        .map(LockToken::getRequestId)
                                        .collect(Collectors.toSet())));
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

    private boolean wasCommitSuccessful(long commitTs) {
        long startTs = getStartTimestamp();

        LongLongMap commitTimestamps =
                AtlasFutures.getUnchecked(commitTimestampLoader.getCommitTimestampsNonBlockingForValidation(
                        null, LongSets.immutable.of(startTs)));
        long storedCommit = commitTimestamps.get(startTs);
        if (storedCommit != commitTs && storedCommit != TransactionConstants.FAILED_COMMIT_TS) {
            throw new SafeIllegalArgumentException(
                    "Commit value is wrong.",
                    SafeArg.of("startTimestamp", startTs),
                    SafeArg.of("commitTimestamp", commitTs));
        }
        return storedCommit == commitTs;
    }

    @Override
    public void useTable(TableReference tableRef, ConstraintCheckable table) {
        constraintsByTableName.put(tableRef, table);
    }

    @Override
    public void onCommitOrAbort(Runnable runnable) {
        closer.register(runnable::run);
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
        boolean anyTableRequiresPreCommitValidation = involvedTables.stream()
                .anyMatch(tableRef -> readSnapshotValidator.doesTableRequirePreCommitValidation(
                        tableRef, !hasPossiblyUnvalidatedReads));
        boolean needsToValidate = !validateLocksOnReads || !hasReads();
        return anyTableRequiresPreCommitValidation && needsToValidate;
    }

    @Override
    public long getAgeMillis() {
        return System.currentTimeMillis() - timeCreated;
    }

    @Override
    public TransactionReadInfo getReadInfo() {
        return dataKeyValueService.getOverallReadInfo();
    }

    @Override
    public TransactionCommitLockInfo getCommitLockInfo() {
        return ImmutableTransactionCommitLockInfo.builder()
                .cellCommitLocksRequested(cellCommitLocksRequested)
                .rowCommitLocksRequested(rowCommitLocksRequested)
                .build();
    }

    @Override
    public TransactionWriteMetadataInfo getWriteMetadataInfo() {
        return ImmutableTransactionWriteMetadataInfo.builder()
                .changeMetadataBuffered(localWriteBuffer.changeMetadataCount())
                .cellChangeMetadataSent(cellChangeMetadataSent)
                .rowChangeMetadataSent(rowChangeMetadataSent)
                .build();
    }

    @Override
    public void reportExpectationsCollectedData() {
        if (isStillRunning()) {
            log.error(
                    "reportExpectationsCollectedData is called on an in-progress transaction",
                    SafeArg.of("state", state.get()));
            return;
        }

        ExpectationsData expectationsData = ImmutableExpectationsData.builder()
                .ageMillis(getAgeMillis())
                .readInfo(getReadInfo())
                .commitLockInfo(getCommitLockInfo())
                .writeMetadataInfo(getWriteMetadataInfo())
                .build();
        reportExpectationsCollectedData(expectationsData, expectationsDataCollectionMetrics);
    }

    @VisibleForTesting
    static void reportExpectationsCollectedData(ExpectationsData expectationsData, ExpectationsMetrics metrics) {
        ExpectationsMetricsReporter.INSTANCE.reportExpectationsCollectedData(expectationsData, metrics);
    }

    private long getStartTimestamp() {
        return startTimestamp.getAsLong();
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
            for (Map.Entry<TableReference, ConcurrentNavigableMap<Cell, byte[]>> entry :
                    localWriteBuffer.getLocalWrites().entrySet()) {
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
            for (Map.Entry<TableReference, ConcurrentNavigableMap<Cell, byte[]>> entry :
                    localWriteBuffer.getLocalWrites().entrySet()) {
                TableReference table = entry.getKey();
                Set<Cell> cells = entry.getValue().keySet();
                tableRefToCells.putAll(table, cells);
            }
        } else {
            AssertUtils.assertAndLog(log, false, "Expected state: " + expectedState + "; actual state: " + actualState);
        }
        return tableRefToCells;
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

    private <T> BatchingVisitable<T> scopeToTransaction(BatchingVisitable<T> delegateVisitable) {
        return new BatchingVisitable<T>() {
            @Override
            public <K extends Exception> boolean batchAccept(int batchSize, AbortingVisitor<? super List<T>, K> visitor)
                    throws K {
                ensureStillRunning();
                return delegateVisitable.batchAccept(batchSize, visitor);
            }
        };
    }

    private <T> Iterator<T> scopeToTransaction(Iterator<T> delegateIterator) {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                ensureStillRunning();
                return delegateIterator.hasNext();
            }

            @Override
            public T next() {
                ensureStillRunning();
                return delegateIterator.next();
            }
        };
    }

    private <T> ListenableFuture<T> scopeToTransaction(ListenableFuture<T> transactionFuture) {
        return Futures.transform(
                transactionFuture,
                txnTaskResult -> {
                    ensureStillRunning();
                    return txnTaskResult;
                },
                MoreExecutors.directExecutor());
    }
}
