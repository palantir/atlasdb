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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.base.Joiner;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.AutoDelegate_KeyValueService;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.api.cache.CacheEntry;
import com.palantir.atlasdb.keyvalue.api.cache.CacheMetrics;
import com.palantir.atlasdb.keyvalue.api.cache.CacheValue;
import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCache;
import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCacheImpl;
import com.palantir.atlasdb.keyvalue.api.cache.ValueCacheSnapshotImpl;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.keyvalue.api.watch.LocksAndMetadata;
import com.palantir.atlasdb.keyvalue.api.watch.NoOpLockWatchManager;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.exceptions.AtlasDbConstraintException;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.AutoDelegate_TransactionKeyValueService;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.ConstraintCheckable;
import com.palantir.atlasdb.transaction.api.ConstraintCheckingTransaction;
import com.palantir.atlasdb.transaction.api.ImmutableGetRangesQuery;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionCommitFailedException;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionFailedNonRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionKeyValueService;
import com.palantir.atlasdb.transaction.api.TransactionLockAcquisitionTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutNonRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.ValueAndChangeMetadata;
import com.palantir.atlasdb.transaction.api.exceptions.MoreCellsPresentThanExpectedException;
import com.palantir.atlasdb.transaction.api.expectations.TransactionCommitLockInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionWriteMetadataInfo;
import com.palantir.atlasdb.transaction.impl.metrics.DefaultMetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.impl.metrics.TableLevelMetricsController;
import com.palantir.atlasdb.transaction.impl.metrics.ToplistDeltaFilteringTableLevelMetricsController;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionMetrics;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetrics;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetricsAssert;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.proxy.MultiDelegateProxy;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockCollections;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.TimeDuration;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.util.result.Result;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
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
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import one.util.streamex.EntryStream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

@SuppressWarnings("checkstyle:all")
public abstract class AbstractSnapshotTransactionTest extends AtlasDbTestCase {
    static final String SYNC = "sync";
    static final String ASYNC = "async";
    private static final Consumer<Long> NO_OP_THROW_IF_CONDITION_INVALID = _timestamp -> {};

    private final String name;
    private final WrapperWithTracker<CallbackAwareTransaction> transactionWrapper;
    private final WrapperWithTracker<TransactionKeyValueService> keyValueServiceWrapper;

    private final Map<String, ExpectationFactory> expectationsMapping =
            ImmutableMap.<String, ExpectationFactory>builder()
                    .put(SYNC, AbstractSnapshotTransactionTest.this::syncGetExpectation)
                    .put(ASYNC, AbstractSnapshotTransactionTest.this::asyncGetExpectation)
                    .buildOrThrow();
    private final TimestampCache timestampCache = new DefaultTimestampCache(
            metricsManager.getRegistry(), () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE);
    private final ExecutorService getRangesExecutor = Executors.newFixedThreadPool(8);
    private final int defaultGetRangesConcurrency = 2;
    private final TransactionOutcomeMetrics transactionOutcomeMetrics = TransactionOutcomeMetrics.create(
            TransactionMetrics.of(metricsManager.getTaggedRegistry()), metricsManager.getTaggedRegistry());
    private final TableLevelMetricsController tableLevelMetricsController =
            ToplistDeltaFilteringTableLevelMetricsController.create(
                    metricsManager, DefaultMetricsFilterEvaluationContext.createDefault());

    private TransactionConfig transactionConfig;

    @FunctionalInterface
    interface ExpectationFactory {
        Expectations apply(KeyValueService keyValueService, Cell cell, long transactionTs, LockService lockService)
                throws InterruptedException;
    }

    private Expectations syncGetExpectation(
            KeyValueService kvMock, Cell cell, long transactionTs, LockService lockMock) {
        return new Expectations() {
            {
                oneOf(kvMock).get(TABLE, ImmutableMap.of(cell, transactionTs));
                will(throwException(new RuntimeException()));
            }
        };
    }

    private Expectations asyncGetExpectation(
            KeyValueService kvMock, Cell cell, long transactionTs, LockService lockMock) {
        return new Expectations() {
            {
                oneOf(kvMock).getAsync(TABLE, ImmutableMap.of(cell, transactionTs));
                will(returnValue(Futures.immediateFailedFuture(new RuntimeException())));
            }
        };
    }

    public AbstractSnapshotTransactionTest(
            String name,
            WrapperWithTracker<CallbackAwareTransaction> transactionWrapper,
            WrapperWithTracker<TransactionKeyValueService> keyValueServiceWrapper) {
        this.name = name;
        this.transactionWrapper = transactionWrapper;
        this.keyValueServiceWrapper = keyValueServiceWrapper;
    }

    private static class UnstableKeyValueService implements AutoDelegate_KeyValueService {

        private final KeyValueService delegate;
        private final Random random;

        private boolean randomlyThrow = false;
        private boolean randomlyHang = false;

        public UnstableKeyValueService(KeyValueService keyValueService, Random random) {
            this.delegate = keyValueService;
            this.random = random;
        }

        @Override
        public KeyValueService delegate() {
            return delegate;
        }

        @Override
        public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
            if (randomlyThrow && random.nextInt(3) == 0) {
                throw new RuntimeException();
            }
            if (randomlyHang && random.nextInt(3) == 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    // Do nothing
                }
                throw new RuntimeException();
            }
            delegate().put(tableRef, values, timestamp);
        }

        public void setRandomlyHang(boolean randomlyHang) {
            this.randomlyHang = randomlyHang;
        }

        public void setRandomlyThrow(boolean randomlyThrow) {
            this.randomlyThrow = randomlyThrow;
        }
    }

    private static final PreCommitCondition ALWAYS_FAILS_CONDITION = new PreCommitCondition() {
        @Override
        public void throwIfConditionInvalid(long timestamp) {
            throw new TransactionFailedRetriableException("Condition failed");
        }

        @Override
        public void cleanup() {}
    };

    static final TableReference TABLE = TableReference.createFromFullyQualifiedName("default.table");
    static final TableReference TABLE1 = TableReference.createFromFullyQualifiedName("default.table1");
    static final TableReference TABLE2 = TableReference.createFromFullyQualifiedName("default.table2");

    static final TableReference TABLE_SWEPT_THOROUGH = TableReference.createFromFullyQualifiedName("default.table3");
    static final TableReference TABLE_SWEPT_CONSERVATIVE =
            TableReference.createFromFullyQualifiedName("default.table4");
    static final TableReference TABLE_SWEPT_THOROUGH_MIGRATION =
            TableReference.createFromFullyQualifiedName("default.table5");

    static final TableReference TABLE_NO_SWEEP = TableReference.createFromFullyQualifiedName("default.table6");

    private static final byte[] ROW_FOO = "foo".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ROW_BAR = "bar".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COL_A = "a".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COL_B = "b".getBytes(StandardCharsets.UTF_8);

    private static final Cell TEST_CELL = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
    private static final Cell TEST_CELL_2 = Cell.create(PtBytes.toBytes("row2"), PtBytes.toBytes("column2"));
    private static final Cell TEST_CELL_3 = Cell.create(PtBytes.toBytes("row3"), PtBytes.toBytes("column3"));
    private static final Cell TEST_CELL_4 = Cell.create(PtBytes.toBytes("row4"), PtBytes.toBytes("column4"));

    private static final byte[] TEST_VALUE = PtBytes.toBytes("value");

    private static final ChangeMetadata UPDATE_CHANGE_METADATA =
            ChangeMetadata.updated(PtBytes.toBytes("abc"), PtBytes.toBytes("def"));
    private static final ChangeMetadata DELETE_CHANGE_METADATA = ChangeMetadata.deleted(PtBytes.toBytes("old"));
    private static final ChangeMetadata UNCHANGED_CHANGE_METADATA = ChangeMetadata.unchanged();

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        transactionConfig = ImmutableTransactionConfig.builder().build();

        keyValueService.createTable(TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(TABLE1, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(TABLE2, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(
                TABLE_SWEPT_THOROUGH,
                getTableMetadataForSweepStrategy(SweepStrategy.THOROUGH).persistToBytes());
        keyValueService.createTable(
                TABLE_SWEPT_CONSERVATIVE,
                getTableMetadataForSweepStrategy(SweepStrategy.CONSERVATIVE).persistToBytes());
        keyValueService.createTable(
                TABLE_SWEPT_THOROUGH_MIGRATION,
                getTableMetadataForSweepStrategy(SweepStrategy.THOROUGH_MIGRATION)
                        .persistToBytes());
        keyValueService.createTable(
                TABLE_NO_SWEEP,
                getTableMetadataForSweepStrategy(SweepStrategy.NOTHING).persistToBytes());
    }

    @Override
    protected TestTransactionManager constructTestTransactionManager() {
        return new TestTransactionManagerImpl(
                metricsManager,
                keyValueService,
                inMemoryTimelockExtension,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                timestampCache,
                sweepQueue,
                deleteExecutor,
                transactionWrapper,
                keyValueServiceWrapper,
                knowledge);
    }

    @Test
    public void testConcurrentWriteChangedConflicts() throws InterruptedException, ExecutionException {
        overrideConflictHandlerForTable(TABLE, ConflictHandler.RETRY_ON_VALUE_CHANGED);
        long val = concurrentlyIncrementValueThousandTimesAndGet();
        assertThat(val).isEqualTo(1000);
    }

    @Test
    public void testConcurrentWriteWriteConflicts() throws InterruptedException, ExecutionException {
        long val = concurrentlyIncrementValueThousandTimesAndGet();
        assertThat(val).isEqualTo(1000);
        TransactionOutcomeMetricsAssert.assertThat(transactionOutcomeMetrics)
                .hasPlaceholderWriteWriteConflictsSatisfying(
                        conflicts -> assertThat(conflicts).isGreaterThanOrEqualTo(1L));
    }

    @Test
    public void testConcurrentWriteIgnoreConflicts() throws InterruptedException, ExecutionException {
        overrideConflictHandlerForTable(TABLE, ConflictHandler.IGNORE_ALL);
        long val = concurrentlyIncrementValueThousandTimesAndGet();
        assertThat(val).isLessThan(1000L);
    }

    @Test
    public void testImmutableTs() {
        final long firstTs = timestampService.getFreshTimestamp();
        long startTs = txManager.runTaskThrowOnConflict(t -> {
            assertThat(firstTs).isLessThan(txManager.getImmutableTimestamp());
            assertThat(txManager.getImmutableTimestamp()).isLessThan(t.getTimestamp());
            assertThat(t.getTimestamp()).isLessThan(timestampService.getFreshTimestamp());
            return t.getTimestamp();
        });
        assertThat(firstTs).isLessThan(txManager.getImmutableTimestamp());

        // Immutable timestamp may not be cleared immediately.
        Awaitility.await("immutable timestamp should advance past our transaction's start timestamp")
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> startTs < txManager.getImmutableTimestamp());
    }

    // If lock happens concurrent with get, we aren't sure that we can rollback the transaction
    @Test
    public void testLockAfterGet() throws Exception {
        byte[] rowName = PtBytes.toBytes("1");
        Mockery mockery = new Mockery();
        mockery.setThreadingPolicy(new Synchroniser());
        final KeyValueService kvMock = mockery.mock(KeyValueService.class);
        final LockService lockMock = mockery.mock(LockService.class);
        LockService lock = MultiDelegateProxy.newProxyInstance(LockService.class, lockService, lockMock);

        final Cell cell = Cell.create(rowName, rowName);
        timestampService.getFreshTimestamp();
        final long startTs = timestampService.getFreshTimestamp();
        final long transactionTs = timestampService.getFreshTimestamp();
        keyValueService.put(TABLE, ImmutableMap.of(cell, PtBytes.EMPTY_BYTE_ARRAY), startTs);

        mockery.checking(expectationsMapping.get(name).apply(kvMock, cell, transactionTs, lockMock));
        mockery.checking(new Expectations() {
            {
                never(lockMock).lockWithFullLockResponse(with(LockClient.ANONYMOUS), with(any(LockRequest.class)));
            }
        });

        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        TransactionKeyValueService kvs = keyValueServiceWrapper.apply(
                txManagerKvs.getTransactionKeyValueService(() -> transactionTs), pathTypeTracker);
        Transaction snapshot = transactionWrapper.apply(
                new SnapshotTransaction(
                        metricsManager,
                        kvs,
                        inMemoryTimelockExtension.getLegacyTimelockService(),
                        NoOpLockWatchManager.create(),
                        transactionService,
                        NoOpCleaner.INSTANCE,
                        () -> transactionTs,
                        ConflictDetectionManagers.create(keyValueService),
                        SweepStrategyManagers.createDefault(keyValueService),
                        transactionTs,
                        Optional.empty(),
                        PreCommitConditions.NO_OP,
                        AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                        null,
                        TransactionReadSentinelBehavior.THROW_EXCEPTION,
                        false,
                        timestampCache,
                        getRangesExecutor,
                        defaultGetRangesConcurrency,
                        MultiTableSweepQueueWriter.NO_OP,
                        new DefaultDeleteExecutor(
                                txManagerKvs,
                                inMemoryTimelockExtension.getLegacyTimelockService()::getFreshTimestamp,
                                MoreExecutors.newDirectExecutorService()),
                        true,
                        () -> transactionConfig,
                        ConflictTracer.NO_OP,
                        tableLevelMetricsController,
                        knowledge),
                pathTypeTracker);
        assertThatThrownBy(() -> snapshot.get(TABLE, ImmutableSet.of(cell))).isInstanceOf(RuntimeException.class);

        mockery.assertIsSatisfied();
    }

    @SuppressWarnings("ThreadPriorityCheck")
    @Test
    public void testTransactionAtomicity() throws Exception {
        // This test runs multiple transactions in parallel, with KeyValueService.put calls throwing
        // a RuntimeException from time to time and hanging other times. which effectively kills the
        // thread. We ensure that every transaction either adds 5 rows to the table or adds 0 rows
        // by checking at the end that the number of rows is a multiple of 5.
        final TableReference tableRef = TABLE;
        Random random = new Random(1);

        final UnstableKeyValueService unstableKvs = new UnstableKeyValueService(keyValueService, random);
        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        final TestTransactionManager unstableTransactionManager = new TestTransactionManagerImpl(
                metricsManager,
                unstableKvs,
                inMemoryTimelockExtension,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                timestampCache,
                sweepQueue,
                MoreExecutors.newDirectExecutorService(),
                transactionWrapper,
                keyValueServiceWrapper,
                knowledge);

        ScheduledExecutorService service = PTExecutors.newScheduledThreadPool(20);

        for (int i = 0; i < 30; i++) {
            final int threadNumber = i;
            service.schedule(
                    (Callable<Void>) () -> {
                        if (threadNumber == 10) {
                            unstableKvs.setRandomlyThrow(true);
                        }
                        if (threadNumber == 20) {
                            unstableKvs.setRandomlyHang(true);
                        }

                        Transaction transaction = unstableTransactionManager.createNewTransaction();
                        BatchingVisitable<RowResult<byte[]>> results = transaction.getRange(
                                tableRef, RangeRequest.builder().build());

                        final MutableInt nextIndex = new MutableInt(0);
                        results.batchAccept(
                                1, AbortingVisitors.batching((AbortingVisitor<RowResult<byte[]>, Exception>) row -> {
                                    byte[] dataBytes = row.getColumns().get(PtBytes.toBytes("data"));
                                    BigInteger dataValue = new BigInteger(dataBytes);
                                    nextIndex.setValue(Math.max(nextIndex.toInteger(), dataValue.intValue() + 1));
                                    return true;
                                }));

                        // nextIndex now contains the least row number not already in the table. Add 5 more
                        // rows to the table.
                        for (int j = 0; j < 5; j++) {
                            int rowNumber = nextIndex.toInteger() + j;
                            Cell cell = Cell.create(PtBytes.toBytes("row" + rowNumber), PtBytes.toBytes("data"));
                            transaction.put(
                                    tableRef,
                                    ImmutableMap.of(
                                            cell, BigInteger.valueOf(rowNumber).toByteArray()));
                            Thread.yield();
                        }
                        transaction.commit();
                        return null;
                    },
                    i * 20,
                    TimeUnit.MILLISECONDS);
        }

        service.shutdown();
        service.awaitTermination(1, TimeUnit.SECONDS);

        // Verify each table has a number of rows that's a multiple of 5
        Transaction verifyTransaction = txManager.createNewTransaction();
        BatchingVisitable<RowResult<byte[]>> results =
                verifyTransaction.getRange(tableRef, RangeRequest.builder().build());

        final MutableInt numRows = new MutableInt(0);
        results.batchAccept(1, AbortingVisitors.batching((AbortingVisitor<RowResult<byte[]>, Exception>) row -> {
            numRows.increment();
            return true;
        }));

        assertThat(numRows.toInteger() % 5).isEqualTo(0);
    }

    @Test
    public void testTransactionIsolation() throws Exception {
        // This test creates multiple partially-done transactions and ensures that even after writes
        // and commits, the value returned by get() is consistent with either the initial value or
        // the most recently written value within the transaction.
        int numColumns = 10;
        int numTransactions = 500;

        Transaction initTransaction = txManager.createNewTransaction();
        for (int i = 0; i < numColumns; i++) {
            Cell cell = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("column" + i));
            BigInteger cellValue = BigInteger.valueOf(i);
            initTransaction.put(TABLE, ImmutableMap.of(cell, cellValue.toByteArray()));
        }
        initTransaction.commit();

        List<Transaction> allTransactions = new ArrayList<>();
        List<List<BigInteger>> writtenValues = new ArrayList<>();
        for (int i = 0; i < numTransactions; i++) {
            allTransactions.add(txManager.createNewTransaction());
            List<BigInteger> initialValues = new ArrayList<>();
            for (int j = 0; j < numColumns; j++) {
                initialValues.add(BigInteger.valueOf(j));
            }
            writtenValues.add(initialValues);
        }

        Random random = new Random(1);
        for (int i = 0; i < 10000 && !allTransactions.isEmpty(); i++) {
            int transactionIndex = random.nextInt(allTransactions.size());
            Transaction t = allTransactions.get(transactionIndex);

            int actionCode = random.nextInt(30);

            if (actionCode == 0) {
                // Commit the transaction and remove it.
                try {
                    t.commit();
                } catch (TransactionConflictException e) {
                    // Ignore any conflicts; the transaction just fails
                }
                allTransactions.remove(transactionIndex);
                writtenValues.remove(transactionIndex);
            } else if (actionCode < 15) {
                // Write a new value to a random column
                int columnNumber = random.nextInt(numColumns);
                Cell cell = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("column" + columnNumber));

                BigInteger newValue = BigInteger.valueOf(random.nextInt(100000));
                t.put(TABLE, ImmutableMap.of(cell, newValue.toByteArray()));
                writtenValues.get(transactionIndex).set(columnNumber, newValue);
            } else {
                // Read and verify the value of a random column
                int columnNumber = random.nextInt(numColumns);
                Cell cell = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("column" + columnNumber));
                byte[] storedValue = t.get(TABLE, Collections.singleton(cell)).get(cell);
                BigInteger expectedValue = writtenValues.get(transactionIndex).get(columnNumber);
                assertThat(new BigInteger(storedValue)).isEqualTo(expectedValue);
            }
        }
    }

    @Test
    public void testGetRowsLocalWritesWithColumnSelection() {
        // This test ensures getRows correctly applies columnSelection when there are local writes
        byte[] row1 = PtBytes.toBytes("row1");
        Cell row1Column1 = Cell.create(row1, PtBytes.toBytes("column1"));
        Cell row1Column2 = Cell.create(row1, PtBytes.toBytes("column2"));
        byte[] row1Column1Value = BigInteger.valueOf(1).toByteArray();
        byte[] row1Column2Value = BigInteger.valueOf(2).toByteArray();

        Transaction snapshotTx = serializableTxManager.createNewTransaction();
        snapshotTx.put(
                TABLE,
                ImmutableMap.of(
                        row1Column1, row1Column1Value,
                        row1Column2, row1Column2Value));

        ColumnSelection column1Selection = ColumnSelection.create(ImmutableList.of(row1Column1.getColumnName()));

        // local writes still apply columnSelection
        RowResult<byte[]> rowResult1 = snapshotTx
                .getRows(TABLE, ImmutableList.of(row1), column1Selection)
                .get(row1);
        assertThat(rowResult1.getColumns()).containsEntry(row1Column1.getColumnName(), row1Column1Value);
        assertThat(rowResult1.getColumns()).doesNotContainEntry(row1Column2.getColumnName(), row1Column2Value);

        RowResult<byte[]> rowResult2 = snapshotTx
                .getRows(TABLE, ImmutableList.of(row1), ColumnSelection.all())
                .get(row1);
        assertThat(rowResult2.getColumns()).containsEntry(row1Column1.getColumnName(), row1Column1Value);
        assertThat(rowResult2.getColumns()).containsEntry(row1Column2.getColumnName(), row1Column2Value);
    }

    @Test
    public void testTransactionWriteWriteConflicts() throws Exception {
        // This test creates various types of conflicting writes and makes sure that write-write
        // conflicts are thrown when necessary, and not thrown when there actually isn't a conflict.
        Cell row1Column1 = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        Cell row1Column2 = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column2"));
        Cell row2Column1 = Cell.create(PtBytes.toBytes("row2"), PtBytes.toBytes("column1"));

        // First transaction commits first, second tries to commit same write
        Transaction t1 = txManager.createNewTransaction();
        Transaction t2 = txManager.createNewTransaction();
        t1.put(TABLE1, ImmutableMap.of(row1Column1, BigInteger.valueOf(1).toByteArray()));
        t2.put(TABLE1, ImmutableMap.of(row1Column1, BigInteger.valueOf(1).toByteArray()));
        t1.commit();
        try {
            t2.commit();
            assertThat(false).isTrue();
        } catch (TransactionConflictException e) {
            // We expect to catch this exception
        }

        // Second transaction commits first, first tries to commit same write
        t1 = txManager.createNewTransaction();
        t2 = txManager.createNewTransaction();
        t1.put(TABLE1, ImmutableMap.of(row1Column1, BigInteger.valueOf(1).toByteArray()));
        t2.put(TABLE1, ImmutableMap.of(row1Column1, BigInteger.valueOf(1).toByteArray()));
        t2.commit();
        try {
            t1.commit();
            assertThat(false).isTrue();
        } catch (TransactionConflictException e) {
            // We expect to catch this exception
        }

        // Transactions committing to different rows
        t1 = txManager.createNewTransaction();
        t2 = txManager.createNewTransaction();
        t1.put(TABLE1, ImmutableMap.of(row1Column1, BigInteger.valueOf(1).toByteArray()));
        t2.put(TABLE1, ImmutableMap.of(row2Column1, BigInteger.valueOf(1).toByteArray()));
        t1.commit();
        t2.commit();

        // Transactions committing to different tables
        t1 = txManager.createNewTransaction();
        t2 = txManager.createNewTransaction();
        t1.put(TABLE1, ImmutableMap.of(row1Column1, BigInteger.valueOf(1).toByteArray()));
        t2.put(TABLE2, ImmutableMap.of(row1Column1, BigInteger.valueOf(1).toByteArray()));
        t1.commit();
        t2.commit();

        // Transactions committing to different columns in the same row
        t1 = txManager.createNewTransaction();
        t2 = txManager.createNewTransaction();
        t1.put(TABLE1, ImmutableMap.of(row1Column1, BigInteger.valueOf(1).toByteArray()));
        t2.put(TABLE1, ImmutableMap.of(row1Column2, BigInteger.valueOf(1).toByteArray()));
        t1.commit();
        t2.commit();
    }

    @Test
    public void readsFromThoroughlySweptTableShouldFailWhenLocksAreInvalid() {
        List<String> successfulTasks = getThoroughTableReadTasks().stream()
                .map(this::runTaskWithInvalidLocks)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        if (!successfulTasks.isEmpty()) {
            fail("Expected read to fail with TransactionFailedRetriableException, but it succeeded for: "
                    + Joiner.on(", ").join(successfulTasks));
        }
    }

    /**
     * Given Pair.of("label", task), return task label iff task succeeds.
     */
    private Optional<String> runTaskWithInvalidLocks(Pair<String, LockAwareTransactionTask<Void, Exception>> task) {
        try {
            txManager.runTaskWithLocksThrowOnConflict(ImmutableList.of(getExpiredHeldLocksToken()), task.getRight());
            return Optional.of(task.getLeft());
        } catch (TransactionFailedNonRetriableException expected) {
            return Optional.empty();
        } catch (Exception e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    @SuppressWarnings("CheckReturnValue")
    private List<Pair<String, LockAwareTransactionTask<Void, Exception>>> getThoroughTableReadTasks(
            TableReference thoroughTable) {
        ImmutableList.Builder<Pair<String, LockAwareTransactionTask<Void, Exception>>> tasks = ImmutableList.builder();
        final int batchHint = 1;

        tasks.add(Pair.of("get", (t, heldLocks) -> {
            t.get(thoroughTable, ImmutableSet.of(Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"))));
            return null;
        }));

        tasks.add(Pair.of("getRange", (t, heldLocks) -> {
            t.getRange(thoroughTable, RangeRequest.all()).batchAccept(batchHint, AbortingVisitors.alwaysTrue());
            return null;
        }));

        tasks.add(Pair.of("getRanges", (t, heldLocks) -> {
            Iterables.getLast(t.getRanges(thoroughTable, Collections.singleton(RangeRequest.all())));
            return null;
        }));

        tasks.add(Pair.of("getRows", (t, heldLocks) -> {
            t.getRows(thoroughTable, ImmutableSet.of(PtBytes.toBytes("row1")), ColumnSelection.all());
            return null;
        }));

        tasks.add(Pair.of(
                "getRowsColumnRange(TableReference, Iterable<byte[]>, BatchColumnRangeSelection)", (t, heldLocks) -> {
                    Collection<BatchingVisitable<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRange(
                                    thoroughTable,
                                    Collections.singleton(PtBytes.toBytes("row1")),
                                    BatchColumnRangeSelection.create(new ColumnRangeSelection(null, null), batchHint))
                            .values();
                    results.forEach(result -> result.batchAccept(batchHint, AbortingVisitors.alwaysTrue()));
                    return null;
                }));

        tasks.add(Pair.of(
                "getRowsColumnRange(TableReference, Iterable<byte[]>, ColumnRangeSelection, int)", (t, heldLocks) -> {
                    Iterators.getLast(t.getRowsColumnRange(
                            thoroughTable,
                            Collections.singleton(PtBytes.toBytes("row1")),
                            new ColumnRangeSelection(null, null),
                            batchHint));
                    return null;
                }));

        tasks.add(Pair.of("getRowsColumnRangeIterator", (t, heldLocks) -> {
            Collection<Iterator<Map.Entry<Cell, byte[]>>> results = t.getRowsColumnRangeIterator(
                            thoroughTable,
                            Collections.singleton(PtBytes.toBytes("row1")),
                            BatchColumnRangeSelection.create(new ColumnRangeSelection(null, null), batchHint))
                    .values();
            results.forEach(Iterators::getLast);
            return null;
        }));

        tasks.add(Pair.of("getRowsIgnoringLocalWrites", (t, heldLocks) -> {
            SnapshotTransaction snapshotTx = unwrapSnapshotTransaction(t);
            snapshotTx.getRowsIgnoringLocalWrites(thoroughTable, Collections.singleton(PtBytes.toBytes("row1")));
            return null;
        }));

        tasks.add(Pair.of("getIgnoringLocalWrites", (t, heldLocks) -> {
            SnapshotTransaction snapshotTx = unwrapSnapshotTransaction(t);
            snapshotTx.getIgnoringLocalWrites(
                    thoroughTable,
                    Collections.singleton(Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"))));
            return null;
        }));

        return tasks.build();
    }

    @SuppressWarnings("CheckReturnValue")
    private List<Pair<String, LockAwareTransactionTask<Void, Exception>>> getThoroughTableReadTasks() {
        List<Pair<String, LockAwareTransactionTask<Void, Exception>>> tasks = new ArrayList<>();
        tasks.addAll(getThoroughTableReadTasks(TABLE_SWEPT_THOROUGH));
        tasks.addAll(getThoroughTableReadTasks(TABLE_SWEPT_THOROUGH_MIGRATION));
        return tasks;
    }

    @Test
    public void testWriteChangedConflictsNoThrow() {
        overrideConflictHandlerForTable(TABLE, ConflictHandler.RETRY_ON_VALUE_CHANGED);
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        Transaction t1 = txManager.createNewTransaction();
        Transaction t2 = txManager.createNewTransaction();
        t1.delete(TABLE, ImmutableSet.of(cell));
        t2.delete(TABLE, ImmutableSet.of(cell));
        t1.commit();
        t2.commit();
    }

    @Test
    public void testWriteChangedConflictsThrow() {
        overrideConflictHandlerForTable(TABLE, ConflictHandler.RETRY_ON_VALUE_CHANGED);
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        Transaction t1 = txManager.createNewTransaction();
        Transaction t2 = txManager.createNewTransaction();
        t1.delete(TABLE, ImmutableSet.of(cell));
        t2.put(TABLE, ImmutableMap.of(cell, new byte[1]));
        t1.commit();
        try {
            t2.commit();
            fail("fail");
        } catch (TransactionConflictException e) {
            // good
        }

        t1 = txManager.createNewTransaction();
        t2 = txManager.createNewTransaction();
        t1.delete(TABLE, ImmutableSet.of(cell));
        t2.put(TABLE, ImmutableMap.of(cell, new byte[1]));
        t2.commit();
        try {
            t1.commit();
            fail("fail");
        } catch (TransactionConflictException e) {
            // good
        }

        t1 = txManager.createNewTransaction();
        t2 = txManager.createNewTransaction();
        t2.delete(TABLE, ImmutableSet.of(cell));
        t1.put(TABLE, ImmutableMap.of(cell, new byte[1]));
        t2.commit();
        try {
            t1.commit();
            fail("fail");
        } catch (TransactionConflictException e) {
            // good
        }

        t1 = txManager.createNewTransaction();
        t2 = txManager.createNewTransaction();
        t2.delete(TABLE, ImmutableSet.of(cell));
        t1.put(TABLE, ImmutableMap.of(cell, new byte[1]));
        t1.commit();
        try {
            t2.commit();
            fail("fail");
        } catch (TransactionConflictException e) {
            // good
        }
    }

    @Test
    public void testWriteWriteConflictsDeletedThrow() {
        overrideConflictHandlerForTable(TABLE, ConflictHandler.RETRY_ON_WRITE_WRITE);
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        Transaction t1 = txManager.createNewTransaction();
        Transaction t2 = txManager.createNewTransaction();
        t1.delete(TABLE, ImmutableSet.of(cell));
        t2.delete(TABLE, ImmutableSet.of(cell));
        t1.commit();
        assertThatThrownBy(t2::commit).isInstanceOf(TransactionConflictException.class);
    }

    @Test
    public void disallowPutOnEmptyObject() {
        Transaction t1 = txManager.createNewTransaction();
        assertThatThrownBy(() -> t1.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.EMPTY_BYTE_ARRAY)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void partiallyFilledRowsShouldBeVisible() {
        byte[] defaultRow = PtBytes.toBytes("row1");
        final Cell emptyCell = Cell.create(defaultRow, PtBytes.toBytes("column1"));
        final Cell writtenCell = Cell.create(defaultRow, PtBytes.toBytes("column2"));
        writeCells(TABLE, ImmutableMap.of(writtenCell, PtBytes.toBytes("writtenCell")));

        RowResult<byte[]> rowResult = readRow(defaultRow);

        assertThat(rowResult).isNotNull();
        assertThat(rowResult.getCellSet()).contains(writtenCell);
        assertThat(rowResult.getCellSet()).doesNotContain(emptyCell);
    }

    @Test
    public void noRetryOnExpiredLockTokens() throws InterruptedException {
        HeldLocksToken expiredLockToken = getExpiredHeldLocksToken();
        try {
            txManager.runTaskWithLocksWithRetry(ImmutableList.of(expiredLockToken), () -> null, (tx, locks) -> {
                tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
                return null;
            });
            fail("fail");
        } catch (TransactionLockTimeoutNonRetriableException e) {
            LockDescriptor descriptor = Iterables.getFirst(expiredLockToken.getLockDescriptors(), null);
            assertThat(e.getMessage()).contains(descriptor.toString());
            assertThat(e.getMessage()).contains("Retry is not possible.");
        }
    }

    @Test
    public void commitIfPreCommitConditionSucceeds() {
        serializableTxManager.runTaskWithConditionThrowOnConflict(PreCommitConditions.NO_OP, (tx, condition) -> {
            tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
            return null;
        });
    }

    @Test
    public void failToCommitIfPreCommitConditionFails() {
        try {
            serializableTxManager.runTaskWithConditionThrowOnConflict(ALWAYS_FAILS_CONDITION, (tx, condition) -> {
                tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
                return null;
            });
            fail("fail");
        } catch (TransactionFailedRetriableException e) {
            assertThat(e.getMessage()).contains("Condition failed");
        }
    }

    @Test
    public void commitWritesCallsPreCommitConditionWithMutationMap() {
        PreCommitCondition condition = mock(PreCommitCondition.class);
        Map<TableReference, Map<Cell, byte[]>> writes = Map.of(
                TABLE, Map.of(TEST_CELL, PtBytes.toBytes("value"), TEST_CELL_2, PtBytes.toBytes("value2")),
                TABLE2, Map.of(TEST_CELL, PtBytes.toBytes("value3"), TEST_CELL_3, PtBytes.toBytes("value3")));
        serializableTxManager.runTaskWithConditionThrowOnConflict(condition, (tx, preCommitCondition) -> {
            for (Map.Entry<TableReference, Map<Cell, byte[]>> entry : writes.entrySet()) {
                tx.put(entry.getKey(), entry.getValue());
            }
            tx.put(TABLE1, Map.of(TEST_CELL_4, PtBytes.toBytes("value4")));
            tx.delete(TABLE1, Set.of(TEST_CELL_4));
            return null;
        });

        Map<TableReference, Map<Cell, byte[]>> expectedWrites =
                ImmutableMap.<TableReference, Map<Cell, byte[]>>builder()
                        .putAll(writes)
                        .put(TABLE1, ImmutableMap.of(TEST_CELL_4, PtBytes.EMPTY_BYTE_ARRAY))
                        .buildOrThrow();

        verify(condition).throwIfConditionInvalid(eq(expectedWrites), anyLong());
    }

    @Test
    public void failToCommitIfPreCommitConditionWithMutationMapFails() {
        PreCommitCondition condition = mock(PreCommitCondition.class);
        doThrow(new TransactionFailedRetriableException("Condition failed"))
                .when(condition)
                .throwIfConditionInvalid(anyMap(), anyLong());
        try {
            serializableTxManager.runTaskWithConditionThrowOnConflict(condition, (tx, preCommitCondition) -> {
                tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
                return null;
            });
            fail("fail");
        } catch (TransactionFailedRetriableException e) {
            assertThat(e.getMessage()).contains("Condition failed");
            serializableTxManager.runTaskReadOnly(tx -> {
                assertThat(tx.get(TABLE, ImmutableSet.of(TEST_CELL))).isEmpty();
                return null;
            });
        }
    }

    @Test
    public void commitWithPreCommitConditionOnRetry() {
        Supplier<PreCommitCondition> conditionSupplier = mock(Supplier.class);
        when(conditionSupplier.get()).thenReturn(ALWAYS_FAILS_CONDITION).thenReturn(PreCommitConditions.NO_OP);

        serializableTxManager.runTaskWithConditionWithRetry(conditionSupplier, (tx, condition) -> {
            tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
            return null;
        });
    }

    @Test
    public void transactionDeletesAsyncOnRollback() {
        DeterministicScheduler executor = new DeterministicScheduler();
        TestTransactionManager deleteTxManager = new TestTransactionManagerImpl(
                metricsManager,
                keyValueService,
                inMemoryTimelockExtension,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                timestampCache,
                sweepQueue,
                executor,
                transactionWrapper,
                keyValueServiceWrapper,
                knowledge);

        Supplier<PreCommitCondition> conditionSupplier = mock(Supplier.class);
        when(conditionSupplier.get()).thenReturn(ALWAYS_FAILS_CONDITION).thenReturn(PreCommitConditions.NO_OP);

        deleteTxManager.runTaskWithConditionWithRetry(conditionSupplier, (tx, condition) -> {
            tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
            return null;
        });

        verify(keyValueService, times(0)).delete(any(), any());

        executor.runUntilIdle();

        verify(keyValueService, times(1)).delete(any(), any());
        TransactionOutcomeMetricsAssert.assertThat(transactionOutcomeMetrics)
                .hasSuccessfulCommits(1)
                .hasFailedCommits(1);
    }

    @Test
    public void runWithRetryFailsOnNonRetriableException() {
        PreCommitCondition nonRetriableFailure = new PreCommitCondition() {
            @Override
            public void throwIfConditionInvalid(long timestamp) {
                throw new TransactionFailedNonRetriableException("Condition failed");
            }

            @Override
            public void cleanup() {}
        };
        Supplier<PreCommitCondition> conditionSupplier = Suppliers.ofInstance(nonRetriableFailure);
        try {
            serializableTxManager.runTaskWithConditionWithRetry(conditionSupplier, (tx, condition) -> {
                tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
                return null;
            });
            fail("fail");
        } catch (TransactionFailedNonRetriableException e) {
            assertThat(e.getMessage()).contains("Condition failed");
        }
    }

    @Test
    public void readTransactionSucceedsIfConditionSucceeds() {
        serializableTxManager.runTaskWithConditionReadOnly(
                PreCommitConditions.NO_OP, (tx, condition) -> tx.get(TABLE, ImmutableSet.of(TEST_CELL)));
        TransactionOutcomeMetricsAssert.assertThat(transactionOutcomeMetrics).hasSuccessfulCommits(1);
    }

    @Test
    public void readTransactionFailsIfConditionFails() {
        try {
            serializableTxManager.runTaskWithConditionReadOnly(
                    ALWAYS_FAILS_CONDITION, (tx, condition) -> tx.get(TABLE, ImmutableSet.of(TEST_CELL)));
            fail("fail");
        } catch (TransactionFailedRetriableException e) {
            assertThat(e.getMessage()).contains("Condition failed");
        }
        TransactionOutcomeMetricsAssert.assertThat(transactionOutcomeMetrics).hasFailedCommits(1);
    }

    @Test
    public void cleanupPreCommitConditionsOnSuccess() {
        MutableLong counter = new MutableLong(0L);
        PreCommitCondition succeedsCondition =
                preCommitConditionFactory(NO_OP_THROW_IF_CONDITION_INVALID, counter::increment);

        serializableTxManager.runTaskWithConditionThrowOnConflict(succeedsCondition, (tx, condition) -> {
            tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
            return null;
        });
        assertThat(counter.intValue()).isEqualTo(1);

        serializableTxManager.runTaskWithConditionReadOnly(
                succeedsCondition, (tx, condition) -> tx.get(TABLE, ImmutableSet.of(TEST_CELL)));
        assertThat(counter.intValue()).isEqualTo(2);
    }

    @Test
    public void cleanupPreCommitConditionsOnFailure() {
        MutableLong counter = new MutableLong(0L);
        PreCommitCondition failsCondition = preCommitConditionFactory(
                _timestamp -> {
                    throw new TransactionFailedRetriableException("Condition failed");
                },
                counter::increment);

        assertThatThrownBy(() ->
                        serializableTxManager.runTaskWithConditionThrowOnConflict(failsCondition, (tx, condition) -> {
                            tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
                            return null;
                        }))
                .isInstanceOf(TransactionFailedRetriableException.class);
        assertThat(counter.intValue()).isEqualTo(1);

        assertThatThrownBy(() -> serializableTxManager.runTaskWithConditionReadOnly(
                        failsCondition, (tx, condition) -> tx.get(TABLE, ImmutableSet.of(TEST_CELL))))
                .isInstanceOf(TransactionFailedRetriableException.class);
        assertThat(counter.intValue()).isEqualTo(2);
    }

    @Test
    public void conditionCleanupRunsBeforeOnSuccessInReadWriteTask() {
        MutableLong counter = new MutableLong(0L);
        PreCommitCondition latchingCondition =
                preCommitConditionFactory(NO_OP_THROW_IF_CONDITION_INVALID, () -> assertThat(counter.getAndAdd(1))
                        .isEqualTo(0));

        serializableTxManager.runTaskWithConditionThrowOnConflict(latchingCondition, (tx, condition) -> {
            tx.onSuccess(() -> assertThat(counter.getAndAdd(1)).isEqualTo(1));
            tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
            return null;
        });
        assertThat(counter.intValue()).isEqualTo(2);
    }

    @Test
    public void conditionCleanupRunsBeforeOnSuccessInReadOnlyTask() {
        MutableLong counter = new MutableLong(0);
        PreCommitCondition preCommitCondition =
                preCommitConditionFactory(NO_OP_THROW_IF_CONDITION_INVALID, () -> assertThat(counter.getAndAdd(1))
                        .isEqualTo(0));
        serializableTxManager.runTaskWithConditionReadOnly(preCommitCondition, (tx, condition) -> {
            tx.onSuccess(() -> {
                assertThat(counter.getAndAdd(1)).isEqualTo(1);
            });
            return tx.get(TABLE, ImmutableSet.of(TEST_CELL));
        });
        assertThat(counter.intValue()).isEqualTo(2);
    }

    @Test
    public void getRowsColumnRangesReturnsInOrderInCaseOfAbortedTxns() {
        byte[] row = "foo".getBytes(StandardCharsets.UTF_8);
        Cell firstCell = Cell.create(row, "a".getBytes(StandardCharsets.UTF_8));
        Cell secondCell = Cell.create(row, "b".getBytes(StandardCharsets.UTF_8));
        byte[] value = new byte[1];

        serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(firstCell, value, secondCell, value));
            return null;
        });

        // this will write into the DB, because the protocol demands we write before we get a commit timestamp
        RuntimeException conditionFailure = new RuntimeException();
        assertThatThrownBy(() -> serializableTxManager.runTaskWithConditionWithRetry(
                        () -> new PreCommitCondition() {
                            @Override
                            public void throwIfConditionInvalid(long timestamp) {
                                throw conditionFailure;
                            }

                            @Override
                            public void cleanup() {}
                        },
                        (tx, condition) -> {
                            tx.put(TABLE, ImmutableMap.of(firstCell, value));
                            return null;
                        }))
                .isSameAs(conditionFailure);

        List<Cell> cells = serializableTxManager.runTaskReadOnly(tx -> BatchingVisitableView.of(tx.getRowsColumnRange(
                                TABLE, ImmutableList.of(row), BatchColumnRangeSelection.create(null, null, 10))
                        .get(row))
                .transform(Map.Entry::getKey)
                .immutableCopy());
        assertThat(cells).containsExactly(firstCell, secondCell);
    }

    @Test
    public void getOtherRowsColumnRangesReturnsInOrderInCaseOfAbortedTxns() {
        byte[] row = "foo".getBytes(StandardCharsets.UTF_8);
        Cell firstCell = Cell.create(row, "a".getBytes(StandardCharsets.UTF_8));
        Cell secondCell = Cell.create(row, "b".getBytes(StandardCharsets.UTF_8));
        byte[] value = new byte[1];

        serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(firstCell, value, secondCell, value));
            return null;
        });

        // this will write into the DB, because the protocol demands we write before we get a commit timestamp
        RuntimeException conditionFailure = new RuntimeException();
        assertThatThrownBy(() -> serializableTxManager.runTaskWithConditionWithRetry(
                        () -> new PreCommitCondition() {
                            @Override
                            public void throwIfConditionInvalid(long timestamp) {
                                throw conditionFailure;
                            }

                            @Override
                            public void cleanup() {}
                        },
                        (tx, condition) -> {
                            tx.put(TABLE, ImmutableMap.of(firstCell, value));
                            return null;
                        }))
                .isSameAs(conditionFailure);

        List<Cell> cells = serializableTxManager.runTaskReadOnly(tx -> Lists.transform(
                Lists.newArrayList(
                        tx.getRowsColumnRange(TABLE, ImmutableList.of(row), new ColumnRangeSelection(null, null), 10)),
                Map.Entry::getKey));
        assertThat(cells).containsExactly(firstCell, secondCell);
    }

    @Test
    public void getRowsColumnRangeMergesLocalWrites() {
        byte[] row = "foo".getBytes(StandardCharsets.UTF_8);
        Cell firstCell = Cell.create(row, "a".getBytes(StandardCharsets.UTF_8));
        Cell secondCell = Cell.create(row, "b".getBytes(StandardCharsets.UTF_8));
        byte[] value = new byte[1];

        serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(firstCell, value, secondCell, value));
            return null;
        });

        Cell inBetweenCell = Cell.create(row, "abba".getBytes(StandardCharsets.UTF_8));
        List<Cell> cells = serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(inBetweenCell, value));
            return Lists.transform(
                    Lists.newArrayList(tx.getRowsColumnRange(
                            TABLE, ImmutableList.of(row), new ColumnRangeSelection(null, null), 10)),
                    Map.Entry::getKey);
        });
        assertThat(cells).containsExactly(firstCell, inBetweenCell, secondCell);
    }

    @Test
    public void getRowsColumnRangeIncludesLocalWritesWhenRowEntirelyAbsentFromKvs() {
        byte[] row = "foo".getBytes(StandardCharsets.UTF_8);
        Cell beforeRange = Cell.create(row, "james".getBytes(StandardCharsets.UTF_8));
        Cell inRangeOne = Cell.create(row, "jeremy".getBytes(StandardCharsets.UTF_8));
        Cell inRangeTwo = Cell.create(row, "tom".getBytes(StandardCharsets.UTF_8));
        Cell afterRange = Cell.create(row, "will".getBytes(StandardCharsets.UTF_8));
        byte[] value = new byte[1];

        List<Cell> cells = serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(beforeRange, value, inRangeOne, value, inRangeTwo, value, afterRange, value));
            return Lists.transform(
                    Lists.newArrayList(tx.getRowsColumnRange(
                            TABLE,
                            ImmutableList.of(row),
                            new ColumnRangeSelection(
                                    RangeRequests.nextLexicographicName(beforeRange.getColumnName()),
                                    RangeRequests.previousLexicographicName(afterRange.getColumnName())),
                            10)),
                    Map.Entry::getKey);
        });
        assertThat(cells).containsExactly(inRangeOne, inRangeTwo);
    }

    @Test
    public void getRowsColumnRangeIncludesRowsWithOnlyLocalWritesCorrectly() {
        byte[] row1 = "apple".getBytes(StandardCharsets.UTF_8);
        byte[] row2 = "banana".getBytes(StandardCharsets.UTF_8);
        byte[] row3 = "cherry".getBytes(StandardCharsets.UTF_8);
        byte[] row4 = "durian".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[1];

        Cell firstCell = Cell.create(row1, "apricot".getBytes(StandardCharsets.UTF_8));
        Cell secondCell = Cell.create(row2, "bamboo".getBytes(StandardCharsets.UTF_8));
        Cell thirdCell = Cell.create(row3, "coconut".getBytes(StandardCharsets.UTF_8));
        Cell fourthCell = Cell.create(row4, "dragonfruit".getBytes(StandardCharsets.UTF_8));

        serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(firstCell, value, thirdCell, value));
            return null;
        });

        List<Cell> cells = serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(secondCell, value));
            return Lists.transform(
                    Lists.newArrayList(tx.getRowsColumnRange(
                            TABLE, ImmutableList.of(row1, row2, row3), new ColumnRangeSelection(null, null), 10)),
                    Map.Entry::getKey);
        });
        assertThat(cells).containsExactly(firstCell, secondCell, thirdCell);

        cells = serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(fourthCell, value));
            return Lists.transform(
                    Lists.newArrayList(tx.getRowsColumnRange(
                            TABLE, ImmutableList.of(row3, row4, row1, row2), new ColumnRangeSelection(null, null), 10)),
                    Map.Entry::getKey);
        });
        assertThat(cells).containsExactly(thirdCell, fourthCell, firstCell, secondCell);
    }

    @Test
    public void getRowsColumnRangeIncludesPrefixRowsWithOnlyLocalWritesCorrectly() {
        byte[] row1 = "first".getBytes(StandardCharsets.UTF_8);
        byte[] row2 = "second".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[1];

        Cell firstCell = Cell.create(row1, "1st".getBytes(StandardCharsets.UTF_8));
        Cell secondCell = Cell.create(row2, "2nd".getBytes(StandardCharsets.UTF_8));

        serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(secondCell, value));
            return null;
        });

        List<Cell> cells = serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(firstCell, value));
            return Lists.transform(
                    Lists.newArrayList(tx.getRowsColumnRange(
                            TABLE, ImmutableList.of(row1, row2), new ColumnRangeSelection(null, null), 10)),
                    Map.Entry::getKey);
        });
        assertThat(cells).containsExactly(firstCell, secondCell);
    }

    @Test
    public void getRowsColumnRangeIncludesSuffixRowsWithOnlyLocalWritesCorrectly() {
        byte[] row1 = "first".getBytes(StandardCharsets.UTF_8);
        byte[] row2 = "second".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[1];

        Cell firstCell = Cell.create(row1, "1st".getBytes(StandardCharsets.UTF_8));
        Cell secondCell = Cell.create(row2, "2nd".getBytes(StandardCharsets.UTF_8));

        serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(firstCell, value));
            return null;
        });

        List<Cell> cells = serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(secondCell, value));
            return Lists.transform(
                    Lists.newArrayList(tx.getRowsColumnRange(
                            TABLE, ImmutableList.of(row1, row2), new ColumnRangeSelection(null, null), 10)),
                    Map.Entry::getKey);
        });
        assertThat(cells).containsExactly(firstCell, secondCell);
    }

    @Test
    public void getRowsColumnRangeHandlesTotallyEmptyRows() {
        byte[] row = "foo".getBytes(StandardCharsets.UTF_8);
        List<Cell> cells = serializableTxManager.runTaskReadOnly(tx -> Lists.transform(
                Lists.newArrayList(
                        tx.getRowsColumnRange(TABLE, ImmutableList.of(row), new ColumnRangeSelection(null, null), 10)),
                Map.Entry::getKey));
        assertThat(cells).isEmpty();
    }

    @Test
    public void getRowsColumnRangeReturnsRowsInUserProvidedOrdering() {
        byte[] row1 = "eggplant".getBytes(StandardCharsets.UTF_8);
        byte[] row2 = "fig".getBytes(StandardCharsets.UTF_8);
        byte[] row3 = "guava".getBytes(StandardCharsets.UTF_8);
        byte[] row4 = "honeydew".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[1];

        Cell firstCell = Cell.create(row1, "eclair".getBytes(StandardCharsets.UTF_8));
        Cell secondCell = Cell.create(row2, "flambe".getBytes(StandardCharsets.UTF_8));
        Cell thirdCell = Cell.create(row3, "gateau".getBytes(StandardCharsets.UTF_8));
        Cell fourthCell = Cell.create(row4, "halva".getBytes(StandardCharsets.UTF_8));

        serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(firstCell, value, secondCell, value, thirdCell, value, fourthCell, value));
            return null;
        });

        List<Cell> cells = serializableTxManager.runTaskReadOnly(tx -> Lists.transform(
                Lists.newArrayList(tx.getRowsColumnRange(
                        TABLE, ImmutableList.of(row2, row3, row1, row4), new ColumnRangeSelection(null, null), 10)),
                Map.Entry::getKey));
        assertThat(cells).containsExactly(secondCell, thirdCell, firstCell, fourthCell);

        cells = serializableTxManager.runTaskReadOnly(tx -> Lists.transform(
                Lists.newArrayList(tx.getRowsColumnRange(
                        TABLE, ImmutableList.of(row4, row3, row2, row1), new ColumnRangeSelection(null, null), 10)),
                Map.Entry::getKey));
        assertThat(cells).containsExactly(fourthCell, thirdCell, secondCell, firstCell);

        cells = serializableTxManager.runTaskReadOnly(tx -> Lists.transform(
                Lists.newArrayList(tx.getRowsColumnRange(
                        TABLE, ImmutableList.of(row3, row1, row4, row2), new ColumnRangeSelection(null, null), 10)),
                Map.Entry::getKey));
        assertThat(cells).containsExactly(thirdCell, firstCell, fourthCell, secondCell);
    }

    @Test
    public void getRowsColumnRangeIteratorHandlesLocalWrites() {
        byte[] row1 = "brass".getBytes(StandardCharsets.UTF_8);
        byte[] row2 = "percussion".getBytes(StandardCharsets.UTF_8);
        byte[] row3 = "woodwinds".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[1];

        Cell firstRowFirstColumn = Cell.create(row1, "trombone".getBytes(StandardCharsets.UTF_8));
        Cell firstRowSecondColumn = Cell.create(row1, "trumpet".getBytes(StandardCharsets.UTF_8));
        Cell secondRow = Cell.create(row2, "gong".getBytes(StandardCharsets.UTF_8));
        Cell thirdRowFirstColumn = Cell.create(row3, "clarinet".getBytes(StandardCharsets.UTF_8));
        Cell thirdRowSecondColumn = Cell.create(row3, "flute".getBytes(StandardCharsets.UTF_8));

        serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(firstRowFirstColumn, value, thirdRowSecondColumn, value));
            return null;
        });

        Map<byte[], List<Cell>> cells = serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, ImmutableMap.of(firstRowSecondColumn, value, secondRow, value, thirdRowFirstColumn, value));
            Map<byte[], Iterator<Entry<Cell, byte[]>>> rowsToIterators = tx.getRowsColumnRangeIterator(
                    TABLE, ImmutableList.of(row1, row2, row3), BatchColumnRangeSelection.create(null, null, 10));
            return EntryStream.of(rowsToIterators)
                    .mapValues(iterator -> Iterators.transform(iterator, Entry::getKey))
                    .<List<Cell>>mapValues(Lists::newArrayList)
                    .toMap();
        });
        assertThat(cells)
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        row1,
                        ImmutableList.of(firstRowFirstColumn, firstRowSecondColumn),
                        row2,
                        ImmutableList.of(secondRow),
                        row3,
                        ImmutableList.of(thirdRowFirstColumn, thirdRowSecondColumn)));
    }

    @Test
    public void testRowsColumnRangesSingleIteratorVersion() {
        runTestForGetRowsColumnRangeSingleIteratorVersion(1, 1, 0);
        runTestForGetRowsColumnRangeSingleIteratorVersion(1, 10, 0);
        runTestForGetRowsColumnRangeSingleIteratorVersion(1, 100, 0);
        runTestForGetRowsColumnRangeSingleIteratorVersion(10, 10, 0);
        runTestForGetRowsColumnRangeSingleIteratorVersion(10, 1, 0);
        runTestForGetRowsColumnRangeSingleIteratorVersion(100, 1, 0);
        runTestForGetRowsColumnRangeSingleIteratorVersion(10, 10, 5);
        runTestForGetRowsColumnRangeSingleIteratorVersion(10, 10, 10);
        runTestForGetRowsColumnRangeSingleIteratorVersion(100, 100, 99);
        // Add a test where neither the numCellsPerRow and the batch size (10) are divisible by each other.
        // This tests what happens when a row's cells are spread across multiple batches.
        runTestForGetRowsColumnRangeSingleIteratorVersion(101, 11, 0);
    }

    private void runTestForGetRowsColumnRangeSingleIteratorVersion(
            int numRows, int numCellsPerRow, int numDeletedCellsPerRow) {
        List<byte[]> expectedRows = new ArrayList<>(numRows);
        List<Cell> expectedCells = new ArrayList<>(numRows * numCellsPerRow);
        List<Cell> expectedDeletedCells = new ArrayList<>(numRows * numCellsPerRow);
        for (int iRow = 0; iRow < numRows; iRow++) {
            String row = String.format("row%02d", iRow);
            expectedRows.add(row.getBytes(StandardCharsets.UTF_8));
            for (int iCell = 0; iCell < numCellsPerRow; iCell++) {
                String cell = String.format("cell%02d", iCell);
                if (iCell < numDeletedCellsPerRow) {
                    expectedDeletedCells.add(
                            Cell.create(row.getBytes(StandardCharsets.UTF_8), cell.getBytes(StandardCharsets.UTF_8)));
                } else {
                    expectedCells.add(
                            Cell.create(row.getBytes(StandardCharsets.UTF_8), cell.getBytes(StandardCharsets.UTF_8)));
                }
            }
        }
        byte[] value = new byte[1];

        serializableTxManager.runTaskWithRetry(tx -> {
            tx.put(TABLE, Maps.toMap(expectedCells, ignored -> value));
            return null;
        });
        keyValueService.addGarbageCollectionSentinelValues(TABLE, expectedDeletedCells);

        List<byte[]> shuffledRows = expectedRows;
        Collections.shuffle(shuffledRows);
        List<Cell> cells = serializableTxManager.runTaskReadOnly(tx -> ImmutableList.copyOf(Iterators.transform(
                tx.getRowsColumnRange(TABLE, shuffledRows, new ColumnRangeSelection(null, null), 10),
                Map.Entry::getKey)));
        expectedCells.sort(Comparator.comparing(
                        (Cell cell) -> ByteBuffer.wrap(cell.getRowName()),
                        Ordering.explicit(Lists.transform(shuffledRows, ByteBuffer::wrap)))
                .thenComparing(Cell::getColumnName, PtBytes.BYTES_COMPARATOR));
        Assertions.assertThat(cells).containsExactlyElementsOf(expectedCells);

        keyValueService.truncateTable(TABLE);
    }

    @Test
    public void commitThrowsIfRolledBackAtCommitTime_expiredLocks() {
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));

        TimelockService timelockService = spy(inMemoryTimelockExtension.getLegacyTimelockService());

        // expire the locks when the pre-commit check happens - this is guaranteed to be after we've written the data
        PreCommitCondition condition =
                unused -> doReturn(ImmutableSet.of()).when(timelockService).refreshLockLeases(any());

        ConjureStartTransactionsResponse conjureResponse = startTransactionWithWatches();
        LockImmutableTimestampResponse res = conjureResponse.getImmutableTimestamp();
        long transactionTs = conjureResponse.getTimestamps().start();

        Transaction snapshot = getSnapshotTransactionWith(timelockService, () -> transactionTs, res, condition);

        // simulate roll back at commit time
        transactionService.putUnlessExists(snapshot.getTimestamp(), TransactionConstants.FAILED_COMMIT_TS);

        snapshot.put(TABLE, ImmutableMap.of(cell, PtBytes.toBytes("value")));

        assertThatExceptionOfType(TransactionLockTimeoutException.class).isThrownBy(snapshot::commit);

        timelockService.unlock(ImmutableSet.of(res.getLock()));

        TransactionOutcomeMetricsAssert.assertThat(transactionOutcomeMetrics)
                .hasFailedCommits(1)
                .hasLocksExpired(1);
    }

    @Test
    public void commitThrowsIfCommitLockAcquisitionFails() {
        TimelockService timelockService = mock(TimelockService.class);
        when(timelockService.lock(any(), any())).thenReturn(LockResponse.timedOut());
        Transaction txn = getSnapshotTransactionWith(timelockService, ImmutableMap.of());

        // We only request commit locks for transactions that write at least one value
        txn.put(TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE));

        assertThatThrownBy(txn::commit)
                .isInstanceOf(TransactionLockAcquisitionTimeoutException.class)
                .hasMessage("Timed out while acquiring commit locks.");
        TransactionOutcomeMetricsAssert.assertThat(transactionOutcomeMetrics)
                .hasFailedCommits(1)
                .hasCommitLockAcquisitionFailures(1);
    }

    @Test
    public void commitThrowsIfRolledBackAtCommitTime_alreadyAborted() {
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));

        TimelockService timelockService = inMemoryTimelockExtension.getLegacyTimelockService();
        ConjureStartTransactionsResponse conjureResponse = startTransactionWithWatches();
        LockImmutableTimestampResponse res = conjureResponse.getImmutableTimestamp();
        long transactionTs = conjureResponse.getTimestamps().start();

        Transaction snapshot =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP);

        // forcing to try to commit a transaction that is already committed
        transactionService.putUnlessExists(transactionTs, TransactionConstants.FAILED_COMMIT_TS);

        snapshot.put(TABLE, ImmutableMap.of(cell, PtBytes.toBytes("value")));

        assertThatExceptionOfType(TransactionCommitFailedException.class).isThrownBy(snapshot::commit);

        timelockService.unlock(Collections.singleton(res.getLock()));
    }

    @Test
    public void commitDoesNotThrowIfAlreadySuccessfullyCommitted() {
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        TimelockService spiedTimeLockService = spy(timelockService);

        ConjureStartTransactionsResponse conjureResponse = startTransactionWithWatches();
        LockImmutableTimestampResponse res = conjureResponse.getImmutableTimestamp();
        long transactionTs = conjureResponse.getTimestamps().start();

        Transaction snapshot =
                getSnapshotTransactionWith(spiedTimeLockService, () -> transactionTs, res, PreCommitConditions.NO_OP);

        when(spiedTimeLockService.getFreshTimestamp()).thenReturn(transactionTs + 1);
        doReturn(transactionTs + 1).when(spiedTimeLockService).getCommitTimestamp(anyLong(), any());

        // forcing to try to commit a transaction that is already committed
        transactionService.putUnlessExists(transactionTs, spiedTimeLockService.getFreshTimestamp());

        snapshot.put(TABLE, ImmutableMap.of(cell, PtBytes.toBytes("value")));
        snapshot.commit();

        spiedTimeLockService.unlock(Collections.singleton(res.getLock()));
    }

    @Test
    public void doNotValidateLocksOnNonEmptyReadsIfThoroughlySwept() {
        putCellsInTable(List.of(TEST_CELL), TABLE_SWEPT_THOROUGH);

        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();
        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        timelockService.unlock(ImmutableSet.of(res.getLock()));

        assertThatCode(() -> transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL)))
                .doesNotThrowAnyException();
    }

    @Test
    public void validateLocksOnEmptyReadsIfThoroughlySwept() {
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();
        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        timelockService.unlock(ImmutableSet.of(res.getLock()));

        assertThatExceptionOfType(TransactionLockTimeoutException.class)
                .isThrownBy(() -> transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL)));
    }

    @Test
    public void doNotValidateLocksOnCommitIfValidationFlagIsFalseAndOnlyReadNonEmptyValuesOnThoroughlySweptTable() {
        putCellsInTable(List.of(TEST_CELL), TABLE_SWEPT_THOROUGH);

        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();
        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, false);

        timelockService.unlock(ImmutableSet.of(res.getLock()));

        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));

        assertThatCode(transaction::commit).doesNotThrowAnyException();
    }

    @Test
    public void validateLocksOnlyOnCommitIfValidationFlagIsFalse() {
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();

        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, false);

        timelockService.unlock(ImmutableSet.of(res.getLock()));
        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));

        assertThatExceptionOfType(TransactionLockTimeoutException.class).isThrownBy(transaction::commit);
    }

    @Test
    public void validateLocksOnCommitIfEmptyReadsFollowedByNonEmptyReadsIfValidationFlagIsFalse() {
        putCellsInTable(List.of(TEST_CELL), TABLE_SWEPT_THOROUGH);

        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();
        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, false);

        timelockService.unlock(ImmutableSet.of(res.getLock()));

        // First reading empty value
        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL_2));
        // Then reading non-empty value.
        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));

        // Make sure that lock is still validated in the end due to the initial empty read
        assertThatExceptionOfType(TransactionLockTimeoutException.class).isThrownBy(transaction::commit);
    }

    @Test
    public void skipImmutableTimestampLockCheckIfReadingEqualsExpectedNumberOfValuesEvenWhenRequestedMore() {
        putCellsInTable(List.of(TEST_CELL), TABLE_SWEPT_THOROUGH);

        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        Transaction transaction = getSnapshotTransactionWith(
                spiedTimeLockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        Result<Map<Cell, byte[]>, MoreCellsPresentThanExpectedException> result =
                transaction.getWithExpectedNumberOfCells(
                        TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL, TEST_CELL_2), 1);
        assertThat(result.isOk()).isTrue();

        transaction.commit();
        spiedTimeLockService.unlock(ImmutableSet.of(res.getLock()));

        verify(spiedTimeLockService, never()).refreshLockLeases(any());
    }

    @Test
    public void performImmutableTimestampLocksIfReadingLessThanExpectedNumberOfValues() {
        putCellsInTable(List.of(TEST_CELL), TABLE_SWEPT_THOROUGH);

        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        Transaction transaction = getSnapshotTransactionWith(
                spiedTimeLockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        Result<Map<Cell, byte[]>, MoreCellsPresentThanExpectedException> result =
                transaction.getWithExpectedNumberOfCells(
                        TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL, TEST_CELL_2, TEST_CELL_3), 2);
        assertThat(result.isOk()).isTrue();
        transaction.commit();
        spiedTimeLockService.unlock(ImmutableSet.of(res.getLock()));

        verify(spiedTimeLockService, times(1)).refreshLockLeases(ImmutableSet.of(res.getLock()));
    }

    @Test
    public void throwsIfReadingMoreThanExpectedNumberOfValues() {
        putCellsInTable(List.of(TEST_CELL, TEST_CELL_2), TABLE_SWEPT_THOROUGH);

        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        Transaction transaction = getSnapshotTransactionWith(
                spiedTimeLockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        Result<Map<Cell, byte[]>, MoreCellsPresentThanExpectedException> result =
                transaction.getWithExpectedNumberOfCells(
                        TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL, TEST_CELL_2), 1);
        assertThat(result.isErr()).isTrue();
        assertThat(result.unwrapErr().getFetchedCells()).containsKeys(TEST_CELL, TEST_CELL_2);

        verify(spiedTimeLockService, never()).refreshLockLeases(any());
    }

    @Test
    public void keepNumberOfExpectedCellsTheSameIfNoneCached() {
        putCellsInTable(List.of(TEST_CELL, TEST_CELL_2, TEST_CELL_4), TABLE_SWEPT_THOROUGH);

        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        CacheMetrics metrics = mock(CacheMetrics.class);
        TransactionScopedCache emptyCache = TransactionScopedCacheImpl.create(
                ValueCacheSnapshotImpl.of(
                        io.vavr.collection.HashMap.empty(),
                        io.vavr.collection.HashSet.of(TABLE_SWEPT_THOROUGH),
                        ImmutableSet.of(TABLE_SWEPT_THOROUGH)),
                metrics);
        LockWatchManagerInternal mockLockWatchManager = mock(LockWatchManagerInternal.class);
        when(mockLockWatchManager.getTransactionScopedCache(anyLong())).thenReturn(emptyCache);

        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        SnapshotTransaction spiedSnapshotTransaction =
                spy(getSnapshotTransactionWith(transactionTs, res, mockLockWatchManager, pathTypeTracker));
        Transaction transaction = transactionWrapper.apply(spiedSnapshotTransaction, pathTypeTracker);

        // Fetching 3 cells, but expect only 2 to be present, for example
        Result<Map<Cell, byte[]>, MoreCellsPresentThanExpectedException> result =
                transaction.getWithExpectedNumberOfCells(
                        TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL, TEST_CELL_2, TEST_CELL_3), 2);
        assertThat(result.isOk()).isTrue();

        // We shouldn't check for locks even though we haven't fetched all 3 cells, because we fetched 2 and passed
        // that as the expected value
        verify(spiedTimeLockService, never()).refreshLockLeases(any());

        // Since no cells were cached, we continue to expect 2 from the underlying kvs.
        verify(spiedSnapshotTransaction)
                .getInternal(
                        eq("getWithExpectedNumberOfCells"),
                        eq(TABLE_SWEPT_THOROUGH),
                        eq(Set.of(TEST_CELL, TEST_CELL_2, TEST_CELL_3)),
                        eq(2L),
                        any(),
                        any());
    }

    @Test
    public void reduceCachedCellsFromNumberOfExpectedCells() {
        putCellsInTable(List.of(TEST_CELL, TEST_CELL_2, TEST_CELL_4), TABLE_SWEPT_THOROUGH);

        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        TransactionScopedCache txnCache =
                createCacheWithEntry(TABLE_SWEPT_THOROUGH, TEST_CELL, "value".getBytes(StandardCharsets.UTF_8));
        LockWatchManagerInternal mockLockWatchManager = mock(LockWatchManagerInternal.class);
        when(mockLockWatchManager.getTransactionScopedCache(anyLong())).thenReturn(txnCache);

        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        SnapshotTransaction spiedSnapshotTransaction =
                spy(getSnapshotTransactionWith(transactionTs, res, mockLockWatchManager, pathTypeTracker));
        Transaction transaction = transactionWrapper.apply(spiedSnapshotTransaction, pathTypeTracker);

        // Fetching 3 cells, but expect only 2 to be present, for example
        Result<Map<Cell, byte[]>, MoreCellsPresentThanExpectedException> result =
                transaction.getWithExpectedNumberOfCells(
                        TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL, TEST_CELL_2, TEST_CELL_3), 2);
        assertThat(result.isOk()).isTrue();

        // We shouldn't check for locks even though we haven't fetched all 3 cells, because we fetched 2 and passed
        // that as the expected value
        verify(spiedTimeLockService, never()).refreshLockLeases(any());

        // Since one cell is cached, should only expect 1 to be present on internal call
        verify(spiedSnapshotTransaction)
                .getInternal(
                        eq("getWithExpectedNumberOfCells"),
                        eq(TABLE_SWEPT_THOROUGH),
                        eq(Set.of(TEST_CELL_2, TEST_CELL_3)), // Don't expect to ask for cell1 because it's cached
                        eq(1L),
                        any(),
                        any());
    }

    @Test
    public void keepNumberOfExpectedCellsIfCachedWithEmptyValue() {
        putCellsInTable(List.of(TEST_CELL, TEST_CELL_2, TEST_CELL_4), TABLE_SWEPT_THOROUGH);

        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        TransactionScopedCache txnCache =
                createCacheWithEntry(TABLE_SWEPT_THOROUGH, TEST_CELL, PtBytes.EMPTY_BYTE_ARRAY);
        LockWatchManagerInternal mockLockWatchManager = mock(LockWatchManagerInternal.class);
        when(mockLockWatchManager.getTransactionScopedCache(anyLong())).thenReturn(txnCache);

        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        SnapshotTransaction spiedSnapshotTransaction =
                spy(getSnapshotTransactionWith(transactionTs, res, mockLockWatchManager, pathTypeTracker));
        Transaction transaction = transactionWrapper.apply(spiedSnapshotTransaction, pathTypeTracker);

        // Fetching 3 cells, but expect only 2 to be present, for example
        Result<Map<Cell, byte[]>, MoreCellsPresentThanExpectedException> result =
                transaction.getWithExpectedNumberOfCells(
                        TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL, TEST_CELL_2, TEST_CELL_3), 2);
        assertThat(result.isOk()).isTrue();

        // We shouldn't check for locks even though we haven't fetched all 3 cells, because we fetched 2 and passed
        // that as the expected value
        verify(spiedTimeLockService, never()).refreshLockLeases(any());

        // Even though Cell One is cached, it has empty value, so we still expect 2 values to be present
        verify(spiedSnapshotTransaction)
                .getInternal(
                        eq("getWithExpectedNumberOfCells"),
                        eq(TABLE_SWEPT_THOROUGH),
                        eq(Set.of(TEST_CELL_2, TEST_CELL_3)), // Don't expect to ask for cell1 because it's cached
                        eq(2L),
                        any(),
                        any());
    }

    @Test
    public void dontFetchCellsIfAllCachedButPropagateWithEmptyRequest() {
        putCellsInTable(List.of(TEST_CELL, TEST_CELL_2), TABLE_SWEPT_THOROUGH);

        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        TransactionScopedCache txnCache = createCacheWithEntries(
                TABLE_SWEPT_THOROUGH,
                Map.of(
                        TEST_CELL,
                        "someValue".getBytes(StandardCharsets.UTF_8),
                        TEST_CELL_2,
                        "someOtherValue".getBytes(StandardCharsets.UTF_8)));

        LockWatchManagerInternal mockLockWatchManager = mock(LockWatchManagerInternal.class);
        when(mockLockWatchManager.getTransactionScopedCache(anyLong())).thenReturn(txnCache);

        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        SnapshotTransaction spiedSnapshotTransaction =
                spy(getSnapshotTransactionWith(transactionTs, res, mockLockWatchManager, pathTypeTracker));
        Transaction transaction = transactionWrapper.apply(spiedSnapshotTransaction, pathTypeTracker);

        Result<Map<Cell, byte[]>, MoreCellsPresentThanExpectedException> result =
                transaction.getWithExpectedNumberOfCells(
                        TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL, TEST_CELL_2), 2);
        assertThat(result.isOk()).isTrue();

        verify(spiedTimeLockService, never()).refreshLockLeases(any());
        verify(spiedSnapshotTransaction)
                .getInternal(
                        eq("getWithExpectedNumberOfCells"),
                        eq(TABLE_SWEPT_THOROUGH),
                        eq(ImmutableSet.of()),
                        anyLong(),
                        any(),
                        any());
    }

    @Test
    public void throwsIfAllValuesCachedButMoreThanExpectedPresent() {
        putCellsInTable(List.of(TEST_CELL, TEST_CELL_2), TABLE_SWEPT_THOROUGH);

        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        Map<Cell, byte[]> cacheContent = Map.of(
                TEST_CELL,
                "someValue".getBytes(StandardCharsets.UTF_8),
                TEST_CELL_2,
                "someOtherValue".getBytes(StandardCharsets.UTF_8));
        TransactionScopedCache txnCache = createCacheWithEntries(TABLE_SWEPT_THOROUGH, cacheContent);

        LockWatchManagerInternal mockLockWatchManager = mock(LockWatchManagerInternal.class);
        when(mockLockWatchManager.getTransactionScopedCache(anyLong())).thenReturn(txnCache);

        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        SnapshotTransaction spiedSnapshotTransaction =
                spy(getSnapshotTransactionWith(transactionTs, res, mockLockWatchManager, pathTypeTracker));
        Transaction transaction = transactionWrapper.apply(spiedSnapshotTransaction, pathTypeTracker);

        Result<Map<Cell, byte[]>, MoreCellsPresentThanExpectedException> result =
                transaction.getWithExpectedNumberOfCells(
                        TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL, TEST_CELL_2), 1);
        assertThat(result.isErr()).isTrue();
        assertThat(result.unwrapErr().getFetchedCells()).containsExactlyInAnyOrderEntriesOf(cacheContent);
        assertThat(result.unwrapErr().getArgs())
                .containsExactlyInAnyOrder(
                        SafeArg.of("expectedNumberOfCells", 1L),
                        SafeArg.of("numberOfCellsRetrieved", 2),
                        UnsafeArg.of("retrievedCells", cacheContent));

        verify(spiedTimeLockService, never()).refreshLockLeases(any());
        verify(spiedSnapshotTransaction, never())
                .getInternal(
                        eq("getWithExpectedNumberOfCells"), eq(TABLE_SWEPT_THOROUGH), any(), anyLong(), any(), any());
    }

    @Test
    public void canReadWithExpectedSizeOneAfterDeletingPresentCellAndWritingToAnother() {
        putCellsInTable(List.of(TEST_CELL), TABLE_SWEPT_THOROUGH);

        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        Map<Cell, byte[]> cacheContent = Map.of(TEST_CELL, "someValue".getBytes(StandardCharsets.UTF_8));
        TransactionScopedCache txnCache = createCacheWithEntries(TABLE_SWEPT_THOROUGH, cacheContent);

        LockWatchManagerInternal mockLockWatchManager = mock(LockWatchManagerInternal.class);
        when(mockLockWatchManager.getTransactionScopedCache(anyLong())).thenReturn(txnCache);

        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        SnapshotTransaction spiedSnapshotTransaction =
                spy(getSnapshotTransactionWith(transactionTs, res, mockLockWatchManager, pathTypeTracker));
        Transaction transaction = transactionWrapper.apply(spiedSnapshotTransaction, pathTypeTracker);

        // Deleting existing cell and writing to a new one to simulate change from LOCAL -> REMOTE or vice versa.
        Map<Cell, byte[]> newCellValue = ImmutableMap.of(TEST_CELL_2, "someNewValue".getBytes(StandardCharsets.UTF_8));
        transaction.put(TABLE_SWEPT_THOROUGH, newCellValue);
        transaction.delete(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));

        Result<Map<Cell, byte[]>, MoreCellsPresentThanExpectedException> result =
                transaction.getWithExpectedNumberOfCells(TABLE_SWEPT_THOROUGH, Set.of(TEST_CELL, TEST_CELL_2), 1);
        assertThat(result.isOk()).isTrue();
        assertThat(result.unwrap()).containsExactlyInAnyOrderEntriesOf(newCellValue);

        verify(spiedTimeLockService, never()).refreshLockLeases(any());
        // No values cached after write and deletion, so expecting to try to fetch both.
        verify(spiedSnapshotTransaction)
                .getInternal(
                        eq("getWithExpectedNumberOfCells"),
                        eq(TABLE_SWEPT_THOROUGH),
                        eq(Set.of(TEST_CELL, TEST_CELL_2)),
                        eq(1L),
                        any(),
                        any());
    }

    private TransactionScopedCache createCacheWithEntry(TableReference table, Cell cell, byte[] value) {
        return createCacheWithEntries(table, ImmutableMap.of(cell, value));
    }

    private TransactionScopedCache createCacheWithEntries(TableReference table, Map<Cell, byte[]> values) {
        CacheMetrics metrics = mock(CacheMetrics.class);
        return TransactionScopedCacheImpl.create(
                ValueCacheSnapshotImpl.of(
                        io.vavr.collection.HashMap.ofAll(EntryStream.of(values)
                                .mapKeys(cell -> CellReference.of(table, cell))
                                .mapValues(value -> CacheEntry.unlocked(CacheValue.of(value)))
                                .toMap()),
                        io.vavr.collection.HashSet.of(table),
                        ImmutableSet.of(table)),
                metrics);
    }

    @Test
    public void doesNotCheckImmutableTsLockIfNonEmptyReadOnThoroughlySwept_WithValidationOnReads() {
        putCellsInTable(List.of(TEST_CELL), TABLE_SWEPT_THOROUGH);

        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        Transaction transaction = getSnapshotTransactionWith(
                spiedTimeLockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));
        transaction.commit();
        spiedTimeLockService.unlock(ImmutableSet.of(res.getLock()));

        verify(spiedTimeLockService, never()).refreshLockLeases(any());
    }

    @Test
    public void checkImmutableTsLockOnceIfEmptyReadOnThoroughlySwept_WithValidationOnReads() {
        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        Transaction transaction = getSnapshotTransactionWith(
                spiedTimeLockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));
        transaction.commit();
        spiedTimeLockService.unlock(ImmutableSet.of(res.getLock()));

        verify(spiedTimeLockService).refreshLockLeases(ImmutableSet.of(res.getLock()));
    }

    @Test
    public void doesNotCheckImmutableTsLockIfNonEmptyReadOnThoroughlySwept_WithoutValidationOnReads() {
        putCellsInTable(List.of(TEST_CELL), TABLE_SWEPT_THOROUGH);

        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        Transaction transaction = getSnapshotTransactionWith(
                spiedTimeLockService, () -> transactionTs, res, PreCommitConditions.NO_OP, false);

        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));
        transaction.commit();
        spiedTimeLockService.unlock(ImmutableSet.of(res.getLock()));

        verify(spiedTimeLockService, never()).refreshLockLeases(any());
    }

    @Test
    public void checkImmutableTsLockOnceIfEmptyReadOnThoroughlySwept_WithoutValidationOnReads() {
        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        Transaction transaction = getSnapshotTransactionWith(
                spiedTimeLockService, () -> transactionTs, res, PreCommitConditions.NO_OP, false);

        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));
        transaction.commit();
        spiedTimeLockService.unlock(ImmutableSet.of(res.getLock()));

        verify(spiedTimeLockService).refreshLockLeases(ImmutableSet.of(res.getLock()));
    }

    @Test
    public void testThrowsIfSweepSentinelSeen() {
        Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        Transaction t1 = txManager.createNewTransaction();
        Transaction t2 = txManager.createNewTransaction();
        t1.getTimestamp();
        t2.getTimestamp();

        t1.put(TABLE, ImmutableMap.of(cell, new byte[1]));
        t1.commit();

        keyValueService.addGarbageCollectionSentinelValues(TABLE, ImmutableSet.of(cell));

        assertThatExceptionOfType(TransactionFailedRetriableException.class)
                .isThrownBy(() -> t2.get(TABLE, ImmutableSet.of(cell)))
                .withMessageContaining("Tried to read a value that has been deleted.");
    }

    @Test
    public void testIgnoresOrphanedSweepSentinel() {
        Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        keyValueService.addGarbageCollectionSentinelValues(TABLE, ImmutableSet.of(cell));
        Transaction txn = txManager.createNewTransaction();
        assertThat(txn.get(TABLE, ImmutableSet.of(cell))).isEmpty();
    }

    @Test
    public void checkImmutableTsLockAfterReadsForConservativeIfFlagIsSet() {
        TimelockService spiedTimeLockService = spy(timelockService);
        long transactionTs = spiedTimeLockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = spiedTimeLockService.lockImmutableTimestamp();

        setTransactionConfig(ImmutableTransactionConfig.builder()
                .lockImmutableTsOnReadOnlyTransactions(true)
                .build());

        Transaction transaction = getSnapshotTransactionWith(
                spiedTimeLockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        transaction.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL));
        verify(spiedTimeLockService).refreshLockLeases(ImmutableSet.of(res.getLock()));

        transaction.commit();
        spiedTimeLockService.unlock(ImmutableSet.of(res.getLock()));
    }

    @Test
    public void getOrphanedSweepSentinelDoesNotThrow() {
        Transaction t1 = txManager.createNewTransaction();
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);
        assertThatCode(() -> t1.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL)))
                .doesNotThrowAnyException();
    }

    @Test
    public void getSweepSentinelUnderCommittedWriteThrowsAndCanBeRetried() {
        Transaction t1 = txManager.createNewTransaction();
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);

        commitWrite(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);

        assertThatThrownBy(() -> t1.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL)))
                .isInstanceOf(TransactionFailedRetriableException.class)
                .hasMessageContaining("Tried to read a value that has been deleted.");

        Transaction retryTxn = txManager.createNewTransaction();
        assertThatCode(() -> retryTxn.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL)))
                .doesNotThrowAnyException();
    }

    @Test
    public void getSweepSentinelUnderLateUncommittedWriteDoesNotThrow() {
        Transaction t1 = txManager.createNewTransaction();
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);
        putUncommittedAtFreshTimestamp(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);

        assertThatCode(() -> t1.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL)))
                .doesNotThrowAnyException();
    }

    @Test
    public void getSweepSentinelUnderEarlyUncommittedWriteDoesNotThrow() {
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);
        putUncommittedAtFreshTimestamp(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);
        Transaction t1 = txManager.createNewTransaction();

        assertThatCode(() -> t1.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL)))
                .doesNotThrowAnyException();
    }

    @Test
    public void getSweepSentinelUnderMultipleUncommittedWritesDoesNotThrow() {
        Transaction t1 = txManager.createNewTransaction();
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);

        for (int transaction = 1; transaction <= 10; transaction++) {
            putUncommittedAtFreshTimestamp(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);
        }

        assertThatCode(() -> t1.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL)))
                .doesNotThrowAnyException();
    }

    @Test
    public void getSweepSentinelUnderHiddenCommittedWriteThrows() {
        Transaction t1 = txManager.createNewTransaction();
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);

        commitWrite(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);

        for (int transaction = 1; transaction <= 10; transaction++) {
            putUncommittedAtFreshTimestamp(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);
        }

        assertThatThrownBy(() -> t1.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL)))
                .isInstanceOf(TransactionFailedRetriableException.class)
                .hasMessageContaining("Tried to read a value that has been deleted.");
    }

    @Test
    public void getOrphanedSentinelAndSentinelUnderUncommittedWriteDoesNotThrow() {
        Transaction t1 = txManager.createNewTransaction();
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL_2);
        putUncommittedAtFreshTimestamp(TABLE_SWEPT_CONSERVATIVE, TEST_CELL_2);

        assertThatCode(() -> t1.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL, TEST_CELL_2)))
                .doesNotThrowAnyException();
    }

    @Test
    public void getOrphanedSentinelAndSentinelUnderCommittedWriteThrows() {
        Transaction t1 = txManager.createNewTransaction();
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL_2);
        commitWrite(TABLE_SWEPT_CONSERVATIVE, TEST_CELL_2);

        assertThatThrownBy(() -> t1.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL, TEST_CELL_2)))
                .isInstanceOf(TransactionFailedRetriableException.class)
                .hasMessageContaining("Tried to read a value that has been deleted.");
    }

    @Test
    public void getSentinelUnderCommittedAndSentinelUnderUncommittedWriteThrows() {
        Transaction t1 = txManager.createNewTransaction();
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL_2);
        putUncommittedAtFreshTimestamp(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);
        commitWrite(TABLE_SWEPT_CONSERVATIVE, TEST_CELL_2);

        assertThatThrownBy(() -> t1.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL, TEST_CELL_2)))
                .isInstanceOf(TransactionFailedRetriableException.class)
                .hasMessageContaining("Tried to read a value that has been deleted.");
    }

    @SuppressWarnings("unchecked") // ArgumentCaptor
    @Test
    public void getSentinelValuesStressTest() {
        Transaction t1 = txManager.createNewTransaction();

        List<Cell> cellsUnderUncommittedWrites = IntStream.rangeClosed(1, 100)
                .boxed()
                .map(num -> Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes(num)))
                .collect(Collectors.toList());
        List<Cell> cellsUnderHiddenCommittedWrites = IntStream.rangeClosed(1, 100)
                .boxed()
                .map(num -> Cell.create(PtBytes.toBytes("row2"), PtBytes.toBytes(num)))
                .collect(Collectors.toList());

        for (int index = 0; index < cellsUnderUncommittedWrites.size(); index++) {
            Cell cell = cellsUnderUncommittedWrites.get(index);
            writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, cell);
            for (int uncommittedValue = 0; uncommittedValue <= index; uncommittedValue++) {
                putUncommittedAtFreshTimestamp(TABLE_SWEPT_CONSERVATIVE, cell);
            }
        }

        for (int index = 0; index < cellsUnderHiddenCommittedWrites.size(); index++) {
            Cell cell = cellsUnderHiddenCommittedWrites.get(index);
            writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, cell);
            commitWrite(TABLE_SWEPT_CONSERVATIVE, cell);
            for (int uncommittedValue = 0; uncommittedValue < index; uncommittedValue++) {
                putUncommittedAtFreshTimestamp(TABLE_SWEPT_CONSERVATIVE, cell);
            }
        }

        ImmutableSet<Cell> cells = ImmutableSet.<Cell>builder()
                .addAll(cellsUnderUncommittedWrites)
                .addAll(cellsUnderHiddenCommittedWrites)
                .build();
        assertThatThrownBy(() -> t1.get(TABLE_SWEPT_CONSERVATIVE, cells))
                .isInstanceOf(TransactionFailedRetriableException.class)
                .hasMessageContaining("Tried to read a value that has been deleted.");

        ArgumentCaptor<Iterable<Long>> captor = ArgumentCaptor.forClass(Iterable.class);
        verify(transactionService, times(100)).get(captor.capture());

        List<Iterable<Long>> stressTestRequests = captor.getAllValues();
        assertThat(stressTestRequests).hasSize(100);
        for (int index = 1; index < stressTestRequests.size(); index++) {
            List<Long> previousTimestamps = StreamSupport.stream(
                            stressTestRequests.get(index - 1).spliterator(), false)
                    .collect(Collectors.toList());
            assertThat(stressTestRequests.get(index)).hasSizeLessThan(previousTimestamps.size());
        }

        Transaction retryTxn = txManager.createNewTransaction();
        assertThatCode(() -> retryTxn.get(TABLE_SWEPT_CONSERVATIVE, cells)).doesNotThrowAnyException();
    }

    @Test
    public void orphanedSentinelsGetDeletedAfterDetectionInThoroughlySweptTable() {
        writeSentinelToTestTable(TABLE_SWEPT_THOROUGH, TEST_CELL);

        assertThat(keyValueService.get(TABLE_SWEPT_THOROUGH, ImmutableMap.of(TEST_CELL, 0L)))
                .isNotEmpty();

        Transaction t1 = txManager.createNewTransaction();
        assertThatCode(() -> t1.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL)))
                .doesNotThrowAnyException();

        assertThat(keyValueService.get(TABLE_SWEPT_THOROUGH, ImmutableMap.of(TEST_CELL, 0L)))
                .isEmpty();
    }

    @Test
    public void orphanedSentinelsCoveredWithUncommitedGetDeletedAfterDetectionInThoroughlySweptTable() {
        writeSentinelToTestTable(TABLE_SWEPT_THOROUGH, TEST_CELL);
        putUncommittedAtFreshTimestamp(TABLE_SWEPT_THOROUGH, TEST_CELL);

        Transaction t1 = txManager.createNewTransaction();
        assertThatCode(() -> t1.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL)))
                .doesNotThrowAnyException();

        assertThat(keyValueService.get(TABLE_SWEPT_THOROUGH, ImmutableMap.of(TEST_CELL, 0L)))
                .isEmpty();
    }

    @Test
    public void nonOrphanedSentinelsDoNotGetDeletedAfterDetectionInThoroughlySweptTable() {
        Transaction t1 = txManager.createNewTransaction();
        writeSentinelToTestTable(TABLE_SWEPT_THOROUGH, TEST_CELL);

        commitWrite(TABLE_SWEPT_THOROUGH, TEST_CELL);

        assertThatThrownBy(() -> t1.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL)))
                .isInstanceOf(TransactionFailedRetriableException.class)
                .hasMessageContaining("Tried to read a value that has been deleted.");

        assertThat(keyValueService.get(TABLE_SWEPT_THOROUGH, ImmutableMap.of(TEST_CELL, 0L)))
                .isNotEmpty();
    }

    @Test
    public void testSentinelsDeletionForDifferentCasesInThoroughTable() {
        Transaction t1 = txManager.createNewTransaction();

        Cell testCell3 = Cell.create(PtBytes.toBytes("super"), PtBytes.toBytes("ez"));

        writeSentinelToTestTable(TABLE_SWEPT_THOROUGH, TEST_CELL);
        writeSentinelToTestTable(TABLE_SWEPT_THOROUGH, TEST_CELL_2);
        writeSentinelToTestTable(TABLE_SWEPT_THOROUGH, testCell3);

        commitWrite(TABLE_SWEPT_THOROUGH, TEST_CELL);
        putUncommittedAtFreshTimestamp(TABLE_SWEPT_THOROUGH, TEST_CELL_2);

        assertThatThrownBy(() -> t1.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL, TEST_CELL_2, testCell3)))
                .isInstanceOf(TransactionFailedRetriableException.class)
                .hasMessageContaining("Tried to read a value that has been deleted.");

        assertThat(keyValueService.get(
                        TABLE_SWEPT_THOROUGH, ImmutableMap.of(TEST_CELL, 0L, TEST_CELL_2, 0L, testCell3, 0L)))
                .containsExactlyEntriesOf(
                        ImmutableMap.of(TEST_CELL, Value.create(new byte[0], Value.INVALID_VALUE_TIMESTAMP)));
    }

    @Test
    public void orphanedSentinelsDoNotGetDeletedAfterDetectionInConservativelySweptTable() {
        writeSentinelToTestTable(TABLE_SWEPT_CONSERVATIVE, TEST_CELL);
        Transaction t1 = txManager.createNewTransaction();
        assertThatCode(() -> t1.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL)))
                .doesNotThrowAnyException();

        assertThat(keyValueService.get(TABLE_SWEPT_CONSERVATIVE, ImmutableMap.of(TEST_CELL, 0L)))
                .isNotEmpty();
    }

    @Test
    public void testColumnOrderThenPreserveInputRowOrder() {
        Cell foo_A = Cell.create(ROW_FOO, COL_A);
        Cell bar_A = Cell.create(ROW_BAR, COL_A);
        Cell foo_B = Cell.create(ROW_FOO, COL_B);
        Cell bar_B = Cell.create(ROW_BAR, COL_B);

        putCellsInTable(ImmutableList.of(foo_B, bar_A, bar_B, foo_A), TABLE);

        Assertions.assertThat(getSortedEntries(
                        TABLE,
                        ImmutableList.of(ROW_FOO, ROW_BAR),
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1000)))
                .containsExactly(foo_A, bar_A, foo_B, bar_B);

        Assertions.assertThat(getSortedEntries(
                        TABLE,
                        ImmutableList.of(ROW_BAR, ROW_FOO),
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1000)))
                .containsExactly(bar_A, foo_A, bar_B, foo_B);
    }

    @Test
    public void getSortedColumnsReturnsCellsSortedByColumn() {
        ImmutableList<byte[]> rows = ImmutableList.of(ROW_FOO, ROW_BAR);
        ImmutableList<byte[]> columns = ImmutableList.of(COL_A, COL_B);

        List<Cell> cells = rows.stream()
                .map(row -> columns.stream().map(col -> Cell.create(row, col)).collect(Collectors.toList()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        putCellsInTable(cells, TABLE);
        List<Cell> entries = getSortedEntries(
                TABLE,
                rows,
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1000));

        List<Cell> entriesSortedByColumn = columns.stream()
                .map(col -> rows.stream().map(row -> Cell.create(row, col)).collect(Collectors.toList()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        Assertions.assertThat(entries).containsExactlyElementsOf(entriesSortedByColumn);
    }

    @Test
    @SuppressWarnings("ReturnValueIgnored") // Part of an assertion!
    public void getSortedColumnsThrowsIfLockIsLost() {
        List<Cell> cells = ImmutableList.of(Cell.create(ROW_FOO, COL_A));
        putCellsInTable(cells, TABLE_SWEPT_THOROUGH);

        ConjureStartTransactionsResponse conjureResponse = startTransactionWithWatches();
        LockImmutableTimestampResponse res = conjureResponse.getImmutableTimestamp();
        long transactionTs = conjureResponse.getTimestamps().start();
        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        timelockService.unlock(ImmutableSet.of(res.getLock()));
        Iterator<Map.Entry<Cell, byte[]>> sortedColumns = transaction.getSortedColumns(
                TABLE_SWEPT_THOROUGH,
                ImmutableList.of(ROW_FOO),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1000));
        assertThatThrownBy(() -> Streams.stream(sortedColumns).forEach(Map.Entry::getKey))
                .isInstanceOf(TransactionLockTimeoutException.class);
    }

    @Test
    public void getSortedColumnsThrowsIfLockIsLostMidway() {
        List<byte[]> rows = LongStream.range(0, 10).mapToObj(PtBytes::toBytes).collect(Collectors.toList());
        List<Cell> cells = rows.stream().map(row -> Cell.create(row, COL_A)).collect(Collectors.toList());
        putCellsInTable(cells, TABLE_SWEPT_THOROUGH);

        ConjureStartTransactionsResponse conjureResponse = startTransactionWithWatches();
        LockImmutableTimestampResponse res = conjureResponse.getImmutableTimestamp();
        long transactionTs = conjureResponse.getTimestamps().start();

        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        int batchHint = 5;
        Iterator<Map.Entry<Cell, byte[]>> sortedColumns = transaction.getSortedColumns(
                TABLE_SWEPT_THOROUGH,
                rows,
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, batchHint));
        assertThat(sortedColumns.next().getKey()).isEqualTo(cells.get(0));

        // lock lost after getting first batch
        timelockService.unlock(ImmutableSet.of(res.getLock()));

        // should still be able to get all but last element of the elements for the first batch;
        // next batch is preemptively fetched when last element of curr batch is retrieved
        List<Cell> retrievedEntries = IntStream.range(1, batchHint - 1)
                .mapToObj(_unused -> sortedColumns.next().getKey())
                .collect(Collectors.toList());
        assertThat(retrievedEntries).hasSameElementsAs(cells.subList(1, batchHint - 1));

        // should throw while fetching the next batch
        assertThatThrownBy(sortedColumns::next).isInstanceOf(TransactionLockTimeoutException.class);
    }

    @Test
    public void getSortedColumnsObeysColumnRangeSelection() {
        List<byte[]> rows = ImmutableList.of(ROW_FOO, ROW_BAR);
        List<Cell> colA_cells = ImmutableList.of(Cell.create(ROW_FOO, COL_A), Cell.create(ROW_BAR, COL_A));

        putCellsInTable(colA_cells, TABLE);
        List<Cell> entries = getSortedEntries(
                TABLE, rows, BatchColumnRangeSelection.create(COL_A, "az".getBytes(StandardCharsets.UTF_8), 1000));
        Assertions.assertThat(entries).containsExactlyElementsOf(colA_cells);

        List<Cell> outOfRangeEntries = getSortedEntries(
                TABLE,
                rows,
                BatchColumnRangeSelection.create(
                        "y".getBytes(StandardCharsets.UTF_8), "z".getBytes(StandardCharsets.UTF_8), 1000));
        Assertions.assertThat(outOfRangeEntries).isEmpty();
    }

    @Test
    public void getSortedColumnsIteratesOverMultipleBatchesIfRequired() {
        ImmutableList<byte[]> rows = ImmutableList.of(ROW_FOO, ROW_BAR);
        ImmutableList<byte[]> columns = ImmutableList.of(COL_A, COL_B);

        List<Cell> cells = rows.stream()
                .map(row -> columns.stream().map(col -> Cell.create(row, col)).collect(Collectors.toList()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        putCellsInTable(cells, TABLE);
        List<Cell> entries = getSortedEntries(
                TABLE, rows, BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));

        List<Cell> entriesSortedByColumn = columns.stream()
                .map(col -> rows.stream().map(row -> Cell.create(row, col)).collect(Collectors.toList()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        Assertions.assertThat(entries).containsExactlyElementsOf(entriesSortedByColumn);
    }

    @Test
    public void getSortedColumnsValidatesLocksOncePerBatch() {
        List<byte[]> rows = LongStream.range(0, 1000).mapToObj(PtBytes::toBytes).collect(Collectors.toList());

        List<Cell> cells = rows.stream().map(row -> Cell.create(row, COL_A)).collect(Collectors.toList());
        putCellsInTable(cells, TABLE_SWEPT_THOROUGH);

        verifyPrefetchValidations(rows, cells, 100, 10, 1000);
        verifyPrefetchValidations(rows, cells, 100, 3, 299);
        verifyPrefetchValidations(rows, cells, 100, 4, 300);
        verifyPrefetchValidations(rows, cells, 100, 4, 301);
    }

    @Test
    public void getSortedColumnsServesInOneRequestForSmallLoad() {
        int numRows = 7;
        int numColumns = 99;
        int expectedBatchHintForKvs = numColumns;

        verifyLoadOnKvs(numColumns, numRows, expectedBatchHintForKvs);
    }

    @Test
    public void getSortedColumnsDistributesLoadAmongRows() {
        int numRows = 7;
        int numColumns = 10_000;
        int expectedBatchHintForKvs = numColumns / numRows;

        verifyLoadOnKvs(numColumns, numRows, expectedBatchHintForKvs);
    }

    @Test
    public void getSortedColumnsDistributesLoadWithMinBound() {
        int numRows = 5;
        int numColumns = 101;

        verifyLoadOnKvs(numColumns, numRows, SnapshotTransaction.MIN_BATCH_SIZE_FOR_DISTRIBUTED_LOAD);
    }

    @Test
    public void runsCallbacksInRegistrationOrder() {
        Transaction transaction = txManager.createNewTransaction();
        Runnable firstCallback = mock(Runnable.class);
        Runnable secondCallback = mock(Runnable.class);
        transaction.onSuccess(firstCallback);
        transaction.onSuccess(secondCallback);
        transaction.commit();

        InOrder inOrder = Mockito.inOrder(firstCallback, secondCallback);
        inOrder.verify(firstCallback).run();
        inOrder.verify(secondCallback).run();
    }

    @Test
    public void cannotRegisterCallbackAfterTransactionCloses() {
        Transaction committingTransaction = txManager.createNewTransaction();
        committingTransaction.commit();
        Transaction abortingTransaction = txManager.createNewTransaction();
        abortingTransaction.abort();

        assertThatThrownBy(() -> committingTransaction.onSuccess(() -> {}))
                .isInstanceOf(CommittedTransactionException.class)
                .hasMessageContaining("Transaction must be uncommitted");
        assertThatThrownBy(() -> abortingTransaction.onSuccess(() -> {}))
                .isInstanceOf(CommittedTransactionException.class)
                .hasMessageContaining("Transaction must be uncommitted");
    }

    @Test
    public void cannotRegisterNullCallback() {
        Transaction transaction = txManager.createNewTransaction();
        assertThatThrownBy(() -> transaction.onSuccess(null)).isInstanceOf(NullPointerException.class);
        transaction.abort();
    }

    @Test
    public void exceptionThrownFromCallbackStopsChain() {
        Transaction transaction = txManager.createNewTransaction();
        List<Runnable> callbacks =
                IntStream.range(0, 3).mapToObj(_unused -> mock(Runnable.class)).collect(Collectors.toList());
        callbacks.forEach(transaction::onSuccess);

        doThrow(RuntimeException.class).when(callbacks.get(1)).run();

        assertThatThrownBy(transaction::commit).isInstanceOf(RuntimeException.class);
        verify(callbacks.get(0)).run();
        verify(callbacks.get(1)).run();
        verify(callbacks.get(2), never()).run();
    }

    @Test
    public void canRegisterCallbacksFromManyThreads() {
        Transaction transaction = txManager.createNewTransaction();
        List<Runnable> callbacks = IntStream.range(0, 1000)
                .mapToObj(_unused -> mock(Runnable.class))
                .collect(Collectors.toList());
        ExecutorService executorService = PTExecutors.newFixedThreadPool(8);
        List<Future<?>> futures = callbacks.stream()
                .map(callback -> executorService.submit(() -> transaction.onSuccess(callback)))
                .collect(Collectors.toList());
        futures.forEach(Futures::getUnchecked);
        transaction.commit();
        callbacks.forEach(callback -> verify(callback).run());
    }

    @Test
    public void transactionStillCommittedEvenIfCallbackThrows() {
        RuntimeException exception = new RuntimeException("boom");
        assertThatThrownBy(() -> txManager.runTaskThrowOnConflict(txn -> {
                    txn.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("tom")));
                    txn.onSuccess(() -> {
                        throw exception;
                    });
                    return null;
                }))
                .isInstanceOf(exception.getClass())
                .hasMessageContaining(exception.getMessage());
        txManager.runTaskReadOnly(txn -> {
            assertThat(txn.get(TABLE, ImmutableSet.of(TEST_CELL)))
                    .containsExactly(Maps.immutableEntry(TEST_CELL, PtBytes.toBytes("tom")));
            return null;
        });
    }

    @Test
    public void transactionStillCommittedEvenIfConditionCleanupThrows() {
        RuntimeException exception = new RuntimeException("boom");
        PreCommitCondition preCommitCondition = preCommitConditionFactory(NO_OP_THROW_IF_CONDITION_INVALID, () -> {
            throw exception;
        });

        assertThatThrownBy(() -> txManager.runTaskWithConditionThrowOnConflict(preCommitCondition, (txn, condition) -> {
                    txn.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("tom")));
                    return null;
                }))
                .isInstanceOf(exception.getClass())
                .hasMessageContaining(exception.getMessage());
        txManager.runTaskReadOnly(txn -> {
            assertThat(txn.get(TABLE, ImmutableSet.of(TEST_CELL)))
                    .containsExactly(Maps.immutableEntry(TEST_CELL, PtBytes.toBytes("tom")));
            return null;
        });
    }

    @Test
    public void preCommitCleanupHappensIfStartTransactionsFails() {
        AtomicBoolean hasRun = new AtomicBoolean(false);
        PreCommitCondition preCommitCondition =
                preCommitConditionFactory(NO_OP_THROW_IF_CONDITION_INVALID, () -> hasRun.set(true));

        inMemoryTimelockExtension.close();

        assertThatThrownBy(() -> txManager.runTaskWithConditionThrowOnConflict(preCommitCondition, (txn, condition) -> {
            txn.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("tom")));
            return null;
        }));

        assertThat(hasRun.get()).isTrue();
    }

    @Test
    public void cannotCompleteFutureAfterTransactionCommit() {
        SettableFuture<Object> blocker = SettableFuture.create();
        ListenableFuture<Map<Cell, byte[]>> getFuture = txManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("jeremy")));
            return Futures.transformAsync(
                    blocker,
                    _unused -> txn.getAsync(TABLE, ImmutableSet.of(TEST_CELL)),
                    MoreExecutors.directExecutor());
        });
        blocker.set(new Object());
        assertThatThrownBy(getFuture::get)
                .isInstanceOf(ExecutionException.class)
                .satisfies(exception ->
                        assertThat(exception.getCause()).isInstanceOf(CommittedTransactionException.class));
    }

    @Test
    public void cannotReadStreamAfterTransactionCommit() {
        Stream<byte[]> values = txManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("tom")));

            BiFunction<RangeRequest, BatchingVisitable<RowResult<byte[]>>, byte[]> singleValueExtractor =
                    ($, visitable) -> Iterables.getOnlyElement(BatchingVisitables.copyToList(visitable))
                            .getOnlyColumnValue();
            return txn.getRanges(ImmutableGetRangesQuery.<byte[]>builder()
                    .tableRef(TABLE)
                    .rangeRequests(ImmutableList.of(RangeRequest.all()))
                    .visitableProcessor(singleValueExtractor)
                    .build());
        });
        assertThatThrownBy(() -> values.collect(Collectors.toList())).isInstanceOf(CommittedTransactionException.class);
    }

    @Test
    public void cannotPerformUnmergedGetRowsColumnRangeAfterTransactionCommit() {
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> visitables = txManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("alice")));
            return txn.getRowsColumnRange(
                    TABLE,
                    ImmutableList.of(TEST_CELL.getRowName()),
                    BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 100));
        });

        assertThatThrownBy(() -> BatchingVisitables.copyToList(visitables.get(TEST_CELL.getRowName())))
                .isInstanceOf(CommittedTransactionException.class);
    }

    @Test
    public void cannotPerformMergedGetRowsColumnRangeAfterTransactionCommit() {
        Iterator<Map.Entry<Cell, byte[]>> entryIterator = txManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("bob")));
            return txn.getRowsColumnRange(
                    TABLE,
                    ImmutableList.of(TEST_CELL.getRowName()),
                    new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY),
                    100);
        });

        assertThatThrownBy(entryIterator::next).isInstanceOf(CommittedTransactionException.class);
    }

    @Test
    public void cannotReadSortedColumnsAfterTransactionCommit() {
        Iterator<Map.Entry<Cell, byte[]>> cellIterator = txManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("will")));

            return txn.getSortedColumns(
                    TABLE,
                    ImmutableList.of(TEST_CELL.getRowName()),
                    BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 100));
        });
        assertThatThrownBy(cellIterator::next).isInstanceOf(CommittedTransactionException.class);
    }

    @Test
    public void cannotReadVisitablesFromGetRangesLazyAfterTransactionCommit() {
        txManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("peyton")));
            txn.put(TABLE, ImmutableMap.of(TEST_CELL_2, PtBytes.toBytes("eli")));
            return null;
        });

        BatchingVisitable<RowResult<byte[]>> leakedVisitable =
                txManager.runTaskThrowOnConflict(txn -> txn.getRangesLazy(
                                TABLE,
                                ImmutableList.of(
                                        RangeRequest.builder()
                                                .startRowInclusive(TEST_CELL.getRowName())
                                                .endRowExclusive(TEST_CELL_2.getRowName())
                                                .batchHint(1)
                                                .build(),
                                        RangeRequest.builder()
                                                .startRowInclusive(TEST_CELL_2.getRowName())
                                                .batchHint(1)
                                                .build()))
                        .findFirst()
                        .orElseThrow(() -> new SafeIllegalStateException("expected at least one visitable!")));
        assertThatThrownBy(() -> BatchingVisitables.copyToList(leakedVisitable))
                .isInstanceOf(CommittedTransactionException.class);
    }

    @Test
    public void testConstraintsNotCheckedOnReadOnlyTransaction() {
        ConstraintCheckable mockConstraint = mock(ConstraintCheckable.class);
        when(mockConstraint.findConstraintFailures(any(), any(), any())).thenReturn(ImmutableList.of());

        writeCells(TABLE, ImmutableMap.of(Cell.create(ROW_FOO, COL_A), "val".getBytes(StandardCharsets.UTF_8)));

        Transaction transaction = txManager.createNewTransaction();
        transaction.useTable(TABLE, mockConstraint);
        Runnable callback = mock(Runnable.class);
        transaction.onSuccess(callback);
        NavigableMap<byte[], RowResult<byte[]>> rows =
                transaction.getRows(TABLE, ImmutableSet.of(ROW_FOO), ColumnSelection.all());
        transaction.commit();

        verify(callback, times(1)).run();
        verifyNoInteractions(mockConstraint);

        RowResult<byte[]> result = rows.get(ROW_FOO);
        assertThat(result.getCellSet()).hasSize(1);
        assertThat(result.getOnlyColumnValue()).asString(StandardCharsets.UTF_8).isEqualTo("val");
    }

    @Test
    public void testConstraintsCheckedOnSuccessfulTransaction() {
        ImmutableMap<Cell, byte[]> writes =
                ImmutableMap.of(Cell.create(ROW_FOO, COL_A), "val".getBytes(StandardCharsets.UTF_8));

        ConstraintCheckable mockConstraint = mock(ConstraintCheckable.class);
        when(mockConstraint.findConstraintFailures(any(), any(), any())).thenReturn(ImmutableList.of());

        Transaction transaction = txManager.createNewTransaction();
        transaction.useTable(TABLE, mockConstraint);
        Runnable callback = mock(Runnable.class);
        transaction.onSuccess(callback);
        transaction.put(TABLE, writes);
        transaction.commit();

        verify(callback, times(1)).run();
        verify(mockConstraint, times(1))
                .findConstraintFailures(
                        eq(writes),
                        any(ConstraintCheckingTransaction.class),
                        eq(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS));
    }

    @Test
    public void testConstraintsViolated() {
        Map<Cell, byte[]> writes = ImmutableMap.of(Cell.create(ROW_FOO, COL_A), "val".getBytes(StandardCharsets.UTF_8));

        String constraintViolated = "test constraint violated";
        ConstraintCheckable mockConstraint = mock(ConstraintCheckable.class);
        when(mockConstraint.findConstraintFailures(any(), any(), any()))
                .thenReturn(ImmutableList.of(constraintViolated));

        Transaction transaction = txManager.createNewTransaction();
        transaction.useTable(TABLE, mockConstraint);
        Runnable callback = mock(Runnable.class);
        transaction.onSuccess(callback);
        transaction.put(TABLE, writes);

        assertThatThrownBy(transaction::commit)
                .isInstanceOf(AtlasDbConstraintException.class)
                .hasMessageContaining(constraintViolated);

        verifyNoInteractions(callback);
        verify(mockConstraint, times(1))
                .findConstraintFailures(
                        eq(writes),
                        any(ConstraintCheckingTransaction.class),
                        eq(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS));
    }

    @Test
    public void setsRequestedCommitLocksCountCorrectly_serializableCell() {
        // Will request commit locks for cells, but not rows
        overrideConflictHandlerForTable(TABLE, ConflictHandler.SERIALIZABLE_CELL);

        // We can only get the commit info from a snapshot transaction
        SnapshotTransaction txn = unwrapSnapshotTransaction(txManager.createNewTransaction());
        txn.put(TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE));
        txn.put(TABLE, ImmutableMap.of(TEST_CELL_2, TEST_VALUE));
        txn.commit();

        TransactionCommitLockInfo commitLockInfo = txn.getCommitLockInfo();
        assertThat(commitLockInfo.cellCommitLocksRequested()).isEqualTo(2);
        // For write transactions, we always lock an additional row in the transaction table
        assertThat(commitLockInfo.rowCommitLocksRequested()).isEqualTo(0 + 1);
    }

    @Test
    public void setsRequestedCommitLocksCountCorrectly_serializable() {
        // Will request commit locks for rows, but not cells
        overrideConflictHandlerForTable(TABLE, ConflictHandler.SERIALIZABLE);

        SnapshotTransaction txn = unwrapSnapshotTransaction(txManager.createNewTransaction());
        txn.put(TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE));
        txn.put(TABLE, ImmutableMap.of(TEST_CELL_2, TEST_VALUE));
        txn.commit();

        TransactionCommitLockInfo commitLockInfo = txn.getCommitLockInfo();
        assertThat(commitLockInfo.cellCommitLocksRequested()).isEqualTo(0);
        assertThat(commitLockInfo.rowCommitLocksRequested()).isEqualTo(2 + 1);
    }

    @Test
    public void setsRequestedCommitLocksCountCorrectly_serializableLockLevelMigration_sameRow() {
        // Will request commit locks for cells and rows
        overrideConflictHandlerForTable(TABLE, ConflictHandler.SERIALIZABLE_LOCK_LEVEL_MIGRATION);

        SnapshotTransaction txn = unwrapSnapshotTransaction(txManager.createNewTransaction());
        txn.put(
                TABLE,
                ImmutableMap.of(Cell.create(PtBytes.toBytes("same_row"), PtBytes.toBytes("column1")), TEST_VALUE));
        txn.put(
                TABLE,
                ImmutableMap.of(
                        Cell.create(
                                PtBytes.toBytes("same_row"),
                                // Writing to the same row, but different column/cell
                                PtBytes.toBytes("column2")),
                        TEST_VALUE));
        txn.commit();

        TransactionCommitLockInfo commitLockInfo = txn.getCommitLockInfo();
        assertThat(commitLockInfo.cellCommitLocksRequested()).isEqualTo(2);
        assertThat(commitLockInfo.rowCommitLocksRequested()).isEqualTo(1 + 1);
    }

    @Test
    public void setsRequestedCellCommitLocksCountCorrectlyForMultipleTables() {
        overrideConflictHandlerForTable(TABLE, ConflictHandler.SERIALIZABLE_CELL);
        overrideConflictHandlerForTable(TABLE2, ConflictHandler.SERIALIZABLE_CELL);

        SnapshotTransaction txn = unwrapSnapshotTransaction(txManager.createNewTransaction());
        txn.put(TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE));
        txn.put(TABLE2, ImmutableMap.of(TEST_CELL_2, TEST_VALUE));
        txn.commit();

        TransactionCommitLockInfo commitLockInfo = txn.getCommitLockInfo();
        assertThat(commitLockInfo.cellCommitLocksRequested()).isEqualTo(2);
        assertThat(commitLockInfo.rowCommitLocksRequested()).isEqualTo(0 + 1);
    }

    @Test
    public void exceptionThrownWhenTooManyPostFilterIterationsOccur() {
        for (int idx = 0; idx < SnapshotTransaction.MAX_POST_FILTERING_ITERATIONS; idx++) {
            putUncommittedAtFreshTimestamp(TABLE_NO_SWEEP, TEST_CELL);
        }
        assertThatLoggableExceptionThrownBy(
                        () -> txManager.runTaskThrowOnConflict(txn -> txn.get(TABLE_NO_SWEEP, Set.of(TEST_CELL))))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageStartingWith("Unable to filter cells")
                .hasExactlyArgs(
                        SafeArg.of("table", TABLE_NO_SWEEP),
                        SafeArg.of("maxIterations", SnapshotTransaction.MAX_POST_FILTERING_ITERATIONS));
    }

    @Test
    public void metadataIsTransferredToCellLocksForPutWithMetadata() {
        TimelockService timelockService = spy(txManager.getTimelockService());
        // Only locks cells
        Transaction txn = getSnapshotTransactionWith(
                timelockService,
                ImmutableMap.of(TABLE, ConflictHandler.SERIALIZABLE_CELL, TABLE2, ConflictHandler.SERIALIZABLE_CELL));

        txn.putWithMetadata(
                TABLE,
                ImmutableMap.of(
                        TEST_CELL,
                        ValueAndChangeMetadata.of(TEST_VALUE, UPDATE_CHANGE_METADATA),
                        TEST_CELL_2,
                        ValueAndChangeMetadata.of(TEST_VALUE, DELETE_CHANGE_METADATA)));
        txn.putWithMetadata(
                TABLE2, ImmutableMap.of(TEST_CELL, ValueAndChangeMetadata.of(TEST_VALUE, UNCHANGED_CHANGE_METADATA)));

        LockDescriptor cellLock =
                AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), TEST_CELL.getRowName(), TEST_CELL.getColumnName());
        LockDescriptor cellLock2 = AtlasCellLockDescriptor.of(
                TABLE.getQualifiedName(), TEST_CELL_2.getRowName(), TEST_CELL_2.getColumnName());
        LockDescriptor cellLock3 = AtlasCellLockDescriptor.of(
                TABLE2.getQualifiedName(), TEST_CELL.getRowName(), TEST_CELL.getColumnName());
        verifyLockWasCalledWithLocksAndMetadataWhenCommitting(
                txn,
                timelockService,
                LocksAndMetadata.of(
                        ImmutableSet.of(cellLock, cellLock2, cellLock3),
                        Optional.of(LockRequestMetadata.of(ImmutableMap.of(
                                cellLock,
                                UPDATE_CHANGE_METADATA,
                                cellLock2,
                                DELETE_CHANGE_METADATA,
                                cellLock3,
                                UNCHANGED_CHANGE_METADATA)))));
    }

    @Test
    public void metadataIsTransferredToCellLocksForDeleteWithMetadata() {
        TimelockService timelockService = spy(txManager.getTimelockService());
        // Only locks cells
        Transaction txn = getSnapshotTransactionWith(
                timelockService,
                ImmutableMap.of(TABLE, ConflictHandler.SERIALIZABLE_CELL, TABLE2, ConflictHandler.SERIALIZABLE_CELL));

        txn.deleteWithMetadata(
                TABLE, ImmutableMap.of(TEST_CELL, UPDATE_CHANGE_METADATA, TEST_CELL_2, DELETE_CHANGE_METADATA));
        txn.deleteWithMetadata(TABLE2, ImmutableMap.of(TEST_CELL, UNCHANGED_CHANGE_METADATA));

        LockDescriptor cellLock =
                AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), TEST_CELL.getRowName(), TEST_CELL.getColumnName());
        LockDescriptor cellLock2 = AtlasCellLockDescriptor.of(
                TABLE.getQualifiedName(), TEST_CELL_2.getRowName(), TEST_CELL_2.getColumnName());
        LockDescriptor cellLock3 = AtlasCellLockDescriptor.of(
                TABLE2.getQualifiedName(), TEST_CELL.getRowName(), TEST_CELL.getColumnName());
        verifyLockWasCalledWithLocksAndMetadataWhenCommitting(
                txn,
                timelockService,
                LocksAndMetadata.of(
                        ImmutableSet.of(cellLock, cellLock2, cellLock3),
                        Optional.of(LockRequestMetadata.of(ImmutableMap.of(
                                cellLock,
                                UPDATE_CHANGE_METADATA,
                                cellLock2,
                                DELETE_CHANGE_METADATA,
                                cellLock3,
                                UNCHANGED_CHANGE_METADATA)))));
    }

    @Test
    public void metadataIsTransferredToRowLocks() {
        TimelockService timelockService = spy(txManager.getTimelockService());
        // Only locks rows
        Transaction txn = getSnapshotTransactionWith(
                timelockService,
                ImmutableMap.of(TABLE, ConflictHandler.SERIALIZABLE, TABLE2, ConflictHandler.SERIALIZABLE));

        txn.put(TABLE, ImmutableMap.of(Cell.create(ROW_FOO, COL_A), TEST_VALUE));
        txn.putWithMetadata(
                TABLE,
                ImmutableMap.of(
                        Cell.create(ROW_FOO, COL_B), ValueAndChangeMetadata.of(TEST_VALUE, UPDATE_CHANGE_METADATA),
                        Cell.create(ROW_BAR, COL_A), ValueAndChangeMetadata.of(TEST_VALUE, DELETE_CHANGE_METADATA)));
        txn.putWithMetadata(
                TABLE2, ImmutableMap.of(TEST_CELL, ValueAndChangeMetadata.of(TEST_VALUE, UNCHANGED_CHANGE_METADATA)));

        LockDescriptor rowLock = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), ROW_FOO);
        LockDescriptor rowLock2 = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), ROW_BAR);
        LockDescriptor rowLock3 = AtlasRowLockDescriptor.of(TABLE2.getQualifiedName(), TEST_CELL.getRowName());
        verifyLockWasCalledWithLocksAndMetadataWhenCommitting(
                txn,
                timelockService,
                LocksAndMetadata.of(
                        ImmutableSet.of(rowLock, rowLock2, rowLock3),
                        Optional.of(LockRequestMetadata.of(ImmutableMap.of(
                                rowLock,
                                UPDATE_CHANGE_METADATA,
                                rowLock2,
                                DELETE_CHANGE_METADATA,
                                rowLock3,
                                UNCHANGED_CHANGE_METADATA)))));
    }

    @Test
    public void metadataCanBeTransferredToCellAndRowLocksSimultaneously() {
        TimelockService timelockService = spy(txManager.getTimelockService());
        // locks both rows and cells
        Transaction txn = getSnapshotTransactionWith(
                timelockService, ImmutableMap.of(TABLE, ConflictHandler.SERIALIZABLE_LOCK_LEVEL_MIGRATION));

        txn.putWithMetadata(
                TABLE, ImmutableMap.of(TEST_CELL, ValueAndChangeMetadata.of(TEST_VALUE, UPDATE_CHANGE_METADATA)));

        LockDescriptor rowLock = AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), TEST_CELL.getRowName());
        LockDescriptor cellLock =
                AtlasCellLockDescriptor.of(TABLE.getQualifiedName(), TEST_CELL.getRowName(), TEST_CELL.getColumnName());
        verifyLockWasCalledWithLocksAndMetadataWhenCommitting(
                txn,
                timelockService,
                LocksAndMetadata.of(
                        ImmutableSet.of(rowLock, cellLock),
                        Optional.of(LockRequestMetadata.of(
                                ImmutableMap.of(rowLock, UPDATE_CHANGE_METADATA, cellLock, UPDATE_CHANGE_METADATA)))));
    }

    @Test
    public void noMetadataResultsInEmptyOptional() {
        TimelockService timelockService = spy(txManager.getTimelockService());
        Transaction txn =
                getSnapshotTransactionWith(timelockService, ImmutableMap.of(TABLE, ConflictHandler.SERIALIZABLE));
        txn.put(TABLE, ImmutableMap.of(TEST_CELL, TEST_VALUE));
        verifyLockWasCalledWithLocksAndMetadataWhenCommitting(
                txn,
                timelockService,
                LocksAndMetadata.of(
                        ImmutableSet.of(AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), TEST_CELL.getRowName())),
                        Optional.empty()));
    }

    @Test
    public void lockCollectionWithoutMetadataIsCorrectForRandomWrites() {
        long randomSeed = System.currentTimeMillis();
        int numberOfRowsPerTable = 10;
        int numberOfCellsPerRow = 10;
        int maxWritesPerCell = 200;

        Random random = new Random(randomSeed);
        Map<TableReference, ConflictHandler> tableToConflictHandler = getTablesForAllConflictHandlers();
        List<TableReference> tables = ImmutableList.copyOf(tableToConflictHandler.keySet());
        TimelockService timelockService = spy(txManager.getTimelockService());
        List<Cell> cells = generateCells(numberOfRowsPerTable, numberOfCellsPerRow);

        Transaction txn = getSnapshotTransactionWith(timelockService, tableToConflictHandler);
        Map<TableReference, Set<Cell>> cellsWithWrites = new HashMap<>();
        for (int i = 0; i < maxWritesPerCell; i++) {
            TableReference table = tables.get(random.nextInt(tables.size()));
            Cell cell = cells.get(random.nextInt(cells.size()));
            if (random.nextBoolean()) {
                txn.put(table, ImmutableMap.of(cell, TEST_VALUE));
            } else {
                txn.delete(table, ImmutableSet.of(cell));
            }
            cellsWithWrites.computeIfAbsent(table, _unused -> new HashSet<>()).add(cell);
        }

        verifyLockWasCalledWithLocksAndMetadataWhenCommitting(
                txn,
                timelockService,
                LocksAndMetadata.of(getExpectedLocks(tableToConflictHandler, cellsWithWrites), Optional.empty()),
                "Expect locks to be passed to TimeLock. Random seed: " + randomSeed);
    }

    @Test
    public void commitFailsIfMultipleCellsInSameRowHaveMetadataAndUsingRowLocks() {
        overrideConflictHandlerForTable(TABLE, ConflictHandler.SERIALIZABLE);
        Cell cell1 = Cell.create(ROW_BAR, COL_A);
        Cell cell2 = Cell.create(ROW_BAR, COL_B);
        Transaction txn = txManager.createNewTransaction();

        txn.putWithMetadata(
                TABLE,
                ImmutableMap.of(
                        cell1,
                        ValueAndChangeMetadata.of(TEST_VALUE, UPDATE_CHANGE_METADATA),
                        cell2,
                        ValueAndChangeMetadata.of(TEST_VALUE, UPDATE_CHANGE_METADATA)));

        assertThatLoggableExceptionThrownBy(txn::commit)
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("Two different cells in the same row have metadata and we create locks on row level.")
                .hasExactlyArgs(
                        LoggingArgs.tableRef(TABLE),
                        UnsafeArg.of("rowName", cell1.getRowName()),
                        UnsafeArg.of("newMetadata", UPDATE_CHANGE_METADATA));
    }

    @Test
    public void reportsMetadataInfoCorrectly() {
        overrideConflictHandlerForTable(TABLE, ConflictHandler.SERIALIZABLE);
        overrideConflictHandlerForTable(TABLE2, ConflictHandler.SERIALIZABLE_LOCK_LEVEL_MIGRATION);
        SnapshotTransaction txn = unwrapSnapshotTransaction(txManager.createNewTransaction());

        txn.putWithMetadata(
                TABLE, ImmutableMap.of(TEST_CELL, ValueAndChangeMetadata.of(TEST_VALUE, UPDATE_CHANGE_METADATA)));
        txn.putWithMetadata(
                TABLE2,
                ImmutableMap.of(
                        TEST_CELL,
                        ValueAndChangeMetadata.of(TEST_VALUE, ChangeMetadata.unchanged()),
                        TEST_CELL_2,
                        ValueAndChangeMetadata.of(TEST_VALUE, ChangeMetadata.unchanged())));
        txn.commit();

        TransactionWriteMetadataInfo writeMetadataInfo = txn.getWriteMetadataInfo();
        assertThat(writeMetadataInfo.changeMetadataBuffered()).isEqualTo(3);
        assertThat(writeMetadataInfo.cellChangeMetadataSent()).isEqualTo(2);
        assertThat(writeMetadataInfo.rowChangeMetadataSent()).isEqualTo(3);
    }

    private void verifyPrefetchValidations(
            List<byte[]> rows,
            List<Cell> cells,
            int batchHint,
            int expectedNumberOfInvocations,
            int numElementsToBeAccessed) {
        TimelockService spiedTimeLockService = spy(timelockService);
        ConjureStartTransactionsResponse conjureResponse = startTransactionWithWatches();
        LockImmutableTimestampResponse res = conjureResponse.getImmutableTimestamp();
        long transactionTs = conjureResponse.getTimestamps().start();
        Transaction transaction = getSnapshotTransactionWith(
                spiedTimeLockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        Iterator<Map.Entry<Cell, byte[]>> sortedColumns = transaction.getSortedColumns(
                TABLE_SWEPT_THOROUGH,
                rows,
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, batchHint));
        List<Cell> entries = IntStream.range(0, numElementsToBeAccessed)
                .mapToObj(_unused -> sortedColumns.next().getKey())
                .collect(Collectors.toList());
        Assertions.assertThat(entries).containsExactlyElementsOf(cells.subList(0, numElementsToBeAccessed));
        verify(spiedTimeLockService, times(expectedNumberOfInvocations))
                .refreshLockLeases(ImmutableSet.of(res.getLock()));
    }

    private ConjureStartTransactionsResponse startTransactionWithWatches() {
        ConjureStartTransactionsResponse conjureResponse =
                inMemoryTimelockExtension.getLockLeaseService().startTransactionsWithWatches(Optional.empty(), 1);
        Set<Long> startTimestamps =
                conjureResponse.getTimestamps().stream().boxed().collect(Collectors.toSet());
        inMemoryTimelockExtension
                .getLockWatchManager()
                .getCache()
                .processStartTransactionsUpdate(startTimestamps, conjureResponse.getLockWatchUpdate());
        return conjureResponse;
    }

    private void verifyLoadOnKvs(int numColumns, int numRows, int expectedBatchHintForKvs) {
        List<byte[]> rows =
                LongStream.range(0, numRows).mapToObj(PtBytes::toBytes).collect(Collectors.toList());
        List<byte[]> columns =
                LongStream.range(0, numColumns).mapToObj(PtBytes::toBytes).collect(Collectors.toList());

        // Only provide entries for the first row
        List<Cell> cells =
                columns.stream().map(column -> Cell.create(rows.get(0), column)).collect(Collectors.toList());
        putCellsInTable(cells, TABLE);

        verifyBatchHintSizesForKvs(rows, cells.size(), expectedBatchHintForKvs);
    }

    private void verifyBatchHintSizesForKvs(List<byte[]> rows, int batchHint, int expectedBatchHintForKvs) {
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();
        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        transaction.getSortedColumns(
                TABLE_SWEPT_THOROUGH,
                rows,
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, batchHint));

        ArgumentCaptor<BatchColumnRangeSelection> perBatchSelection =
                ArgumentCaptor.forClass(BatchColumnRangeSelection.class);
        // In-memory kvs does not honour batch hint; returns all cells in one call
        verify(keyValueService)
                .getRowsColumnRange(eq(TABLE_SWEPT_THOROUGH), eq(rows), perBatchSelection.capture(), eq(transactionTs));
        assertThat(perBatchSelection.getValue().getBatchHint()).isEqualTo(expectedBatchHintForKvs);
    }

    private List<Cell> getSortedEntries(
            TableReference table, List<byte[]> rows, BatchColumnRangeSelection batchColumnRangeSelection) {
        return serializableTxManager.runTaskWithRetry(tx -> {
            Iterator<Map.Entry<Cell, byte[]>> sortedColumns =
                    tx.getSortedColumns(table, rows, batchColumnRangeSelection);
            return Streams.stream(sortedColumns).map(Map.Entry::getKey).collect(Collectors.toList());
        });
    }

    private void putCellsInTable(List<Cell> cells, TableReference table) {
        byte[] value = new byte[1];
        Map<Cell, byte[]> cellMap = KeyedStream.of(cells).map(_unused -> value).collectToMap();
        txManager.runTaskWithRetry(tx -> {
            tx.put(table, cellMap);
            return null;
        });
    }

    private void putUncommittedAtFreshTimestamp(TableReference tableRef, Cell cell) {
        keyValueService.put(
                tableRef,
                ImmutableMap.of(cell, PtBytes.toBytes("i am uncommitted")),
                txManager.createNewTransaction().getTimestamp());
    }

    private void writeSentinelToTestTable(TableReference tableRef, Cell cell) {
        keyValueService.put(tableRef, ImmutableMap.of(cell, new byte[0]), Value.INVALID_VALUE_TIMESTAMP);
    }

    private void commitWrite(TableReference tableSweptThorough, Cell testCell) {
        Transaction txn = txManager.createNewTransaction();
        txn.put(tableSweptThorough, ImmutableMap.of(testCell, PtBytes.toBytes("something")));
        txn.commit();
    }

    private void setTransactionConfig(TransactionConfig config) {
        transactionConfig = config;
    }

    private Transaction getSnapshotTransactionWith(
            TimelockService timelockService, Map<TableReference, ConflictHandler> tableConflictHandlers) {
        ConjureStartTransactionsResponse conjureResponse = startTransactionWithWatches();
        LockImmutableTimestampResponse lockImmutableTimestampResponse = conjureResponse.getImmutableTimestamp();
        long transactionTs = conjureResponse.getTimestamps().start();
        return getSnapshotTransactionWith(
                timelockService,
                () -> transactionTs,
                lockImmutableTimestampResponse,
                unused -> {},
                true,
                tableConflictHandlers);
    }

    private Transaction getSnapshotTransactionWith(
            TimelockService timelockService,
            Supplier<Long> startTs,
            LockImmutableTimestampResponse lockImmutableTimestampResponse,
            PreCommitCondition preCommitCondition) {
        return getSnapshotTransactionWith(
                timelockService, startTs, lockImmutableTimestampResponse, preCommitCondition, true);
    }

    private SnapshotTransaction getSnapshotTransactionWith(
            long transactionTs,
            LockImmutableTimestampResponse res,
            LockWatchManagerInternal mockLockWatchManager,
            PathTypeTracker pathTypeTracker) {
        LongSupplier startTimestampSupplier = Suppliers.ofInstance(transactionTs)::get;
        TransactionKeyValueService wrappedKeyValueService = keyValueServiceWrapper.apply(
                txManagerKvs.getTransactionKeyValueService(startTimestampSupplier), pathTypeTracker);
        return new SnapshotTransaction(
                metricsManager,
                wrappedKeyValueService,
                timelockService,
                mockLockWatchManager,
                transactionService,
                NoOpCleaner.INSTANCE,
                startTimestampSupplier,
                TestConflictDetectionManagers.createWithStaticConflictDetection(
                        ImmutableMap.of(TABLE, ConflictHandler.RETRY_ON_WRITE_WRITE)),
                SweepStrategyManagers.createDefault(keyValueService),
                res.getImmutableTimestamp(),
                Optional.of(res.getLock()),
                PreCommitConditions.NO_OP,
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                null,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                false,
                timestampCache,
                getRangesExecutor,
                defaultGetRangesConcurrency,
                MultiTableSweepQueueWriter.NO_OP,
                new DefaultDeleteExecutor(
                        txManagerKvs, timelockService::getFreshTimestamp, MoreExecutors.newDirectExecutorService()),
                true,
                () -> transactionConfig,
                ConflictTracer.NO_OP,
                tableLevelMetricsController,
                knowledge);
    }

    private Transaction getSnapshotTransactionWith(
            TimelockService timelockService,
            Supplier<Long> startTs,
            LockImmutableTimestampResponse lockImmutableTimestampResponse,
            PreCommitCondition preCommitCondition,
            boolean validateLocksOnReads) {
        return getSnapshotTransactionWith(
                timelockService,
                startTs,
                lockImmutableTimestampResponse,
                preCommitCondition,
                validateLocksOnReads,
                ImmutableMap.of(TABLE, ConflictHandler.RETRY_ON_WRITE_WRITE));
    }

    private Transaction getSnapshotTransactionWith(
            TimelockService timelockService,
            Supplier<Long> startTs,
            LockImmutableTimestampResponse lockImmutableTimestampResponse,
            PreCommitCondition preCommitCondition,
            boolean validateLocksOnReads,
            Map<TableReference, ConflictHandler> tableConflictHandlers) {
        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        TransactionKeyValueService wrappedKeyValueService =
                keyValueServiceWrapper.apply(txManagerKvs.getTransactionKeyValueService(startTs::get), pathTypeTracker);
        SnapshotTransaction transaction = new SnapshotTransaction(
                metricsManager,
                wrappedKeyValueService,
                timelockService,
                inMemoryTimelockExtension.getLockWatchManager(),
                transactionService,
                NoOpCleaner.INSTANCE,
                startTs::get,
                TestConflictDetectionManagers.createWithStaticConflictDetection(tableConflictHandlers),
                SweepStrategyManagers.createDefault(keyValueService),
                lockImmutableTimestampResponse.getImmutableTimestamp(),
                Optional.of(lockImmutableTimestampResponse.getLock()),
                preCommitCondition,
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                null,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                false,
                timestampCache,
                getRangesExecutor,
                defaultGetRangesConcurrency,
                MultiTableSweepQueueWriter.NO_OP,
                new DefaultDeleteExecutor(
                        txManagerKvs, timelockService::getFreshTimestamp, MoreExecutors.newDirectExecutorService()),
                validateLocksOnReads,
                () -> transactionConfig,
                ConflictTracer.NO_OP,
                tableLevelMetricsController,
                knowledge);
        return transactionWrapper.apply(transaction, pathTypeTracker);
    }

    private void writeCells(TableReference table, ImmutableMap<Cell, byte[]> cellsToWrite) {
        Transaction writeTransaction = txManager.createNewTransaction();
        writeTransaction.put(table, cellsToWrite);
        writeTransaction.commit();
    }

    private RowResult<byte[]> readRow(byte[] defaultRow) {
        Transaction readTransaction = txManager.createNewTransaction();
        SortedMap<byte[], RowResult<byte[]>> allRows =
                readTransaction.getRows(TABLE, ImmutableSet.of(defaultRow), ColumnSelection.all());
        return allRows.get(defaultRow);
    }

    private TableMetadata getTableMetadataForSweepStrategy(SweepStrategy sweepStrategy) {
        return TableMetadata.builder().sweepStrategy(sweepStrategy).build();
    }

    private HeldLocksToken getExpiredHeldLocksToken() {
        ImmutableSortedMap.Builder<LockDescriptor, LockMode> builder = ImmutableSortedMap.naturalOrder();
        builder.put(
                AtlasRowLockDescriptor.of(
                        TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                        TransactionConstants.getValueForTimestamp(0L)),
                LockMode.WRITE);
        long creationDateMs = System.currentTimeMillis();
        long expirationDateMs = creationDateMs - 1;
        TimeDuration lockTimeout = SimpleTimeDuration.of(0, TimeUnit.SECONDS);
        long versionId = 0L;
        return new HeldLocksToken(
                BigInteger.ZERO,
                lockClient,
                creationDateMs,
                expirationDateMs,
                LockCollections.of(builder.buildOrThrow()),
                lockTimeout,
                versionId,
                "Dummy thread");
    }

    private long concurrentlyIncrementValueThousandTimesAndGet() throws InterruptedException, ExecutionException {
        CompletionService<Void> executor = new ExecutorCompletionService<>(PTExecutors.newFixedThreadPool(8));
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        Transaction t1 = txManager.createNewTransaction();
        t1.put(TABLE, ImmutableMap.of(cell, EncodingUtils.encodeVarLong(0L)));
        t1.commit();
        for (int i = 0; i < 1000; i++) {
            executor.submit(() -> {
                txManager.runTaskWithRetry((TxTask) t -> {
                    long prev = EncodingUtils.decodeVarLong(t.get(TABLE, ImmutableSet.of(cell))
                            .values()
                            .iterator()
                            .next());
                    t.put(TABLE, ImmutableMap.of(cell, EncodingUtils.encodeVarLong(prev + 1)));
                    return null;
                });
                return null;
            });
        }
        for (int i = 0; i < 1000; i++) {
            Future<Void> future = executor.take();
            future.get();
        }
        t1 = txManager.createNewTransaction();
        return EncodingUtils.decodeVarLong(
                t1.get(TABLE, ImmutableSet.of(cell)).values().iterator().next());
    }

    /**
     * Hack to get reference to underlying {@link SnapshotTransaction}. See how transaction managers are composed at
     * {@link AtlasDbTestCase#setUp()}.
     */
    private static SnapshotTransaction unwrapSnapshotTransaction(Transaction transaction) {
        if (transaction instanceof ForwardingTransaction) {
            return unwrapSnapshotTransaction(((ForwardingTransaction) transaction).delegate());
        }
        return (SnapshotTransaction) transaction;
    }

    private static PreCommitCondition preCommitConditionFactory(
            Consumer<Long> throwIfConditionInvalid, Runnable cleanup) {
        return new PreCommitCondition() {
            @Override
            public void throwIfConditionInvalid(long timestamp) {
                throwIfConditionInvalid.accept(timestamp);
            }

            @Override
            public void cleanup() {
                cleanup.run();
            }
        };
    }

    private static void verifyLockWasCalledWithLocksAndMetadataWhenCommitting(
            Transaction txn, TimelockService spiedTimelockService, LocksAndMetadata locksAndMetadata) {
        verifyLockWasCalledWithLocksAndMetadataWhenCommitting(
                txn, spiedTimelockService, locksAndMetadata, "Expect locks and metadata to be passed to TimeLock");
    }

    private static void verifyLockWasCalledWithLocksAndMetadataWhenCommitting(
            Transaction txn,
            TimelockService spiedTimelockService,
            LocksAndMetadata locksAndMetadata,
            String assertionMessage) {
        txn.commit();

        verify(spiedTimelockService)
                .lock(
                        argThat(lockRequest -> {
                            // We always acquire a lock on the transaction table
                            Set<LockDescriptor> locksWithTransactionTableLock = new ImmutableSet.Builder<
                                            LockDescriptor>()
                                    .addAll(locksAndMetadata.lockDescriptors())
                                    .add(AtlasRowLockDescriptor.of(
                                            TransactionConstants.TRANSACTION_TABLE.getQualifiedName(),
                                            TransactionConstants.getValueForTimestamp(txn.getTimestamp())))
                                    .build();
                            assertThat(lockRequest.getLockDescriptors())
                                    .as(assertionMessage)
                                    .containsExactlyInAnyOrderElementsOf(locksWithTransactionTableLock);
                            assertThat(lockRequest.getMetadata())
                                    .as(assertionMessage)
                                    .isEqualTo(locksAndMetadata.metadata());
                            return true;
                        }),
                        any());
    }

    private Map<TableReference, ConflictHandler> getTablesForAllConflictHandlers() {
        int numberOfTables = ConflictHandler.values().length;
        Namespace namespace = Namespace.create("all_conflict_handlers");
        return KeyedStream.of(IntStream.range(0, numberOfTables).boxed())
                .mapKeys(i -> {
                    TableReference tableReference = TableReference.create(
                            namespace, String.format("table_%d_%s", i, ConflictHandler.values()[i].name()));
                    keyValueService.createTable(tableReference, AtlasDbConstants.GENERIC_TABLE_METADATA);
                    return tableReference;
                })
                .map(i -> ConflictHandler.values()[i])
                .collectToMap();
    }

    private static Set<LockDescriptor> getExpectedLocks(
            Map<TableReference, ConflictHandler> tableToConflictHandler,
            Map<TableReference, Set<Cell>> cellsWithWrites) {
        ImmutableSet.Builder<LockDescriptor> locksBuilder = ImmutableSet.builder();
        cellsWithWrites.forEach((table, writes) -> {
            ConflictHandler conflictHandler = tableToConflictHandler.get(table);
            writes.forEach(cell -> {
                if (conflictHandler.lockCellsForConflicts()) {
                    locksBuilder.add(AtlasCellLockDescriptor.of(
                            table.getQualifiedName(), cell.getRowName(), cell.getColumnName()));
                }
                if (conflictHandler.lockRowsForConflicts()) {
                    locksBuilder.add(AtlasRowLockDescriptor.of(table.getQualifiedName(), cell.getRowName()));
                }
            });
        });
        return locksBuilder.build();
    }

    private static List<Cell> generateCells(int numberOfRowsPerTable, int numberOfCellsPerRow) {
        return IntStream.range(0, numberOfRowsPerTable * numberOfCellsPerRow)
                .mapToObj(i ->
                        Cell.create(PtBytes.toBytes("row" + (i % numberOfCellsPerRow)), PtBytes.toBytes("column" + i)))
                .collect(Collectors.toUnmodifiableList());
    }

    static class VerifyingKeyValueServiceDelegate implements AutoDelegate_TransactionKeyValueService {
        private final TransactionKeyValueService delegate;
        private final PathTypeTracker pathTypeTracker;

        VerifyingKeyValueServiceDelegate(TransactionKeyValueService keyValueService, PathTypeTracker pathTypeTracker) {
            this.delegate = keyValueService;
            this.pathTypeTracker = pathTypeTracker;
        }

        @Override
        public TransactionKeyValueService delegate() {
            return delegate;
        }

        @Override
        public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
            pathTypeTracker.checkNotInAsync();
            return AtlasFutures.getUnchecked(delegate.getAsync(tableRef, timestampByCell));
        }

        @Override
        public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
            pathTypeTracker.expectedToBeInAsync();
            return delegate.getAsync(tableRef, timestampByCell);
        }
    }
}
