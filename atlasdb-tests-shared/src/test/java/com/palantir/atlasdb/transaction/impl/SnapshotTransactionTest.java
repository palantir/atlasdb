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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.api.watch.NoOpLockWatchManager;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.ImmutableGetRangesQuery;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionCommitFailedException;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionFailedNonRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutNonRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.metrics.DefaultMetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.impl.metrics.TableLevelMetricsController;
import com.palantir.atlasdb.transaction.impl.metrics.ToplistDeltaFilteringTableLevelMetricsController;
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
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.timestamp.TimestampService;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
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
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.Matchers;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

@SuppressWarnings("checkstyle:all")
@RunWith(Parameterized.class)
public class SnapshotTransactionTest extends AtlasDbTestCase {
    private static final String SYNC = "sync";
    private static final String ASYNC = "async";

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {
            {SYNC, WrapperWithTracker.TRANSACTION_NO_OP, WrapperWithTracker.KEY_VALUE_SERVICE_NO_OP},
            {
                ASYNC,
                (WrapperWithTracker<Transaction>) GetAsyncDelegate::new,
                (WrapperWithTracker<KeyValueService>) VerifyingKeyValueServiceDelegate::new
            }
        };
        return Arrays.asList(data);
    }

    private final String name;
    private final WrapperWithTracker<Transaction> transactionWrapper;
    private final WrapperWithTracker<KeyValueService> keyValueServiceWrapper;
    private final Map<String, ExpectationFactory> expectationsMapping =
            ImmutableMap.<String, ExpectationFactory>builder()
                    .put(SYNC, SnapshotTransactionTest.this::syncGetExpectation)
                    .put(ASYNC, SnapshotTransactionTest.this::asyncGetExpectation)
                    .build();
    private final TimestampCache timestampCache = new DefaultTimestampCache(
            metricsManager.getRegistry(), () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE);
    private final ExecutorService getRangesExecutor = Executors.newFixedThreadPool(8);
    private final int defaultGetRangesConcurrency = 2;
    private final TransactionOutcomeMetrics transactionOutcomeMetrics =
            TransactionOutcomeMetrics.create(metricsManager);
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

    public SnapshotTransactionTest(
            String name,
            WrapperWithTracker<Transaction> transactionWrapper,
            WrapperWithTracker<KeyValueService> keyValueServiceWrapper) {
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

    private static final byte[] ROW_FOO = "foo".getBytes();
    private static final byte[] ROW_BAR = "bar".getBytes();
    private static final byte[] COL_A = "a".getBytes();
    private static final byte[] COL_B = "b".getBytes();

    private static final Cell TEST_CELL = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
    private static final Cell TEST_CELL_2 = Cell.create(PtBytes.toBytes("row2"), PtBytes.toBytes("column2"));

    @Override
    @Before
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
    }

    @Override
    protected TestTransactionManager constructTestTransactionManager() {
        return new TestTransactionManagerImpl(
                metricsManager,
                keyValueService,
                timestampService,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                timestampCache,
                sweepQueue,
                MoreExecutors.newDirectExecutorService(),
                transactionWrapper,
                keyValueServiceWrapper);
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
                        conflicts -> assertThat(conflicts).is(new HamcrestCondition<>(greaterThanOrEqualTo(1L))));
    }

    @Test
    public void testConcurrentWriteIgnoreConflicts() throws InterruptedException, ExecutionException {
        overrideConflictHandlerForTable(TABLE, ConflictHandler.IGNORE_ALL);
        long val = concurrentlyIncrementValueThousandTimesAndGet();
        assertThat(val).is(new HamcrestCondition<>(Matchers.lessThan(1000L)));
    }

    @Test
    public void testImmutableTs() throws Exception {
        final long firstTs = timestampService.getFreshTimestamp();
        long startTs = txManager.runTaskThrowOnConflict(t -> {
            assertThat(firstTs).isLessThan(txManager.getImmutableTimestamp());
            assertThat(txManager.getImmutableTimestamp()).isLessThan(t.getTimestamp());
            assertThat(t.getTimestamp()).isLessThan(timestampService.getFreshTimestamp());
            return t.getTimestamp();
        });
        assertThat(firstTs).isLessThan(txManager.getImmutableTimestamp());
        assertThat(startTs).isLessThan(txManager.getImmutableTimestamp());
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
        Transaction snapshot = transactionWrapper.apply(
                new SnapshotTransaction(
                        metricsManager,
                        keyValueServiceWrapper.apply(kvMock, pathTypeTracker),
                        new LegacyTimelockService(timestampService, lock, lockClient),
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
                        MoreExecutors.newDirectExecutorService(),
                        true,
                        () -> transactionConfig,
                        ConflictTracer.NO_OP,
                        tableLevelMetricsController),
                pathTypeTracker);
        assertThatThrownBy(() -> snapshot.get(TABLE, ImmutableSet.of(cell))).isInstanceOf(RuntimeException.class);

        mockery.assertIsSatisfied();
    }

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
                timestampService,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                timestampCache,
                sweepQueue,
                MoreExecutors.newDirectExecutorService(),
                transactionWrapper,
                keyValueServiceWrapper);

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
        assertThat(rowResult1.getColumns())
                .is(new HamcrestCondition<>(hasEntry(row1Column1.getColumnName(), row1Column1Value)));
        assertThat(rowResult1.getColumns())
                .is(new HamcrestCondition<>(not(hasEntry(row1Column2.getColumnName(), row1Column2Value))));

        RowResult<byte[]> rowResult2 = snapshotTx
                .getRows(TABLE, ImmutableList.of(row1), ColumnSelection.all())
                .get(row1);
        assertThat(rowResult2.getColumns())
                .is(new HamcrestCondition<>(hasEntry(row1Column1.getColumnName(), row1Column1Value)));
        assertThat(rowResult2.getColumns())
                .is(new HamcrestCondition<>(hasEntry(row1Column2.getColumnName(), row1Column2Value)));
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

    @Test(expected = IllegalArgumentException.class)
    public void disallowPutOnEmptyObject() {
        Transaction t1 = txManager.createNewTransaction();
        t1.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.EMPTY_BYTE_ARRAY));
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
                timestampService,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                timestampCache,
                sweepQueue,
                executor,
                transactionWrapper,
                keyValueServiceWrapper);

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
        PreCommitCondition succeedsCondition = new PreCommitCondition() {
            @Override
            public void throwIfConditionInvalid(long timestamp) {}

            @Override
            public void cleanup() {
                counter.increment();
            }
        };

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
        PreCommitCondition failsCondition = new PreCommitCondition() {
            @Override
            public void throwIfConditionInvalid(long timestamp) {
                throw new TransactionFailedRetriableException("Condition failed");
            }

            @Override
            public void cleanup() {
                counter.increment();
            }
        };

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
    public void getRowsColumnRangesReturnsInOrderInCaseOfAbortedTxns() {
        byte[] row = "foo".getBytes();
        Cell firstCell = Cell.create(row, "a".getBytes());
        Cell secondCell = Cell.create(row, "b".getBytes());
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
        assertThat(cells).isEqualTo(ImmutableList.of(firstCell, secondCell));
    }

    @Test
    public void getOtherRowsColumnRangesReturnsInOrderInCaseOfAbortedTxns() {
        byte[] row = "foo".getBytes();
        Cell firstCell = Cell.create(row, "a".getBytes());
        Cell secondCell = Cell.create(row, "b".getBytes());
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
        assertThat(cells).isEqualTo(ImmutableList.of(firstCell, secondCell));
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
            expectedRows.add(row.getBytes());
            for (int iCell = 0; iCell < numCellsPerRow; iCell++) {
                String cell = String.format("cell%02d", iCell);
                if (iCell < numDeletedCellsPerRow) {
                    expectedDeletedCells.add(Cell.create(row.getBytes(), cell.getBytes()));
                } else {
                    expectedCells.add(Cell.create(row.getBytes(), cell.getBytes()));
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
        Assertions.assertThat(cells).isEqualTo(expectedCells);

        keyValueService.truncateTable(TABLE);
    }

    @Test
    public void commitThrowsIfRolledBackAtCommitTime_expiredLocks() {
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));

        TimelockService timelockService = spy(new LegacyTimelockService(timestampService, lockService, lockClient));

        // expire the locks when the pre-commit check happens - this is guaranteed to be after we've written the data
        PreCommitCondition condition =
                unused -> doReturn(ImmutableSet.of()).when(timelockService).refreshLockLeases(any());

        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();
        long transactionTs = timelockService.getFreshTimestamp();

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
    public void commitThrowsIfRolledBackAtCommitTime_alreadyAborted() {
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));

        TimelockService timelockService = new LegacyTimelockService(timestampService, lockService, lockClient);
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();
        long transactionTs = timelockService.getFreshTimestamp();

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
        TimestampService timestampServiceSpy = spy(timestampService);

        TimelockService timelockService = new LegacyTimelockService(timestampServiceSpy, lockService, lockClient);
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();

        Transaction snapshot =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP);

        when(timestampServiceSpy.getFreshTimestamp()).thenReturn(10000000L);

        // forcing to try to commit a transaction that is already committed
        transactionService.putUnlessExists(transactionTs, timelockService.getFreshTimestamp());

        snapshot.put(TABLE, ImmutableMap.of(cell, PtBytes.toBytes("value")));
        snapshot.commit();

        timelockService.unlock(Collections.singleton(res.getLock()));
    }

    @Test
    public void validateLocksOnReadsIfThoroughlySwept() {
        TimelockService timelockService = new LegacyTimelockService(timestampService, lockService, lockClient);
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();

        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        timelockService.unlock(ImmutableSet.of(res.getLock()));

        assertThatExceptionOfType(TransactionLockTimeoutException.class)
                .isThrownBy(() -> transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL)));
    }

    @Test
    public void validateLocksOnlyOnCommitIfValidationFlagIsFalse() {
        TimelockService timelockService = new LegacyTimelockService(timestampService, lockService, lockClient);
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();

        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, false);

        timelockService.unlock(ImmutableSet.of(res.getLock()));
        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));

        assertThatExceptionOfType(TransactionLockTimeoutException.class).isThrownBy(transaction::commit);
    }

    @Test
    public void checkImmutableTsLockOnceIfThoroughlySwept_WithValidationOnReads() {
        TimelockService timelockService = spy(new LegacyTimelockService(timestampService, lockService, lockClient));
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();

        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));
        transaction.commit();
        timelockService.unlock(ImmutableSet.of(res.getLock()));

        verify(timelockService).refreshLockLeases(ImmutableSet.of(res.getLock()));
    }

    @Test
    public void checkImmutableTsLockOnceIfThoroughlySwept_WithoutValidationOnReads() {
        TimelockService timelockService = spy(new LegacyTimelockService(timestampService, lockService, lockClient));
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();

        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, false);

        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));
        transaction.commit();
        timelockService.unlock(ImmutableSet.of(res.getLock()));

        verify(timelockService).refreshLockLeases(ImmutableSet.of(res.getLock()));
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
        TimelockService timelockService = spy(new LegacyTimelockService(timestampService, lockService, lockClient));
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();

        setTransactionConfig(ImmutableTransactionConfig.builder()
                .lockImmutableTsOnReadOnlyTransactions(true)
                .build());

        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        transaction.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL));
        verify(timelockService).refreshLockLeases(ImmutableSet.of(res.getLock()));

        transaction.commit();
        timelockService.unlock(ImmutableSet.of(res.getLock()));
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
    public void getSortedColumnsThrowsIfLockIsLost() {
        List<Cell> cells = ImmutableList.of(Cell.create(ROW_FOO, COL_A));
        putCellsInTable(cells, TABLE_SWEPT_THOROUGH);

        TimelockService timelockService = new LegacyTimelockService(timestampService, lockService, lockClient);
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();
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

        TimelockService timelockService = new LegacyTimelockService(timestampService, lockService, lockClient);
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();

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
        List<Cell> entries =
                getSortedEntries(TABLE, rows, BatchColumnRangeSelection.create(COL_A, "az".getBytes(), 1000));
        Assertions.assertThat(entries).containsExactlyElementsOf(colA_cells);

        List<Cell> outOfRangeEntries =
                getSortedEntries(TABLE, rows, BatchColumnRangeSelection.create("y".getBytes(), "z".getBytes(), 1000));
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
    public void callbacksOnlyRunOnceEvenOnMultipleCommits() {
        Transaction transaction = txManager.createNewTransaction();
        Runnable callback = mock(Runnable.class);
        transaction.onSuccess(callback);
        transaction.commit();
        transaction.commit();

        verify(callback, times(1)).run();
    }

    @Test
    public void transactionStillCommittedEvenIfCallbackThrows() {
        assertThatThrownBy(() -> txManager.runTaskThrowOnConflict(txn -> {
                    txn.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("tom")));
                    txn.onSuccess(() -> {
                        throw new RuntimeException("boom");
                    });
                    return null;
                }))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("boom");
        txManager.runTaskReadOnly(txn -> {
            assertThat(txn.get(TABLE, ImmutableSet.of(TEST_CELL)))
                    .containsExactly(Maps.immutableEntry(TEST_CELL, PtBytes.toBytes("tom")));
            return null;
        });
    }

    @Test
    public void cannotUseCompletedFutureAfterTransactionCommit() throws Exception {
        ListenableFuture<Map<Cell, byte[]>> getFuture = txManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("jolyon")));
            ListenableFuture<Map<Cell, byte[]>> async = txn.getAsync(TABLE, ImmutableSet.of(TEST_CELL));
            async.get();
            return async;
        });
        // Although the future is done before the transaction ended, this is still a risky pattern, so we prefer to
        // not allow it.
        assertThatThrownBy(getFuture::get)
                .isInstanceOf(TransactionConflictException.class);
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

    private void verifyPrefetchValidations(
            List<byte[]> rows,
            List<Cell> cells,
            int batchHint,
            int expectedNumberOfInvocations,
            int numElementsToBeAccessed) {
        TimelockService timelockService = spy(new LegacyTimelockService(timestampService, lockService, lockClient));
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res = timelockService.lockImmutableTimestamp();
        Transaction transaction =
                getSnapshotTransactionWith(timelockService, () -> transactionTs, res, PreCommitConditions.NO_OP, true);

        Iterator<Map.Entry<Cell, byte[]>> sortedColumns = transaction.getSortedColumns(
                TABLE_SWEPT_THOROUGH,
                rows,
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, batchHint));
        List<Cell> entries = IntStream.range(0, numElementsToBeAccessed)
                .mapToObj(_unused -> sortedColumns.next().getKey())
                .collect(Collectors.toList());
        Assertions.assertThat(entries).containsExactlyElementsOf(cells.subList(0, numElementsToBeAccessed));
        verify(timelockService, times(expectedNumberOfInvocations)).refreshLockLeases(ImmutableSet.of(res.getLock()));
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
        TimelockService timelockService = spy(new LegacyTimelockService(timestampService, lockService, lockClient));
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
            TimelockService timelockService,
            Supplier<Long> startTs,
            LockImmutableTimestampResponse lockImmutableTimestampResponse,
            PreCommitCondition preCommitCondition) {
        return getSnapshotTransactionWith(
                timelockService, startTs, lockImmutableTimestampResponse, preCommitCondition, true);
    }

    private Transaction getSnapshotTransactionWith(
            TimelockService timelockService,
            Supplier<Long> startTs,
            LockImmutableTimestampResponse lockImmutableTimestampResponse,
            PreCommitCondition preCommitCondition,
            boolean validateLocksOnReads) {
        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        return transactionWrapper.apply(
                new SnapshotTransaction(
                        metricsManager,
                        keyValueServiceWrapper.apply(keyValueService, pathTypeTracker),
                        timelockService,
                        NoOpLockWatchManager.create(),
                        transactionService,
                        NoOpCleaner.INSTANCE,
                        startTs,
                        TestConflictDetectionManagers.createWithStaticConflictDetection(
                                ImmutableMap.of(TABLE, ConflictHandler.RETRY_ON_WRITE_WRITE)),
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
                        MoreExecutors.newDirectExecutorService(),
                        validateLocksOnReads,
                        () -> transactionConfig,
                        ConflictTracer.NO_OP,
                        tableLevelMetricsController),
                pathTypeTracker);
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
                LockCollections.of(builder.build()),
                lockTimeout,
                versionId,
                "Dummy thread");
    }

    private long concurrentlyIncrementValueThousandTimesAndGet() throws InterruptedException, ExecutionException {
        CompletionService<Void> executor = new ExecutorCompletionService<Void>(PTExecutors.newFixedThreadPool(8));
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

    private static class VerifyingKeyValueServiceDelegate extends ForwardingKeyValueService {
        private final KeyValueService delegate;
        private final PathTypeTracker pathTypeTracker;

        VerifyingKeyValueServiceDelegate(KeyValueService keyValueService, PathTypeTracker pathTypeTracker) {
            this.delegate = keyValueService;
            this.pathTypeTracker = pathTypeTracker;
        }

        @Override
        public KeyValueService delegate() {
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
