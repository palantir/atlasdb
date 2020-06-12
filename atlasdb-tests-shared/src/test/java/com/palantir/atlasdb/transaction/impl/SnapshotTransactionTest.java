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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
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
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetrics;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetricsAssert;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.proxy.MultiDelegateProxy;
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
import com.palantir.timestamp.TimestampService;

@SuppressWarnings("checkstyle:all")
@RunWith(Parameterized.class)
public class SnapshotTransactionTest extends AtlasDbTestCase {
    private static final String SYNC = "sync";
    private static final String ASYNC = "async";

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {
                {
                    SYNC,
                    WrapperWithTracker.TRANSACTION_NO_OP,
                    WrapperWithTracker.KEY_VALUE_SERVICE_NO_OP
                },
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
    private final Map<String, ExpectationFactory>
            expectationsMapping =
            ImmutableMap.<String, ExpectationFactory>builder()
                    .put(SYNC, SnapshotTransactionTest.this::syncGetExpectation)
                    .put(ASYNC, SnapshotTransactionTest.this::asyncGetExpectation)
                    .build();
    private final TimestampCache timestampCache = new DefaultTimestampCache(
            metricsManager.getRegistry(), () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE);
    private final ExecutorService getRangesExecutor = Executors.newFixedThreadPool(8);
    private final int defaultGetRangesConcurrency = 2;
    private final TransactionOutcomeMetrics transactionOutcomeMetrics
            = TransactionOutcomeMetrics.create(metricsManager);

    private TransactionConfig transactionConfig;

    @FunctionalInterface
    interface ExpectationFactory {

        Expectations apply(
                KeyValueService keyValueService,
                Cell cell,
                long transactionTs,
                LockService lockService) throws InterruptedException;
    }

    private Expectations syncGetExpectation(
            KeyValueService kvMock,
            Cell cell,
            long transactionTs,
            LockService lockMock) {
        return new Expectations() {{
            oneOf(kvMock).get(TABLE, ImmutableMap.of(cell, transactionTs));
            will(throwException(new RuntimeException()));
        }};
    }

    private Expectations asyncGetExpectation(
            KeyValueService kvMock,
            Cell cell,
            long transactionTs,
            LockService lockMock) {
        return new Expectations() {{
            oneOf(kvMock).getAsync(TABLE, ImmutableMap.of(cell, transactionTs));
            will(returnValue(Futures.immediateFailedFuture(new RuntimeException())));
        }};
    }

    public SnapshotTransactionTest(
            String name,
            WrapperWithTracker<Transaction> transactionWrapper,
            WrapperWithTracker<KeyValueService> keyValueServiceWrapper) {
        this.name = name;
        this.transactionWrapper = transactionWrapper;
        this.keyValueServiceWrapper = keyValueServiceWrapper;
    }
    private class UnstableKeyValueService implements AutoDelegate_KeyValueService {

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

    private static final Cell TEST_CELL = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));

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
                getTableMetadataForSweepStrategy(SweepStrategy.THOROUGH_MIGRATION).persistToBytes());
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
        assertEquals(1000, val);
    }

    @Test
    public void testConcurrentWriteWriteConflicts() throws InterruptedException, ExecutionException {
        long val = concurrentlyIncrementValueThousandTimesAndGet();
        assertEquals(1000, val);
        TransactionOutcomeMetricsAssert.assertThat(transactionOutcomeMetrics)
                .hasPlaceholderWriteWriteConflictsSatisfying(conflicts ->
                        assertThat(conflicts, greaterThanOrEqualTo(1L)));
    }

    @Test
    public void testConcurrentWriteIgnoreConflicts() throws InterruptedException, ExecutionException {
        overrideConflictHandlerForTable(TABLE, ConflictHandler.IGNORE_ALL);
        long val = concurrentlyIncrementValueThousandTimesAndGet();
        assertThat(val, Matchers.lessThan(1000L));
    }

    @Test
    public void testImmutableTs() throws Exception {
        final long firstTs = timestampService.getFreshTimestamp();
        long startTs = txManager.runTaskThrowOnConflict(t -> {
            Assert.assertTrue(firstTs < txManager.getImmutableTimestamp());
            Assert.assertTrue(txManager.getImmutableTimestamp() < t.getTimestamp());
            Assert.assertTrue(t.getTimestamp() < timestampService.getFreshTimestamp());
            return t.getTimestamp();
        });
        Assert.assertTrue(firstTs < txManager.getImmutableTimestamp());
        Assert.assertTrue(startTs < txManager.getImmutableTimestamp());
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
        mockery.checking(new Expectations() {{
            never(lockMock).lockWithFullLockResponse(with(LockClient.ANONYMOUS), with(any(LockRequest.class)));
        }});

        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        Transaction snapshot = transactionWrapper.apply(
                new SnapshotTransaction(
                        metricsManager,
                        keyValueServiceWrapper.apply(kvMock, pathTypeTracker),
                        new LegacyTimelockService(timestampService, lock, lockClient),
                        NoOpLockWatchManager.INSTANCE,
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
                        ConflictTracer.NO_OP),
                pathTypeTracker);
        try {
            snapshot.get(TABLE, ImmutableSet.of(cell));
            fail();
        } catch (RuntimeException e) {
            //expected
        }

        mockery.assertIsSatisfied();
    }

    @Ignore("Was ignored long ago, and now we need to fix the mocking logic.")
    // This tests that uncommitted values are deleted and cleaned up
    @SuppressWarnings("unchecked")
    @Test
    public void testPutCleanup() throws Exception {
        byte[] rowName = PtBytes.toBytes("1");
        Mockery m = new Mockery();
        final KeyValueService kvMock = m.mock(KeyValueService.class);
        KeyValueService kv = MultiDelegateProxy.newProxyInstance(KeyValueService.class, keyValueService, kvMock);

        final Cell cell = Cell.create(rowName, rowName);
        timestampService.getFreshTimestamp();
        final long startTs = timestampService.getFreshTimestamp();
        final long transactionTs = timestampService.getFreshTimestamp();
        keyValueService.put(TABLE, ImmutableMap.of(cell, rowName), startTs);

        final Sequence seq = m.sequence("seq");
        m.checking(new Expectations() {{
            oneOf(kvMock).getLatestTimestamps(TABLE, ImmutableMap.of(cell, Long.MAX_VALUE));
            inSequence(seq);
            oneOf(kvMock).get(with(TransactionConstants.TRANSACTION_TABLE), with(any(Map.class)));
            inSequence(seq);
            oneOf(kvMock).putUnlessExists(with(TransactionConstants.TRANSACTION_TABLE), with(any(Map.class)));
            inSequence(seq);
            oneOf(kvMock).delete(TABLE, Multimaps.forMap(ImmutableMap.of(cell, startTs)));
            inSequence(seq);
            oneOf(kvMock).getLatestTimestamps(TABLE, ImmutableMap.of(cell, startTs));
            inSequence(seq);
            oneOf(kvMock).multiPut(with(any(Map.class)), with(transactionTs));
            inSequence(seq);
            oneOf(kvMock).putUnlessExists(with(TransactionConstants.TRANSACTION_TABLE), with(any(Map.class)));
            inSequence(seq);
        }});

        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        Transaction snapshot = transactionWrapper.apply(
                new SnapshotTransaction(
                        metricsManager,
                        keyValueServiceWrapper.apply(keyValueService, pathTypeTracker),
                        null,
                        null,
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
                        ConflictTracer.NO_OP),
                pathTypeTracker);
        snapshot.delete(TABLE, ImmutableSet.of(cell));
        snapshot.commit();

        m.assertIsSatisfied();
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
            service.schedule((Callable<Void>) () -> {
                if (threadNumber == 10) {
                    unstableKvs.setRandomlyThrow(true);
                }
                if (threadNumber == 20) {
                    unstableKvs.setRandomlyHang(true);
                }

                Transaction transaction = unstableTransactionManager.createNewTransaction();
                BatchingVisitable<RowResult<byte[]>> results =
                        transaction.getRange(tableRef, RangeRequest.builder().build());

                final MutableInt nextIndex = new MutableInt(0);
                results.batchAccept(1, AbortingVisitors.batching(
                        (AbortingVisitor<RowResult<byte[]>, Exception>) row -> {
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
                    transaction.put(tableRef,
                            ImmutableMap.of(cell, BigInteger.valueOf(rowNumber).toByteArray()));
                    Thread.yield();
                }
                transaction.commit();
                return null;
            }, i * 20, TimeUnit.MILLISECONDS);
        }

        service.shutdown();
        service.awaitTermination(1, TimeUnit.SECONDS);

        // Verify each table has a number of rows that's a multiple of 5
        Transaction verifyTransaction = txManager.createNewTransaction();
        BatchingVisitable<RowResult<byte[]>> results =
                verifyTransaction.getRange(tableRef, RangeRequest.builder().build());

        final MutableInt numRows = new MutableInt(0);
        results.batchAccept(1,
                AbortingVisitors.batching((AbortingVisitor<RowResult<byte[]>, Exception>) row -> {
                    numRows.increment();
                    return true;
                }));

        Assert.assertEquals(0, numRows.toInteger() % 5);
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

        List<Transaction> allTransactions = Lists.newArrayList();
        List<List<BigInteger>> writtenValues = Lists.newArrayList();
        for (int i = 0; i < numTransactions; i++) {
            allTransactions.add(txManager.createNewTransaction());
            List<BigInteger> initialValues = Lists.newArrayList();
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
                assertEquals(expectedValue, new BigInteger(storedValue));
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
        snapshotTx.put(TABLE, ImmutableMap.of(
                row1Column1, row1Column1Value,
                row1Column2, row1Column2Value));

        ColumnSelection column1Selection = ColumnSelection.create(ImmutableList.of(row1Column1.getColumnName()));

        // local writes still apply columnSelection
        RowResult<byte[]> rowResult1 = snapshotTx.getRows(TABLE, ImmutableList.of(row1), column1Selection).get(row1);
        assertThat(rowResult1.getColumns(), hasEntry(row1Column1.getColumnName(), row1Column1Value));
        assertThat(rowResult1.getColumns(), not(hasEntry(row1Column2.getColumnName(), row1Column2Value)));

        RowResult<byte[]> rowResult2 = snapshotTx.getRows(TABLE, ImmutableList.of(row1), ColumnSelection.all())
                .get(row1);
        assertThat(rowResult2.getColumns(), hasEntry(row1Column1.getColumnName(), row1Column1Value));
        assertThat(rowResult2.getColumns(), hasEntry(row1Column2.getColumnName(), row1Column2Value));
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
            assertTrue(false);
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
            assertTrue(false);
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
            Assert.fail("Expected read to fail with TransactionFailedRetriableException, but it succeeded for: " +
                    Joiner.on(", ").join(successfulTasks));
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

        tasks.add(Pair.of("getRowsColumnRange(TableReference, Iterable<byte[]>, BatchColumnRangeSelection)",
                (t, heldLocks) -> {
                    Collection<BatchingVisitable<Map.Entry<Cell, byte[]>>> results =
                            t.getRowsColumnRange(thoroughTable, Collections.singleton(PtBytes.toBytes("row1")),
                                    BatchColumnRangeSelection.create(new ColumnRangeSelection(null, null), batchHint))
                                    .values();
                    results.forEach(result -> result.batchAccept(batchHint, AbortingVisitors.alwaysTrue()));
                    return null;
                }));

        tasks.add(Pair.of("getRowsColumnRange(TableReference, Iterable<byte[]>, ColumnRangeSelection, int)",
                (t, heldLocks) -> {
                    Iterators.getLast(
                            t.getRowsColumnRange(thoroughTable, Collections.singleton(PtBytes.toBytes("row1")),
                                    new ColumnRangeSelection(null, null), batchHint));
                    return null;
                }));

        tasks.add(Pair.of("getRowsColumnRangeIterator",
                (t, heldLocks) -> {
                    Collection<Iterator<Map.Entry<Cell, byte[]>>> results =
                            t.getRowsColumnRangeIterator(thoroughTable, Collections.singleton(PtBytes.toBytes("row1")),
                                    BatchColumnRangeSelection.create(new ColumnRangeSelection(null, null), batchHint))
                                    .values();
                    results.forEach(Iterators::getLast);
                    return null;
                }));

        tasks.add(Pair.of("getRowsIgnoringLocalWrites",
                (t, heldLocks) -> {
                    SnapshotTransaction snapshotTx = unwrapSnapshotTransaction(t);
                    snapshotTx.getRowsIgnoringLocalWrites(
                            thoroughTable,
                            Collections.singleton(PtBytes.toBytes("row1")));
                    return null;
                }));

        tasks.add(Pair.of("getIgnoringLocalWrites",
                (t, heldLocks) -> {
                    SnapshotTransaction snapshotTx = unwrapSnapshotTransaction(t);
                    snapshotTx.getIgnoringLocalWrites(thoroughTable,
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
            fail();
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
            fail();
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
            fail();
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
            fail();
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
        try {
            t2.commit();
            fail();
        } catch (TransactionConflictException e) {
            // good
        }
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

        assertThat(rowResult, is(notNullValue()));
        assertThat(rowResult.getCellSet(), hasItem(writtenCell));
        assertThat(rowResult.getCellSet(), not(hasItem(emptyCell)));
    }

    @Test
    public void noRetryOnExpiredLockTokens() throws InterruptedException {
        HeldLocksToken expiredLockToken = getExpiredHeldLocksToken();
        try {
            txManager.runTaskWithLocksWithRetry(ImmutableList.of(expiredLockToken), () -> null, (tx, locks) -> {
                tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
                return null;
            });
            fail();
        } catch (TransactionLockTimeoutNonRetriableException e) {
            LockDescriptor descriptor = Iterables.getFirst(expiredLockToken.getLockDescriptors(), null);
            assertThat(e.getMessage(), containsString(descriptor.toString()));
            assertThat(e.getMessage(), containsString("Retry is not possible."));
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
            fail();
        } catch (TransactionFailedRetriableException e) {
            assertThat(e.getMessage(), containsString("Condition failed"));
        }
    }

    @Test
    public void commitWithPreCommitConditionOnRetry() {
        Supplier<PreCommitCondition> conditionSupplier = mock(Supplier.class);
        when(conditionSupplier.get()).thenReturn(ALWAYS_FAILS_CONDITION)
                .thenReturn(PreCommitConditions.NO_OP);

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
        when(conditionSupplier.get()).thenReturn(ALWAYS_FAILS_CONDITION)
                .thenReturn(PreCommitConditions.NO_OP);

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
            fail();
        } catch (TransactionFailedNonRetriableException e) {
            assertThat(e.getMessage(), containsString("Condition failed"));
        }
    }

    @Test
    public void readTransactionSucceedsIfConditionSucceeds() {
        serializableTxManager.runTaskWithConditionReadOnly(PreCommitConditions.NO_OP,
                (tx, condition) -> tx.get(TABLE, ImmutableSet.of(TEST_CELL)));
        TransactionOutcomeMetricsAssert.assertThat(transactionOutcomeMetrics)
                .hasSuccessfulCommits(1);
    }

    @Test
    public void readTransactionFailsIfConditionFails() {
        try {
            serializableTxManager.runTaskWithConditionReadOnly(ALWAYS_FAILS_CONDITION,
                    (tx, condition) -> tx.get(TABLE, ImmutableSet.of(TEST_CELL)));
            fail();
        } catch (TransactionFailedRetriableException e) {
            assertThat(e.getMessage(), containsString("Condition failed"));
        }
        TransactionOutcomeMetricsAssert.assertThat(transactionOutcomeMetrics)
                .hasFailedCommits(1);
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
        assertThat(counter.intValue(), is(1));

        serializableTxManager.runTaskWithConditionReadOnly(succeedsCondition,
                (tx, condition) -> tx.get(TABLE, ImmutableSet.of(TEST_CELL)));
        assertThat(counter.intValue(), is(2));
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

        try {
            serializableTxManager.runTaskWithConditionThrowOnConflict(failsCondition, (tx, condition) -> {
                tx.put(TABLE, ImmutableMap.of(TEST_CELL, PtBytes.toBytes("value")));
                return null;
            });
            fail();
        } catch (TransactionFailedRetriableException e) {
            // expected
        }
        assertThat(counter.intValue(), is(1));

        try {
            serializableTxManager.runTaskWithConditionReadOnly(failsCondition,
                    (tx, condition) -> tx.get(TABLE, ImmutableSet.of(TEST_CELL)));
            fail();
        } catch (TransactionFailedRetriableException e) {
            // expected
        }
        assertThat(counter.intValue(), is(2));
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
        assertThatThrownBy(() -> serializableTxManager.runTaskWithConditionWithRetry(() ->
                new PreCommitCondition() {
                    @Override
                    public void throwIfConditionInvalid(long timestamp) {
                        throw conditionFailure;
                    }

                    @Override
                    public void cleanup() {}
                }, (tx, condition) -> {
            tx.put(TABLE, ImmutableMap.of(firstCell, value));
            return null;
        })).isSameAs(conditionFailure);

        List<Cell> cells = serializableTxManager.runTaskReadOnly(tx ->
                BatchingVisitableView.of(tx.getRowsColumnRange(
                        TABLE,
                        ImmutableList.of(row),
                        BatchColumnRangeSelection.create(null, null, 10)).get(row))
                        .transform(Map.Entry::getKey)
                        .immutableCopy());
        assertEquals(ImmutableList.of(firstCell, secondCell), cells);
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
        assertThatThrownBy(() -> serializableTxManager.runTaskWithConditionWithRetry(() ->
                new PreCommitCondition() {
                    @Override
                    public void throwIfConditionInvalid(long timestamp) {
                        throw conditionFailure;
                    }

                    @Override
                    public void cleanup() {}
                }, (tx, condition) -> {
            tx.put(TABLE, ImmutableMap.of(firstCell, value));
            return null;
        })).isSameAs(conditionFailure);

        List<Cell> cells = serializableTxManager.runTaskReadOnly(tx ->
                Lists.transform(
                        Lists.newArrayList(tx.getRowsColumnRange(
                                TABLE,
                                ImmutableList.of(row),
                                new ColumnRangeSelection(null, null),
                                10)),
                        Map.Entry::getKey));
        assertEquals(ImmutableList.of(firstCell, secondCell), cells);
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
            int numRows,
            int numCellsPerRow,
            int numDeletedCellsPerRow) {
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
        List<Cell> cells = serializableTxManager.runTaskReadOnly(tx ->
                ImmutableList.copyOf(Iterators.transform(
                        tx.getRowsColumnRange(
                                TABLE,
                                shuffledRows,
                                new ColumnRangeSelection(null, null),
                                10),
                        Map.Entry::getKey)));
        expectedCells.sort(Comparator
                .comparing(
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

        LockImmutableTimestampResponse res =
                timelockService.lockImmutableTimestamp();
        long transactionTs = timelockService.getFreshTimestamp();

        Transaction snapshot = getSnapshotTransactionWith(
                timelockService,
                () -> transactionTs,
                res,
                condition);

        //simulate roll back at commit time
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
        LockImmutableTimestampResponse res =
                timelockService.lockImmutableTimestamp();
        long transactionTs = timelockService.getFreshTimestamp();

        Transaction snapshot = getSnapshotTransactionWith(
                timelockService,
                () -> transactionTs,
                res,
                PreCommitConditions.NO_OP);

        //forcing to try to commit a transaction that is already committed
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
        LockImmutableTimestampResponse res =
                timelockService.lockImmutableTimestamp();

        Transaction snapshot = getSnapshotTransactionWith(
                timelockService,
                () -> transactionTs,
                res,
                PreCommitConditions.NO_OP);

        when(timestampServiceSpy.getFreshTimestamp()).thenReturn(10000000L);

        //forcing to try to commit a transaction that is already committed
        transactionService.putUnlessExists(transactionTs, timelockService.getFreshTimestamp());

        snapshot.put(TABLE, ImmutableMap.of(cell, PtBytes.toBytes("value")));
        snapshot.commit();

        timelockService.unlock(Collections.singleton(res.getLock()));
    }

    @Test
    public void validateLocksOnReadsIfThoroughlySwept() {
        TimelockService timelockService = new LegacyTimelockService(timestampService, lockService, lockClient);
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res =
                timelockService.lockImmutableTimestamp();

        Transaction transaction = getSnapshotTransactionWith(
                timelockService,
                () -> transactionTs,
                res,
                PreCommitConditions.NO_OP,
                true);

        timelockService.unlock(ImmutableSet.of(res.getLock()));

        assertThatExceptionOfType(TransactionLockTimeoutException.class).isThrownBy(() ->
                transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL)));
    }

    @Test
    public void validateLocksOnlyOnCommitIfValidationFlagIsFalse() {
        TimelockService timelockService = new LegacyTimelockService(timestampService, lockService, lockClient);
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res =
                timelockService.lockImmutableTimestamp();

        Transaction transaction = getSnapshotTransactionWith(
                timelockService,
                () -> transactionTs,
                res,
                PreCommitConditions.NO_OP,
                false);

        timelockService.unlock(ImmutableSet.of(res.getLock()));
        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));

        assertThatExceptionOfType(TransactionLockTimeoutException.class).isThrownBy(() -> transaction.commit());
    }

    @Test
    public void checkImmutableTsLockOnceIfThoroughlySwept_WithValidationOnReads() {
        TimelockService timelockService = spy(new LegacyTimelockService(timestampService, lockService, lockClient));
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res =
                timelockService.lockImmutableTimestamp();

        Transaction transaction = getSnapshotTransactionWith(
                timelockService,
                () -> transactionTs,
                res,
                PreCommitConditions.NO_OP,
                true);

        transaction.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(TEST_CELL));
        transaction.commit();
        timelockService.unlock(ImmutableSet.of(res.getLock()));

        verify(timelockService).refreshLockLeases(ImmutableSet.of(res.getLock()));
    }

    @Test
    public void checkImmutableTsLockOnceIfThoroughlySwept_WithoutValidationOnReads() {
        TimelockService timelockService = spy(new LegacyTimelockService(timestampService, lockService, lockClient));
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res =
                timelockService.lockImmutableTimestamp();

        Transaction transaction = getSnapshotTransactionWith(
                timelockService,
                () -> transactionTs,
                res,
                PreCommitConditions.NO_OP,
                false);

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
        assertThat(txn.get(TABLE, ImmutableSet.of(cell)), is(ImmutableMap.of()));
    }

    @Test
    public void checkImmutableTsLockAfterReadsForConservativeIfFlagIsSet() {
        TimelockService timelockService = spy(new LegacyTimelockService(timestampService, lockService, lockClient));
        long transactionTs = timelockService.getFreshTimestamp();
        LockImmutableTimestampResponse res =
                timelockService.lockImmutableTimestamp();

        setTransactionConfig(ImmutableTransactionConfig.builder()
                .lockImmutableTsOnReadOnlyTransactions(true)
                .build());

        Transaction transaction = getSnapshotTransactionWith(
                timelockService,
                () -> transactionTs,
                res,
                PreCommitConditions.NO_OP,
                true);

        transaction.get(TABLE_SWEPT_CONSERVATIVE, ImmutableSet.of(TEST_CELL));
        verify(timelockService).refreshLockLeases(ImmutableSet.of(res.getLock()));

        transaction.commit();
        timelockService.unlock(ImmutableSet.of(res.getLock()));

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
                timelockService,
                startTs,
                lockImmutableTimestampResponse,
                preCommitCondition,
                true);
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
                        NoOpLockWatchManager.INSTANCE,
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
                        ConflictTracer.NO_OP),
                pathTypeTracker);
    }

    private void writeCells(TableReference table, ImmutableMap<Cell, byte[]> cellsToWrite) {
        Transaction writeTransaction = txManager.createNewTransaction();
        writeTransaction.put(table, cellsToWrite);
        writeTransaction.commit();
    }

    private RowResult<byte[]> readRow(byte[] defaultRow) {
        Transaction readTransaction = txManager.createNewTransaction();
        SortedMap<byte[], RowResult<byte[]>> allRows = readTransaction.getRows(TABLE, ImmutableSet.of(defaultRow), ColumnSelection.all());
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
        CompletionService<Void> executor = new ExecutorCompletionService<Void>(
                PTExecutors.newFixedThreadPool(8));
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        Transaction t1 = txManager.createNewTransaction();
        t1.put(TABLE, ImmutableMap.of(cell, EncodingUtils.encodeVarLong(0L)));
        t1.commit();
        for (int i = 0; i < 1000; i++) {
            executor.submit(() -> {
                txManager.runTaskWithRetry((TxTask) t -> {
                    long prev = EncodingUtils.decodeVarLong(
                            t.get(TABLE, ImmutableSet.of(cell)).values().iterator().next());
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
        return EncodingUtils.decodeVarLong(t1.get(TABLE, ImmutableSet.of(cell)).values().iterator().next());
    }

    /**
     * Hack to get reference to underlying {@link SnapshotTransaction}. See how transaction managers are composed at
     * {@link AtlasDbTestCase#setUp()}.
     */
    private static SnapshotTransaction unwrapSnapshotTransaction(Transaction transaction) {
        if (transaction instanceof ForwardingTransaction)
            return unwrapSnapshotTransaction(((ForwardingTransaction) transaction).delegate());
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
