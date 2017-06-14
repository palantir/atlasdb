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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TrackingKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ExpirationStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.PartitionStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.proxy.MultiDelegateProxy;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockCollections;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.TimeDuration;
import com.palantir.remoting2.tracing.Tracers;

@SuppressWarnings("checkstyle:all")
public class SnapshotTransactionTest extends AtlasDbTestCase {
    protected final TimestampCache timestampCache = TimestampCache.create();

    private class UnstableKeyValueService extends ForwardingKeyValueService {
        private final KeyValueService delegate;
        private final Random random;

        private boolean randomlyThrow = false;
        private boolean randomlyHang = false;

        public UnstableKeyValueService(KeyValueService keyValueService, Random random) {
            this.delegate = keyValueService;
            this.random = random;
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
            super.put(tableRef, values, timestamp);
        }

        public void setRandomlyHang(boolean randomlyHang) {
            this.randomlyHang = randomlyHang;
        }

        public void setRandomlyThrow(boolean randomlyThrow) {
            this.randomlyThrow = randomlyThrow;
        }

        @Override
        protected KeyValueService delegate() {
            return delegate;
        }
    }
    static final TableReference TABLE = TableReference.createFromFullyQualifiedName("default.table");
    static final TableReference TABLE1 = TableReference.createFromFullyQualifiedName("default.table1");
    static final TableReference TABLE2 = TableReference.createFromFullyQualifiedName("default.table2");

    static final TableReference TABLE_SWEPT_THOROUGH = TableReference.createFromFullyQualifiedName("default.table2");

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Some KV stores need more nodes to be up to accomplish a delete, so we model that here as throwing
        keyValueService = new TrackingKeyValueService(keyValueService) {
            @Override
            public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
                throw new RuntimeException("cannot delete");
            }
        };
        keyValueService.createTable(TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(TABLE1, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(TABLE2, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void testConcurrentWriteChangedConflicts() throws InterruptedException, ExecutionException {
        overrideConflictHandlerForTable(TABLE, ConflictHandler.RETRY_ON_VALUE_CHANGED);
        CompletionService<Void> executor = new ExecutorCompletionService<Void>(
                Tracers.wrap(PTExecutors.newFixedThreadPool(8)));
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        Transaction t1 = txManager.createNewTransaction();
        t1.put(TABLE, ImmutableMap.of(cell, EncodingUtils.encodeVarLong(0L)));
        t1.commit();
        for (int i = 0 ; i < 1000 ; i++) {
            executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    txManager.runTaskWithRetry(new TxTask() {
                        @Override
                        public Void execute(Transaction t) throws RuntimeException {
                            long prev = EncodingUtils.decodeVarLong(t.get(TABLE, ImmutableSet.of(cell)).values().iterator().next());
                            t.put(TABLE, ImmutableMap.of(cell, EncodingUtils.encodeVarLong(prev+1)));
                            return null;
                        }
                    });
                    return null;
                }
            });
        }
        for (int i = 0; i < 1000 ; i++) {
            Future<Void> future = executor.take();
            future.get();
        }
        t1 = txManager.createNewTransaction();
        long val = EncodingUtils.decodeVarLong(t1.get(TABLE, ImmutableSet.of(cell)).values().iterator().next());
        assertEquals(1000, val);
    }

    @Test
    public void testConcurrentWriteWriteConflicts() throws InterruptedException, ExecutionException {
        CompletionService<Void> executor = new ExecutorCompletionService<Void>(
                Tracers.wrap(PTExecutors.newFixedThreadPool(8)));
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        Transaction t1 = txManager.createNewTransaction();
        t1.put(TABLE, ImmutableMap.of(cell, EncodingUtils.encodeVarLong(0L)));
        t1.commit();
        for (int i = 0 ; i < 1000 ; i++) {
            executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    txManager.runTaskWithRetry(new TxTask() {
                        @Override
                        public Void execute(Transaction t) throws RuntimeException {
                            long prev = EncodingUtils.decodeVarLong(t.get(TABLE, ImmutableSet.of(cell)).values().iterator().next());
                            t.put(TABLE, ImmutableMap.of(cell, EncodingUtils.encodeVarLong(prev+1)));
                            return null;
                        }
                    });
                    return null;
                }
            });
        }
        for (int i = 0; i < 1000 ; i++) {
            Future<Void> future = executor.take();
            future.get();
        }
        t1 = txManager.createNewTransaction();
        long val = EncodingUtils.decodeVarLong(t1.get(TABLE, ImmutableSet.of(cell)).values().iterator().next());
        assertEquals(1000, val);
    }

    @Test
    public void testImmutableTs() throws Exception {
        final long firstTs = timestampService.getFreshTimestamp();
        long startTs = txManager.runTaskThrowOnConflict(new TransactionTask<Long, RuntimeException>() {
            @Override
            public Long execute(Transaction t) throws RuntimeException {
                Assert.assertTrue(firstTs < txManager.getImmutableTimestamp());
                Assert.assertTrue(txManager.getImmutableTimestamp() < t.getTimestamp());
                Assert.assertTrue(t.getTimestamp() < timestampService.getFreshTimestamp());
                return t.getTimestamp();
            }
        });
        Assert.assertTrue(firstTs < txManager.getImmutableTimestamp());
        Assert.assertTrue(startTs < txManager.getImmutableTimestamp());
    }

    // If lock happens concurrent with get, we aren't sure that we can rollback the transaction
    @Test
    public void testLockAfterGet() throws Exception {
        byte[] rowName = PtBytes.toBytes("1");
        Mockery m = new Mockery();
        final KeyValueService kvMock = m.mock(KeyValueService.class);
        final LockService lockMock = m.mock(LockService.class);
        LockService lock = MultiDelegateProxy.newProxyInstance(LockService.class, lockService, lockMock);

        final Cell cell = Cell.create(rowName, rowName);
        timestampService.getFreshTimestamp();
        final long startTs = timestampService.getFreshTimestamp();
        final long transactionTs = timestampService.getFreshTimestamp();
        keyValueService.put(TABLE, ImmutableMap.of(cell, PtBytes.EMPTY_BYTE_ARRAY), startTs);

        m.checking(new Expectations() {{
            oneOf(kvMock).get(TABLE, ImmutableMap.of(cell, transactionTs)); will(throwException(new RuntimeException()));
            never(lockMock).lockWithFullLockResponse(with(LockClient.ANONYMOUS), with(any(LockRequest.class)));
        }});

        SnapshotTransaction snapshot = new SnapshotTransaction(
                kvMock,
                lock,
                timestampService,
                transactionService,
                NoOpCleaner.INSTANCE,
                transactionTs,
                TestConflictDetectionManagers.createWithStaticConflictDetection(
                        ImmutableMap.of(TABLE, ConflictHandler.RETRY_ON_WRITE_WRITE)),
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                timestampCache);
        try {
            snapshot.get(TABLE, ImmutableSet.of(cell));
            fail();
        } catch (RuntimeException e) {
            //expected
        }

        m.assertIsSatisfied();
    }

    @Ignore("Until we know what we want to do with GC this will be ignored.")
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
        keyValueService.put(TABLE, ImmutableMap.of(cell, PtBytes.EMPTY_BYTE_ARRAY), startTs);

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

        SnapshotTransaction snapshot = new SnapshotTransaction(
                kv,
                lockService,
                timestampService,
                transactionService,
                NoOpCleaner.INSTANCE,
                transactionTs,
                TestConflictDetectionManagers.createWithStaticConflictDetection(
                        ImmutableMap.of(TABLE, ConflictHandler.RETRY_ON_WRITE_WRITE)),
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                timestampCache);
        snapshot.put(TABLE, ImmutableMap.of(cell, PtBytes.EMPTY_BYTE_ARRAY));
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
        final TestTransactionManager unstableTransactionManager = new TestTransactionManagerImpl(
                unstableKvs,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager);

        ScheduledExecutorService service = Tracers.wrap(PTExecutors.newScheduledThreadPool(20));

        for (int i = 0; i < 30; i++) {
            final int threadNumber = i;
            service.schedule(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
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
                    results.batchAccept(1, AbortingVisitors.batching(new AbortingVisitor<RowResult<byte[]>, Exception>() {
                        @Override
                        public boolean visit(RowResult<byte[]> row) throws Exception {
                            byte[] dataBytes = row.getColumns().get(PtBytes.toBytes("data"));
                            BigInteger dataValue = new BigInteger(dataBytes);
                            nextIndex.setValue(Math.max(nextIndex.toInteger(), dataValue.intValue() + 1));
                            return true;
                        }
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
                }
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
                AbortingVisitors.batching(new AbortingVisitor<RowResult<byte[]>, Exception>() {
            @Override
            public boolean visit(RowResult<byte[]> row) throws Exception {
                numRows.increment();
                return true;
            }
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
    public void readsFromThoroughlySweptTableShouldFailWhenLocksAreInvalid() throws Exception {
        keyValueService.createTable(
                TABLE_SWEPT_THOROUGH,
                getTableMetadataForSweepStrategy(SweepStrategy.THOROUGH).persistToBytes());
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
        } catch (TransactionFailedRetriableException expected) {
            return Optional.empty();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private List<Pair<String, LockAwareTransactionTask<Void, Exception>>> getThoroughTableReadTasks() {
        ImmutableList.Builder<Pair<String, LockAwareTransactionTask<Void, Exception>>> tasks = ImmutableList.builder();
        final int batchHint = 1;

        tasks.add(Pair.of("get", (t, heldLocks) -> {
            t.get(TABLE_SWEPT_THOROUGH, ImmutableSet.of(Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"))));
            return null;
        }));

        tasks.add(Pair.of("getRange", (t, heldLocks) -> {
            t.getRange(TABLE_SWEPT_THOROUGH, RangeRequest.all()).batchAccept(batchHint, AbortingVisitors.alwaysTrue());
            return null;
        }));

        tasks.add(Pair.of("getRanges", (t, heldLocks) -> {
            Iterables.getLast(t.getRanges(TABLE_SWEPT_THOROUGH, Collections.singleton(RangeRequest.all())));
            return null;
        }));

        tasks.add(Pair.of("getRows", (t, heldLocks) -> {
            t.getRows(TABLE_SWEPT_THOROUGH, ImmutableSet.of(PtBytes.toBytes("row1")), ColumnSelection.all());
            return null;
        }));

        tasks.add(Pair.of("getRowsColumnRange(TableReference, Iterable<byte[]>, BatchColumnRangeSelection)",
                (t, heldLocks) -> {
                    Collection<BatchingVisitable<Map.Entry<Cell, byte[]>>> results =
                            t.getRowsColumnRange(TABLE_SWEPT_THOROUGH, Collections.singleton(PtBytes.toBytes("row1")),
                                    BatchColumnRangeSelection.create(new ColumnRangeSelection(null, null), batchHint))
                                    .values();
                    results.forEach(result -> result.batchAccept(batchHint, AbortingVisitors.alwaysTrue()));
                    return null;
                }));

        tasks.add(Pair.of("getRowsColumnRange(TableReference, Iterable<byte[]>, ColumnRangeSelection, int)",
                (t, heldLocks) -> {
                    t.getRowsColumnRange(TABLE_SWEPT_THOROUGH, Collections.singleton(PtBytes.toBytes("row1")),
                            new ColumnRangeSelection(null, null), batchHint);
                    return null;
                }));

        tasks.add(Pair.of("getRowsIgnoringLocalWrites",
                (t, heldLocks) -> {
                    SnapshotTransaction snapshotTx = unwrapSnapshotTransaction(t);
                    snapshotTx.getRowsIgnoringLocalWrites(
                            TABLE_SWEPT_THOROUGH,
                            Collections.singleton(PtBytes.toBytes("row1")));
                    return null;
                }));

        tasks.add(Pair.of("getIgnoringLocalWrites",
                (t, heldLocks) -> {
                    SnapshotTransaction snapshotTx = unwrapSnapshotTransaction(t);
                    snapshotTx.getIgnoringLocalWrites(TABLE_SWEPT_THOROUGH,
                            Collections.singleton(Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"))));
                    return null;
                }));

        return tasks.build();
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

    @Test (expected = IllegalArgumentException.class)
    public void disallowPutOnEmptyObject() {
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        Transaction t1 = txManager.createNewTransaction();
        t1.put(TABLE, ImmutableMap.of(cell, PtBytes.EMPTY_BYTE_ARRAY));
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
        Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
        HeldLocksToken expiredLockToken = getExpiredHeldLocksToken();
        try {
            txManager.runTaskWithLocksWithRetry(ImmutableList.of(expiredLockToken), () -> null, (tx, locks) -> {
                tx.put(TABLE, ImmutableMap.of(cell, PtBytes.toBytes("value")));
                return null;
            });
        } catch (TransactionLockTimeoutException e) {
            Set<LockRefreshToken> expectedTokens = ImmutableSet.of(expiredLockToken.getLockRefreshToken());
            assertThat(e.getMessage(), containsString(expectedTokens.toString()));
            assertThat(e.getMessage(), containsString("Retry is not possible."));
        }
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
        return new TableMetadata(
                new NameMetadataDescription(),
                new ColumnMetadataDescription(),
                ConflictHandler.RETRY_ON_WRITE_WRITE,
                CachePriority.WARM,
                PartitionStrategy.ORDERED,
                false,
                0,
                false,
                sweepStrategy,
                ExpirationStrategy.NEVER,
                false);
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


    /**
     * Hack to get reference to underlying {@link SnapshotTransaction}. See how transaction managers are composed at
     * {@link AtlasDbTestCase#setUp()}.
     */
    private static SnapshotTransaction unwrapSnapshotTransaction(Transaction cachingTransaction) {
        Transaction unwrapped = ((CachingTransaction) cachingTransaction).delegate();
        return ((RawTransaction) unwrapped).delegate();
    }

}
