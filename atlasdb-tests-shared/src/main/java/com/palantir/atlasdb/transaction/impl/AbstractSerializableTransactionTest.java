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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.KvsManager;
import com.palantir.atlasdb.keyvalue.impl.TransactionManagerManager;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionSerializableConflictException;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.impl.LegacyTimelockService;

public abstract class AbstractSerializableTransactionTest extends AbstractTransactionTest {

    public AbstractSerializableTransactionTest(KvsManager kvsManager, TransactionManagerManager tmManager) {
        super(kvsManager, tmManager);
    }

    @Override
    protected TransactionManager createManager() {
        MultiTableSweepQueueWriter sweepQueue = getSweepQueueWriterUninitialized();
        SerializableTransactionManager txManager = SerializableTransactionManager.createForTest(
                MetricsManagers.createForTests(),
                keyValueService,
                timestampService,
                timestampManagementService,
                lockClient,
                lockService,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictDetectionManager,
                SweepStrategyManagers.createDefault(keyValueService),
                NoOpCleaner.INSTANCE,
                AbstractTransactionTest.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                sweepQueue);
        sweepQueue.initialize(txManager);
        return txManager;
    }

    @Override
    protected Transaction startTransaction() {
        ImmutableMap<TableReference, ConflictHandler> tablesToWriteWrite = ImmutableMap.of(
                TEST_TABLE,
                ConflictHandler.SERIALIZABLE,
                TransactionConstants.TRANSACTION_TABLE,
                ConflictHandler.IGNORE_ALL);
        return new SerializableTransaction(
                MetricsManagers.createForTests(),
                keyValueService,
                new LegacyTimelockService(timestampService, lockService, lockClient),
                transactionService,
                NoOpCleaner.INSTANCE,
                Suppliers.ofInstance(timestampService.getFreshTimestamp()),
                TestConflictDetectionManagers.createWithStaticConflictDetection(tablesToWriteWrite),
                SweepStrategyManagers.createDefault(keyValueService),
                0L,
                Optional.empty(),
                PreCommitConditions.NO_OP,
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                null,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                true,
                timestampCache,
                AbstractTransactionTest.GET_RANGES_EXECUTOR,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                getSweepQueueWriterInitialized(),
                MoreExecutors.newDirectExecutorService(),
                true,
                () -> ImmutableTransactionConfig.builder().build()) {
            @Override
            protected Map<Cell, byte[]> transformGetsForTesting(Map<Cell, byte[]> map) {
                return Maps.transformValues(map, input -> input.clone());
            }
        };
    }

    protected MultiTableSweepQueueWriter getSweepQueueWriterUninitialized() {
        return MultiTableSweepQueueWriter.NO_OP;
    }

    protected MultiTableSweepQueueWriter getSweepQueueWriterInitialized() {
        return MultiTableSweepQueueWriter.NO_OP;
    }

    @Test
    public void testReadOnlySerializableTransactionsIgnoreReadWriteConflicts() {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "100");
        t0.commit();

        Transaction t1 = startTransaction();
        get(t1, "row1", "col1");

        Transaction t2 = startTransaction();
        put(t2, "row1", "col1", "101");
        t2.commit();

        // Succeeds, even though t1 is serializable, because it's read-only.
        t1.commit();
    }

    @Test
    public void testClassicWriteSkew() {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "100");
        put(t0, "row2", "col1", "100");
        t0.commit();

        Transaction t1 = startTransaction();
        Transaction t2 = startTransaction();
        withdrawMoney(t1, true, false);
        withdrawMoney(t2, false, false);

        t1.commit();
        try {
            t2.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // this is expected to throw because it is a write skew
        }
    }

    @Test
    public void testClassicWriteSkew2() {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "100");
        put(t0, "row2", "col1", "100");
        t0.commit();

        Transaction t1 = startTransaction();
        Transaction t2 = startTransaction();
        withdrawMoney(t1, true, false);
        withdrawMoney(t2, false, false);

        t2.commit();
        try {
            t1.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // this is expectecd to throw because it is a write skew
        }
    }

    @Test(expected = TransactionFailedRetriableException.class)
    public void testConcurrentWriteSkew() throws InterruptedException, BrokenBarrierException {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "100");
        put(t0, "row2", "col1", "100");
        t0.commit();

        final CyclicBarrier barrier = new CyclicBarrier(2);

        final Transaction t1 = startTransaction();
        ExecutorService exec = PTExecutors.newCachedThreadPool();
        Future<?> future = exec.submit((Callable<Void>) () -> {
            withdrawMoney(t1, true, false);
            barrier.await();
            t1.commit();
            return null;
        });

        Transaction t2 = startTransaction();
        withdrawMoney(t2, false, false);

        barrier.await();
        t2.commit();
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        }
    }

    @Test
    public void testClassicWriteSkewCell() {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "100");
        put(t0, "row2", "col1", "100");
        t0.commit();

        Transaction t1 = startTransaction();
        Transaction t2 = startTransaction();
        withdrawMoney(t1, true, true);
        withdrawMoney(t2, false, true);

        t1.commit();
        try {
            t2.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // this is expectecd to throw because it is a write skew
        }
    }

    @Test
    public void testClassicWriteSkew2Cell() {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "100");
        put(t0, "row2", "col1", "100");
        t0.commit();

        Transaction t1 = startTransaction();
        Transaction t2 = startTransaction();
        withdrawMoney(t1, true, true);
        withdrawMoney(t2, false, true);

        t2.commit();
        try {
            t1.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // this is expectecd to throw because it is a write skew
        }
    }

    @Test(expected = TransactionFailedRetriableException.class)
    public void testConcurrentWriteSkewCell() throws InterruptedException, BrokenBarrierException {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "100");
        put(t0, "row2", "col1", "100");
        t0.commit();

        final CyclicBarrier barrier = new CyclicBarrier(2);

        final Transaction t1 = startTransaction();
        ExecutorService exec = PTExecutors.newCachedThreadPool();
        Future<?> future = exec.submit((Callable<Void>) () -> {
            withdrawMoney(t1, true, true);
            barrier.await();
            t1.commit();
            return null;
        });

        Transaction t2 = startTransaction();
        withdrawMoney(t2, false, true);

        barrier.await();
        t2.commit();
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        }
    }


    private void withdrawMoney(Transaction txn, boolean account, boolean isCellGet) {
        long account1 = Long.valueOf(isCellGet ? getCell(txn, "row1", "col1") : get(txn, "row1", "col1"));
        long account2 = Long.valueOf(isCellGet ? getCell(txn, "row2", "col1") : get(txn, "row2", "col1"));
        if (account) {
            account1 -= 150;
        } else {
            account2 -= 150;
        }
        Assert.assertTrue(account1 + account2 >= 0);
        if (account) {
            put(txn, "row1", "col1", String.valueOf(account1));
        } else {
            put(txn, "row2", "col1", String.valueOf(account2));
        }
    }

    @Test
    public void testCycleWithReadOnly() {
        // readOnly has a r/w dep on t2 and t2 has a r/w on t1 and t1 has a w/r dep on readOnly
        // This creates a cycle that is valid under SI, but not SSI
        // The main issue is that readOnly reads an invalid state of the world. because it reads the updated value of
        // t1, but the old value of t2.

        String initialValue = "100";
        String newValue = "101";
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", initialValue);
        put(t0, "row2", "col1", initialValue);
        t0.commit();

        Transaction t1 = startTransaction();
        put(t1, "row1", "col1", newValue);
        Transaction t2 = startTransaction();
        String row1Get = get(t2, "row1", "col1");
        assertEquals(initialValue, row1Get);
        put(t2, "row2", "col1", row1Get);

        t1.commit();
        Transaction readOnly = startTransaction();
        assertEquals(newValue, get(readOnly, "row1", "col1"));
        assertEquals(initialValue, get(readOnly, "row2", "col1"));

        try {
            t2.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // this is expectecd to throw because it is a write skew
        }
    }

    @Test
    public void testLargerCycleWithReadOnly() {
        String initialValue = "100";
        String newValue = "101";
        String newValue2 = "102";
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", initialValue);
        put(t0, "row2", "col1", initialValue);
        t0.commit();

        Transaction t1 = startTransaction();
        put(t1, "row1", "col1", newValue);
        Transaction t2 = startTransaction();
        String row1Get = get(t2, "row1", "col1");
        assertEquals(initialValue, row1Get);
        put(t2, "row2", "col1", row1Get);

        t1.commit();
        Transaction t3 = startTransaction();
        put(t3, "row1", "col1", newValue2);
        t3.commit();
        Transaction readOnly = startTransaction();
        assertEquals(newValue2, get(readOnly, "row1", "col1"));
        assertEquals(initialValue, get(readOnly, "row2", "col1"));

        try {
            t2.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // this is expectecd to throw because it is a write skew
        }
    }

    @Test
    public void testNonPhantomRead() {
        String initialValue = "100";
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", initialValue);
        put(t0, "row2", "col1", initialValue);
        t0.commit();

        Transaction t1 = startTransaction();
        RowResult<byte[]> first = BatchingVisitables.getFirst(t1.getRange(TEST_TABLE, RangeRequest.builder().build()));
        put(t1, "row22", "col1", initialValue);

        Transaction t2 = startTransaction();
        put(t2, "row11", "col1", initialValue);
        t2.commit();

        t1.commit();
    }

    @Test
    public void testPhantomReadFail() {
        String initialValue = "100";
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", initialValue);
        put(t0, "row2", "col1", initialValue);
        t0.commit();

        Transaction t1 = startTransaction();
        RowResult<byte[]> first = BatchingVisitables.getFirst(t1.getRange(TEST_TABLE, RangeRequest.builder().build()));
        put(t1, "row22", "col1", initialValue);

        Transaction t2 = startTransaction();
        put(t2, "row0", "col1", initialValue);
        t2.commit();

        try {
            t1.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // this is expectecd to throw because it is a write skew
        }
    }

    @Test
    public void testPhantomReadFail2() {
        String initialValue = "100";
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", initialValue);
        put(t0, "row2", "col1", initialValue);
        t0.commit();

        Transaction t1 = startTransaction();
        BatchingVisitables.copyToList(t1.getRange(TEST_TABLE, RangeRequest.builder().build()));
        put(t1, "row22", "col1", initialValue);

        Transaction t2 = startTransaction();
        put(t2, "row3", "col1", initialValue);
        t2.commit();

        try {
            t1.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // this is expectecd to throw because it is a write skew
        }
    }

    @Test
    public void testCellReadWriteFailure() {
        String initialValue = "100";
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", initialValue);
        put(t0, "row2", "col1", initialValue);
        t0.commit();

        Transaction t1 = startTransaction();
        BatchingVisitables.copyToList(t1.getRange(TEST_TABLE, RangeRequest.builder().build()));
        put(t1, "row22", "col1", initialValue);

        Transaction t2 = startTransaction();
        put(t2, "row3", "col1", initialValue);
        t2.commit();

        try {
            t1.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // this is expectecd to throw because it is a write skew
        }
    }

    @Test
    public void testCellReadWriteFailure2() {
        String initialValue = "100";
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", initialValue);
        put(t0, "row2", "col1", initialValue);
        t0.commit();

        Transaction t1 = startTransaction();
        BatchingVisitables.copyToList(t1.getRange(TEST_TABLE, RangeRequest.builder().build()));
        put(t1, "row22", "col1", initialValue);

        Transaction t2 = startTransaction();
        put(t2, "row2", "col1", "101");
        t2.commit();

        try {
            t1.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // this is expectecd to throw because it is a write skew
        }
    }

    @Test
    public void testColumnSelection() {
        String initialValue = "100";
        Transaction t0 = startTransaction();
        String firstCol = "col1";
        put(t0, "row1", firstCol, initialValue);
        put(t0, "row1", "col2", initialValue);
        put(t0, "row2", firstCol, initialValue);
        t0.commit();

        Transaction t1 = startTransaction();
        BatchingVisitables.copyToList(getRangeRetainingCol(t1, firstCol));
        get(t1, "row1", "col2");

        // We need to do at least one put so we don't get caught by the read only code path
        put(t1, "row22", "col2", initialValue);

        t1.commit();
    }

    @Test
    public void testColumnSelection2() {
        String initialValue = "100";
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", initialValue);
        put(t0, "row1", "col2", initialValue);
        put(t0, "row2", "col1", initialValue);
        t0.commit();

        Transaction t1 = startTransaction();
        BatchingVisitables.copyToList(getRangeRetainingCol(t1, "col1"));
        BatchingVisitables.copyToList(getRangeRetainingCol(t1, "col2"));

        // We need to do at least one put so we don't get caught by the read only code path
        put(t1, "row22", "col2", initialValue);

        t1.commit();
    }

    private BatchingVisitable<RowResult<byte[]>> getRangeRetainingCol(Transaction txn, String col) {
        return txn.getRange(TEST_TABLE,
                RangeRequest.builder().retainColumns(ImmutableList.of(PtBytes.toBytes(col))).build());
    }

    @Test
    public void testColumnRangeReadUnsupported() {
        Transaction t1 = startTransaction();
        try {
            t1.getRowsColumnRange(TEST_TABLE, ImmutableList.of(PtBytes.toBytes("row1")),
                    new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY), 1);
            fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testColumnRangeReadSupported() {
        Transaction t1 = startTransaction();
        // The transactions table is registered as IGNORE_ALL, so the request is supported
        // Reading at timestamp 0 to avoid any repercussions for in-flight transactions
        t1.getRowsColumnRange(TransactionConstants.TRANSACTION_TABLE,
                ImmutableList.of(ValueType.VAR_LONG.convertFromJava(0L)),
                new ColumnRangeSelection(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY), 1);
    }

    @Test
    public void testColumnRangeReadWriteConflict() {
        byte[] row = PtBytes.toBytes("row1");
        writeColumns();

        Transaction t1 = startTransaction();
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRange =
                t1.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row),
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        // Serializable transaction records only the first column as read.
        Map.Entry<Cell, byte[]> read = BatchingVisitables.getFirst(Iterables.getOnlyElement(columnRange.values()));
        assertEquals(Cell.create(row, PtBytes.toBytes("col0")), read.getKey());
        // Write to avoid the read only path.
        put(t1, "row1_1", "col0", "v0");

        Transaction t2 = startTransaction();
        put(t2, "row1", "col0", "v0_0");
        t2.commit();

        try {
            t1.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // expected
        }
    }

    @Test
    public void testColumnRangeReadWriteConflictOnNewCell() {
        byte[] row = PtBytes.toBytes("row1");
        writeColumns();

        Transaction t1 = startTransaction();
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRange =
                t1.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row),
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        // Serializable transaction records only the first column as read.
        Map.Entry<Cell, byte[]> read = BatchingVisitables.getFirst(Iterables.getOnlyElement(columnRange.values()));
        assertEquals(Cell.create(row, PtBytes.toBytes("col0")), read.getKey());
        // Write to avoid the read only path.
        put(t1, "row1_1", "col0", "v0");

        Transaction t2 = startTransaction();
        // Write on the start of the range.
        put(t2, "row1", "col", "v");
        t2.commit();

        try {
            t1.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // expected
        }
    }

    @Test
    public void testColumnRangeReadWriteNoConflict() {
        byte[] row = PtBytes.toBytes("row1");
        writeColumns();

        Transaction t1 = startTransaction();
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRange =
                t1.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row),
                        BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1));
        // Serializable transaction records only the first column as read.
        Map.Entry<Cell, byte[]> read = BatchingVisitables.getFirst(Iterables.getOnlyElement(columnRange.values()));
        assertEquals(Cell.create(row, PtBytes.toBytes("col0")), read.getKey());
        // Write to avoid the read only path.
        put(t1, "row1_1", "col0", "v0");

        Transaction t2 = startTransaction();
        put(t2, "row1", "col1", "v0_0");
        t2.commit();

        t1.commit();
    }

    @Test
    public void testColumnRangeReadWriteEmptyRange() {
        byte[] row = PtBytes.toBytes("row1");

        Transaction t1 = startTransaction();
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRange =
                t1.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row),
                        BatchColumnRangeSelection.create(PtBytes.toBytes("col"), PtBytes.toBytes("col0"), 1));
        assertNull(BatchingVisitables.getFirst(Iterables.getOnlyElement(columnRange.values())));
        // Write to avoid the read only path.
        put(t1, "row1_1", "col0", "v0");

        Transaction t2 = startTransaction();
        put(t2, "row1", "col", "v0");
        t2.commit();

        try {
            t1.commit();
            fail();
        } catch (TransactionSerializableConflictException e) {
            // expected
        }
    }

    @Test
    public void testColumnRangeReadWriteEmptyRangeUnread() {
        byte[] row = PtBytes.toBytes("row1");

        Transaction t1 = startTransaction();
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRange =
                t1.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row),
                        BatchColumnRangeSelection.create(PtBytes.toBytes("col"), PtBytes.toBytes("col0"), 1));
        // Intentionally not reading anything from the result, so we shouldn't get a conflict.
        // Write to avoid the read only path.
        put(t1, "row1_1", "col0", "v0");

        Transaction t2 = startTransaction();
        put(t2, "row1", "col", "v0");
        t2.commit();

        t1.commit();
    }

    @Test
    public void testMultipleReadsToSameColumnRange() {
        String rowString = "row1";
        byte[] row = PtBytes.toBytes(rowString);
        byte[] rowDifferentReference = PtBytes.toBytes(rowString);

        Transaction t1 = startTransaction();
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRange =
                t1.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row),
                        BatchColumnRangeSelection.create(PtBytes.toBytes("col"), PtBytes.toBytes("col0"), 1));
        columnRange.values().forEach(visitable -> visitable.batchAccept(10, t -> true));
        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRangeAgain =
                t1.getRowsColumnRange(TEST_TABLE, ImmutableList.of(rowDifferentReference),
                        BatchColumnRangeSelection.create(PtBytes.toBytes("col"), PtBytes.toBytes("col0"), 1));
        columnRangeAgain.values().forEach(visitable -> visitable.batchAccept(10, t -> true));
        put(t1, "mutation to ensure", "conflict", "handling");
        t1.commit();
    }

    @Test
    public void testMultipleReadsToSameColumnRangeAcrossRows() {
        byte[] row = PtBytes.toBytes("row");
        byte[] differentRow = PtBytes.toBytes("differentRow");

        Transaction transaction = startTransaction();
        BatchColumnRangeSelection sameColumnRangeSelection =
                BatchColumnRangeSelection.create(PtBytes.toBytes("col"), PtBytes.toBytes("col0"), 1);

        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRangeResultForRow =
                transaction.getRowsColumnRange(TEST_TABLE, ImmutableList.of(row), sameColumnRangeSelection);
        columnRangeResultForRow.values().forEach(visitable -> visitable.batchAccept(10, t -> true));

        Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> columnRangeResultForDifferentRow =
                transaction.getRowsColumnRange(TEST_TABLE, ImmutableList.of(differentRow), sameColumnRangeSelection);
        columnRangeResultForDifferentRow.values().forEach(visitable -> visitable.batchAccept(10, t -> true));
        put(transaction, "mutation to ensure", "conflict", "handling");
        transaction.commit();
    }

    private void writeColumns() {
        Transaction t1 = startTransaction();
        int totalPuts = 101;
        byte[] row = PtBytes.toBytes("row1");
        // Record expected results using byte ordering
        ImmutableSortedMap.Builder<Cell, byte[]> writes = ImmutableSortedMap
                .orderedBy(Ordering.from(UnsignedBytes.lexicographicalComparator()).onResultOf(Cell::getColumnName));
        for (int i = 0; i < totalPuts; i++) {
            put(t1, "row1", "col" + i, "v" + i);
            writes.put(Cell.create(row, PtBytes.toBytes("col" + i)), PtBytes.toBytes("v" + i));
        }
        t1.commit();
    }
}
