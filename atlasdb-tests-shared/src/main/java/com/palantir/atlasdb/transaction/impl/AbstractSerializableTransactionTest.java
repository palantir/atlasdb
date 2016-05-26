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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionSerializableConflictException;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockRefreshToken;


public abstract class AbstractSerializableTransactionTest extends AbstractTransactionTest {

    @Override
    protected TransactionManager getManager() {
        return new SerializableTransactionManager(
                keyValueService,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictDetectionManager,
                SweepStrategyManagers.createDefault(keyValueService),
                NoOpCleaner.INSTANCE);
    }

    @Override
    protected Transaction startTransaction() {
        ImmutableMap<TableReference, ConflictHandler> tablesToWriteWrite = ImmutableMap.of(
                TEST_TABLE,
                ConflictHandler.SERIALIZABLE,
                TransactionConstants.TRANSACTION_TABLE,
                ConflictHandler.IGNORE_ALL);
        return new SerializableTransaction(
                keyValueService,
                lockService,
                timestampService,
                transactionService,
                NoOpCleaner.INSTANCE,
                Suppliers.ofInstance(timestampService.getFreshTimestamp()),
                ConflictDetectionManagers.fromMap(tablesToWriteWrite),
                SweepStrategyManagers.createDefault(keyValueService),
                0L,
                ImmutableList.<LockRefreshToken>of(),
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                null,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                true) {
            @Override
            protected Map<Cell, byte[]> transformGetsForTesting(Map<Cell, byte[]> map) {
                return Maps.transformValues(map, new Function<byte[], byte[]>() {
                    @Override
                    public byte[] apply(byte[] input) {
                        return input.clone();
                    }
                });
            }
        };
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
            // this is expectecd to throw because it is a write skew
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

    @Test(expected=TransactionFailedRetriableException.class)
    public void testConcurrentWriteSkew() throws InterruptedException, BrokenBarrierException {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "100");
        put(t0, "row2", "col1", "100");
        t0.commit();

        final CyclicBarrier barrier = new CyclicBarrier(2);

        final Transaction t1 = startTransaction();
        ExecutorService exec = PTExecutors.newCachedThreadPool();
        Future<?> f = exec.submit( new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                withdrawMoney(t1, true, false);
                barrier.await();
                t1.commit();
                return null;
            }
        });

        Transaction t2 = startTransaction();
        withdrawMoney(t2, false, false);

        barrier.await();
        t2.commit();
        try {
            f.get();
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

    @Test(expected=TransactionFailedRetriableException.class)
    public void testConcurrentWriteSkewCell() throws InterruptedException, BrokenBarrierException {
        Transaction t0 = startTransaction();
        put(t0, "row1", "col1", "100");
        put(t0, "row2", "col1", "100");
        t0.commit();

        final CyclicBarrier barrier = new CyclicBarrier(2);

        final Transaction t1 = startTransaction();
        ExecutorService exec = PTExecutors.newCachedThreadPool();
        Future<?> f = exec.submit( new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                withdrawMoney(t1, true, true);
                barrier.await();
                t1.commit();
                return null;
            }
        });

        Transaction t2 = startTransaction();
        withdrawMoney(t2, false, true);

        barrier.await();
        t2.commit();
        try {
            f.get();
            fail();
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        }
    }


    private void withdrawMoney(Transaction t, boolean account, boolean isCellGet) {
        long account1 = Long.valueOf(isCellGet ? getCell(t, "row1", "col1") : get(t, "row1", "col1"));
        long account2 = Long.valueOf(isCellGet ? getCell(t, "row2", "col1") : get(t, "row2", "col1"));
        if (account) {
            account1 -= 150;
        } else {
            account2 -= 150;
        }
        Assert.assertTrue(account1 + account2 >= 0);
        if (account) {
            put(t, "row1", "col1", String.valueOf(account1));
        } else {
            put(t, "row2", "col1", String.valueOf(account2));
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
        put(t0, "row1", "col1", initialValue);
        put(t0, "row1", "col2", initialValue);
        put(t0, "row2", "col1", initialValue);
        t0.commit();

        Transaction t1 = startTransaction();
        BatchingVisitables.copyToList(t1.getRange(TEST_TABLE, RangeRequest.builder().retainColumns(ImmutableList.of(PtBytes.toBytes("col1"))).build()));
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
        BatchingVisitables.copyToList(t1.getRange(TEST_TABLE, RangeRequest.builder().retainColumns(ImmutableList.of(PtBytes.toBytes("col1"))).build()));
        BatchingVisitables.copyToList(t1.getRange(TEST_TABLE, RangeRequest.builder().retainColumns(ImmutableList.of(PtBytes.toBytes("col2"))).build()));

        // We need to do at least one put so we don't get caught by the read only code path
        put(t1, "row22", "col2", initialValue);

        t1.commit();
    }

}
