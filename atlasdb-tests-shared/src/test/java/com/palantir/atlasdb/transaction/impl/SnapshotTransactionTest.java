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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.protos.generated.TransactionService;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionCommitFailedException;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionFailedNonRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.logging.CommitProfileProcessor;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetrics;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetricsAssert;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.concurrent.PTExecutors;

@SuppressWarnings("checkstyle:all")
public class SnapshotTransactionTest extends AtlasDbTestCase {
    protected final TimestampCache timestampCache = new TimestampCache(
            metricsManager.getRegistry(), () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE);
    protected final ExecutorService getRangesExecutor = Executors.newFixedThreadPool(8);
    protected final int defaultGetRangesConcurrency = 2;
    protected final TransactionOutcomeMetrics transactionOutcomeMetrics
            = TransactionOutcomeMetrics.create(metricsManager);

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

    static final TableReference TABLE_SWEPT_THOROUGH = TableReference.createFromFullyQualifiedName("default.table2");

    private static final Cell TEST_CELL = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        keyValueService.createTable(TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(TABLE1, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(TABLE2, AtlasDbConstants.GENERIC_TABLE_METADATA);
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
    }

    @Test
    public void testImmutableTs() throws Exception {
        final long firstTs = james.getFreshTimestamp().getTimestamp();
        long startTs = txManager.runTaskThrowOnConflict(t -> {
            Assert.assertTrue(firstTs <= txManager.getImmutableTimestamp());
            Assert.assertTrue(txManager.getImmutableTimestamp() < t.getTimestamp());
            Assert.assertTrue(t.getTimestamp() < james.getFreshTimestamp().getTimestamp());
            return t.getTimestamp();
        });
        Assert.assertTrue(firstTs < txManager.getImmutableTimestamp());
        Assert.assertTrue(startTs < txManager.getImmutableTimestamp());
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
                metricsManager,
                unstableKvs,
                james,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                sweepQueue,
                MoreExecutors.newDirectExecutorService());

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
                james,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                sweepQueue,
                executor);

        Supplier<PreCommitCondition> conditionSupplier = mock(Supplier.class);
        when(conditionSupplier.get()).thenReturn(ALWAYS_FAILS_CONDITION)
                .thenReturn(PreCommitConditions.NO_OP);

        deleteTxManager.runTaskWithConditionWithRetry(conditionSupplier, (tx, condition) -> {
            tx.get(TABLE, ImmutableSet.of(TEST_CELL));
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
        assertThatThrownBy(() ->  serializableTxManager.runTaskWithConditionWithRetry(() -> new PreCommitCondition() {
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
    public void commitThrowsIfRolledBackAtCommitTime_alreadyAborted() {
        final Cell cell = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));

        TransactionService.TimestampRange range = james.startTransactions(1);
        long transactionTs = range.getLower();
        SnapshotTransaction snapshot = new SnapshotTransaction(
                metricsManager,
                keyValueService,
                james,
                transactionService,
                NoOpCleaner.INSTANCE,
                transactionTs,
                TestConflictDetectionManagers.createWithStaticConflictDetection(
                        ImmutableMap.of(TABLE, ConflictHandler.RETRY_ON_WRITE_WRITE)),
                SweepStrategyManagers.createDefault(keyValueService),
                range.getImmutable(),
                PreCommitConditions.NO_OP,
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                null,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                false,
                timestampCache,
                10_000L,
                getRangesExecutor,
                defaultGetRangesConcurrency,
                sweepQueue,
                MoreExecutors.newDirectExecutorService(),
                CommitProfileProcessor.createNonLogging(metricsManager));

        //forcing to try to commit a transaction that is already committed
        transactionService.putUnlessExists(transactionTs, TransactionConstants.FAILED_COMMIT_TS);

        snapshot.put(TABLE, ImmutableMap.of(cell, PtBytes.toBytes("value")));

        assertThatExceptionOfType(TransactionCommitFailedException.class).isThrownBy(snapshot::commit);
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
                false,
                0,
                false,
                sweepStrategy,
                false);
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
                txManager.runTaskWithRetry(t -> {
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
    private static SnapshotTransaction unwrapSnapshotTransaction(Transaction cachingTransaction) {
        Transaction unwrapped = ((CachingTransaction) cachingTransaction).delegate();
        return (SnapshotTransaction) unwrapped;
    }

}
