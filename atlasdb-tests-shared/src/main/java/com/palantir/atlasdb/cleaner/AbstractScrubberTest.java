/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.cleaner;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public abstract class AbstractScrubberTest {
    private static final TableReference TABLE_NAME = TableReference.createWithEmptyNamespace("scrub_test");
    private static final String COL = "c";
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};
    private KeyValueService kvs;
    private TransactionManager transactionManager;

    @Before
    public void create() throws Exception {
        kvs = getKeyValueService();
        TransactionTables.createTables(kvs);
        kvs.dropTable(TABLE_NAME);
        TableMetadata emptyMetadata = new TableMetadata();
        TableMetadata metadata = new TableMetadata(emptyMetadata.getRowMetadata(), emptyMetadata.getColumns(),
                ConflictHandler.IGNORE_ALL);
        kvs.createTable(TABLE_NAME, metadata.persistToBytes());
    }

    @After
    public void teardown() {
        kvs.dropTable(TABLE_NAME);
        TransactionTables.deleteTables(kvs);
    }

    @Test
    public void testSingleScrub() {
        long putTs = put("r1", "v1");
        long deleteTs = delete("r1");
        Assert.assertEquals("anonymous assert D6A687", ImmutableSet.of(-1L, deleteTs), getAllTs("r1"));
        Assert.assertEquals("anonymous assert 04499B", Value.create(EMPTY_BYTE_ARRAY, -1), getAtTs("r1", putTs));
        Assert.assertEquals("anonymous assert CCF9F3", Value.create(EMPTY_BYTE_ARRAY, deleteTs),
                getAtTs("r1", Long.MAX_VALUE));
    }

    @Test
    public void testSingleScrubNonAggressive() {
        long putTs = put("r1", "v1");
        long deleteTs = deleteNonAggressive("r1");
        Assert.assertEquals("anonymous assert 078C1B", ImmutableSet.of(putTs, deleteTs), getAllTs("r1"));
        Assert.assertEquals("anonymous assert 780E2F", Value.create("v1".getBytes(), putTs), getAtTs("r1", putTs + 1));
        Assert.assertEquals("anonymous assert C37B42", Value.create(EMPTY_BYTE_ARRAY, deleteTs),
                getAtTs("r1", deleteTs + 1));
        Assert.assertEquals("anonymous assert 647BA8", Value.create(EMPTY_BYTE_ARRAY, deleteTs),
                getAtTs("r1", Long.MAX_VALUE));
    }

    @Test
    public void testMultipleScrub() {
        put("r1", "v1");
        long putTs = put("r1", "v2");
        long deleteTs = delete("r1");
        Assert.assertEquals("anonymous assert 576FEA", ImmutableSet.of(-1L, deleteTs), getAllTs("r1"));
        Assert.assertEquals("anonymous assert 9F5029", Value.create(EMPTY_BYTE_ARRAY, -1), getAtTs("r1", putTs));
        Assert.assertEquals("anonymous assert 2D1C9C", Value.create(EMPTY_BYTE_ARRAY, deleteTs),
                getAtTs("r1", Long.MAX_VALUE));
    }

    @Test
    public void testMultipleScrubNonAggressive() {
        long putTs1 = put("r1", "v1");
        long putTs2 = put("r1", "v2");
        long deleteTs = deleteNonAggressive("r1");
        Assert.assertEquals("anonymous assert D72465", ImmutableSet.of(putTs1, putTs2, deleteTs), getAllTs("r1"));
        Assert.assertEquals("anonymous assert 764AF9", Value.create("v1".getBytes(), putTs1),
                getAtTs("r1", putTs1 + 1));
        Assert.assertEquals("anonymous assert 8F0D3C", Value.create("v2".getBytes(), putTs2),
                getAtTs("r1", putTs2 + 1));
        Assert.assertEquals("anonymous assert B89C1C", Value.create(EMPTY_BYTE_ARRAY, deleteTs),
                getAtTs("r1", Long.MAX_VALUE));
    }

    @Test
    public void testPastScrub() {
        put("r1", "v1");
        final CountDownLatch deleteLatch = new CountDownLatch(1);
        final CountDownLatch writeLatch = new CountDownLatch(1);
        ExecutorService exec = PTExecutors.newFixedThreadPool(2);

        long deleteTs = 0L;
        long putTs = 0L;
        try {
            Future<Long> deleteTsFuture = exec.submit(new Callable<Long>() {
                @Override
                public Long call() {
                    return getTxManager().runTaskWithRetry(new TransactionTask<Long, RuntimeException>() {
                        @Override
                        public Long execute(Transaction t) throws RuntimeException {
                            // Delete starts before we write.
                            // The transaction won't actually get a start timestamp until it has to, so force it to by doing a read.
                            t.get(TABLE_NAME, ImmutableSet.of(createCell("r1")));
                            deleteLatch.countDown();
                            t.setTransactionType(Transaction.TransactionType.AGGRESSIVE_HARD_DELETE);
                            t.delete(TABLE_NAME, ImmutableSet.of(createCell("r1")));
                            try {
                                // Wait for the write to commit. Scrub will run after the delete commits.
                                writeLatch.await();
                            } catch (InterruptedException e) {
                                throw Throwables.rewrapAndThrowUncheckedException(e);
                            }
                            return t.getTimestamp();
                        }
                    });
                }
            });

            // Wait for the delete transaction to start.
            try {
                deleteLatch.await();
            } catch (InterruptedException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }

            Future<Long> putTsFuture = exec.submit(new Callable<Long>() {
                @Override
                public Long call() {
                    long putTs = getTxManager().runTaskWithRetry(new TransactionTask<Long, RuntimeException>() {
                        @Override
                        public Long execute(Transaction t) throws RuntimeException {
                            t.get(TABLE_NAME, ImmutableSet.of(createCell("r1")));
                            t.put(TABLE_NAME, ImmutableMap.of(createCell("r1"), "v2".getBytes()));
                            return t.getTimestamp();
                        }
                    });
                    // Delete will commit and scrub the past. Even without conflict detection on this table, the latest value won't be scrubbed.
                    writeLatch.countDown();
                    return putTs;
                }
            });

            deleteTs = Futures.getUnchecked(deleteTsFuture);
            putTs = Futures.getUnchecked(putTsFuture);
        } finally {
            if (exec != null) {
                exec.shutdownNow();
            }
        }

        Assert.assertTrue("anonymous assert 89C1C0", deleteTs < putTs);
        Assert.assertEquals("anonymous assert EACB73", Value.create(EMPTY_BYTE_ARRAY, -1L),
                getAtTs("r1", deleteTs - 1));
        Assert.assertEquals("anonymous assert 501A71", Value.create(EMPTY_BYTE_ARRAY, deleteTs),
                getAtTs("r1", putTs - 1));
        Assert.assertEquals("anonymous assert E0F201", Value.create("v2".getBytes(), putTs),
                getAtTs("r1", Long.MAX_VALUE));
    }


    private long put(final String row, final String value) {
        return getTxManager().runTaskWithRetry((t) -> {
            t.put(TABLE_NAME, ImmutableMap.of(createCell(row), value.getBytes()));
            return t.getTimestamp();
        });
    }

    private long delete(final String row) {
        return getTxManager().runTaskWithRetry((t) -> {
            t.setTransactionType(Transaction.TransactionType.AGGRESSIVE_HARD_DELETE);
            t.delete(TABLE_NAME, ImmutableSet.of(createCell(row)));
            return t.getTimestamp();
        });
    }

    private long deleteNonAggressive(final String row) {
        return getTxManager().runTaskWithRetry((t) -> {
            t.setTransactionType(Transaction.TransactionType.HARD_DELETE);
            t.delete(TABLE_NAME, ImmutableSet.of(createCell(row)));
            return t.getTimestamp();
        });
    }

    private Value getAtTs(final String row, long timestamp) {
        Cell cell = createCell(row);
        return kvs.get(TABLE_NAME, ImmutableMap.of(cell, timestamp)).get(cell);
    }

    private Set<Long> getAllTs(final String row) {
        return ImmutableSet.copyOf(
                kvs.getAllTimestamps(TABLE_NAME, ImmutableSet.of(createCell(row)),
                        Long.MAX_VALUE).values());
    }

    private Cell createCell(String row) {
        return Cell.create(row.getBytes(), COL.getBytes());
    }

    protected abstract KeyValueService getKeyValueService();

    private TransactionManager getTxManager() {
        if (transactionManager == null) {
            TimestampService timestampService = new InMemoryTimestampService();

            LockClient lockClient = LockClient.of("fake lock client");
            LockService lockService = LockServiceImpl.create(new LockServerOptions() {
                protected static final long serialVersionUID = 1L;

                @Override
                public boolean isStandaloneServer() {
                    return false;
                }

            });

            TransactionService transactionService = TransactionServices.createTransactionService(kvs);
            ConflictDetectionManager conflictDetectionManager = ConflictDetectionManagers.createDefault(kvs);
            SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(kvs);
            Cleaner cleaner = new DefaultCleanerBuilder(
                    kvs,
                    lockService,
                    timestampService,
                    lockClient,
                    ImmutableList.of(),
                    transactionService).buildCleaner();

            transactionManager =  new SerializableTransactionManager(kvs,
                    timestampService,
                    lockClient,
                    lockService,
                    transactionService,
                    Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                    conflictDetectionManager,
                    sweepStrategyManager,
                    cleaner);
        }
        return transactionManager;
    };
}