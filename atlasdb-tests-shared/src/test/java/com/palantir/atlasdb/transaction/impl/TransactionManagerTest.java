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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.watch.NoOpLockWatchManager;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.metrics.DefaultMetricsFilterEvaluationContext;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchCacheImpl;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import java.util.UUID;
import org.junit.ClassRule;
import org.junit.Test;

public class TransactionManagerTest extends TransactionTestSetup {
    @ClassRule
    public static final TestResourceManager TRM = TestResourceManager.inMemory();

    public TransactionManagerTest() {
        super(TRM, TRM);
    }

    @Override
    protected KeyValueService getKeyValueService() {
        // create new kvs every time because some tests close it
        KeyValueService kvs = new InMemoryKeyValueService(false);
        TRM.registerKvs(kvs);
        return kvs;
    }

    @Override
    protected TransactionManager getManager() {
        // create new transaction manager every time because some tests close it
        return createAndRegisterManager();
    }

    @Test
    public void shouldSuccessfullyCloseTransactionManagerMultipleTimes() {
        txMgr.close();
        txMgr.close();
    }

    @Test
    public void shouldNotRunTaskWithRetryWithClosedTransactionManager() {
        txMgr.close();

        assertThatThrownBy(() -> txMgr.runTaskWithRetry((TransactionTask<Void, RuntimeException>) txn -> {
                    put(txn, "row1", "col1", "v1");
                    return null;
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operations cannot be performed on closed TransactionManager.");
    }

    @Test
    public void shouldNotRunTaskThrowOnConflictWithClosedTransactionManager() {
        txMgr.close();
        assertThatThrownBy(() -> txMgr.runTaskThrowOnConflict((TransactionTask<Void, RuntimeException>) txn -> {
                    put(txn, "row1", "col1", "v1");
                    return null;
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operations cannot be performed on closed TransactionManager.");
    }

    @Test
    public void shouldNotRunTaskReadOnlyWithClosedTransactionManager() {
        txMgr.close();

        assertThatThrownBy(() -> txMgr.runTaskReadOnly((TransactionTask<Void, RuntimeException>) txn -> {
                    put(txn, "row1", "col1", "v1");
                    return null;
                }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operations cannot be performed on closed TransactionManager.");
    }

    @Test
    public void shouldNotMakeRemoteCallsInAReadonlyTransactionIfNoWorkIsDone() {
        TimestampService mockTimestampService = mock(TimestampService.class);
        TimestampManagementService mockTimestampManagementService = mock(TimestampManagementService.class);
        LockService mockLockService = mock(LockService.class);
        TransactionManager txnManagerWithMocks = SerializableTransactionManager.createForTest(
                metricsManager,
                getKeyValueService(),
                mockTimestampService,
                mockTimestampManagementService,
                LockClient.of("foo"),
                mockLockService,
                transactionService,
                () -> AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS,
                conflictDetectionManager,
                sweepStrategyManager,
                NoOpCleaner.INSTANCE,
                AbstractTransactionTest.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                MultiTableSweepQueueWriter.NO_OP);

        // fetch an immutable timestamp once so it's cached
        when(mockTimestampService.getFreshTimestamp()).thenReturn(1L);
        when(mockLockService.getMinLockedInVersionId("foo")).thenReturn(1L);
        txnManagerWithMocks.getImmutableTimestamp();
        verify(mockTimestampService).getFreshTimestamp();
        verify(mockLockService).getMinLockedInVersionId("foo");

        // now execute a read transaction
        txnManagerWithMocks.runTaskReadOnly(txn -> null);
        verifyNoMoreInteractions(mockLockService);
        verifyNoMoreInteractions(mockTimestampService);
        verifyNoMoreInteractions(mockTimestampManagementService);
    }

    @Test
    public void shouldConflictIfImmutableTimestampLockExpiresEvenIfNoWritesOnThoroughSweptTable() {
        TransactionManager txnManagerWithMocks = setupTransactionManager();
        assertThatThrownBy(() -> txnManagerWithMocks.runTaskThrowOnConflict(txn -> {
                    get(txn, TEST_TABLE_THOROUGH, "row1", "col1");
                    return null;
                }))
                .isInstanceOf(TransactionFailedRetriableException.class);
    }

    @Test
    public void shouldNotConflictIfImmutableTimestampLockExpiresEvenIfNoWritesOnNonThoroughSweptTable() {
        TransactionManager txnManagerWithMocks = setupTransactionManager();
        txnManagerWithMocks.runTaskThrowOnConflict(txn -> {
            get(txn, TEST_TABLE, "row1", "col1");
            return null;
        });
    }

    @Test
    public void shouldNotConflictIfImmutableTimestampLockExpiresIfNoReadsOrWrites() {
        TransactionManager txnManagerWithMocks = setupTransactionManager();
        txnManagerWithMocks.runTaskThrowOnConflict(txn -> null);
    }

    @Test
    public void writeDoesNotPopulateTimestampCache() {
        Long writerStartTimestamp = txMgr.runTaskWithRetry(txn -> {
            put(txn, TEST_TABLE, "row", "column", "test");
            return txn.getTimestamp();
        });

        assertThat(timestampCache.getCommitTimestampIfPresent(writerStartTimestamp))
                .isNull();
    }

    @Test
    public void readCachesWritersStartToCommitTimestamp() {
        Transaction writer = txMgr.runTaskWithRetry(txn -> {
            put(txn, TEST_TABLE, "row", "column", "test");
            return txn;
        });

        assertThat(timestampCache.getCommitTimestampIfPresent(writer.getTimestamp()))
                .isNull();

        txMgr.runTaskWithRetry(txn -> get(txn, TEST_TABLE, "row", "column"));

        assertThat(timestampCache.getCommitTimestampIfPresent(writer.getTimestamp()))
                .isEqualTo(transactionService.get(writer.getTimestamp()));
    }

    @Test
    public void overwriteCachesPreviousWritersStartToCommitTimestamp() {
        Long writerStartTimestamp = txMgr.runTaskWithRetry(txn -> {
            put(txn, TEST_TABLE, "row", "column", "first");
            return txn.getTimestamp();
        });

        assertThat(timestampCache.getCommitTimestampIfPresent(writerStartTimestamp))
                .isNull();

        txMgr.runTaskWithRetry(txn -> {
            put(txn, TEST_TABLE, "row", "column", "second");
            return null;
        });

        assertThat(timestampCache.getCommitTimestampIfPresent(writerStartTimestamp))
                .isEqualTo(transactionService.get(writerStartTimestamp));
    }

    @Test
    public void readThanAbortStillCachesWritersCommitTimestamp() {
        Long writerStartTimestamp = txMgr.runTaskWithRetry(txn -> {
            put(txn, TEST_TABLE, "row", "column", "test");
            return txn.getTimestamp();
        });

        assertThat(timestampCache.getCommitTimestampIfPresent(writerStartTimestamp))
                .isNull();

        assertThatThrownBy(() -> txMgr.runTaskWithRetry(txn -> {
                    get(txn, TEST_TABLE, "row", "column");
                    throw new RuntimeException("abort");
                }))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("abort");

        assertThat(timestampCache.getCommitTimestampIfPresent(writerStartTimestamp))
                .isEqualTo(transactionService.get(writerStartTimestamp));
    }

    private TransactionManager setupTransactionManager() {
        TimelockService timelock = mock(TimelockService.class);
        TimestampManagementService timeManagement = mock(TimestampManagementService.class);
        LockService mockLockService = mock(LockService.class);
        LockWatchCache lockWatchCache = LockWatchCacheImpl.noop();
        TransactionManager txnManagerWithMocks = new SerializableTransactionManager(
                metricsManager,
                keyValueService,
                timelock,
                NoOpLockWatchManager.create(),
                timeManagement,
                mockLockService,
                transactionService,
                () -> AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS,
                conflictDetectionManager,
                sweepStrategyManager,
                NoOpCleaner.INSTANCE,
                DefaultTimestampCache.createForTests(),
                false,
                AbstractTransactionTest.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                MultiTableSweepQueueWriter.NO_OP,
                MoreExecutors.newDirectExecutorService(),
                true,
                () -> ImmutableTransactionConfig.builder().build(),
                ConflictTracer.NO_OP,
                DefaultMetricsFilterEvaluationContext.createDefault());

        when(timelock.getFreshTimestamp()).thenReturn(1L);
        when(timelock.lockImmutableTimestamp())
                .thenReturn(LockImmutableTimestampResponse.of(2L, LockToken.of(UUID.randomUUID())));
        when(timelock.startIdentifiedAtlasDbTransactionBatch(1))
                .thenReturn(ImmutableList.of(StartIdentifiedAtlasDbTransactionResponse.of(
                        LockImmutableTimestampResponse.of(2L, LockToken.of(UUID.randomUUID())),
                        TimestampAndPartition.of(1L, 1))));
        TRM.registerTransactionManager(txnManagerWithMocks);
        return txnManagerWithMocks;
    }
}
