/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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


import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;

import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.CloseableResourceManager;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

public class TransactionManagerTest extends TransactionTestSetup {
    @ClassRule
    public static final CloseableResourceManager KVS = CloseableResourceManager.inMemory();

    @Override
    protected KeyValueService getKeyValueService() {
        // create new kvs every time because some tests close it
        return KVS.createKvs();
    }

    @Override
    protected void registerTransactionManager(TransactionManager transactionManager) {
        KVS.registerTransactionManager(transactionManager);
    }

    @Override
    protected Optional<TransactionManager> getRegisteredTransactionManager() {
        // force creation of new transaction manager every time because some tests close it
        return Optional.empty();
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
                mockTimestampService, mockTimestampManagementService,
                LockClient.of("foo"), mockLockService, transactionService,
                () -> AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS,
                conflictDetectionManager, sweepStrategyManager, NoOpCleaner.INSTANCE,
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

    private TransactionManager setupTransactionManager() {
        TimelockService timelock = mock(TimelockService.class);
        TimestampManagementService timeManagement = mock(TimestampManagementService.class);
        LockService mockLockService = mock(LockService.class);
        TransactionManager txnManagerWithMocks = new SerializableTransactionManager(metricsManager,
                keyValueService,
                timelock,
                timeManagement,
                mockLockService,
                transactionService,
                () -> AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS,
                conflictDetectionManager,
                sweepStrategyManager,
                NoOpCleaner.INSTANCE,
                TimestampCache.createForTests(),
                false,
                AbstractTransactionTest.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                MultiTableSweepQueueWriter.NO_OP,
                MoreExecutors.newDirectExecutorService(),
                true,
                () -> ImmutableTransactionConfig.builder().build());

        when(timelock.getFreshTimestamp()).thenReturn(1L);
        when(timelock.lockImmutableTimestamp(any())).thenReturn(
                LockImmutableTimestampResponse.of(2L, LockToken.of(UUID.randomUUID())));
        when(timelock.startAtlasDbTransaction(any())).thenReturn(
                StartAtlasDbTransactionResponse.of(
                        LockImmutableTimestampResponse.of(2L, LockToken.of(UUID.randomUUID())), 1L));
        registerTransactionManager(txnManagerWithMocks);
        return txnManagerWithMocks;
    }
}
