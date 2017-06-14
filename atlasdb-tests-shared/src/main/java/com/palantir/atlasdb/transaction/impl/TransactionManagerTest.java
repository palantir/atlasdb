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


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.remoting2.tracing.Tracers;
import com.palantir.timestamp.TimestampService;

public class TransactionManagerTest extends TransactionTestSetup {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Override
    @After
    public void tearDown() {
        // these tests close the txMgr, so we need to also close the keyValueService
        // and null it out so that it gets recreated for the next test
        keyValueService.close();
        keyValueService = null;
    }

    @Test
    public void shouldSuccessfullyCloseTransactionManagerMultipleTimes() throws Exception {
        txMgr.close();
        txMgr.close();
    }

    @Test
    public void shouldNotRunTaskWithRetryWithClosedTransactionManager() throws Exception {
        txMgr.close();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operations cannot be performed on closed TransactionManager");

        txMgr.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction txn) throws RuntimeException {
                put(txn, "row1", "col1", "v1");
                return null;
            }
        });
    }

    @Test
    public void shouldNotRunTaskThrowOnConflictWithClosedTransactionManager() throws Exception {
        txMgr.close();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operations cannot be performed on closed TransactionManager");

        txMgr.runTaskThrowOnConflict(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction txn) throws RuntimeException {
                put(txn, "row1", "col1", "v1");
                return null;
            }
        });
    }

    @Test
    public void shouldNotRunTaskReadOnlyWithClosedTransactionManager() throws Exception {
        txMgr.close();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operations cannot be performed on closed TransactionManager");

        txMgr.runTaskReadOnly(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction txn) throws RuntimeException {
                put(txn, "row1", "col1", "v1");
                return null;
            }
        });
    }

    @Test
    public void shouldNotMakeRemoteCallsInAReadonlyTransactionIfNoWorkIsDone() {
        TimestampService mockTimestampService = mock(TimestampService.class);
        RemoteLockService mockLockService = mock(RemoteLockService.class);
        TransactionManager txnManagerWithMocks = new SerializableTransactionManager(getKeyValueService(),
                mockTimestampService, LockClient.of("foo"), mockLockService, transactionService,
                () -> AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS,
                conflictDetectionManager, sweepStrategyManager, NoOpCleaner.INSTANCE);

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
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return new InMemoryKeyValueService(false,
                Tracers.wrap(PTExecutors.newSingleThreadExecutor(PTExecutors.newNamedThreadFactory(true))));
    }
}
