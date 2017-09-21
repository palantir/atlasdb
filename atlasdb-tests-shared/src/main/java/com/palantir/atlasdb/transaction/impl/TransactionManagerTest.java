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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.remoting2.tracing.Tracers;
import com.palantir.timestamp.TimestampService;

public class TransactionManagerTest extends TransactionTestSetup {

    @Test
    public void shouldSuccessfullyCloseTransactionManagerMultipleTimes() throws Exception {
        txMgr.close();
        txMgr.close();
    }

    @Test
    public void shouldNotRunTaskWithRetryWithClosedTransactionManager() throws Exception {
        txMgr.close();

        assertThatThrownBy(() -> txMgr.runTaskWithRetry((TransactionTask<Void, RuntimeException>) txn -> {
            put(txn, "row1", "col1", "v1");
            return null;
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operations cannot be performed on closed TransactionManager.");
    }

    @Test
    public void shouldNotRunTaskThrowOnConflictWithClosedTransactionManager() throws Exception {
        txMgr.close();
        assertThatThrownBy(() -> txMgr.runTaskThrowOnConflict((TransactionTask<Void, RuntimeException>) txn -> {
            put(txn, "row1", "col1", "v1");
            return null;
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operations cannot be performed on closed TransactionManager.");
    }

    @Test
    public void shouldNotRunTaskReadOnlyWithClosedTransactionManager() throws Exception {
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
        LockService mockLockService = mock(LockService.class);
        TransactionManager txnManagerWithMocks = new SerializableTransactionManager(getKeyValueService(),
                mockTimestampService, LockClient.of("foo"), mockLockService, transactionService,
                () -> AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS,
                conflictDetectionManager, sweepStrategyManager, NoOpCleaner.INSTANCE, 16);

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
