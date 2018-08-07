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

import org.junit.Test;

import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.timelock.hackweek.DefaultTransactionService;
import com.palantir.atlasdb.timelock.hackweek.JamesTransactionService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.concurrent.PTExecutors;

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

    @Override
    protected KeyValueService getKeyValueService() {
        return new InMemoryKeyValueService(false,
                PTExecutors.newSingleThreadExecutor(PTExecutors.newNamedThreadFactory(true)));
    }

    private TransactionManager setupTransactionManager() {
        JamesTransactionService james = new DefaultTransactionService();
        TransactionManager txnManagerWithMocks = new SerializableTransactionManager(metricsManager,
                keyValueService,
                james,
                transactionService,
                () -> AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS,
                conflictDetectionManager,
                sweepStrategyManager,
                NoOpCleaner.INSTANCE,
                TimestampCache.createForTests(),
                false,
                () -> AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
                AbstractTransactionTest.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                MultiTableSweepQueueWriter.NO_OP,
                MoreExecutors.newDirectExecutorService());
        return txnManagerWithMocks;
    }
}
