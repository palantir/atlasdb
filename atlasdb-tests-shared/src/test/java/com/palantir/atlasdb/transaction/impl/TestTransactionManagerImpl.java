/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.transaction.impl;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AssertLockedKeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampService;

public class TestTransactionManagerImpl extends SnapshotTransactionManager implements TestTransactionManager {
    public TestTransactionManagerImpl(KeyValueService keyValueService,
                                      TimestampService timestampService,
                                      LockClient lockClient,
                                      LockService lockService,
                                      TransactionService transactionService,
                                      ConflictDetectionManager conflictDetectionManager,
                                      SweepStrategyManager sweepStrategyManager) {
        super(
                createAssertKeyValue(keyValueService, lockService),
                timestampService,
                lockClient,
                lockService,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictDetectionManager,
                sweepStrategyManager,
                NoOpCleaner.INSTANCE);
    }

    public TestTransactionManagerImpl(KeyValueService keyValueService,
                                      TimestampService timestampService,
                                      LockClient lockClient,
                                      LockService lockService,
                                      TransactionService transactionService,
                                      AtlasDbConstraintCheckingMode constraintCheckingMode) {
        super(
                createAssertKeyValue(keyValueService, lockService),
                timestampService,
                lockClient,
                lockService,
                transactionService,
                Suppliers.ofInstance(constraintCheckingMode),
                ConflictDetectionManagers.createDefault(keyValueService),
                SweepStrategyManagers.createDefault(keyValueService),
                NoOpCleaner.INSTANCE);
    }

    @Override
    protected boolean shouldStopRetrying(int numTimesFailed) {
        return false;
    }

    private static KeyValueService createAssertKeyValue(KeyValueService kv, LockService lock) {
        return new AssertLockedKeyValueService(kv, lock);
    }

    @Override
    public Transaction commitAndStartNewTransaction(Transaction t) {
        t.commit();
        return createNewTransaction();
    }

    @Override
    public Transaction createNewTransaction() {
        return new SnapshotTransaction(
                keyValueService,
                lockService,
                timestampService,
                transactionService,
                cleaner,
                timestampService.getFreshTimestamp(),
                conflictDetectionManager.get(),
                constraintModeSupplier.get(),
                TransactionReadSentinelBehavior.THROW_EXCEPTION);
    }
}
