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

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AssertLockedKeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.timestamp.TimestampService;

public class TestTransactionManagerImpl extends SerializableTransactionManager implements TestTransactionManager {

    private final Map<TableReference, ConflictHandler> conflictHandlerOverrides = new HashMap<>();

    private static final int GET_RANGES_THREAD_POOL_SIZE = 16;
    private static final int DEFAULT_GET_RANGES_CONCURRENCY = 4;

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
                NoOpCleaner.INSTANCE,
                GET_RANGES_THREAD_POOL_SIZE,
                DEFAULT_GET_RANGES_CONCURRENCY);
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
                ConflictDetectionManagers.createWithoutWarmingCache(keyValueService),
                SweepStrategyManagers.createDefault(keyValueService),
                NoOpCleaner.INSTANCE,
                GET_RANGES_THREAD_POOL_SIZE,
                DEFAULT_GET_RANGES_CONCURRENCY);
    }

    @Override
    protected boolean shouldStopRetrying(int numTimesFailed) {
        return false;
    }

    private static KeyValueService createAssertKeyValue(KeyValueService kv, LockService lock) {
        return new AssertLockedKeyValueService(kv, lock);
    }

    @Override
    public Transaction commitAndStartNewTransaction(Transaction tx) {
        tx.commit();
        return createNewTransaction();
    }

    @Override
    public Transaction createNewTransaction() {
        Map<TableReference, ConflictHandler> conflictHandlersWithOverrides = new HashMap<>();
        conflictHandlersWithOverrides.putAll(conflictDetectionManager.getCachedValues());
        conflictHandlersWithOverrides.putAll(conflictHandlerOverrides);
        return new SnapshotTransaction(
                keyValueService,
                timelockService,
                transactionService,
                cleaner,
                timelockService.getFreshTimestamp(),
                TestConflictDetectionManagers.createWithStaticConflictDetection(conflictHandlersWithOverrides),
                constraintModeSupplier.get(),
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                timestampValidationReadCache,
                getRangesExecutor,
                defaultGetRangesConcurrency);
    }

    @Override
    public void overrideConflictHandlerForTable(TableReference table, ConflictHandler conflictHandler) {
        conflictHandlerOverrides.put(table, conflictHandler);
    }
}
