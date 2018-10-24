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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AssertLockedKeyValueService;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.logging.CommitProfileProcessor;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

public class TestTransactionManagerImpl extends SerializableTransactionManager implements TestTransactionManager {
    private static final TransactionConfig TRANSACTION_CONFIG = ImmutableTransactionConfig.builder().build();

    private final Map<TableReference, ConflictHandler> conflictHandlerOverrides = new HashMap<>();
    private Optional<Long> unreadableTs = Optional.empty();

    @SuppressWarnings("Indentation") // Checkstyle complains about lambda in constructor.
    public TestTransactionManagerImpl(MetricsManager metricsManager,
            KeyValueService keyValueService,
            TimestampService timestampService,
            TimestampManagementService timestampManagementService,
            LockClient lockClient,
            LockService lockService,
            TransactionService transactionService,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            MultiTableSweepQueueWriter sweepQueue,
            ExecutorService deleteExecutor) {
        super(metricsManager,
                createAssertKeyValue(keyValueService, lockService),
                new LegacyTimelockService(timestampService, lockService, lockClient),
                timestampManagementService,
                lockService,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictDetectionManager,
                sweepStrategyManager,
                NoOpCleaner.INSTANCE,
                TimestampCache.createForTests(),
                false,
                AbstractTransactionTest.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                sweepQueue,
                deleteExecutor,
                true,
                () -> TRANSACTION_CONFIG);
    }

    @SuppressWarnings("Indentation") // Checkstyle complains about lambda in constructor.
    public TestTransactionManagerImpl(MetricsManager metricsManager,
            KeyValueService keyValueService,
            TimestampService timestampService,
            TimestampManagementService timestampManagementService,
            LockClient lockClient,
            LockService lockService,
            TransactionService transactionService,
            AtlasDbConstraintCheckingMode constraintCheckingMode) {
        super(metricsManager,
                createAssertKeyValue(keyValueService, lockService),
                new LegacyTimelockService(timestampService, lockService, lockClient),
                timestampManagementService,
                lockService,
                transactionService,
                Suppliers.ofInstance(constraintCheckingMode),
                ConflictDetectionManagers.createWithoutWarmingCache(keyValueService),
                SweepStrategyManagers.createDefault(keyValueService),
                NoOpCleaner.INSTANCE,
                TimestampCache.createForTests(),
                false,
                AbstractTransactionTest.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                MultiTableSweepQueueWriter.NO_OP,
                MoreExecutors.newDirectExecutorService(),
                true,
                () -> TRANSACTION_CONFIG);
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
        long startTimestamp = timelockService.getFreshTimestamp();
        return new SnapshotTransaction(metricsManager,
                keyValueService,
                timelockService,
                transactionService,
                NoOpCleaner.INSTANCE,
                () -> startTimestamp,
                getConflictDetectionManager(),
                SweepStrategyManagers.createDefault(keyValueService),
                startTimestamp,
                Optional.empty(),
                PreCommitConditions.NO_OP,
                AtlasDbConstraintCheckingMode.NO_CONSTRAINT_CHECKING,
                null,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                false,
                timestampValidationReadCache,
                getRangesExecutor,
                defaultGetRangesConcurrency,
                sweepQueueWriter,
                deleteExecutor,
                CommitProfileProcessor.createNonLogging(metricsManager),
                validateLocksOnReads,
                () -> TRANSACTION_CONFIG);
    }

    @Override
    ConflictDetectionManager getConflictDetectionManager() {
        return TestConflictDetectionManagers.createWithStaticConflictDetection(getConflictHandlerWithOverrides());
    }

    @Override
    public void overrideConflictHandlerForTable(TableReference table, ConflictHandler conflictHandler) {
        conflictHandlerOverrides.put(table, conflictHandler);
    }

    @Override
    public long getUnreadableTimestamp() {
        return unreadableTs.orElse(super.getUnreadableTimestamp());
    }

    public void setUnreadableTimestamp(long timestamp) {
        unreadableTs = Optional.of(timestamp);
    }

    private Map<TableReference, ConflictHandler> getConflictHandlerWithOverrides() {
        Map<TableReference, ConflictHandler> conflictHandlersWithOverrides = new HashMap<>();
        conflictHandlersWithOverrides.putAll(conflictDetectionManager.getCachedValues());
        conflictHandlersWithOverrides.putAll(conflictHandlerOverrides);
        return conflictHandlersWithOverrides;
    }
}
