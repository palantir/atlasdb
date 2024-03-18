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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cell.api.TransactionKeyValueService;
import com.palantir.atlasdb.cell.api.TransactionKeyValueServiceManager;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AssertLockedKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.DelegatingTransactionKeyValueServiceManager;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.CommitTimestampLoader;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.KeyValueSnapshotReaderManager;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.PreCommitRequirementValidator;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.metrics.DefaultMetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.LockToken;
import com.palantir.timelock.paxos.AbstractInMemoryTimelockExtension;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.LongSupplier;

public class TestTransactionManagerImpl extends SerializableTransactionManager implements TestTransactionManager {
    static final TransactionConfig TRANSACTION_CONFIG =
            ImmutableTransactionConfig.builder().build();

    private final Map<TableReference, ConflictHandler> conflictHandlerOverrides = new HashMap<>();
    private final WrapperWithTracker<CallbackAwareTransaction> transactionWrapper;
    private Optional<Long> unreadableTs = Optional.empty();

    public TestTransactionManagerImpl(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
            AbstractInMemoryTimelockExtension abstractInMemoryTimelockExtension,
            LockService lockService,
            TransactionService transactionService,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            TimestampCache timestampCache,
            MultiTableSweepQueueWriter sweepQueue,
            TransactionKnowledgeComponents knowledge,
            ExecutorService deleteExecutor,
            KeyValueSnapshotReaderManager keyValueSnapshotReaderManager) {
        this(
                metricsManager,
                keyValueService,
                abstractInMemoryTimelockExtension,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                timestampCache,
                sweepQueue,
                deleteExecutor,
                WrapperWithTracker.TRANSACTION_NO_OP,
                knowledge,
                keyValueSnapshotReaderManager);
    }

    public TestTransactionManagerImpl(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
            AbstractInMemoryTimelockExtension services,
            LockService lockService,
            TransactionService transactionService,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            TimestampCache timestampCache,
            MultiTableSweepQueueWriter sweepQueue,
            ExecutorService deleteExecutor,
            WrapperWithTracker<CallbackAwareTransaction> transactionWrapper,
            TransactionKnowledgeComponents knowledge,
            KeyValueSnapshotReaderManager keyValueSnapshotReaderManager) {
        this(
                metricsManager,
                createAssertKeyValue(keyValueService, lockService),
                services,
                lockService,
                transactionService,
                conflictDetectionManager,
                sweepStrategyManager,
                timestampCache,
                sweepQueue,
                deleteExecutor,
                transactionWrapper,
                knowledge,
                keyValueSnapshotReaderManager);
    }

    private TestTransactionManagerImpl(
            MetricsManager metricsManager,
            TransactionKeyValueServiceManager keyValueService,
            AbstractInMemoryTimelockExtension services,
            LockService lockService,
            TransactionService transactionService,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            TimestampCache timestampCache,
            MultiTableSweepQueueWriter sweepQueue,
            ExecutorService deleteExecutor,
            WrapperWithTracker<CallbackAwareTransaction> transactionWrapper,
            TransactionKnowledgeComponents knowledge,
            KeyValueSnapshotReaderManager keyValueSnapshotReaderManager) {
        super(
                metricsManager,
                keyValueService,
                services.getLegacyTimelockService(),
                services.getLockWatchManager(),
                services.getTimestampManagementService(),
                lockService,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictDetectionManager,
                sweepStrategyManager,
                NoOpCleaner.INSTANCE,
                timestampCache,
                false,
                AbstractTransactionTest.GET_RANGES_THREAD_POOL_SIZE,
                AbstractTransactionTest.DEFAULT_GET_RANGES_CONCURRENCY,
                sweepQueue,
                new DefaultDeleteExecutor(keyValueService.getKeyValueService().orElseThrow(), deleteExecutor),
                true,
                () -> TRANSACTION_CONFIG,
                ConflictTracer.NO_OP,
                DefaultMetricsFilterEvaluationContext.createDefault(),
                Optional.empty(),
                knowledge,
                keyValueSnapshotReaderManager);
        this.transactionWrapper = transactionWrapper;
    }

    @Override
    protected boolean shouldStopRetrying(int numTimesFailed) {
        return false;
    }

    private static TransactionKeyValueServiceManager createAssertKeyValue(KeyValueService kv, LockService lock) {
        return new DelegatingTransactionKeyValueServiceManager(new AssertLockedKeyValueService(kv, lock));
    }

    @Override
    public Transaction commitAndStartNewTransaction(Transaction tx) {
        tx.commit();
        return createNewTransaction();
    }

    @Override
    public Transaction createNewTransaction() {
        return Iterables.getOnlyElement(startTransactions(ImmutableList.of(PreCommitConditions.NO_OP)));
    }

    @Override
    protected CallbackAwareTransaction createTransaction(
            long immutableTimestamp,
            LongSupplier startTimestampSupplier,
            LockToken immutableTsLock,
            PreCommitCondition preCommitCondition) {
        PathTypeTracker pathTypeTracker = PathTypeTrackers.constructSynchronousTracker();
        CommitTimestampLoader loader =
                createCommitTimestampLoader(immutableTimestamp, startTimestampSupplier, Optional.of(immutableTsLock));
        PreCommitRequirementValidator validator =
                createPreCommitConditionValidator(Optional.of(immutableTsLock), preCommitCondition);

        TransactionKeyValueService transactionKeyValueService =
                transactionKeyValueServiceManager.getTransactionKeyValueService(startTimestampSupplier);
        return transactionWrapper.apply(
                new SerializableTransaction(
                        metricsManager,
                        transactionKeyValueService,
                        timelockService,
                        lockWatchManager,
                        transactionService,
                        cleaner,
                        startTimestampSupplier,
                        getConflictDetectionManager(),
                        sweepStrategyManager,
                        immutableTimestamp,
                        Optional.of(immutableTsLock),
                        preCommitCondition,
                        constraintModeSupplier.get(),
                        cleaner.getTransactionReadTimeoutMillis(),
                        TransactionReadSentinelBehavior.THROW_EXCEPTION,
                        allowHiddenTableAccess,
                        timestampValidationReadCache,
                        getRangesExecutor,
                        defaultGetRangesConcurrency,
                        sweepQueueWriter,
                        deleteExecutor,
                        validateLocksOnReads,
                        transactionConfig,
                        ConflictTracer.NO_OP,
                        tableLevelMetricsController,
                        knowledge,
                        keyValueSnapshotReaderManager,
                        loader,
                        validator),
                pathTypeTracker);
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
        return unreadableTs.orElseGet(super::getUnreadableTimestamp);
    }

    @Override
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
