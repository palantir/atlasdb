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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cell.api.TransactionKeyValueService;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionKeyValueServiceManager;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.metrics.DefaultMetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.impl.metrics.MetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampManagementService;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

@SuppressWarnings("TooManyArguments") // Legacy
public class SerializableTransactionManager extends SnapshotTransactionManager {
    private final ConflictTracer conflictTracer;

    public static TransactionManager createInstrumented(
            MetricsManager metricsManager,
            TransactionKeyValueServiceManager transactionKeyValueServiceManager,
            TimelockService timelockService,
            LockWatchManagerInternal lockWatchManager,
            TimestampManagementService timestampManagementService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            Supplier<Boolean> initializationPrerequisite,
            boolean allowHiddenTableAccess,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            boolean initializeAsync,
            TimestampCache timestampCache,
            MultiTableSweepQueueWriter sweepQueueWriter,
            Callback<TransactionManager> callback,
            boolean validateLocksOnReads,
            Supplier<TransactionConfig> transactionConfig,
            ConflictTracer conflictTracer,
            MetricsFilterEvaluationContext metricsFilterEvaluationContext,
            Optional<Integer> sharedGetRangesPoolSize,
            TransactionKnowledgeComponents knowledge) {
        return create(
                metricsManager,
                transactionKeyValueServiceManager,
                timelockService,
                lockWatchManager,
                timestampManagementService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                initializationPrerequisite,
                allowHiddenTableAccess,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                initializeAsync,
                timestampCache,
                sweepQueueWriter,
                callback,
                PTExecutors.newSingleThreadScheduledExecutor(
                        new NamedThreadFactory("AsyncInitializer-SerializableTransactionManager", true)),
                validateLocksOnReads,
                transactionConfig,
                true,
                conflictTracer,
                metricsFilterEvaluationContext,
                sharedGetRangesPoolSize,
                knowledge);
    }

    public static TransactionManager create(
            MetricsManager metricsManager,
            TransactionKeyValueServiceManager transactionKeyValueServiceManager,
            TimelockService timelockService,
            LockWatchManagerInternal lockWatchManager,
            TimestampManagementService timestampManagementService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            Supplier<Boolean> initializationPrerequisite,
            boolean allowHiddenTableAccess,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            boolean initializeAsync,
            TimestampCache timestampCache,
            MultiTableSweepQueueWriter sweepQueueWriter,
            Callback<TransactionManager> callback,
            boolean validateLocksOnReads,
            Supplier<TransactionConfig> transactionConfig,
            ConflictTracer conflictTracer,
            MetricsFilterEvaluationContext metricsFilterEvaluationContext,
            Optional<Integer> sharedGetRangesPoolSize,
            TransactionKnowledgeComponents knowledge) {
        return create(
                metricsManager,
                transactionKeyValueServiceManager,
                timelockService,
                lockWatchManager,
                timestampManagementService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                initializationPrerequisite,
                allowHiddenTableAccess,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                initializeAsync,
                timestampCache,
                sweepQueueWriter,
                callback,
                PTExecutors.newSingleThreadScheduledExecutor(
                        new NamedThreadFactory("AsyncInitializer-SerializableTransactionManager", true)),
                validateLocksOnReads,
                transactionConfig,
                conflictTracer,
                metricsFilterEvaluationContext,
                sharedGetRangesPoolSize,
                knowledge);
    }

    public static TransactionManager create(
            MetricsManager metricsManager,
            TransactionKeyValueServiceManager transactionKeyValueServiceManager,
            TimelockService timelockService,
            LockWatchManagerInternal lockWatchManager,
            TimestampManagementService timestampManagementService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            Supplier<Boolean> initializationPrerequisite,
            boolean allowHiddenTableAccess,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            boolean initializeAsync,
            TimestampCache timestampCache,
            MultiTableSweepQueueWriter sweepQueueWriter,
            Callback<TransactionManager> callback,
            ScheduledExecutorService initializer,
            boolean validateLocksOnReads,
            Supplier<TransactionConfig> transactionConfig,
            ConflictTracer conflictTracer,
            MetricsFilterEvaluationContext metricsFilterEvaluationContext,
            Optional<Integer> sharedGetRangesPoolSize,
            TransactionKnowledgeComponents knowledge) {
        return create(
                metricsManager,
                transactionKeyValueServiceManager,
                timelockService,
                lockWatchManager,
                timestampManagementService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                initializationPrerequisite,
                allowHiddenTableAccess,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                initializeAsync,
                timestampCache,
                sweepQueueWriter,
                callback,
                initializer,
                validateLocksOnReads,
                transactionConfig,
                false,
                conflictTracer,
                metricsFilterEvaluationContext,
                sharedGetRangesPoolSize,
                knowledge);
    }

    private static TransactionManager create(
            MetricsManager metricsManager,
            TransactionKeyValueServiceManager transactionKeyValueServiceManager,
            TimelockService timelockService,
            LockWatchManagerInternal lockWatchManager,
            TimestampManagementService timestampManagementService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            Supplier<Boolean> initializationPrerequisite,
            boolean allowHiddenTableAccess,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            boolean initializeAsync,
            TimestampCache timestampCache,
            MultiTableSweepQueueWriter sweepQueueWriter,
            Callback<TransactionManager> callback,
            ScheduledExecutorService initializer,
            boolean validateLocksOnReads,
            Supplier<TransactionConfig> transactionConfig,
            boolean shouldInstrument,
            ConflictTracer conflictTracer,
            MetricsFilterEvaluationContext metricsFilterEvaluationContext,
            Optional<Integer> sharedGetRangesPoolSize,
            TransactionKnowledgeComponents knowledge) {
        TransactionManager transactionManager = new SerializableTransactionManager(
                metricsManager,
                transactionKeyValueServiceManager,
                timelockService,
                lockWatchManager,
                timestampManagementService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                timestampCache,
                allowHiddenTableAccess,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                sweepQueueWriter,
                // TODO(jakubk): This will be updated in further PRs as it needs to use the same API as sweep.
                new DefaultDeleteExecutor(
                        transactionKeyValueServiceManager.getKeyValueService().orElseThrow(),
                        DefaultTaskExecutors.createDefaultDeleteExecutor()),
                validateLocksOnReads,
                transactionConfig,
                conflictTracer,
                metricsFilterEvaluationContext,
                sharedGetRangesPoolSize,
                knowledge);

        if (shouldInstrument) {
            transactionManager = AtlasDbMetrics.instrumentTimed(
                    metricsManager.getRegistry(), TransactionManager.class, transactionManager);
        }

        if (!initializeAsync) {
            callback.runWithRetry(transactionManager);
        }

        return initializeAsync
                ? new InitializeCheckingWrapper(transactionManager, initializationPrerequisite, callback, initializer)
                : transactionManager;
    }

    public static SerializableTransactionManager createForTest(
            MetricsManager metricsManager,
            TransactionKeyValueServiceManager transactionKeyValueServiceManager,
            TimelockService legacyTimeLockService,
            TimestampManagementService timestampManagementService,
            LockService lockService,
            LockWatchManagerInternal lockWatchManager,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            MultiTableSweepQueueWriter sweepQueue,
            TransactionKnowledgeComponents knowledge) {
        return new SerializableTransactionManager(
                metricsManager,
                transactionKeyValueServiceManager,
                legacyTimeLockService,
                lockWatchManager,
                timestampManagementService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                new DefaultTimestampCache(metricsManager.getRegistry(), () -> 1000L),
                false,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                sweepQueue,
                new DefaultDeleteExecutor(
                        transactionKeyValueServiceManager.getKeyValueService().orElseThrow(),
                        DefaultTaskExecutors.createDefaultDeleteExecutor()),
                true,
                () -> ImmutableTransactionConfig.builder().build(),
                ConflictTracer.NO_OP,
                DefaultMetricsFilterEvaluationContext.createDefault(),
                Optional.empty(),
                knowledge);
    }

    public SerializableTransactionManager(
            MetricsManager metricsManager,
            TransactionKeyValueServiceManager transactionKeyValueServiceManager,
            TimelockService timelockService,
            LockWatchManagerInternal lockWatchManager,
            TimestampManagementService timestampManagementService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            TimestampCache timestampCache,
            boolean allowHiddenTableAccess,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            MultiTableSweepQueueWriter sweepQueueWriter,
            DeleteExecutor deleteExecutor,
            boolean validateLocksOnReads,
            Supplier<TransactionConfig> transactionConfig,
            ConflictTracer conflictTracer,
            MetricsFilterEvaluationContext metricsFilterEvaluationContext,
            Optional<Integer> sharedGetRangesPoolSize,
            TransactionKnowledgeComponents knowledge) {
        super(
                metricsManager,
                transactionKeyValueServiceManager,
                timelockService,
                lockWatchManager,
                timestampManagementService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                timestampCache,
                sweepQueueWriter,
                deleteExecutor,
                validateLocksOnReads,
                transactionConfig,
                conflictTracer,
                metricsFilterEvaluationContext,
                sharedGetRangesPoolSize,
                knowledge);
        this.conflictTracer = conflictTracer;
    }

    @Override
    protected CallbackAwareTransaction createTransaction(
            long immutableTimestamp,
            LongSupplier startTimestampSupplier,
            LockToken immutableTsLock,
            PreCommitCondition preCommitCondition) {
        CommitTimestampLoader loader =
                createCommitTimestampLoader(immutableTimestamp, startTimestampSupplier, Optional.of(immutableTsLock));
        PreCommitConditionValidator validator =
                createPreCommitConditionValidator(Optional.of(immutableTsLock), preCommitCondition);

        TransactionKeyValueService transactionKeyValueService =
                transactionKeyValueServiceManager.getTransactionKeyValueService(startTimestampSupplier);
        return new SerializableTransaction(
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
                conflictTracer,
                tableLevelMetricsController,
                knowledge,
                createDefaultSnapshotReader(startTimestampSupplier, transactionKeyValueService, loader, validator),
                loader,
                validator);
    }

    @VisibleForTesting
    ConflictDetectionManager getConflictDetectionManager() {
        return conflictDetectionManager;
    }
}
