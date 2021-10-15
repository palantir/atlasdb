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
import com.google.common.collect.ImmutableSet;
import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.keyvalue.api.watch.NoOpLockWatchManager;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.AutoDelegate_TransactionManager;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.metrics.DefaultMetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.impl.metrics.MetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.NotInitializedException;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampManagementService;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class SerializableTransactionManager extends SnapshotTransactionManager {
    private static final SafeLogger log = SafeLoggerFactory.get(SerializableTransactionManager.class);

    private final ConflictTracer conflictTracer;

    public static class InitializeCheckingWrapper implements AutoDelegate_TransactionManager {
        private final TransactionManager txManager;
        private final Supplier<Boolean> initializationPrerequisite;
        private final Callback<TransactionManager> callback;

        private State status = State.INITIALIZING;
        private Throwable callbackThrowable = null;

        private final ScheduledExecutorService executorService;

        InitializeCheckingWrapper(
                TransactionManager manager,
                Supplier<Boolean> initializationPrerequisite,
                Callback<TransactionManager> callBack,
                ScheduledExecutorService initializer) {
            this.txManager = manager;
            this.initializationPrerequisite = initializationPrerequisite;
            this.callback = callBack;
            this.executorService = initializer;
            scheduleInitializationCheckAndCallback();
        }

        @Override
        public TransactionManager delegate() {
            assertOpen();
            if (!isInitialized()) {
                throw new NotInitializedException("TransactionManager");
            }
            return txManager;
        }

        @Override
        public boolean isInitialized() {
            assertOpen();
            return status == State.READY && isInitializedInternal();
        }

        @Override
        public LockService getLockService() {
            assertOpen();
            return txManager.getLockService();
        }

        @Override
        public void registerClosingCallback(Runnable closingCallback) {
            assertOpen();
            txManager.registerClosingCallback(closingCallback);
        }

        @Override
        public void close() {
            closeInternal(State.CLOSED);
        }

        @VisibleForTesting
        boolean isClosedByClose() {
            return status == State.CLOSED;
        }

        @VisibleForTesting
        boolean isClosedByCallbackFailure() {
            return status == State.CLOSED_BY_CALLBACK_FAILURE;
        }

        private void assertOpen() {
            if (status == State.CLOSED) {
                throw new SafeIllegalStateException("Operations cannot be performed on closed TransactionManager.");
            }
            if (status == State.CLOSED_BY_CALLBACK_FAILURE) {
                throw new SafeIllegalStateException(
                        "Operations cannot be performed on closed TransactionManager."
                                + " Closed due to a callback failure.",
                        callbackThrowable);
            }
        }

        private void runCallbackIfInitializedOrScheduleForLater(Runnable callbackTask) {
            if (isInitializedInternal()) {
                callbackTask.run();
            } else {
                scheduleInitializationCheckAndCallback();
            }
        }

        private void scheduleInitializationCheckAndCallback() {
            executorService.schedule(
                    () -> {
                        if (status != State.INITIALIZING) {
                            return;
                        }
                        runCallbackIfInitializedOrScheduleForLater(this::runCallbackWithRetry);
                    },
                    1_000,
                    TimeUnit.MILLISECONDS);
        }

        private boolean isInitializedInternal() {
            // Note that the PersistentLockService is also initialized asynchronously as part of
            // TransactionManagers.create; however, this is not required for the TransactionManager to fulfil
            // requests (note that it is not accessible from any TransactionManager implementation), so we omit
            // checking here whether it is initialized.
            return txManager.getKeyValueService().isInitialized()
                    && txManager.getTimelockService().isInitialized()
                    && txManager.getTimestampService().isInitialized()
                    && txManager.getCleaner().isInitialized()
                    && initializationPrerequisite.get();
        }

        private void runCallbackWithRetry() {
            try {
                callback.runWithRetry(txManager);
                changeStateToReady();
            } catch (Throwable e) {
                changeStateToCallbackFailure(e);
            }
        }

        private void changeStateToReady() {
            if (checkAndSetStatus(ImmutableSet.of(State.INITIALIZING), State.READY)) {
                executorService.shutdown();
            }
        }

        private void changeStateToCallbackFailure(Throwable th) {
            log.error(
                    "Callback failed and was not able to perform its cleanup task. "
                            + "Closing the TransactionManager.",
                    th);
            callbackThrowable = th;
            closeInternal(State.CLOSED_BY_CALLBACK_FAILURE);
        }

        private void closeInternal(State newStatus) {
            if (checkAndSetStatus(ImmutableSet.of(State.INITIALIZING, State.READY), newStatus)) {
                callback.blockUntilSafeToShutdown();
                executorService.shutdown();
                txManager.close();
            }
        }

        private synchronized boolean checkAndSetStatus(Set<State> expected, State desired) {
            if (expected.contains(status)) {
                status = desired;
                return true;
            }
            return false;
        }

        private enum State {
            INITIALIZING,
            READY,
            CLOSED,
            CLOSED_BY_CALLBACK_FAILURE
        }
    }

    public static TransactionManager createInstrumented(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
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
            MetricsFilterEvaluationContext metricsFilterEvaluationContext) {

        return create(
                metricsManager,
                keyValueService,
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
                metricsFilterEvaluationContext);
    }

    public static TransactionManager create(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
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
            MetricsFilterEvaluationContext metricsFilterEvaluationContext) {

        return create(
                metricsManager,
                keyValueService,
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
                metricsFilterEvaluationContext);
    }

    public static TransactionManager create(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
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
            MetricsFilterEvaluationContext metricsFilterEvaluationContext) {
        return create(
                metricsManager,
                keyValueService,
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
                metricsFilterEvaluationContext);
    }

    private static TransactionManager create(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
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
            MetricsFilterEvaluationContext metricsFilterEvaluationContext) {
        TransactionManager transactionManager = new SerializableTransactionManager(
                metricsManager,
                keyValueService,
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
                DefaultTaskExecutors.createDefaultDeleteExecutor(),
                validateLocksOnReads,
                transactionConfig,
                conflictTracer,
                metricsFilterEvaluationContext);

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
            KeyValueService keyValueService,
            TimelockService legacyTimeLockService,
            TimestampManagementService timestampManagementService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            MultiTableSweepQueueWriter sweepQueue) {
        return new SerializableTransactionManager(
                metricsManager,
                keyValueService,
                legacyTimeLockService,
                NoOpLockWatchManager.create(),
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
                DefaultTaskExecutors.createDefaultDeleteExecutor(),
                true,
                () -> ImmutableTransactionConfig.builder().build(),
                ConflictTracer.NO_OP,
                DefaultMetricsFilterEvaluationContext.createDefault());
    }

    public SerializableTransactionManager(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
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
            ExecutorService deleteExecutor,
            boolean validateLocksOnReads,
            Supplier<TransactionConfig> transactionConfig,
            ConflictTracer conflictTracer,
            MetricsFilterEvaluationContext metricsFilterEvaluationContext) {
        super(
                metricsManager,
                keyValueService,
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
                metricsFilterEvaluationContext);
        this.conflictTracer = conflictTracer;
    }

    @Override
    protected Transaction createTransaction(
            long immutableTimestamp,
            Supplier<Long> startTimestampSupplier,
            LockToken immutableTsLock,
            PreCommitCondition preCommitCondition) {
        return new SerializableTransaction(
                metricsManager,
                keyValueService,
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
                tableLevelMetricsController);
    }

    @VisibleForTesting
    ConflictDetectionManager getConflictDetectionManager() {
        return conflictDetectionManager;
    }
}
