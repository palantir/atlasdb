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

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.monitoring.TimestampTracker;
import com.palantir.atlasdb.monitoring.TimestampTrackerImpl;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.NotInitializedException;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.processors.AutoDelegate;
import com.palantir.timestamp.TimestampService;

@AutoDelegate(typeToExtend = SerializableTransactionManager.class)
public class SerializableTransactionManager extends SnapshotTransactionManager {

    public static class InitializeCheckingWrapper extends AutoDelegate_SerializableTransactionManager {
        private final SerializableTransactionManager txManager;
        private final Supplier<Boolean> initializationPrerequisite;
        private final Callback<SerializableTransactionManager> callback;

        private State status = State.INITIALIZING;
        private Throwable callbackThrowable = null;

        private final ScheduledExecutorService executorService = PTExecutors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("AsyncInitializer-SerializableTransactionManager", true));

        InitializeCheckingWrapper(SerializableTransactionManager manager,
                Supplier<Boolean> initializationPrerequisite,
                Callback<SerializableTransactionManager> callBack) {
            this.txManager = manager;
            this.initializationPrerequisite = initializationPrerequisite;
            this.callback = callBack;
            scheduleInitializationCheckAndCallback();
        }

        @Override
        public SerializableTransactionManager delegate() {
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
                throw new IllegalStateException("Operations cannot be performed on closed TransactionManager.");
            }
            if (status == State.CLOSED_BY_CALLBACK_FAILURE) {
                throw new IllegalStateException("Operations cannot be performed on closed TransactionManager."
                            + " Closed due to a callback failure.", callbackThrowable);
            }
        }

        private void scheduleInitializationCheckAndCallback() {
            executorService.schedule(() -> {
                if (status != State.INITIALIZING) {
                    return;
                }
                if (isInitializedInternal()) {
                    runCallback();
                } else {
                    scheduleInitializationCheckAndCallback();
                }
            }, 1_000, TimeUnit.MILLISECONDS);
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

        private void runCallback() {
            try {
                callback.runWithRetry(txManager);
                checkAndSetStatus(ImmutableSet.of(State.INITIALIZING), State.READY);
            } catch (Throwable e) {
                log.error("Callback failed and was not able to perform its cleanup task. "
                        + "Closing the TransactionManager.", e);
                callbackThrowable = e;
                closeInternal(State.CLOSED_BY_CALLBACK_FAILURE);
            }
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
            INITIALIZING, READY, CLOSED, CLOSED_BY_CALLBACK_FAILURE
        }
    }

    /*
     * This constructor is necessary for the InitializeCheckingWrapper. We initialize a dummy transaction manager and
     * use the delegate instead.
     */
    // TODO(ssouza): it's hard to change the interface of STM with this.
    // We should extract interfaces and delete this hack.
    protected SerializableTransactionManager() {
        this(null, null, null, null, null, null, null, null,
                null, null, () -> 1L, false, null, 1, 1,
                MultiTableSweepQueueWriter.NO_OP, null);
    }

    public static SerializableTransactionManager create(MetricsManager metricsManager,
            KeyValueService keyValueService,
            TimelockService timelockService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            Supplier<Boolean> initializationPrerequisite,
            boolean allowHiddenTableAccess,
            Supplier<Long> lockAcquireTimeoutMs,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            boolean initializeAsync,
            Supplier<Long> timestampCacheSize,
            MultiTableSweepQueueWriter sweepQueueWriter,
            Callback<SerializableTransactionManager> callback) {
        TimestampTracker timestampTracker = TimestampTrackerImpl.createWithDefaultTrackers(
                metricsManager, timelockService, cleaner, initializeAsync);
        SerializableTransactionManager serializableTransactionManager = new SerializableTransactionManager(
                metricsManager,
                keyValueService,
                timelockService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                timestampTracker,
                timestampCacheSize,
                allowHiddenTableAccess,
                lockAcquireTimeoutMs,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                sweepQueueWriter,
                Executors.newSingleThreadExecutor());

        if (!initializeAsync) {
            callback.runWithRetry(serializableTransactionManager);
        }

        return initializeAsync
                ? new InitializeCheckingWrapper(serializableTransactionManager, initializationPrerequisite, callback)
                : serializableTransactionManager;
    }

    public static SerializableTransactionManager createForTest(MetricsManager metricsManager,
            KeyValueService keyValueService,
            TimestampService timestampService,
            LockClient lockClient,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            Supplier<Long> timestampCacheSize,
            MultiTableSweepQueueWriter sweepQueue) {
        return new SerializableTransactionManager(
                metricsManager,
                keyValueService,
                new LegacyTimelockService(timestampService, lockService, lockClient),
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                TimestampTrackerImpl.createNoOpTracker(metricsManager),
                timestampCacheSize,
                false,
                () -> AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                sweepQueue,
                Executors.newSingleThreadExecutor());
    }

    // Canonical constructor.
    public SerializableTransactionManager(MetricsManager metricsManager,
            KeyValueService keyValueService,
            TimelockService timelockService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            TimestampTracker timestampTracker,
            Supplier<Long> timestampCacheSize,
            boolean allowHiddenTableAccess,
            Supplier<Long> lockAcquireTimeoutMs,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            MultiTableSweepQueueWriter sweepQueueWriter,
            ExecutorService deleteExecutor) {
        super(
                metricsManager,
                keyValueService,
                timelockService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess,
                lockAcquireTimeoutMs,
                timestampTracker,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency,
                timestampCacheSize,
                sweepQueueWriter,
                deleteExecutor
        );
    }

    @Override
    protected SnapshotTransaction createTransaction(long immutableTimestamp,
            Supplier<Long> startTimestampSupplier,
            LockToken immutableTsLock,
            PreCommitCondition preCommitCondition) {
        return new SerializableTransaction(
                metricsManager,
                keyValueService,
                timelockService,
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
                lockAcquireTimeoutMs.get(),
                getRangesExecutor,
                defaultGetRangesConcurrency,
                sweepQueueWriter,
                deleteExecutor);
    }

    @VisibleForTesting
    ConflictDetectionManager getConflictDetectionManager() {
        return conflictDetectionManager;
    }

}

