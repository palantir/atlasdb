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

import com.google.common.base.Supplier;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.monitoring.TimestampTracker;
import com.palantir.atlasdb.monitoring.TimestampTrackerImpl;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
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
        private final SerializableTransactionManager manager;
        private final Supplier<Boolean> initializationPrerequisite;

        public InitializeCheckingWrapper(SerializableTransactionManager manager,
                Supplier<Boolean> initializationPrerequisite) {
            this.manager = manager;
            this.initializationPrerequisite = initializationPrerequisite;
        }

        @Override
        public SerializableTransactionManager delegate() {
            if (!isInitialized()) {
                throw new NotInitializedException("TransactionManager");
            }

            return manager;
        }

        @Override
        public boolean isInitialized() {
            // Note that the PersistentLockService is also initialized asynchronously as part of
            // TransactionManagers.create; however, this is not required for the TransactionManager to fulfil
            // requests (note that it is not accessible from any TransactionManager implementation), so we omit
            // checking here whether it is initialized.
            return manager.getKeyValueService().isInitialized()
                    && manager.getTimelockService().isInitialized()
                    && manager.getTimestampService().isInitialized()
                    && manager.getCleaner().isInitialized()
                    && initializationPrerequisite.get();
        }

        @Override
        public TimelockService getTimelockService() {
            return manager.getTimelockService();
        }

        @Override
        public LockService getLockService() {
            return manager.getLockService();
        }

        @Override
        public TimestampService getTimestampService() {
            return manager.getTimestampService();
        }

        @Override
        public KeyValueService getKeyValueService() {
            return manager.getKeyValueService();
        }

        @Override
        public void registerClosingCallback(Runnable closingCallback) {
            manager.registerClosingCallback(closingCallback);
        }
    }

    /*
     * This constructor is necessary for the InitializeCheckingWrapper. We initialize a dummy transaction manager and
     * use the delegate instead.
     */
    // TODO(ssouza): it's hard to change the interface of STM with this.
    // We should extract interfaces and delete this hack.
    protected SerializableTransactionManager() {
        this(null, null, null, null, null, null, null, null, null, () -> 1L, false, null, 1, 1);
    }

    public static SerializableTransactionManager create(KeyValueService keyValueService,
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
            Supplier<Long> timestampCacheSize) {
        TimestampTracker timestampTracker = TimestampTrackerImpl.createWithDefaultTrackers(
                timelockService, cleaner, initializeAsync);
        SerializableTransactionManager serializableTransactionManager = new SerializableTransactionManager(
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
                defaultGetRangesConcurrency);

        return initializeAsync
                ? new InitializeCheckingWrapper(serializableTransactionManager, initializationPrerequisite)
                : serializableTransactionManager;
    }

    public static SerializableTransactionManager createForTest(KeyValueService keyValueService,
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
            Supplier<Long> timestampCacheSize) {
        return new SerializableTransactionManager(keyValueService,
                new LegacyTimelockService(timestampService, lockService, lockClient),
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                TimestampTrackerImpl.createNoOpTracker(),
                timestampCacheSize,
                false,
                () -> AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency);
    }

    /**
     * @deprecated Use {@link SerializableTransactionManager#create} to create this class.
     */
    @Deprecated
    // Used by internal product.
    public SerializableTransactionManager(KeyValueService keyValueService,
            TimestampService timestampService,
            LockClient lockClient,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess,
            TimestampTracker timestampTracker,
            int concurrentGetRangesThreadPoolSize,
            int defaultGetRangesConcurrency,
            Supplier<Long> timestampCacheSize) {
        this(
                keyValueService,
                new LegacyTimelockService(timestampService, lockService, lockClient),
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                timestampTracker,
                timestampCacheSize,
                allowHiddenTableAccess,
                () -> AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
                concurrentGetRangesThreadPoolSize,
                defaultGetRangesConcurrency
        );
    }

    // Canonical constructor.
    public SerializableTransactionManager(KeyValueService keyValueService,
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
            int defaultGetRangesConcurrency) {
        super(
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
                timestampCacheSize);
    }

    @Override
    protected SnapshotTransaction createTransaction(long immutableTimestamp,
            Supplier<Long> startTimestampSupplier,
            LockToken immutableTsLock,
            AdvisoryLockPreCommitCheck advisoryLockCheck) {
        return new SerializableTransaction(
                keyValueService,
                timelockService,
                transactionService,
                cleaner,
                startTimestampSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                immutableTimestamp,
                Optional.of(immutableTsLock),
                advisoryLockCheck,
                constraintModeSupplier.get(),
                cleaner.getTransactionReadTimeoutMillis(),
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                allowHiddenTableAccess,
                timestampValidationReadCache,
                lockAcquireTimeoutMs.get(),
                getRangesExecutor,
                defaultGetRangesConcurrency);
    }

}

