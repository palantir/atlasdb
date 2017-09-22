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
        private SerializableTransactionManager manager;
        private volatile boolean isInitialized = false;

        public InitializeCheckingWrapper(SerializableTransactionManager manager) {
            this.manager = manager;
        }

        @Override
        public SerializableTransactionManager delegate() {
            if (!isInitialized) {
                try {
                    // SerializableTransactionManager should throw until the underlying KVS is initialized.
                    // TODO(ssouza): replace with KVS healthcheck status when that gets implemented.
                    manager.getKeyValueService().getClusterAvailabilityStatus();
                } catch (NotInitializedException e) {
                    log.info("The KeyValueService is not initialized yet!");
                    throw e;
                }
                isInitialized = true;
            }
            return manager;
        }

        @Override
        public LockService getLockService() {
            return manager.getLockService();
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
    protected SerializableTransactionManager() {
        this(null, null, null, null, null, null, null, null, null, 0);
    }

    public static SerializableTransactionManager create(KeyValueService keyValueService,
            TimelockService timelockService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess,
            Supplier<Long> lockAcquireTimeoutMs,
            int concurrentGetRangesThreadPoolSize,
            boolean initializeAsync) {
        SerializableTransactionManager serializableTransactionManager = new SerializableTransactionManager(
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
                concurrentGetRangesThreadPoolSize);

        return initializeAsync ? new InitializeCheckingWrapper(serializableTransactionManager)
                : serializableTransactionManager;
    }

    public SerializableTransactionManager(KeyValueService keyValueService,
            TimestampService timestampService,
            LockClient lockClient,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            int concurrentGetRangesThreadPoolSize) {
        this(keyValueService,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                false,
                concurrentGetRangesThreadPoolSize);
    }

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
            int concurrentGetRangesThreadPoolSize) {
        this(
                keyValueService,
                new LegacyTimelockService(timestampService, lockService, lockClient),
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess,
                concurrentGetRangesThreadPoolSize);
    }

    public SerializableTransactionManager(KeyValueService keyValueService,
            TimelockService timelockService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess,
            int concurrentGetRangesThreadPoolSize) {
        this(
                keyValueService,
                timelockService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess,
                () -> AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
                concurrentGetRangesThreadPoolSize);
    }

    public SerializableTransactionManager(KeyValueService keyValueService,
            TimelockService timelockService,
            LockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess,
            Supplier<Long> lockAcquireTimeoutMs,
            int concurrentGetRangesThreadPoolSize) {
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
                concurrentGetRangesThreadPoolSize);
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
                getRangesExecutor);
    }

}

