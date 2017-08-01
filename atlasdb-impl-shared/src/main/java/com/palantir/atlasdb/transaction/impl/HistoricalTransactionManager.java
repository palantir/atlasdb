/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampService;

/**
 * Transaction manager to be used ONLY for migrations. Can read at a specific timestamp and
 * exposes the key value service for direct use.
 */
public class HistoricalTransactionManager extends SerializableTransactionManager {

    public HistoricalTransactionManager(KeyValueService keyValueService,
            TimestampService timestampService,
            LockClient lockClient,
            RemoteLockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner) {
        super(keyValueService,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner);
    }

    public HistoricalTransactionManager(KeyValueService keyValueService,
            TimestampService timestampService,
            LockClient lockClient,
            RemoteLockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess) {
        super(keyValueService,
                timestampService,
                lockClient,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess);
    }

    public HistoricalTransactionManager(KeyValueService keyValueService,
            TimelockService timelockService,
            RemoteLockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess) {
        super(keyValueService,
                timelockService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess);
    }

    public HistoricalTransactionManager(KeyValueService keyValueService,
            TimelockService timelockService,
            RemoteLockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess,
            Supplier<Long> lockAcquireTimeoutMs) {
        super(keyValueService,
                timelockService,
                lockService,
                transactionService,
                constraintModeSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                cleaner,
                allowHiddenTableAccess,
                lockAcquireTimeoutMs);
    }

    public <T, E extends Exception> T runTaskReadOnlyAtTimestamp(TransactionTask<T, E> task, long timestamp) throws E {
        checkOpen();
        long immutableTs = getApproximateImmutableTimestamp();
        SnapshotTransaction transaction = new SnapshotTransaction(
                keyValueService,
                timelockService,
                transactionService,
                NoOpCleaner.INSTANCE,
                Suppliers.ofInstance(timestamp),
                conflictDetectionManager,
                sweepStrategyManager,
                immutableTs,
                Optional.empty(),
                AdvisoryLockPreCommitCheck.NO_OP,
                constraintModeSupplier.get(),
                cleaner.getTransactionReadTimeoutMillis(),
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                allowHiddenTableAccess,
                timestampValidationReadCache,
                lockAcquireTimeoutMs.get());
        return runTaskThrowOnConflict(task, new ReadTransaction(transaction, sweepStrategyManager));
    }

    public KeyValueService getKeyValueService() {
        return keyValueService;
    }

}
