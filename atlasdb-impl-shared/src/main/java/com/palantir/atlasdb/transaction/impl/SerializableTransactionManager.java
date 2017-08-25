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

import java.time.Duration;
import java.util.Optional;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampService;

public class SerializableTransactionManager extends SnapshotTransactionManager {

    public SerializableTransactionManager(KeyValueService keyValueService,
            TimestampService timestampService,
            LockClient lockClient,
            RemoteLockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            int maxConcurrentGetRanges,
            Duration concurrentGetRangesTimeout) {
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
                maxConcurrentGetRanges,
                concurrentGetRangesTimeout);
    }

    public SerializableTransactionManager(KeyValueService keyValueService,
            TimestampService timestampService,
            LockClient lockClient,
            RemoteLockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess,
            int maxConcurrentGetRanges,
            Duration concurrentGetRangesTimeout) {
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
                maxConcurrentGetRanges,
                concurrentGetRangesTimeout);
    }

    public SerializableTransactionManager(KeyValueService keyValueService,
            TimelockService timelockService,
            RemoteLockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess,
            int maxConcurrentGetRanges,
            Duration concurrentGetRangesTimeout) {
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
                maxConcurrentGetRanges,
                concurrentGetRangesTimeout);
    }

    public SerializableTransactionManager(KeyValueService keyValueService,
            TimelockService timelockService,
            RemoteLockService lockService,
            TransactionService transactionService,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Cleaner cleaner,
            boolean allowHiddenTableAccess,
            Supplier<Long> lockAcquireTimeoutMs,
            int maxConcurrentGetRanges,
            Duration concurrentGetRangesTimeout) {
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
                maxConcurrentGetRanges,
                concurrentGetRangesTimeout);
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
                concurrentGetRangesTimeout);
    }

}

