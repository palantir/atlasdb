/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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
import java.util.concurrent.ExecutorService;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.queue.SweepQueueWriter;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

final class TransactionFactory {
    private final KeyValueService keyValueService;
    private final TimelockService timelockService;
    private final TransactionService transactionService;
    private final Cleaner cleaner;
    private final ConflictDetectionManager conflictDetectionManager;
    private final SweepStrategyManager sweepStrategyManager;
    private final Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier;
    private final boolean allowHiddenTableAccess;
    private final TimestampCache timestampValidationReadCache;
    private final Supplier<Long> lockAcquireTimeoutMs;
    private final ExecutorService getRangesExecutor;
    private final int defaultGetRangesConcurrency;
    private final SweepQueueWriter sweepQueueWriter;

    TransactionFactory(KeyValueService keyValueService,
            TimelockService timelockService,
            TransactionService transactionService,
            Cleaner cleaner,
            ConflictDetectionManager conflictDetectionManager,
            SweepStrategyManager sweepStrategyManager,
            Supplier<AtlasDbConstraintCheckingMode> constraintModeSupplier,
            boolean allowHiddenTableAccess,
            TimestampCache timestampValidationReadCache,
            Supplier<Long> lockAcquireTimeoutMs,
            ExecutorService getRangesExecutor,
            int defaultGetRangesConcurrency,
            SweepQueueWriter sweepQueueWriter) {
        this.keyValueService = keyValueService;
        this.timelockService = timelockService;
        this.transactionService = transactionService;
        this.cleaner = cleaner;
        this.conflictDetectionManager = conflictDetectionManager;
        this.sweepStrategyManager = sweepStrategyManager;
        this.constraintModeSupplier = constraintModeSupplier;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.timestampValidationReadCache = timestampValidationReadCache;
        this.lockAcquireTimeoutMs = lockAcquireTimeoutMs;
        this.getRangesExecutor = getRangesExecutor;
        this.defaultGetRangesConcurrency = defaultGetRangesConcurrency;
        this.sweepQueueWriter = sweepQueueWriter;
    }

    SnapshotTransaction createSerializableTransaction(
            long immutableTimestamp,
            Supplier<Long> startTimestampSupplier,
            LockToken immutableTsLock,
            PreCommitCondition preCommitCondition) {
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
                preCommitCondition,
                constraintModeSupplier.get(),
                cleaner.getTransactionReadTimeoutMillis(),
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                allowHiddenTableAccess,
                timestampValidationReadCache,
                lockAcquireTimeoutMs.get(),
                getRangesExecutor,
                defaultGetRangesConcurrency,
                sweepQueueWriter);
    }

    SnapshotTransaction createReadOnlyTransaction(PreCommitCondition condition,
            Supplier<Long> startTimestampSupplier,
            Long immutableTs) {
        return new SnapshotTransaction(
                keyValueService,
                timelockService,
                transactionService,
                NoOpCleaner.INSTANCE,
                startTimestampSupplier,
                conflictDetectionManager,
                sweepStrategyManager,
                immutableTs,
                Optional.empty(),
                condition,
                constraintModeSupplier.get(),
                cleaner.getTransactionReadTimeoutMillis(),
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                allowHiddenTableAccess,
                timestampValidationReadCache,
                lockAcquireTimeoutMs.get(),
                getRangesExecutor,
                defaultGetRangesConcurrency,
                sweepQueueWriter::enqueue);
    }

    Transaction createForTests() {
        return new SnapshotTransaction(
                keyValueService,
                timelockService,
                transactionService,
                cleaner,
                timelockService.getFreshTimestamp(),
                conflictDetectionManager,
                constraintModeSupplier.get(),
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                timestampValidationReadCache,
                getRangesExecutor,
                defaultGetRangesConcurrency,
                sweepQueueWriter);
    }
}
