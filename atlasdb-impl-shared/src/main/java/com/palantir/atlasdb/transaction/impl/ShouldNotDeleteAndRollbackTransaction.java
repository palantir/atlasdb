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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.NoOpCleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.cache.LockWatchValueCacheImpl;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.metrics.SimpleTableLevelMetricsController;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.watch.NoOpLockWatchEventCache;

/**
 * This will read the values of all committed transactions.
 */
public class ShouldNotDeleteAndRollbackTransaction extends SnapshotTransaction {

    private static final ExecutorService IGNORING_EXECUTOR = new AbstractExecutorService() {
        @Override
        public void shutdown() {}

        @Override
        public List<Runnable> shutdownNow() {
            return null;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return false;
        }

        @Override
        public void execute(Runnable command) {
            // Should not be called
        }
    };

    public ShouldNotDeleteAndRollbackTransaction(
            MetricsManager metricsManager,
            KeyValueService keyValueService,
            TransactionService transactionService,
            long startTimeStamp,
            AtlasDbConstraintCheckingMode constraintCheckingMode,
            TransactionReadSentinelBehavior readSentinelBehavior,
            boolean allowHiddenTableAccess,
            TimestampCache timestampCache,
            ExecutorService getRangesExecutor,
            int defaultGetRangesConcurrency,
            Supplier<TransactionConfig> transactionConfig) {
        super(
                metricsManager,
                keyValueService,
                null,
                null,
                new LockWatchValueCacheImpl(NoOpLockWatchEventCache.create()),
                transactionService,
                NoOpCleaner.INSTANCE,
                () -> startTimeStamp,
                ConflictDetectionManagers.createWithNoConflictDetection(),
                SweepStrategyManagers.createDefault(keyValueService),
                startTimeStamp,
                Optional.empty(),
                PreCommitConditions.NO_OP,
                constraintCheckingMode,
                null,
                readSentinelBehavior,
                allowHiddenTableAccess,
                timestampCache,
                getRangesExecutor,
                defaultGetRangesConcurrency,
                MultiTableSweepQueueWriter.NO_OP,
                IGNORING_EXECUTOR,
                true,
                transactionConfig,
                ConflictTracer.NO_OP,
                new SimpleTableLevelMetricsController(metricsManager));
    }

    @Override
    protected boolean shouldDeleteAndRollback() {
        // We don't want to delete any data or roll back any transactions because we don't participate in the
        // transaction protocol.  We just want to skip over anything we find that isn't committed
        return false;
    }
}
