/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionKeyValueServiceManager;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.metrics.MetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampManagementService;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

@SuppressWarnings("TooManyArguments")
public interface TransactionManagerFactory {
    String getType();

    TransactionManager createInstrumented(
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
            TransactionKnowledgeComponents knowledge);

    TransactionManager create(
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
            TransactionKnowledgeComponents knowledge);
}
