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

package com.palantir.atlasdb.sweep.asts;

import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.BackgroundSweeper;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketAssignerState;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketAssignerState.ClosingFromOpen;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketAssignerState.ImmediatelyClosing;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketAssignerState.Opening;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketAssignerState.Start;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketAssignerState.Visitor;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketAssignerState.WaitingUntilCloseable;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketAssigner.BucketAssignerEventHandler;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketAssigner.IterationResult.OperationResult;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultSweepAssignedBucketStore;
import com.palantir.atlasdb.sweep.asts.locks.LockableFactory;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.asts.progress.DefaultBucketProgressStore;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepProgressMetrics;
import com.palantir.atlasdb.sweep.queue.AbandonedTransactionConsumer;
import com.palantir.atlasdb.sweep.queue.DefaultSingleBatchSweeper;
import com.palantir.atlasdb.sweep.queue.NumberOfShardsProvider.MismatchBehaviour;
import com.palantir.atlasdb.sweep.queue.ShardedBackgroundSweepScheduler;
import com.palantir.atlasdb.sweep.queue.SingleBatchSweeper;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;
import com.palantir.atlasdb.sweep.queue.SweepQueueComponents;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class BackgroundTargetedSweeperFactory {
    private BackgroundTargetedSweeperFactory() {
        // Utility class
    }

    // TODO(mdaudali): Add tests to show that we switch correctly.
    //  This will change in the future when we have the autoscaling work.
    @SuppressWarnings("TooManyArguments")
    public static BackgroundSweeper create(
            KeyValueService keyValueService,
            TimelockService timelockService,
            PuncherStore puncherStore,
            SweepQueueComponents components,
            SpecialTimestampsSupplier specialTimestampsSupplier,
            List<SweeperStrategy> strategies,
            TargetedSweepInstallConfig installConfig,
            Refreshable<TargetedSweepRuntimeConfig> runtimeConfig,
            AbandonedTransactionConsumer abandonedTransactionConsumer,
            TargetedSweepMetrics metrics,
            TargetedSweepProgressMetrics progressMetrics) {
        if (installConfig.enableBucketBasedSweep()) {
            return createBucketBasedSweeper(
                    keyValueService,
                    timelockService,
                    puncherStore,
                    components,
                    specialTimestampsSupplier,
                    strategies,
                    installConfig,
                    runtimeConfig,
                    progressMetrics,
                    metrics);
        } else {
            return createLegacyQueueBasedSweeper(
                    components,
                    installConfig,
                    timelockService,
                    specialTimestampsSupplier,
                    metrics,
                    runtimeConfig,
                    abandonedTransactionConsumer);
        }
    }

    // TODO(mdaudali): There'll be a better place for this later.
    public static MismatchBehaviour getMismatchBehaviour(TargetedSweepInstallConfig config) {
        if (config.enableBucketBasedSweep()) {
            return MismatchBehaviour.IGNORE_UPDATES;
        } else {
            return MismatchBehaviour.UPDATE;
        }
    }

    private static BackgroundSweeper createLegacyQueueBasedSweeper(
            SweepQueueComponents components,
            TargetedSweepInstallConfig installConfig,
            TimelockService timelockService,
            SpecialTimestampsSupplier specialTimestampsSupplier,
            TargetedSweepMetrics metrics,
            Refreshable<TargetedSweepRuntimeConfig> runtimeConfig,
            AbandonedTransactionConsumer abandonedTransactionConsumer) {
        SingleBatchSweeper singleBatchSweeper = new DefaultSingleBatchSweeper(
                metrics,
                components.shardProgress(),
                abandonedTransactionConsumer,
                components.reader(),
                components.deleter(),
                components.cleaner());
        return ShardedBackgroundSweepScheduler.create(
                installConfig.conservativeThreads(),
                installConfig.thoroughThreads(),
                timelockService,
                components.numberOfShardsProvider(),
                singleBatchSweeper,
                specialTimestampsSupplier,
                metrics,
                runtimeConfig);
    }

    @SuppressWarnings("TooManyArguments")
    private static BackgroundSweeper createBucketBasedSweeper(
            KeyValueService keyValueService,
            TimelockService timelockService,
            PuncherStore puncherStore,
            SweepQueueComponents components,
            SpecialTimestampsSupplier specialTimestampsSupplier,
            List<SweeperStrategy> strategies,
            TargetedSweepInstallConfig installConfig,
            Refreshable<TargetedSweepRuntimeConfig> runtimeConfig,
            TargetedSweepProgressMetrics progressMetrics,
            TargetedSweepMetrics metrics) {
        DefaultSweepAssignedBucketStore sweepAssignedBucketStore =
                DefaultSweepAssignedBucketStore.create(keyValueService);
        BucketProgressStore bucketProgressStore = DefaultBucketProgressStore.create(keyValueService);
        LockableFactory<ExclusiveTask> exclusiveTaskLockableFactory = LockableFactory.create(
                timelockService,
                // We will just retry later if we fail to acquire the lock at this time - liveness on a given sweeper
                // for a given shard and strategy is not critical, and neither for the background tasks.
                Refreshable.only(Duration.ofMillis(1)),
                task -> StringLockDescriptor.of(task.safeLockDescriptor()));
        LockableFactory<SweepableBucket> bucketLockableFactory = LockableFactory.create(
                timelockService,
                Refreshable.only(Duration.ofMillis(1)),
                bucket -> StringLockDescriptor.of(bucket.toString()));
        BucketAssignerEventHandler bucketAssignerEventHandler = new DefaultBucketAssignerEventHandler(progressMetrics);
        return BucketBasedTargetedSweeper.create(
                sweepAssignedBucketStore,
                bucketProgressStore,
                bucketAssignerEventHandler,
                puncherStore,
                timelockService,
                exclusiveTaskLockableFactory,
                bucketLockableFactory,
                components,
                specialTimestampsSupplier,
                strategies,
                components.numberOfShardsProvider().getNumberOfShards(),
                installConfig.bucketBasedSweepThreads(),
                runtimeConfig.map(TargetedSweepRuntimeConfig::autoScalingConfig),
                runtimeConfig.map(TargetedSweepRuntimeConfig::enabled),
                metrics,
                progressMetrics);
    }

    // TOOD(mdaudali): This will also be used in the autoscaling work, so will be refactored out at that point.
    private static final class DefaultBucketAssignerEventHandler implements BucketAssignerEventHandler {
        private static final SafeLogger log = SafeLoggerFactory.get(DefaultBucketAssignerEventHandler.class);
        private final TargetedSweepProgressMetrics progressMetrics;
        private final AtomicLong lastOpenedBucket = new AtomicLong(-1);
        private final AtomicInteger bucketAssignerStateRepresentation = new AtomicInteger(-1);
        private final AtomicInteger bucketAssignerStatusRepresentation = new AtomicInteger(-1);

        private DefaultBucketAssignerEventHandler(TargetedSweepProgressMetrics progressMetrics) {
            this.progressMetrics = progressMetrics;
            progressMetrics.openBucketIdentifier(lastOpenedBucket::get);
            progressMetrics.bucketAssignerState(bucketAssignerStateRepresentation::get);
            progressMetrics.bucketAssignerStatus(bucketAssignerStatusRepresentation::get);
        }

        @Override
        public void progressedToBucketIdentifier(long bucketIdentifier) {
            lastOpenedBucket.set(bucketIdentifier);
        }

        @Override
        public void operationResultForIteration(OperationResult operationResult) {
            int representation;
            switch (operationResult) {
                case INCOMPLETE:
                    representation = 0;
                    break;
                case CLOSED:
                    representation = 1;
                    break;
                case CONTENTION_ON_WRITES:
                    representation = 2;
                    break;
                default:
                    log.warn("Unknown operation result: {}", SafeArg.of("operationResult", operationResult));
                    return;
            }
            bucketAssignerStatusRepresentation.set(representation);
        }

        @Override
        public void createdMaxBucketsInSingleIteration() {
            progressMetrics
                    .completedMaxBucketsForSingleBucketAssignerIteration()
                    .mark();
        }

        @Override
        public void bucketAssignerState(BucketAssignerState bucketAssignerState) {
            int representation = bucketAssignerState.accept(new Visitor<Integer>() {
                @Override
                public Integer visit(Start start) {
                    return 0;
                }

                @Override
                public Integer visit(Opening opening) {
                    return 1;
                }

                @Override
                public Integer visit(WaitingUntilCloseable waitingUntilCloseable) {
                    return 2;
                }

                @Override
                public Integer visit(ClosingFromOpen closingFromOpen) {
                    return 3;
                }

                @Override
                public Integer visit(ImmediatelyClosing immediatelyClosing) {
                    return 4;
                }
            });
            bucketAssignerStateRepresentation.set(representation);
        }
    }
}
