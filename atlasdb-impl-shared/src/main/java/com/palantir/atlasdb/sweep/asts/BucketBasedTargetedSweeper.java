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

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.sweep.BackgroundSweeper;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketWriter;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketAssigner;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketAssigner.BucketAssignerEventHandler;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketCloseTimestampCalculator;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketWriter;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultSweepAssignedBucketStore;
import com.palantir.atlasdb.sweep.asts.locks.LockableFactory;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepProgressMetrics;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;
import com.palantir.atlasdb.sweep.queue.SweepQueueCleaner;
import com.palantir.atlasdb.sweep.queue.SweepQueueComponents;
import com.palantir.atlasdb.sweep.queue.SweepQueueProgressUpdater;
import com.palantir.atlasdb.sweep.queue.config.AutoScalingTargetedSweepRuntimeConfig;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.v2.TimelockService;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class BucketBasedTargetedSweeper implements BackgroundSweeper {
    private final ScheduledExecutorService backgroundTaskExecutor;
    // Separating the executors so that lag on the background executor does not affect the sweepers.
    private final ScheduledExecutorService sweeperTaskExecutor;
    private final List<DynamicTaskScheduler> scheduledTasks;

    private BucketBasedTargetedSweeper(
            ScheduledExecutorService backgroundTaskExecutor,
            ScheduledExecutorService sweeperTaskExecutor,
            List<DynamicTaskScheduler> scheduledTasks) {
        this.backgroundTaskExecutor = backgroundTaskExecutor;
        this.sweeperTaskExecutor = sweeperTaskExecutor;
        this.scheduledTasks = List.copyOf(scheduledTasks);
    }

    // TODO(mdaudali): Will address this later. It's possible I just inject the created components in.
    //  but I think this will change with some of the thread autoscaling work so deferring.
    @SuppressWarnings("TooManyArguments")
    public static BackgroundSweeper create(
            DefaultSweepAssignedBucketStore sweepAssignedBucketStore,
            BucketProgressStore bucketProgressStore,
            BucketAssignerEventHandler bucketAssignerEventHandler,
            PuncherStore puncherStore,
            TimelockService timelockService,
            LockableFactory<ExclusiveTask> exclusiveTaskLockableFactory,
            LockableFactory<SweepableBucket> bucketLockableFactory,
            SweepQueueComponents components,
            SpecialTimestampsSupplier timestamps,
            List<SweeperStrategy> strategies,
            int numShards,
            int numThreads,
            Refreshable<AutoScalingTargetedSweepRuntimeConfig> runtimeConfig,
            Refreshable<Boolean> isSweepingEnabled,
            TargetedSweepMetrics metrics,
            TargetedSweepProgressMetrics progressMetrics) {
        ScheduledExecutorService sweeperTaskExecutor = PTExecutors.newScheduledThreadPoolExecutor(numThreads);
        // Enough threads for each of the background tasks to run concurrently. This is possibly overkill given the
        // cadence of those background tasks.
        ScheduledExecutorService backgroundTaskExecutor =
                PTExecutors.newScheduledThreadPoolExecutor(2 + (numShards * strategies.size()));

        SingleBucketSweepTask sweepTask =
                createSweepTask(sweepAssignedBucketStore, bucketProgressStore, components, timestamps, metrics);

        CandidateSweepableBucketRetriever retriever = createCandidateSweepableBucketRetriever(
                // Currently playing around with the jitter, but does not need to be configurable IMO.
                sweepAssignedBucketStore,
                strategies,
                numShards,
                runtimeConfig.map(AutoScalingTargetedSweepRuntimeConfig::minWaitBetweenWorkRefresh),
                Refreshable.only(Duration.ofSeconds(2)));

        SweepStateCoordinator coordinator = createCoordinator(retriever, bucketLockableFactory, progressMetrics);
        DefaultBucketAssigner assigner = createBucketAssigner(
                sweepAssignedBucketStore,
                strategies,
                numShards,
                puncherStore,
                timelockService,
                bucketAssignerEventHandler);

        ShardProgressUpdater shardProgressUpdater =
                createShardProgressUpdater(sweepAssignedBucketStore, bucketProgressStore, components.cleaner());

        List<DynamicTaskScheduler> scheduledTasks = ImmutableList.<DynamicTaskScheduler>builder()
                .add(DynamicTaskScheduler.create(
                        backgroundTaskExecutor,
                        runtimeConfig.map(AutoScalingTargetedSweepRuntimeConfig::minAutoRefreshWorkInterval),
                        retriever::requestUpdate,
                        "CandidateSweepableBucketRetrieverUpdate"))
                .add(DynamicTaskScheduler.createForExclusiveTask(
                        backgroundTaskExecutor,
                        runtimeConfig.map(AutoScalingTargetedSweepRuntimeConfig::minAssigningBucketsInterval),
                        exclusiveTaskLockableFactory.createLockable(ImmutableExclusiveTask.builder()
                                .task(assigner::run)
                                .safeLockDescriptor("BucketAssigner")
                                .build()),
                        "BucketAssigner"))
                .addAll(createSingleSweepIterationTasks(
                        coordinator,
                        sweepTask,
                        numThreads,
                        sweeperTaskExecutor,
                        runtimeConfig.map(AutoScalingTargetedSweepRuntimeConfig::minSingleSweepIterationInterval),
                        isSweepingEnabled))
                .addAll(createShardProgressUpdaterTasks(
                        shardProgressUpdater,
                        strategies,
                        numShards,
                        exclusiveTaskLockableFactory,
                        backgroundTaskExecutor,
                        runtimeConfig.map(AutoScalingTargetedSweepRuntimeConfig::minShardProgressUpdaterInterval)))
                .build();
        return new BucketBasedTargetedSweeper(backgroundTaskExecutor, sweeperTaskExecutor, scheduledTasks);
    }

    private static ShardProgressUpdater createShardProgressUpdater(
            DefaultSweepAssignedBucketStore sweepAssignedBucketStore,
            BucketProgressStore bucketProgressStore,
            SweepQueueCleaner cleaner) {
        return new DefaultShardProgressUpdater(
                bucketProgressStore,
                new SweepQueueProgressUpdater(cleaner),
                sweepAssignedBucketStore,
                sweepAssignedBucketStore,
                sweepAssignedBucketStore);
    }

    private static List<DynamicTaskScheduler> createShardProgressUpdaterTasks(
            ShardProgressUpdater updater,
            List<SweeperStrategy> strategies,
            int numShards,
            LockableFactory<ExclusiveTask> lockableFactory,
            ScheduledExecutorService executor,
            Refreshable<Duration> interval) {
        return IntStream.range(0, numShards)
                .boxed()
                .flatMap(i -> strategies.stream().map(strategy -> ShardAndStrategy.of(i, strategy)))
                .map(shardAndStrategy -> {
                    String name = "ShardProgressUpdater-Shard-" + shardAndStrategy.shard() + "-Strategy-"
                            + shardAndStrategy.strategy();
                    return DynamicTaskScheduler.createForExclusiveTask(
                            executor,
                            interval,
                            lockableFactory.createLockable(ImmutableExclusiveTask.builder()
                                    .task(() -> updater.updateProgress(shardAndStrategy))
                                    .safeLockDescriptor(name)
                                    .build()),
                            name);
                })
                .collect(Collectors.toList());
    }

    private static SweepStateCoordinator createCoordinator(
            CandidateSweepableBucketRetriever retriever,
            LockableFactory<SweepableBucket> bucketLockableFactory,
            TargetedSweepProgressMetrics progressMetrics) {
        return DefaultSweepStateCoordinator.create(retriever, bucketLockableFactory, progressMetrics);
    }

    private static SingleBucketSweepTask createSweepTask(
            DefaultSweepAssignedBucketStore sweepAssignedBucketStore,
            BucketProgressStore bucketProgressStore,
            SweepQueueComponents components,
            SpecialTimestampsSupplier timestamps,
            TargetedSweepMetrics metrics) {
        return new DefaultSingleBucketSweepTask(
                bucketProgressStore,
                components.reader(),
                components.deleter(),
                components.cleaner(),
                timestamps,
                metrics,
                bucket -> {
                    bucketProgressStore.deleteBucketProgress(bucket);
                    sweepAssignedBucketStore.deleteBucketEntry(bucket);
                },
                () -> sweepAssignedBucketStore.getBucketStateAndIdentifier().bucketIdentifier());
    }

    private static List<DynamicTaskScheduler> createSingleSweepIterationTasks(
            SweepStateCoordinator coordinator,
            SingleBucketSweepTask sweepTask,
            int numThreads,
            ScheduledExecutorService executor,
            Refreshable<Duration> interval,
            Refreshable<Boolean> isEnabled) {
        return IntStream.range(0, numThreads)
                .mapToObj(i -> DynamicTaskScheduler.create(
                        executor,
                        interval,
                        () -> {
                            if (isEnabled.get()) {
                                coordinator.tryRunTaskWithBucket(sweepTask::runOneIteration);
                            }
                        },
                        "SingleBucketSweepTask-" + i))
                .collect(Collectors.toList());
    }

    private static CandidateSweepableBucketRetriever createCandidateSweepableBucketRetriever(
            DefaultSweepAssignedBucketStore sweepAssignedBucketStore,
            List<SweeperStrategy> strategies,
            int numShards,
            Refreshable<Duration> debouncerDuration,
            Refreshable<Duration> maxJitter) {
        SweepableBucketRetriever retriever = DefaultSweepableBucketRetriever.create(
                numShards, strategies, sweepAssignedBucketStore, sweepAssignedBucketStore);
        return DefaultCandidateSweepableBucketRetriever.create(retriever, debouncerDuration, maxJitter);
    }

    private static DefaultBucketAssigner createBucketAssigner(
            DefaultSweepAssignedBucketStore sweepAssignedBucketStore,
            List<SweeperStrategy> strategies,
            int numShards,
            PuncherStore puncherStore,
            TimelockService timelockService,
            BucketAssignerEventHandler bucketAssignerEventHandler) {
        BucketWriter bucketWriter = DefaultBucketWriter.create(sweepAssignedBucketStore, strategies, numShards);
        DefaultBucketCloseTimestampCalculator calculator =
                DefaultBucketCloseTimestampCalculator.create(puncherStore, timelockService);
        return DefaultBucketAssigner.create(
                sweepAssignedBucketStore,
                bucketWriter,
                sweepAssignedBucketStore,
                bucketAssignerEventHandler,
                calculator::getBucketCloseTimestamp);
    }

    @Override
    public void runInBackground() {
        scheduledTasks.forEach(DynamicTaskScheduler::start);
    }

    @Override
    public void shutdown() {
        sweeperTaskExecutor.shutdown();
        backgroundTaskExecutor.shutdown();
    }
}
