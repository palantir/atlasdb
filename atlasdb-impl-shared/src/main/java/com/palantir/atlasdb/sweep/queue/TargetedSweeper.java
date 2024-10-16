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
package com.palantir.atlasdb.sweep.queue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.BackgroundSweeper;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.metrics.LastSweptTimestampUpdater;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader.ReadBatchingRuntimeContext;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.ImmutableTargetedSweepRuntimeConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.NotInitializedException;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.function.Supplier;

@SuppressWarnings({"FinalClass", "Not final for mocking in tests"})
public class TargetedSweeper implements MultiTableSweepQueueWriter, BackgroundSweeper {
    private static final SafeLogger log = SafeLoggerFactory.get(TargetedSweeper.class);

    private final boolean shouldResetAndStopSweep;
    private final Supplier<TargetedSweepRuntimeConfig> runtime;
    private final TargetedSweepInstallConfig install;
    private final List<Follower> followers;
    private final MetricsManager metricsManager;
    private final TargetedSweepMetrics.MetricsConfiguration metricsConfiguration;

    private final AbandonedTransactionConsumer abandonedTransactionConsumer;

    private final KeyValueService keyValueService;

    private LastSweptTimestampUpdater lastSweptTimestampUpdater;
    private TargetedSweepMetrics metrics;
    private SweepQueue queue;
    private SpecialTimestampsSupplier timestampsSupplier;
    private ShardedBackgroundSweepScheduler sweeper;
    private volatile boolean isInitialized = false;

    private TargetedSweeper(
            MetricsManager metricsManager,
            Supplier<TargetedSweepRuntimeConfig> runtime,
            TargetedSweepInstallConfig install,
            List<Follower> followers,
            AbandonedTransactionConsumer abandonedTransactionConsumer,
            KeyValueService keyValueService) {
        this.metricsManager = metricsManager;
        this.runtime = runtime;
        this.shouldResetAndStopSweep = install.resetTargetedSweepQueueProgressAndStopSweep();
        this.followers = followers;
        this.metricsConfiguration = install.metricsConfiguration();
        this.abandonedTransactionConsumer = abandonedTransactionConsumer;
        this.keyValueService = keyValueService;
        this.install = install;
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    /**
     * Creates a targeted sweeper, without initializing any of the necessary resources. You must call the
     * {@link #initializeWithoutRunning(SpecialTimestampsSupplier, TimelockService, KeyValueService,
     * TransactionService, TargetedSweepFollower)} method before any writes can be made to the sweep queue, or before
     * the background sweep job can run.
     *
     * @param runtime live reloadable TargetedSweepRuntimeConfig.
     * @param install TargetedSweepInstallConfig, which is not live reloadable.
     * @param followers follower used for sweeps, as defined by your schema.
     * @return returns an uninitialized targeted sweeper
     */
    public static TargetedSweeper createUninitialized(
            MetricsManager metrics,
            Supplier<TargetedSweepRuntimeConfig> runtime,
            TargetedSweepInstallConfig install,
            List<Follower> followers,
            AbandonedTransactionConsumer abandonedTransactionConsumer,
            KeyValueService kvs) {
        return new TargetedSweeper(metrics, runtime, install, followers, abandonedTransactionConsumer, kvs);
    }

    public static TargetedSweeper createUninitializedForTest(
            KeyValueService kvs, MetricsManager metricsManager, Supplier<TargetedSweepRuntimeConfig> runtime) {
        TargetedSweepInstallConfig install = ImmutableTargetedSweepInstallConfig.builder()
                .conservativeThreads(0)
                .thoroughThreads(0)
                .build();
        return createUninitialized(metricsManager, runtime, install, ImmutableList.of(), _unused -> {}, kvs);
    }

    public static TargetedSweeper createUninitializedForTest(KeyValueService kvs, Supplier<Integer> shards) {
        Supplier<TargetedSweepRuntimeConfig> runtime = () -> ImmutableTargetedSweepRuntimeConfig.builder()
                .shards(shards.get())
                .build();
        return createUninitializedForTest(kvs, MetricsManagers.createForTests(), runtime);
    }

    @Override
    public void initialize(TransactionManager txManager) {
        initializeWithoutRunning(txManager);
        runInBackground();
    }

    public void initializeWithoutRunning(TransactionManager txManager) {
        initializeWithoutRunning(
                SpecialTimestampsSupplier.create(txManager),
                txManager.getTimelockService(),
                keyValueService,
                txManager.getTransactionService(),
                new TargetedSweepFollower(followers, txManager));
    }

    /**
     * This method initializes all the resources necessary for the targeted sweeper. This method should only be called
     * once the kvs is ready.
     * @param timestamps supplier of unreadable and immutable timestamps.
     * @param timelockService TimeLockService to use for synchronizing iterations of sweep on different nodes
     * @param kvs key value service that must be already initialized.
     * @param transaction transaction service for checking if values were committed and rolling back if necessary
     * @param follower followers used for sweeps.
     */
    public void initializeWithoutRunning(
            SpecialTimestampsSupplier timestamps,
            TimelockService timelockService,
            KeyValueService kvs,
            TransactionService transaction,
            TargetedSweepFollower follower) {
        if (isInitialized) {
            return;
        }
        Preconditions.checkState(
                kvs.isInitialized(), "Attempted to initialize targeted sweeper with an uninitialized backing KVS.");
        metrics = TargetedSweepMetrics.create(
                metricsManager,
                timelockService,
                kvs,
                metricsConfiguration,
                runtime.get().shards());
        queue = SweepQueue.create(
                metrics,
                kvs,
                timelockService,
                Suppliers.compose(TargetedSweepRuntimeConfig::shards, runtime::get),
                transaction,
                abandonedTransactionConsumer,
                follower,
                ReadBatchingRuntimeContext.builder()
                        .maximumPartitions(this::getPartitionBatchLimit)
                        .cellsThreshold(() -> runtime.get().batchCellThreshold())
                        .build(),
                table -> runtime.get().tablesToTrackDeletions().apply(table));
        timestampsSupplier = timestamps;
        sweeper = ShardedBackgroundSweepScheduler.create(
                install.conservativeThreads(),
                install.thoroughThreads(),
                timelockService,
                queue,
                // We could just get this from the queue inside create, but since I'm in the process of ripping out
                // the queue, I'm going to pass it in explicitly.
                queue.getNumberOfShardsProvider(),
                timestamps,
                metrics,
                runtime);
        lastSweptTimestampUpdater = new LastSweptTimestampUpdater(
                queue.getNumberOfShardsProvider(),
                queue::getLastSweptTimestamps,
                metrics,
                PTExecutors.newSingleThreadScheduledExecutor(
                        new NamedThreadFactory("last-swept-timestamp-metric-update", true)));
        isInitialized = true;
    }

    @Override
    public void runInBackground() {
        assertInitialized();
        if (shouldResetAndStopSweep) {
            log.warn("This AtlasDB node is operating in a mode where it is attempting to reset the progress of "
                    + "targeted sweep. While in this mode, your data is not getting swept: please restart your node "
                    + "once it is confirmed that sweep progress has been reset.");
            queue.resetSweepProgress();
        } else {
            sweeper.runInBackground();
            lastSweptTimestampUpdater.schedule(metricsConfiguration.lastSweptTimestampUpdaterDelayMillis());
        }
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        assertInitialized();
        queue.enqueue(writes);
    }

    /**
     * Sweeps the next batch for the given shard and strategy. If the sweep is successful, we delete the processed
     * writes from the sweep queue and then update the sweep queue progress accordingly.
     *
     * @param shardStrategy shard and strategy to use
     * @return number of entries swept
     */
    // Visible for testing
    public long sweepNextBatch(ShardAndStrategy shardStrategy, long maxTsExclusive) {
        assertInitialized();
        return queue.sweepNextBatch(shardStrategy, maxTsExclusive);
    }

    @VisibleForTesting
    long processShard(ShardAndStrategy shardAndStrategy) {
        long maxTsExclusive = Sweeper.of(shardAndStrategy).getSweepTimestamp(timestampsSupplier);
        return sweepNextBatch(shardAndStrategy, maxTsExclusive);
    }

    @Override
    public void close() {
        sweeper.shutdown();
        lastSweptTimestampUpdater.close();
    }

    @Override
    public SweeperStrategy getSweepStrategy(TableReference tableReference) {
        return queue.getSweepStrategy(tableReference);
    }

    @Override
    public void shutdown() {
        close();
    }

    private void assertInitialized() {
        if (!isInitialized) {
            throw new NotInitializedException("Targeted Sweeper");
        }
    }

    private int getPartitionBatchLimit() {
        return runtime.get().enableAutoTuning()
                ? Integer.MAX_VALUE
                : runtime.get().maximumPartitionsToBatchInSingleRead();
    }
}
