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

package com.palantir.atlasdb.sweep.queue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.BackgroundSweeper;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.metrics.SweepOutcome;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.SafeArg;

@SuppressWarnings({"FinalClass", "Not final for mocking in tests"})
public class TargetedSweeper implements MultiTableSweepQueueWriter, BackgroundSweeper {
    private static final Logger log = LoggerFactory.getLogger(TargetedSweeper.class);
    private final Supplier<Boolean> runSweep;
    private final Supplier<Integer> shardsConfig;
    private final List<Follower> followers;
    private final MetricsManager metricsManager;

    private TargetedSweepMetrics metrics;
    private SweepQueue queue;
    private SpecialTimestampsSupplier timestampsSupplier;
    private BackgroundSweepScheduler conservativeScheduler;
    private BackgroundSweepScheduler thoroughScheduler;

    private volatile boolean isInitialized = false;

    private TargetedSweeper(MetricsManager metricsManager, Supplier<Boolean> runSweep, Supplier<Integer> shardsConfig,
            int conservativeThreads, int thoroughThreads, List<Follower> followers) {
        this.metricsManager = metricsManager;
        this.runSweep = runSweep;
        this.shardsConfig = shardsConfig;
        this.conservativeScheduler = new BackgroundSweepScheduler(conservativeThreads,
                TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
        this.thoroughScheduler = new BackgroundSweepScheduler(thoroughThreads,
                TableMetadataPersistence.SweepStrategy.THOROUGH);
        this.followers = followers;
    }

    /**
     * Creates a targeted sweeper, without initializing any of the necessary resources. You must call the
     * TargetedSweepFollower)} method before any writes can be made to the sweep queue, or before the background sweep
     * job can run.
     *
     * @param enabled live reloadable config controlling whether background threads should perform targeted sweep.
     * @param shardsConfig live reloadable config specifying the desired number of shards. Since the number of shards
     * must never be reduced, this will be ignored if the persisted number of shards is greater.
     * @param conservativeThreads number of conservative threads to use for background targeted sweep.
     * @param thoroughThreads number of thorough threads to use for background targeted sweep.
     * @param followers follower used for sweeps, as defined by your schema.
     * @return returns an uninitialized targeted sweeper
     */
    public static TargetedSweeper createUninitialized(MetricsManager metrics, Supplier<Boolean> enabled,
            Supplier<Integer> shardsConfig, int conservativeThreads, int thoroughThreads, List<Follower> followers) {
        return new TargetedSweeper(metrics, enabled, shardsConfig, conservativeThreads, thoroughThreads, followers);
    }

    @VisibleForTesting
    static TargetedSweeper createUninitializedForTest(MetricsManager metricsManager, Supplier<Boolean> enabled,
            Supplier<Integer> shards) {
        return createUninitialized(metricsManager, enabled, shards, 0, 0, ImmutableList.of());
    }

    public static TargetedSweeper createUninitializedForTest(Supplier<Integer> shards) {
        return createUninitializedForTest(MetricsManagers.createForTests(), () -> true, shards);
    }

    @Override
    public void initialize(TransactionManager txManager) {
        initializeWithoutRunning(txManager);
        runInBackground();
    }

    public void initializeWithoutRunning(TransactionManager txManager) {
        initializeWithoutRunning(SpecialTimestampsSupplier.create(txManager),
                txManager.getKeyValueService(),
                new TargetedSweepFollower(followers, txManager));
    }

    /**
     * This method initializes all the resources necessary for the targeted sweeper. This method should only be called
     * once the kvs is ready.
     * @param timestamps supplier of unreadable and immutable timestamps.
     * @param kvs key value service that must be already initialized.
     * @param follower followers used for sweeps.
     */
    public void initializeWithoutRunning(SpecialTimestampsSupplier timestamps,
            KeyValueService kvs, TargetedSweepFollower follower) {
        if (isInitialized) {
            return;
        }
        Preconditions.checkState(kvs.isInitialized(),
                "Attempted to initialize targeted sweeper with an uninitialized backing KVS.");
        metrics = TargetedSweepMetrics.create(metricsManager, kvs, SweepQueueUtils.REFRESH_TIME);
        queue = SweepQueue.create(metrics, kvs, shardsConfig, follower);
        timestampsSupplier = timestamps;
        isInitialized = true;
    }

    @Override
    public void runInBackground() {
        assertInitialized();
        conservativeScheduler.scheduleBackgroundThreads();
        thoroughScheduler.scheduleBackgroundThreads();
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
     */
    @SuppressWarnings("checkstyle:RegexpMultiline") // Suppress VisibleForTesting warning
    @VisibleForTesting
    public void sweepNextBatch(ShardAndStrategy shardStrategy) {
        assertInitialized();
        if (!runSweep.get()) {
            metrics.registerOccurrenceOf(SweepOutcome.DISABLED);
            return;
        }
        long maxTsExclusive = Sweeper.of(shardStrategy).getSweepTimestamp(timestampsSupplier);
        queue.sweepNextBatch(shardStrategy, maxTsExclusive);
    }

    @Override
    public void close() {
        conservativeScheduler.close();
        thoroughScheduler.close();
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

    private class BackgroundSweepScheduler implements AutoCloseable {
        private final int numThreads;
        private final TableMetadataPersistence.SweepStrategy sweepStrategy;
        private final AtomicLong counter = new AtomicLong(0);

        private ScheduledExecutorService executorService;

        private BackgroundSweepScheduler(int numThreads, TableMetadataPersistence.SweepStrategy sweepStrategy) {
            this.numThreads = numThreads;
            this.sweepStrategy = sweepStrategy;
        }

        private void scheduleBackgroundThreads() {
            if (numThreads > 0 && executorService == null) {
                executorService = PTExecutors
                        .newScheduledThreadPoolExecutor(numThreads, new NamedThreadFactory("Targeted Sweep", true));
                for (int i = 0; i < numThreads; i++) {
                    executorService.scheduleWithFixedDelay(this::runOneIteration, 1, 5, TimeUnit.SECONDS);
                }
            }
        }

        private void runOneIteration() {
            Optional<TargetedSweeperLock> maybeLock = Optional.empty();
            try {
                maybeLock = tryToAcquireLockForNextShardAndStrategy();
                maybeLock.ifPresent(lock -> sweepNextBatch(lock.getShardAndStrategy()));
            } catch (InsufficientConsistencyException e) {
                metrics.registerOccurrenceOf(SweepOutcome.NOT_ENOUGH_DB_NODES_ONLINE);
                logException(e, maybeLock);
            } catch (Throwable th) {
                metrics.registerOccurrenceOf(SweepOutcome.ERROR);
                logException(th, maybeLock);
            } finally {
                maybeLock.ifPresent(TargetedSweeperLock::unlock);
            }
        }

        private Optional<TargetedSweeperLock> tryToAcquireLockForNextShardAndStrategy() {
            return IntStream.range(0, queue.getNumShards())
                    .map(ignore -> getShardAndIncrement())
                    .mapToObj(shard -> TargetedSweeperLock.tryAcquire(shard, sweepStrategy))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .findFirst();
        }

        private int getShardAndIncrement() {
            return (int) (counter.getAndIncrement() % queue.getNumShards());
        }

        private void logException(Throwable th, Optional<TargetedSweeperLock> maybeLock) {
            if (maybeLock.isPresent()) {
                log.warn("Targeted sweep for {} failed and will be retried later.",
                        SafeArg.of("shardStrategy", maybeLock.get().getShardAndStrategy().toText()), th);
            } else {
                log.warn("Targeted sweep for sweep strategy {} failed and will be retried later.",
                        SafeArg.of("sweepStrategy", sweepStrategy), th);
            }
        }

        @Override
        public void close() {
            if (executorService != null) {
                executorService.shutdown();
            }
        }
    }
}
