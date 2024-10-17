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

package com.palantir.atlasdb.sweep.queue;

import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.metrics.SweepOutcome;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;

final class ShardedStrategySpecificBackgroundSweepScheduler implements AutoCloseable {
    private static final SafeLogger log = SafeLoggerFactory.get(ShardedStrategySpecificBackgroundSweepScheduler.class);
    private final int numThreads;
    private final SweeperStrategy sweepStrategy;
    private final AtomicLong counter = new AtomicLong(0);
    private final SweepDelay delay;
    private final SpecialTimestampsSupplier timestampsSupplier;
    private final TimelockService timelock;
    private final SweepQueue queue;
    private final NumberOfShardsProvider numberOfShardsProvider;
    private final SingleBatchSweeper sweeper;

    private final TargetedSweepMetrics metrics;
    private final BooleanSupplier enableAutoTuningSupplier;
    private final BooleanSupplier enabledSupplier;

    private ScalingSweepTaskScheduler scheduler;

    private ShardedStrategySpecificBackgroundSweepScheduler(
            int numThreads,
            SweeperStrategy sweepStrategy,
            SweepDelay sweepDelay,
            TimelockService timelock,
            SweepQueue queue,
            NumberOfShardsProvider numberOfShardsProvider,
            SpecialTimestampsSupplier timestampsSupplier,
            SingleBatchSweeper sweeper,
            TargetedSweepMetrics metrics,
            BooleanSupplier enableAutoTuningSupplier,
            BooleanSupplier enabledSupplier) {
        this.numThreads = numThreads;
        this.sweepStrategy = sweepStrategy;
        this.delay = sweepDelay;
        this.timelock = timelock;
        this.queue = queue;
        this.numberOfShardsProvider = numberOfShardsProvider;
        this.timestampsSupplier = timestampsSupplier;
        this.sweeper = sweeper;
        this.metrics = metrics;
        this.enableAutoTuningSupplier = enableAutoTuningSupplier;
        this.enabledSupplier = enabledSupplier;
    }

    static ShardedStrategySpecificBackgroundSweepScheduler create(
            int numThreads,
            SweeperStrategy sweepStrategy,
            TimelockService timelock,
            SweepQueue queue,
            NumberOfShardsProvider numberOfShardsProvider,
            SingleBatchSweeper sweeper,
            SpecialTimestampsSupplier timestampsSupplier,
            TargetedSweepMetrics metrics,
            Supplier<TargetedSweepRuntimeConfig> runtime) {
        return new ShardedStrategySpecificBackgroundSweepScheduler(
                numThreads,
                sweepStrategy,
                new SweepDelay(
                        () -> runtime.get().pauseMillis(),
                        millis -> metrics.updateSweepDelayMetric(sweepStrategy, millis),
                        () -> runtime.get().batchCellThreshold()),
                timelock,
                queue,
                numberOfShardsProvider,
                timestampsSupplier,
                sweeper,
                metrics,
                () -> runtime.get().enableAutoTuning(),
                () -> runtime.get().enabled());
    }

    void scheduleBackgroundThreads() {
        if (numThreads > 0 && scheduler == null) {
            scheduler = ScalingSweepTaskScheduler.createStarted(
                    delay, numThreads, this::runOneIteration, enableAutoTuningSupplier);
        }
    }

    private SweepIterationResult runOneIteration() {
        if (!enabledSupplier.getAsBoolean()) {
            return SweepIterationResults.disabled();
        }

        Optional<TargetedSweeperLock> maybeLock = Optional.empty();
        try {
            maybeLock = tryToAcquireLockForNextShardAndStrategy();
            return maybeLock
                    .map(targetedSweeperLock ->
                            SweepIterationResults.success(processShard(targetedSweeperLock.getShardAndStrategy())))
                    .orElseGet(SweepIterationResults::unableToAcquireShard);
        } catch (InsufficientConsistencyException e) {
            metrics.registerOccurrenceOf(sweepStrategy, SweepOutcome.NOT_ENOUGH_DB_NODES_ONLINE);
            logException(e, maybeLock);
            return SweepIterationResults.insufficientConsistency();
        } catch (Throwable th) {
            metrics.registerOccurrenceOf(sweepStrategy, SweepOutcome.ERROR);
            logException(th, maybeLock);
            return SweepIterationResults.otherError();
        } finally {
            try {
                maybeLock.ifPresent(TargetedSweeperLock::unlock);
            } catch (Throwable th) {
                logUnlockException(th, maybeLock);
            }
        }
    }

    /**
     * Sweeps the next batch for the given shard and strategy. If the sweep is successful, we delete the processed
     * writes from the sweep queue and then update the sweep queue progress accordingly.
     *
     * @param shardAndStrategy shard and strategy to use
     * @return number of entries swept
     */
    private long processShard(ShardAndStrategy shardAndStrategy) {
        long maxTsExclusive = Sweeper.of(shardAndStrategy).getSweepTimestamp(timestampsSupplier);
        return sweeper.sweepNextBatch(shardAndStrategy, maxTsExclusive);
    }

    private Optional<TargetedSweeperLock> tryToAcquireLockForNextShardAndStrategy() {
        return IntStream.range(0, numberOfShardsProvider.getNumberOfShards(sweepStrategy))
                .map(ignore -> getShardAndIncrement())
                .mapToObj(shard -> TargetedSweeperLock.tryAcquire(shard, sweepStrategy, timelock))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    private int getShardAndIncrement() {
        return (int) (counter.getAndIncrement() % numberOfShardsProvider.getNumberOfShards());
    }

    private void logException(Throwable th, Optional<TargetedSweeperLock> maybeLock) {
        if (maybeLock.isPresent()) {
            log.warn(
                    "Targeted sweep for {} failed and will be retried later.",
                    SafeArg.of(
                            "shardStrategy",
                            maybeLock.get().getShardAndStrategy().toText()),
                    th);
        } else {
            log.warn(
                    "Targeted sweep for sweep strategy {} failed and will be retried later.",
                    SafeArg.of("sweepStrategy", sweepStrategy),
                    th);
        }
    }

    private void logUnlockException(Throwable th, Optional<TargetedSweeperLock> maybeLock) {
        if (maybeLock.isPresent()) {
            log.info(
                    "Failed to unlock targeted sweep lock for {}.",
                    SafeArg.of(
                            "shardStrategy",
                            maybeLock.get().getShardAndStrategy().toText()),
                    th);
        } else {
            log.info(
                    "Failed to unlock targeted sweep lock for sweep strategy {}.",
                    SafeArg.of("sweepStrategy", sweepStrategy),
                    th);
        }
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.close();
        }
    }
}
