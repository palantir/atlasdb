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
package com.palantir.atlasdb.sweep.metrics;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.util.AccumulatingValueMetric;
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.time.Clock;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.util.AggregatingVersionedMetric;
import com.palantir.util.AggregatingVersionedSupplier;
import com.palantir.util.CachedComposedSupplier;

@SuppressWarnings("checkstyle:FinalClass") // non-final for mocking
public class TargetedSweepMetrics {
    private static final Logger log = LoggerFactory.getLogger(TargetedSweepMetrics.class);
    private static final long ONE_WEEK = TimeUnit.DAYS.toMillis(7L);
    private final Map<TableMetadataPersistence.SweepStrategy, MetricsForStrategy> metricsForStrategyMap;

    private TargetedSweepMetrics(MetricsManager metricsManager,
                Function<Long, Long> tsToMillis, Clock clock, long millis) {
        metricsForStrategyMap = ImmutableMap.of(
                TableMetadataPersistence.SweepStrategy.CONSERVATIVE,
                new MetricsForStrategy(metricsManager, AtlasDbMetricNames.TAG_CONSERVATIVE, tsToMillis, clock, millis),
                TableMetadataPersistence.SweepStrategy.THOROUGH,
                new MetricsForStrategy(metricsManager, AtlasDbMetricNames.TAG_THOROUGH, tsToMillis, clock, millis));
    }

    public static TargetedSweepMetrics create(MetricsManager metricsManager, TimelockService timelock,
            KeyValueService kvs, long millis) {
        return createWithClock(metricsManager, kvs, timelock::currentTimeMillis, millis);
    }

    public static TargetedSweepMetrics createWithClock(
            MetricsManager metricsManager, KeyValueService kvs, Clock clock, long millis) {
        return new TargetedSweepMetrics(
                metricsManager,
                ts -> getMillisForTimestampBoundedAtOneWeek(kvs, ts, clock),
                clock,
                millis);
    }

    private static long getMillisForTimestampBoundedAtOneWeek(KeyValueService kvs, long ts, Clock clock) {
        return KeyValueServicePuncherStore
                .getMillisForTimestampIfNotPunchedBefore(kvs, ts, clock.getTimeMillis() - ONE_WEEK);
    }

    public void updateEnqueuedWrites(ShardAndStrategy shardStrategy, long writes) {
        getMetrics(shardStrategy).updateEnqueuedWrites(writes);
    }

    public void updateEntriesRead(ShardAndStrategy shardStrategy, long writes) {
        getMetrics(shardStrategy).updateEntriesRead(writes);
    }

    public void updateNumberOfTombstones(ShardAndStrategy shardStrategy, long tombstones) {
        getMetrics(shardStrategy).updateNumberOfTombstones(tombstones);
    }

    public void updateAbortedWritesDeleted(ShardAndStrategy shardStrategy, long deletes) {
        getMetrics(shardStrategy).updateAbortedWritesDeleted(deletes);
    }

    public void updateSweepTimestamp(ShardAndStrategy shardStrategy, long value) {
        getMetrics(shardStrategy).updateSweepTimestamp(value);
    }

    public void updateProgressForShard(ShardAndStrategy shardStrategy, long lastSweptTs) {
        getMetrics(shardStrategy).updateProgressForShard(shardStrategy.shard(), lastSweptTs);
    }

    public void registerOccurrenceOf(ShardAndStrategy shardStrategy, SweepOutcome outcome) {
        registerOccurrenceOf(shardStrategy.strategy(), outcome);
    }

    public void registerOccurrenceOf(TableMetadataPersistence.SweepStrategy strategy, SweepOutcome outcome) {
        getMetrics(strategy).registerOccurrenceOf(outcome);
    }

    public void registerEntriesReadInBatch(ShardAndStrategy shardStrategy, long batchSize) {
        getMetrics(shardStrategy).registerEntriesReadInBatch(batchSize);
    }

    private MetricsForStrategy getMetrics(ShardAndStrategy shardStrategy) {
        return getMetrics(shardStrategy.strategy());
    }

    private MetricsForStrategy getMetrics(TableMetadataPersistence.SweepStrategy strategy) {
        return metricsForStrategyMap.get(strategy);
    }

    private static final class MetricsForStrategy {
        private final Map<String, String> tag;
        private final MetricsManager manager;
        private final AccumulatingValueMetric enqueuedWrites;
        private final AccumulatingValueMetric entriesRead;
        private final AccumulatingValueMetric tombstonesPut;
        private final AccumulatingValueMetric abortedWritesDeleted;
        private final CurrentValueMetric<Long> sweepTimestamp;
        private final AggregatingVersionedMetric<Long> lastSweptTs;
        private final SweepOutcomeMetrics outcomeMetrics;
        private final Histogram batchSizeMetrics;

        private MetricsForStrategy(MetricsManager manager, String strategy, Function<Long, Long> tsToMillis,
                Clock wallClock, long recomputeMillis) {
            tag = ImmutableMap.of(AtlasDbMetricNames.TAG_STRATEGY, strategy);
            this.manager = manager;
            enqueuedWrites = registerAccumulating(AtlasDbMetricNames.ENQUEUED_WRITES);
            entriesRead = registerAccumulating(AtlasDbMetricNames.ENTRIES_READ);
            tombstonesPut = registerAccumulating(AtlasDbMetricNames.TOMBSTONES_PUT);
            abortedWritesDeleted = registerAccumulating(AtlasDbMetricNames.ABORTED_WRITES_DELETED);
            sweepTimestamp = register(AtlasDbMetricNames.SWEEP_TS, new CurrentValueMetric<>());
            lastSweptTs = registerLastSweptTsMetric(recomputeMillis);

            registerMillisSinceLastSweptMetric(tsToMillis, wallClock, recomputeMillis);

            outcomeMetrics = SweepOutcomeMetrics.registerTargeted(manager, tag);
            batchSizeMetrics = registerBatchSizeMetrics();

            Supplier<Double> meanBatchSize = Suppliers.memoizeWithExpiration(
                    () -> batchSizeMetrics.getSnapshot().getMean(), 30, TimeUnit.SECONDS);
            register(AtlasDbMetricNames.BATCH_SIZE_MEAN, meanBatchSize::get);
        }

        private AccumulatingValueMetric registerAccumulating(String name) {
            return register(name, new AccumulatingValueMetric());
        }

        @SuppressWarnings("unchecked")
        private <T extends Gauge<?>> T register(String name, T metric) {
            return (T) manager.registerOrGet(TargetedSweepMetrics.class, name, metric, tag);
        }

        private AggregatingVersionedMetric<Long> registerLastSweptTsMetric(long millis) {
            AggregatingVersionedSupplier<Long> lastSweptTimestamp = AggregatingVersionedSupplier.min(millis);
            return register(AtlasDbMetricNames.LAST_SWEPT_TS, new AggregatingVersionedMetric<>(lastSweptTimestamp));
        }

        private void registerMillisSinceLastSweptMetric(Function<Long, Long> tsToMillis, Clock wallClock, long millis) {
            Supplier<Long> millisSinceLastSweptTs = new CachedComposedSupplier<>(
                    sweptTs -> estimateMillisSinceTs(sweptTs, wallClock, tsToMillis),
                    lastSweptTs::getVersionedValue,
                    millis,
                    wallClock);

            register(AtlasDbMetricNames.LAG_MILLIS, millisSinceLastSweptTs::get);
        }

        private Long estimateMillisSinceTs(Long sweptTs, Clock clock, Function<Long, Long> tsToMillis) {
            if (sweptTs == null) {
                return null;
            }
            long timeBeforeRecomputing = System.currentTimeMillis();
            long result = clock.getTimeMillis() - tsToMillis.apply(sweptTs);

            long timeTaken = System.currentTimeMillis() - timeBeforeRecomputing;
            if (timeTaken > TimeUnit.SECONDS.toMillis(10)) {
                log.warn("Recomputing the millisSinceLastSwept metric took {} ms.", SafeArg.of("timeTaken", timeTaken));
            } else if (timeTaken > TimeUnit.SECONDS.toMillis(1)) {
                log.info("Recomputing the millisSinceLastSwept metric took {} ms.", SafeArg.of("timeTaken", timeTaken));
            }
            return result;
        }

        private Histogram registerBatchSizeMetrics() {
            Supplier<Histogram> supplier = () -> new Histogram(new SlidingTimeWindowArrayReservoir(1, TimeUnit.HOURS));
            return manager.registerOrGetTaggedHistogram(TargetedSweepMetrics.class, AtlasDbMetricNames.BATCH_SIZE, tag,
                    supplier);
        }

        private void updateEnqueuedWrites(long writes) {
            enqueuedWrites.accumulateValue(writes);
        }

        private void updateEntriesRead(long writes) {
            entriesRead.accumulateValue(writes);
        }

        private void updateNumberOfTombstones(long tombstones) {
            tombstonesPut.accumulateValue(tombstones);
        }

        private void updateAbortedWritesDeleted(long deletes) {
            abortedWritesDeleted.accumulateValue(deletes);
        }

        private void updateSweepTimestamp(long value) {
            sweepTimestamp.setValue(value);
        }

        private void updateProgressForShard(int shard, long sweptTs) {
            lastSweptTs.update(shard, sweptTs);
        }

        public void registerOccurrenceOf(SweepOutcome outcome) {
            outcomeMetrics.registerOccurrenceOf(outcome);
        }

        public void registerEntriesReadInBatch(long batchSize) {
            batchSizeMetrics.update(batchSize);
        }
    }
}
