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

package com.palantir.atlasdb.sweep.metrics;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.codahale.metrics.Gauge;
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
import com.palantir.common.time.SystemClock;
import com.palantir.util.AggregatingVersionedSupplier;
import com.palantir.util.CachedComposedSupplier;

public final class TargetedSweepMetrics {
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

    public static TargetedSweepMetrics create(MetricsManager metricsManager, KeyValueService kvs, long millis) {
        return createWithClock(metricsManager, kvs, new SystemClock(), millis);
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

    private MetricsForStrategy getMetrics(ShardAndStrategy shardStrategy) {
        return metricsForStrategyMap.get(shardStrategy.strategy());
    }

    private static Long minimum(Collection<Long> currentValues) {
        return currentValues.stream().min(Comparator.naturalOrder()).orElse(null);
    }

    private static final class MetricsForStrategy {
        private final AccumulatingValueMetric enqueuedWrites = new AccumulatingValueMetric();
        private final AccumulatingValueMetric entriesRead = new AccumulatingValueMetric();
        private final AccumulatingValueMetric tombstonesPut = new AccumulatingValueMetric();
        private final AccumulatingValueMetric abortedWritesDeleted = new AccumulatingValueMetric();
        private final CurrentValueMetric<Long> sweepTimestamp = new CurrentValueMetric<>();
        private final AggregatingVersionedSupplier<Long> lastSweptTsSupplier;

        private MetricsForStrategy(MetricsManager manager, String strategy, Function<Long, Long> tsToMillis,
                Clock wallClock, long recomputeMillis) {
            Map<String, String> tag = ImmutableMap.of(AtlasDbMetricNames.TAG_STRATEGY, strategy);
            register(manager, AtlasDbMetricNames.ENQUEUED_WRITES, enqueuedWrites, tag);
            register(manager, AtlasDbMetricNames.ENTRIES_READ, entriesRead, tag);
            register(manager, AtlasDbMetricNames.TOMBSTONES_PUT, tombstonesPut, tag);
            register(manager, AtlasDbMetricNames.ABORTED_WRITES_DELETED, abortedWritesDeleted, tag);
            register(manager, AtlasDbMetricNames.SWEEP_TS, sweepTimestamp, tag);

            lastSweptTsSupplier = new AggregatingVersionedSupplier<>(TargetedSweepMetrics::minimum, recomputeMillis);
            Supplier<Long> millisSinceLastSweptTs = new CachedComposedSupplier<>(
                    lastSweptTs -> estimateMillisSinceTs(lastSweptTs, wallClock, tsToMillis),
                    lastSweptTsSupplier);

            register(manager, AtlasDbMetricNames.LAST_SWEPT_TS, () -> lastSweptTsSupplier.get().value(), tag);
            register(manager, AtlasDbMetricNames.LAG_MILLIS, millisSinceLastSweptTs::get, tag);
        }

        private void register(MetricsManager manager, String name, Gauge<Long> metric, Map<String, String> tag) {
            manager.registerMetric(TargetedSweepMetrics.class, name, metric, tag);
        }

        private static Long estimateMillisSinceTs(Long timestamp, Clock clock, Function<Long, Long> tsToMillis) {
            if (timestamp == null) {
                return null;
            }
            return clock.getTimeMillis() - tsToMillis.apply(timestamp);
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

        private void updateProgressForShard(int shard, long lastSweptTs) {
            lastSweptTsSupplier.update(shard, lastSweptTs);
        }
    }
}
