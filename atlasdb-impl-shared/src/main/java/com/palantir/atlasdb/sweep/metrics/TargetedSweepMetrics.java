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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.util.AccumulatingValueMetric;
import com.palantir.atlasdb.util.AggregateRecomputingMetric;
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.atlasdb.util.MetricsManager;

public class TargetedSweepMetrics {
    private final MetricsManager metricsManager;
    private final Map<TableMetadataPersistence.SweepStrategy, MetricsForStrategy> metricsForStrategyMap;

    private TargetedSweepMetrics(long millis) {
        metricsManager = new MetricsManager();
        metricsForStrategyMap = ImmutableMap.of(
                TableMetadataPersistence.SweepStrategy.CONSERVATIVE,
                new MetricsForStrategy(metricsManager, AtlasDbMetricNames.PREFIX_CONSERVATIVE, millis),
                TableMetadataPersistence.SweepStrategy.THOROUGH,
                new MetricsForStrategy(metricsManager, AtlasDbMetricNames.PREFIX_THOROUGH, millis));
    }

    public static TargetedSweepMetrics withRecomputingInterval(long millis) {
        return new TargetedSweepMetrics(millis);
    }

    public void updateEnqueuedWrites(ShardAndStrategy shardStrategy, long writes) {
        getMetrics(shardStrategy).updateEnqueuedWrites(writes);
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

    private static long minimum(Collection<Long> currentValues) {
        return currentValues.stream().min(Comparator.naturalOrder()).orElse(-1L);
    }

    private static Gauge getOrCreate(MetricsManager manager, String prefix, String name,
            MetricRegistry.MetricSupplier<Gauge> supplier) {
        return manager.registerOrGetGauge(TargetedSweepMetrics.class, prefix, name, supplier);
    }

    @VisibleForTesting
    public void clear() {
        metricsManager.deregisterMetrics();
    }

    private static class MetricsForStrategy {
        private final AccumulatingValueMetric enqueuedWrites;
        private final AccumulatingValueMetric tombstonesPut;
        private final AccumulatingValueMetric abortedWritesDeleted;
        private final CurrentValueMetric<Long> sweepTimestamp;
        private final AggregateRecomputingMetric sweptTs;

        private MetricsForStrategy(MetricsManager manager, String prefix, long recomputeMillis) {
            enqueuedWrites = (AccumulatingValueMetric) getOrCreate(manager, prefix,
                    AtlasDbMetricNames.ENQUEUED_WRITES, AccumulatingValueMetric::new);
            tombstonesPut = (AccumulatingValueMetric) getOrCreate(manager, prefix,
                    AtlasDbMetricNames.TOMBSTONES_PUT, AccumulatingValueMetric::new);
            abortedWritesDeleted = (AccumulatingValueMetric) getOrCreate(manager, prefix,
                    AtlasDbMetricNames.ABORTED_WRITES_DELETED, AccumulatingValueMetric::new);
            sweepTimestamp = (CurrentValueMetric<Long>) getOrCreate(manager, prefix,
                    AtlasDbMetricNames.SWEEP_TS, CurrentValueMetric::new);
            sweptTs = (AggregateRecomputingMetric) getOrCreate(manager, prefix,
                    AtlasDbMetricNames.SWEPT_TS,
                    () -> new AggregateRecomputingMetric(TargetedSweepMetrics::minimum, recomputeMillis));
        }

        public void updateEnqueuedWrites(long writes) {
            enqueuedWrites.accumulateValue(writes);
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
            sweptTs.update(shard, lastSweptTs);
        }
    }
}
