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
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.util.AccumulatingValueMetric;
import com.palantir.atlasdb.util.AggregateRecomputingMetric;
import com.palantir.atlasdb.util.CurrentValueMetric;
import com.palantir.atlasdb.util.MetricsManager;

public final class TargetedSweepMetrics {
    private final Map<TableMetadataPersistence.SweepStrategy, MetricsForStrategy> metricsForStrategyMap;

    private TargetedSweepMetrics(long millis) {
        MetricsManager metricsManager = new MetricsManager();
        metricsForStrategyMap = ImmutableMap.of(
                TableMetadataPersistence.SweepStrategy.CONSERVATIVE,
                new MetricsForStrategy(metricsManager, AtlasDbMetricNames.TAG_CONSERVATIVE, millis),
                TableMetadataPersistence.SweepStrategy.THOROUGH,
                new MetricsForStrategy(metricsManager, AtlasDbMetricNames.TAG_THOROUGH, millis));
    }

    public static TargetedSweepMetrics withRecomputingInterval(long millis) {
        return new TargetedSweepMetrics(millis);
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

    private static long minimum(Collection<Long> currentValues) {
        return currentValues.stream().min(Comparator.naturalOrder()).orElse(-1L);
    }

    private static class MetricsForStrategy {
        private final AccumulatingValueMetric enqueuedWrites = new AccumulatingValueMetric();
        private final AccumulatingValueMetric entriesRead = new AccumulatingValueMetric();
        private final AccumulatingValueMetric tombstonesPut = new AccumulatingValueMetric();
        private final AccumulatingValueMetric abortedWritesDeleted = new AccumulatingValueMetric();
        private final CurrentValueMetric<Long> sweepTimestamp = new CurrentValueMetric<>();
        private final AggregateRecomputingMetric sweptTimestamp;

        private MetricsForStrategy(MetricsManager manager, String strategy, long recomputeMillis) {
            sweptTimestamp = new AggregateRecomputingMetric(TargetedSweepMetrics::minimum, recomputeMillis);
            Map<String, String> tag = ImmutableMap.of(AtlasDbMetricNames.TAG_STRATEGY, strategy);
            register(manager, AtlasDbMetricNames.ENQUEUED_WRITES, enqueuedWrites, tag);
            register(manager, AtlasDbMetricNames.ENTRIES_READ, entriesRead, tag);
            register(manager, AtlasDbMetricNames.TOMBSTONES_PUT, tombstonesPut, tag);
            register(manager, AtlasDbMetricNames.ABORTED_WRITES_DELETED, abortedWritesDeleted, tag);
            register(manager, AtlasDbMetricNames.SWEEP_TS, sweepTimestamp, tag);
            register(manager, AtlasDbMetricNames.SWEPT_TS, sweptTimestamp, tag);
        }

        private static void register(MetricsManager manager, String name, Gauge<Long> metric, Map<String, String> tag) {
            manager.registerMetric(TargetedSweepMetrics.class, name, metric, tag);
        }

        public void updateEnqueuedWrites(long writes) {
            enqueuedWrites.accumulateValue(writes);
        }

        public void updateEntriesRead(long writes) {
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
            sweptTimestamp.update(shard, lastSweptTs);
        }
    }
}
