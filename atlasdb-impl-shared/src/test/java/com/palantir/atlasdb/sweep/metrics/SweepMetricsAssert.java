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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.sweep.BackgroundSweeperImpl;
import com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.SlidingWindowMeanGauge;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.Map;
import javax.annotation.CheckReturnValue;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.WritableAssertionInfo;
import org.assertj.core.data.Offset;
import org.assertj.core.internal.Doubles;
import org.assertj.core.internal.LongArrays;
import org.assertj.core.internal.Objects;

public final class SweepMetricsAssert extends AbstractAssert<SweepMetricsAssert, MetricsManager> {
    private final MetricsManager metrics;
    private final Objects objects = Objects.instance();
    private final LongArrays arrays = LongArrays.instance();
    private final Doubles doubles = Doubles.instance();
    private final WritableAssertionInfo info = new WritableAssertionInfo();

    public SweepMetricsAssert(MetricsManager actual) {
        super(actual, SweepMetricsAssert.class);
        this.metrics = actual;
    }

    @CheckReturnValue
    public static SweepMetricsAssert assertThat(MetricsManager metricsManager) {
        return new SweepMetricsAssert(metricsManager);
    }

    public void hasNotRegisteredEnqueuedWritesConservativeMetric() {
        objects.assertNull(info, getGaugeConservative(AtlasDbMetricNames.ENQUEUED_WRITES));
    }

    public void hasEnqueuedWritesConservativeEqualTo(long value) {
        objects.assertEqual(
                info, getGaugeConservative(AtlasDbMetricNames.ENQUEUED_WRITES).getValue(), value);
    }

    public void hasEntriesReadConservativeEqualTo(long value) {
        objects.assertEqual(
                info, getGaugeConservative(AtlasDbMetricNames.ENTRIES_READ).getValue(), value);
    }

    public void hasTombstonesPutConservativeEqualTo(long value) {
        objects.assertEqual(
                info, getGaugeConservative(AtlasDbMetricNames.TOMBSTONES_PUT).getValue(), value);
    }

    public void hasAbortedWritesDeletedConservativeEquals(long value) {
        objects.assertEqual(
                info,
                getGaugeConservative(AtlasDbMetricNames.ABORTED_WRITES_DELETED).getValue(),
                value);
    }

    public void hasSweepTimestampConservativeEqualTo(Long value) {
        objects.assertEqual(
                info, getGaugeConservative(AtlasDbMetricNames.SWEEP_TS).getValue(), value);
    }

    public void hasLastSweptTimestampConservativeEqualTo(Long value) {
        objects.assertEqual(
                info, getGaugeConservative(AtlasDbMetricNames.LAST_SWEPT_TS).getValue(), value);
    }

    public void hasMillisSinceLastSweptConservativeEqualTo(Long value) {
        hasMillisSinceLastSweptConservativeForShardEqualTo(-1, value);
    }

    public void hasMillisSinceLastSweptConservativeForShardEqualTo(int shard, Long value) {
        objects.assertEqual(
                info, getGaugeConservative(AtlasDbMetricNames.LAG_MILLIS, shard).getValue(), value);
    }

    public void hasEnqueuedWritesThoroughEqualTo(long value) {
        objects.assertEqual(
                info, getGaugeThorough(AtlasDbMetricNames.ENQUEUED_WRITES).getValue(), value);
    }

    public void hasEntriesReadThoroughEqualTo(long value) {
        objects.assertEqual(
                info, getGaugeThorough(AtlasDbMetricNames.ENTRIES_READ).getValue(), value);
    }

    public void hasTombstonesPutThoroughEqualTo(long value) {
        objects.assertEqual(
                info, getGaugeThorough(AtlasDbMetricNames.TOMBSTONES_PUT).getValue(), value);
    }

    public void hasAbortedWritesDeletedThoroughEqualTo(long value) {
        objects.assertEqual(
                info,
                getGaugeThorough(AtlasDbMetricNames.ABORTED_WRITES_DELETED).getValue(),
                value);
    }

    public void hasSweepTimestampThoroughEqualTo(long value) {
        objects.assertEqual(info, getGaugeThorough(AtlasDbMetricNames.SWEEP_TS).getValue(), value);
    }

    public void hasLastSweptTimestampThoroughEqualTo(long value) {
        objects.assertEqual(
                info, getGaugeThorough(AtlasDbMetricNames.LAST_SWEPT_TS).getValue(), value);
    }

    public void hasMillisSinceLastSweptThoroughEqualTo(Long value) {
        hasMillisSinceLastSweptThoroughForShardEqualTo(-1, value);
    }

    public void hasMillisSinceLastSweptThoroughForShardEqualTo(int shard, Long value) {
        objects.assertEqual(
                info, getGaugeThorough(AtlasDbMetricNames.LAG_MILLIS, shard).getValue(), value);
    }

    public void containsEntriesReadInBatchConservative(long... outcomes) {
        arrays.assertContainsExactlyInAnyOrder(
                info, getBatchSizeSnapshotConservative().getValues(), outcomes);
    }

    public void containsEntriesReadInBatchThorough(long... outcomes) {
        arrays.assertContainsExactlyInAnyOrder(
                info, getBatchSizeSnapshotThorough().getValues(), outcomes);
    }

    public void hasEntriesReadInBatchMeanConservativeEqualTo(double value) {
        doubles.assertIsCloseTo(info, getEntriesReadInBatchMeanConservative(), value, Offset.offset(0.1));
    }

    public void hasEntriesReadInBatchMeanThoroughEqualTo(double value) {
        doubles.assertIsCloseTo(info, getEntriesReadInBatchMeanThorough(), value, Offset.offset(0.1));
    }

    public void hasNotRegisteredLegacyOutcome(SweepOutcome outcome) {
        objects.assertNull(info, getGaugeForLegacyOutcome(outcome));
    }

    public void hasLegacyOutcomeEqualTo(SweepOutcome outcome, long value) {
        objects.assertEqual(info, getGaugeForLegacyOutcome(outcome).getValue(), value);
    }

    public void hasTargetedOutcomeEqualTo(SweeperStrategy strategy, SweepOutcome outcome, Long value) {
        objects.assertEqual(info, getGaugeForTargetedOutcome(strategy, outcome).getValue(), value);
    }

    public void hasNotRegisteredTargetedOutcome(SweeperStrategy strategy, SweepOutcome outcome) {
        objects.assertNull(info, getGaugeForTargetedOutcome(strategy, outcome));
    }

    private <N> Gauge<N> getGaugeConservative(String name) {
        return getGaugeForTargetedSweep(AtlasDbMetricNames.TAG_CONSERVATIVE, name);
    }

    private <N> Gauge<N> getGaugeConservative(String name, int shard) {
        Map<String, String> tags = ImmutableMap.of(
                AtlasDbMetricNames.TAG_STRATEGY,
                AtlasDbMetricNames.TAG_CONSERVATIVE,
                AtlasDbMetricNames.TAG_SHARD,
                Integer.toString(shard));
        return getGauge("targetedSweepProgress", name, tags);
    }

    private <N> Gauge<N> getGaugeThorough(String name) {
        return getGaugeForTargetedSweep(AtlasDbMetricNames.TAG_THOROUGH, name);
    }

    private <N> Gauge<N> getGaugeThorough(String name, int shard) {
        Map<String, String> tags = ImmutableMap.of(
                AtlasDbMetricNames.TAG_STRATEGY,
                AtlasDbMetricNames.TAG_THOROUGH,
                AtlasDbMetricNames.TAG_SHARD,
                Integer.toString(shard));
        return getGauge("targetedSweepProgress", name, tags);
    }

    private <N> Gauge<N> getGaugeForTargetedSweep(String strategy, String name) {
        Map<String, String> tag = ImmutableMap.of(AtlasDbMetricNames.TAG_STRATEGY, strategy);
        return getGauge("targetedSweepProgress", name, tag);
    }

    private Gauge<Long> getGaugeForLegacyOutcome(SweepOutcome outcome) {
        return getGauge(
                BackgroundSweeperImpl.class,
                AtlasDbMetricNames.SWEEP_OUTCOME,
                ImmutableMap.of(AtlasDbMetricNames.TAG_OUTCOME, outcome.name()));
    }

    private Gauge<Long> getGaugeForTargetedOutcome(SweeperStrategy strategy, SweepOutcome outcome) {
        return getGauge(
                TargetedSweepMetrics.class,
                AtlasDbMetricNames.SWEEP_OUTCOME,
                ImmutableMap.of(
                        AtlasDbMetricNames.TAG_OUTCOME,
                        outcome.name(),
                        AtlasDbMetricNames.TAG_STRATEGY,
                        getTagForStrategy(strategy)));
    }

    private Double getEntriesReadInBatchMeanConservative() {
        Gauge<Double> gauge = getGaugeConservative(AtlasDbMetricNames.BATCH_SIZE_MEAN);
        return gauge.getValue();
    }

    private Double getEntriesReadInBatchMeanThorough() {
        Gauge<Double> gauge = getGaugeThorough(AtlasDbMetricNames.BATCH_SIZE_MEAN);
        return gauge.getValue();
    }

    private Snapshot getBatchSizeSnapshotConservative() {
        Gauge<Double> gauge = getGaugeConservative(AtlasDbMetricNames.BATCH_SIZE_MEAN);
        return ((SlidingWindowMeanGauge) gauge).getSnapshot();
    }

    private Snapshot getBatchSizeSnapshotThorough() {
        Gauge<Double> gauge = getGaugeThorough(AtlasDbMetricNames.BATCH_SIZE_MEAN);
        return ((SlidingWindowMeanGauge) gauge).getSnapshot();
    }

    private static String getTagForStrategy(SweeperStrategy strategy) {
        return strategy == SweeperStrategy.CONSERVATIVE
                ? AtlasDbMetricNames.TAG_CONSERVATIVE
                : AtlasDbMetricNames.TAG_THOROUGH;
    }

    private <T, N> Gauge<N> getGauge(Class<T> metricClass, String name, Map<String, String> tag) {
        MetricName metricName = getMetricName(metricClass, name, tag);

        return (Gauge<N>) metrics.getTaggedRegistry().getMetrics().get(metricName);
    }

    private <T, N> Gauge<N> getGauge(String metricNamespace, String name, Map<String, String> tag) {
        MetricName metricName = MetricName.builder()
                .safeName(metricNamespace + "." + name)
                .safeTags(tag)
                .build();

        return (Gauge<N>) metrics.getTaggedRegistry().getMetrics().get(metricName);
    }

    private <T> MetricName getMetricName(Class<T> metricClass, String name, Map<String, String> tag) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(metricClass, name))
                .safeTags(tag)
                .build();
    }
}
