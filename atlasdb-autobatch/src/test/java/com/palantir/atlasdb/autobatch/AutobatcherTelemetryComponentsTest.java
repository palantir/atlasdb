/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.LockFreeExponentiallyDecayingReservoir;
import com.codahale.metrics.Reservoir;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

public final class AutobatcherTelemetryComponentsTest {
    private static final String SAFE_LOGGABLE_PURPOSE = "test-purpose";
    private static final int SIZE = 1001;

    @Test
    public void creationDoesNotReportAnyMetricsIfNoMarkWasTriggered() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry);

        assertWaitTimeMetricsAreNotReported(registry);
        assertRunningTimeMetricsAreNotReported(registry);
        assertWaitTimePercentageMetricsAreNotReported(registry);
        assertTotalTimeMetricsAreNotReported(registry);
    }

    @Test
    public void markWaitingTimeMetricsReportsOnlyWaitingTimeHistogramWithAdditionalPercentiles() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        AutobatcherTelemetryComponents telemetryComponents =
                AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry);

        Long[] waitNanos = createRandomPositiveLongs();
        for (Long waitTime : waitNanos) {
            telemetryComponents.markWaitingTimeMetrics(Duration.ofNanos(waitTime));
        }

        assertWaitTimeMetricsAreReported(registry, waitNanos);
        assertRunningTimeMetricsAreNotReported(registry);
        assertWaitTimePercentageMetricsAreNotReported(registry);
        assertTotalTimeMetricsAreNotReported(registry);
    }

    @Test
    public void markWaitingTimeAndRunningTimeReportsAllMetricsWithAdditionalPercentiles() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        AutobatcherTelemetryComponents telemetryComponents =
                AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry);

        Long[] waitNanos = createRandomPositiveLongs();
        Long[] runningNanos = createRandomPositiveLongs();
        for (int index = 0; index < SIZE; index++) {
            telemetryComponents.markWaitingTimeAndRunningTimeMetrics(
                    Duration.ofNanos(waitNanos[index]), Duration.ofNanos(runningNanos[index]));
        }

        assertWaitTimeMetricsAreReported(registry, waitNanos);
        assertWaitTimePercentageMetricsAreReported(registry, computeWaitPercentages(waitNanos, runningNanos));
        assertRunningTimeMetricsAreReported(registry, runningNanos);
        assertTotalTimeMetricsAreReported(registry, computeTotals(waitNanos, runningNanos));
    }

    @Test
    public void markWaitingTimeAndRunningTimeDoesNotThrowAndDoesNotReportWaitTimePercentageWhenTotalTimeIsZero() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        AutobatcherTelemetryComponents telemetryComponents =
                AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry);
        telemetryComponents.markWaitingTimeAndRunningTimeMetrics(Duration.ZERO, Duration.ZERO);
        assertWaitTimePercentageMetricsAreNotReported(registry);
    }

    private void assertWaitTimeMetricsAreReported(TaggedMetricRegistry registry, Long[] values) {
        assertThat(getWaitTimeHistogram(registry).getSnapshot().getValues()).containsExactlyInAnyOrder(values);
        assertThat(getWaitTimeP1Gauge(registry).getValue()).isEqualTo(calculatePercentile(values, 1));
        assertThat(getWaitTimeP50Gauge(registry).getValue()).isEqualTo(calculatePercentile(values, 50));
    }

    private void assertWaitTimePercentageMetricsAreReported(TaggedMetricRegistry registry, Long[] values) {
        assertThat(getWaitTimePercentageHistogram(registry).getSnapshot().getValues())
                .containsExactlyInAnyOrder(values);
        assertThat(getWaitTimePercentageP1Gauge(registry).getValue()).isEqualTo(calculatePercentile(values, 1));
        assertThat(getWaitTimePercentageP50Gauge(registry).getValue()).isEqualTo(calculatePercentile(values, 50));
    }

    private void assertRunningTimeMetricsAreReported(TaggedMetricRegistry registry, Long[] values) {
        assertThat(getRunningTimeHistogram(registry).getSnapshot().getValues()).containsExactlyInAnyOrder(values);
        assertThat(getRunningTimeP1Gauge(registry).getValue()).isEqualTo(calculatePercentile(values, 1));
        assertThat(getRunningTimeP50Gauge(registry).getValue()).isEqualTo(calculatePercentile(values, 50));
    }

    private void assertTotalTimeMetricsAreReported(TaggedMetricRegistry registry, Long[] values) {
        assertThat(getTotalTimeHistogram(registry).getSnapshot().getValues()).containsExactlyInAnyOrder(values);
        assertThat(getTotalTimeP1Gauge(registry).getValue()).isEqualTo(calculatePercentile(values, 1));
        assertThat(getTotalTimeP50Gauge(registry).getValue()).isEqualTo(calculatePercentile(values, 50));
    }

    private void assertWaitTimeMetricsAreNotReported(TaggedMetricRegistry registry) {
        assertThat(getWaitTimeHistogram(registry)).isNull();
    }

    private void assertRunningTimeMetricsAreNotReported(TaggedMetricRegistry registry) {
        assertThat(getRunningTimeHistogram(registry)).isNull();
    }

    private void assertWaitTimePercentageMetricsAreNotReported(TaggedMetricRegistry registry) {
        assertThat(getWaitTimePercentageHistogram(registry)).isNull();
    }

    private void assertTotalTimeMetricsAreNotReported(TaggedMetricRegistry registry) {
        assertThat(getTotalTimeHistogram(registry)).isNull();
    }

    private static Histogram getWaitTimeHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.waitTimeNanosMetricName());
    }

    private static Gauge<Double> getWaitTimeP1Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.waitTimeNanosP1MetricName());
    }

    private static Gauge<Double> getWaitTimeP50Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.waitTimeNanosMedianMetricName());
    }

    private static Histogram getRunningTimeHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.runningTimeNanosMetricName());
    }

    private static Gauge<Double> getRunningTimeP1Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.runningTimeNanosP1MetricName());
    }

    private static Gauge<Double> getRunningTimeP50Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.runningTimeNanosMedianMetricName());
    }

    private static Histogram getTotalTimeHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.totalTimeNanosMetricName());
    }

    private static Gauge<Double> getTotalTimeP1Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.totalTimeNanosP1MetricName());
    }

    private static Gauge<Double> getTotalTimeP50Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.totalTimeNanosMedianMetricName());
    }

    private static Histogram getWaitTimePercentageHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.waitTimePercentageMetricName());
    }

    private static Gauge<Double> getWaitTimePercentageP1Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.waitTimePercentageP1MetricName());
    }

    private static Gauge<Double> getWaitTimePercentageP50Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.waitTimePercentageMedianMetricName());
    }

    private Long[] computeWaitPercentages(Long[] waitNanos, Long[] runningNanos) {
        int length = waitNanos.length;
        List<Long> percentagesList = new ArrayList<>();
        for (int index = 0; index < length; index++) {
            long totalNanos = waitNanos[index] + runningNanos[index];
            if (totalNanos > 0) {
                percentagesList.add((100 * waitNanos[index]) / totalNanos);
            }
        }
        return percentagesList.toArray(new Long[0]);
    }

    private Long[] computeTotals(Long[] waitNanos, Long[] runningNanos) {
        int length = waitNanos.length;
        Long[] totalNanos = new Long[length];
        for (int index = 0; index < length; index++) {
            totalNanos[index] = waitNanos[index] + runningNanos[index];
        }
        return totalNanos;
    }

    private static Long[] createRandomPositiveLongs() {
        Long[] longs = new Long[SIZE];
        for (int index = 0; index < SIZE; index++) {
            longs[index] = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
        }
        return longs;
    }

    private static double calculatePercentile(Long[] values, double percentile) {
        Reservoir reservoir = LockFreeExponentiallyDecayingReservoir.builder().build();
        for (long value : values) {
            reservoir.update(value);
        }
        return reservoir.getSnapshot().getValue(percentile / 100.0);
    }
}
