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
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public final class AutobatcherTelemetryComponentsTest {

    private static final String SAFE_LOGGABLE_PURPOSE = "test-purpose";

    @Test
    public void markWaitingTimeMetricsReportsWaitingTimeHistogramWithAdditionalPercentiles() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        AutobatcherTelemetryComponents telemetryComponents =
                AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry);

        Long[] markedMillis = new Long[1001];
        for (int i = 0; i <= 1000; i++) {
            markedMillis[i] = (long) i;
            telemetryComponents.markWaitingTimeMetrics(Duration.ofMillis(i));
        }

        assertThat(getWaitTimeHistogram(registry).getSnapshot().getValues()).containsExactlyInAnyOrder(markedMillis);
        assertThat(getWaitTimeP1Gauge(registry).getValue()).isEqualTo(10);
        assertThat(getWaitTimeP5Gauge(registry).getValue()).isEqualTo(50);
        assertThat(getWaitTimeP50Gauge(registry).getValue()).isEqualTo(500);
        assertThat(getWaitTimeP999Gauge(registry).getValue()).isEqualTo(999);
    }

    @Test
    public void markWaitingTimeMetricsDoesNotReportWaitingTimePercentageAndRunningTimeMetrics() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        AutobatcherTelemetryComponents telemetryComponents =
                AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry);

        for (int i = 0; i <= 1000; i++) {
            telemetryComponents.markWaitingTimeMetrics(Duration.ofMillis(i));
        }

        assertThat(getWaitTimePercentageHistogram(registry)).isNull();
        assertThat(getRunningTimeHistogram(registry)).isNull();
    }

    @Test
    public void markWaitingTimeAndRunningTimeReportsWaitingTimeHistogramWithAdditionalPercentiles() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        AutobatcherTelemetryComponents telemetryComponents =
                AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry);

        Long[] markedMillis = new Long[1001];
        for (int i = 0; i <= 1000; i++) {
            markedMillis[i] = (long) i;
            telemetryComponents.markWaitingTimeAndRunningTimeMetrics(Duration.ofMillis(i), Duration.ZERO);
        }

        assertThat(getWaitTimeHistogram(registry).getSnapshot().getValues()).containsExactlyInAnyOrder(markedMillis);
        assertThat(getWaitTimeP1Gauge(registry).getValue()).isEqualTo(10);
        assertThat(getWaitTimeP5Gauge(registry).getValue()).isEqualTo(50);
        assertThat(getWaitTimeP50Gauge(registry).getValue()).isEqualTo(500);
        assertThat(getWaitTimeP999Gauge(registry).getValue()).isEqualTo(999);
    }

    @Test
    public void markWaitingTimeAndRunningTimeReportsRunningTimeHistogramWithAdditionalPercentiles() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        AutobatcherTelemetryComponents telemetryComponents =
                AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry);

        Long[] markedMillis = new Long[1001];
        for (int i = 0; i <= 1000; i++) {
            markedMillis[i] = (long) i;
            telemetryComponents.markWaitingTimeAndRunningTimeMetrics(Duration.ZERO, Duration.ofMillis(i));
        }

        assertThat(getRunningTimeHistogram(registry).getSnapshot().getValues()).containsExactlyInAnyOrder(markedMillis);
        assertThat(getRunningTimeP1Gauge(registry).getValue()).isEqualTo(10);
        assertThat(getRunningTimeP5Gauge(registry).getValue()).isEqualTo(50);
        assertThat(getRunningTimeP50Gauge(registry).getValue()).isEqualTo(500);
        assertThat(getRunningTimeP999Gauge(registry).getValue()).isEqualTo(999);
    }

    @Test
    public void markWaitingTimeAndRunningTimeReportsWaitingTimePercentageHistogramWithAdditionalPercentiles() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        AutobatcherTelemetryComponents telemetryComponents =
                AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry);

        Long[] percentages = new Long[101];
        for (int i = 0; i <= 100; i++) {
            percentages[i] = (long) i;
            telemetryComponents.markWaitingTimeAndRunningTimeMetrics(Duration.ofMillis(i), Duration.ofMillis(100 - i));
        }

        assertThat(getWaitTimePercentageHistogram(registry).getSnapshot().getValues())
                .containsExactlyInAnyOrder(percentages);
        assertThat(getWaitTimePercentageP1Gauge(registry).getValue()).isEqualTo(1);
        assertThat(getWaitTimePercentageP5Gauge(registry).getValue()).isEqualTo(5);
        assertThat(getWaitTimePercentageP50Gauge(registry).getValue()).isEqualTo(50);
        assertThat(getWaitTimePercentageP999Gauge(registry).getValue()).isEqualTo(100);
    }

    @Test
    public void markWaitingTimeAndRunningTimeDoesNotThrowAndDoesNotReportWaitTimePercentageWhenTotalTimeIsZero() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        AutobatcherTelemetryComponents telemetryComponents =
                AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry);
        telemetryComponents.markWaitingTimeAndRunningTimeMetrics(Duration.ZERO, Duration.ZERO);
        assertThat(getWaitTimePercentageHistogram(registry)).isNull();
    }

    private static Histogram getWaitTimeHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.waitTimeMillisMetricName());
    }

    private static Gauge<Double> getWaitTimeP1Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.waitTimeMillisP1MetricName());
    }

    private static Gauge<Double> getWaitTimeP5Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.waitTimeMillisP5MetricName());
    }

    private static Gauge<Double> getWaitTimeP50Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.waitTimeMillisMedianMetricName());
    }

    private static Gauge<Double> getWaitTimeP999Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.waitTimeMillisP999MetricName());
    }

    private static Histogram getRunningTimeHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.runningTimeMillisMetricName());
    }

    private static Gauge<Double> getRunningTimeP1Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.runningTimeMillisP1MetricName());
    }

    private static Gauge<Double> getRunningTimeP5Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.runningTimeMillisP5MetricName());
    }

    private static Gauge<Double> getRunningTimeP50Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.runningTimeMillisMedianMetricName());
    }

    private static Gauge<Double> getRunningTimeP999Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.runningTimeMillisP999MetricName());
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

    private static Gauge<Double> getWaitTimePercentageP5Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.waitTimePercentageP5MetricName());
    }

    private static Gauge<Double> getWaitTimePercentageP50Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.waitTimePercentageMedianMetricName());
    }

    private static Gauge<Double> getWaitTimePercentageP999Gauge(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Gauge<Double>) registry.getMetrics().get(overheadMetrics.waitTimePercentageP999MetricName());
    }
}
