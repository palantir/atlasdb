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

import com.codahale.metrics.Histogram;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher.DisruptorFuture;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public final class DisruptorAutobatcherTest {
    private static final String SAFE_LOGGABLE_PURPOSE = "test-purpose";

    @Test
    public void metricsAreNotReportedWhenRunningIsNotTriggered() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        DisruptorFuture<String> future =
                new DisruptorFuture<>(AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry));
        future.setException(new RuntimeException("Test"));
        assertNoWaitTimeAndRunningTimeMetricsAreProduced(registry);
    }

    @Test
    public void onlyWaitTimeIsReportedWhenRunningIsTriggeredButNoValueWasSet() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        FakeTicker fakeTicker = new FakeTicker();
        DisruptorFuture<String> future = new DisruptorFuture<>(
                fakeTicker, AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry));
        int waitTimeMillis = 10;
        fakeTicker.advance(waitTimeMillis, TimeUnit.MILLISECONDS);
        future.running();
        future.setException(new RuntimeException("Test"));
        assertOnlyWaitTimeMetricsAreProduced(registry, waitTimeMillis);
    }

    @Test
    public void allWaitAndRunningTimeMetricsAreReportedWhenRunningIsTriggeredAndValueWasSet() {
        TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
        FakeTicker fakeTicker = new FakeTicker();
        DisruptorFuture<String> future = new DisruptorFuture<>(
                fakeTicker, AutobatcherTelemetryComponents.create(SAFE_LOGGABLE_PURPOSE, registry));
        long waitTimeMillis = 10;
        fakeTicker.advance(waitTimeMillis, TimeUnit.MILLISECONDS);
        future.running();
        long runningTimeMillis = 90;
        fakeTicker.advance(runningTimeMillis, TimeUnit.MILLISECONDS);
        future.set("Test");
        assertWaitTimeAndRunningTimeMetricsAreProduced(registry, waitTimeMillis, runningTimeMillis);
    }

    private void assertWaitTimeAndRunningTimeMetricsAreProduced(
            TaggedMetricRegistry registry, long waitTimeMillis, long runningTimeMillis) {
        assertThat(getWaitTimeHistogram(registry).getSnapshot().getValues()).containsExactly(waitTimeMillis);
        assertThat(getWaitTimePercentageHistogram(registry).getSnapshot().getValues())
                .containsExactly((100 * waitTimeMillis) / (waitTimeMillis + runningTimeMillis));
        assertThat(getRunningTimeHistogram(registry).getSnapshot().getValues()).containsExactly(runningTimeMillis);
    }

    private void assertNoWaitTimeAndRunningTimeMetricsAreProduced(TaggedMetricRegistry registry) {
        assertThat(getWaitTimeHistogram(registry)).isNull();
        assertThat(getWaitTimePercentageHistogram(registry)).isNull();
        assertThat(getRunningTimeHistogram(registry)).isNull();
    }

    private void assertOnlyWaitTimeMetricsAreProduced(TaggedMetricRegistry registry, int waitTimeMillis) {
        assertThat(getWaitTimeHistogram(registry).getSnapshot().getValues()).containsExactly(waitTimeMillis);
        assertThat(getWaitTimePercentageHistogram(registry)).isNull();
        assertThat(getRunningTimeHistogram(registry)).isNull();
    }

    private static Histogram getWaitTimeHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.waitTimeMillisMetricName());
    }

    private static Histogram getRunningTimeHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.runningTimeMillisMetricName());
    }

    private static Histogram getWaitTimePercentageHistogram(TaggedMetricRegistry registry) {
        AutobatchOverheadMetrics overheadMetrics = AutobatchOverheadMetrics.builder()
                .registry(registry)
                .operationType(SAFE_LOGGABLE_PURPOSE)
                .build();
        return (Histogram) registry.getMetrics().get(overheadMetrics.waitTimePercentageMetricName());
    }
}
