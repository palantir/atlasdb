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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import one.util.streamex.EntryStream;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Test;

public final class TopListFilteredCounterTest {
    private static final Duration INITIAL_DELAY = Duration.ofMillis(10);
    private static final Duration RESET_INTERVAL = Duration.ofMillis(100);

    private static final String TAG_1 = "tag1";
    private static final String TAG_2 = "tag2";
    private static final String TAG_3 = "tag3";
    private static final String TAG_4 = "tag4";

    private static final MetricName METRIC_NAME_1 = createMetricNameForTag(TAG_1);
    private static final MetricName METRIC_NAME_2 = createMetricNameForTag(TAG_2);
    private static final MetricName METRIC_NAME_3 = createMetricNameForTag(TAG_3);
    private static final MetricName METRIC_NAME_4 = createMetricNameForTag(TAG_4);

    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    private final TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();

    @Test
    public void nothingIsReportedBeforeInitialDelay() {
        TopListFilteredCounter<String> counter =
                createCounterWithMaxSizeTwo(TopListFilteredCounterTest::createMetricNameForTag);

        counter.inc(TAG_1, 1);
        counter.inc(TAG_2, 2);

        tick(INITIAL_DELAY.minusMillis(1));
        assertThat(registry.getMetrics()).isEmpty();
    }

    @Test
    public void topMetricsAreReportedAfterInitialDelay() {
        TopListFilteredCounter<String> counter =
                createCounterWithMaxSizeTwo(TopListFilteredCounterTest::createMetricNameForTag);

        counter.inc(TAG_1, 5);
        counter.inc(TAG_2, 2);
        counter.inc(TAG_3, 3);
        counter.inc(TAG_4, 0);
        tick(INITIAL_DELAY);

        assertThatAllMetricAreCountersAndHaveValues(METRIC_NAME_1, 5, METRIC_NAME_3, 3);
    }

    @Test
    public void topMetricsAreUpdatedPeriodicallyPickingTheMaximumValuesUpToMaxSizeCount() {
        TopListFilteredCounter<String> counter =
                createCounterWithMaxSizeTwo(TopListFilteredCounterTest::createMetricNameForTag);

        counter.inc(TAG_1, 5);
        counter.inc(TAG_2, 1);
        counter.inc(TAG_3, 3);
        counter.inc(TAG_4, 0);
        tick(INITIAL_DELAY);
        counter.inc(TAG_1, 2);
        counter.inc(TAG_4, 10);
        tick(RESET_INTERVAL);
        assertThatAllMetricAreCountersAndHaveValues(METRIC_NAME_1, 7, METRIC_NAME_4, 10);

        counter.inc(TAG_3, 5);
        tick(RESET_INTERVAL.minusMillis(1));
        // no updates until the full period has passed
        assertThatAllMetricAreCountersAndHaveValues(METRIC_NAME_1, 7, METRIC_NAME_4, 10);
        tick(Duration.ofMillis(1));
        assertThatAllMetricAreCountersAndHaveValues(METRIC_NAME_3, 8, METRIC_NAME_4, 10);
    }

    @Test
    public void topMetricsUpdatingIsResilientToTransientException() {
        TopListFilteredCounter<String> counter =
                createCounterWithMaxSizeTwo(new ThrowOnFirstInvocationMetricNameCreator());

        counter.inc(TAG_1, 5);
        counter.inc(TAG_3, 1);
        tick(INITIAL_DELAY);
        counter.inc(TAG_2, 2);
        tick(RESET_INTERVAL);

        assertThatAllMetricAreCountersAndHaveValues(METRIC_NAME_1, 5, METRIC_NAME_2, 2);
    }

    private void assertThatAllMetricAreCountersAndHaveValues(
            MetricName metric1, long value1, MetricName metric2, long value2) {
        Map<MetricName, Metric> metrics = registry.getMetrics();
        assertThatAllMetricsAreCounters(metrics);
        assertThat(castAllMetricsToCounters(metrics))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        metric1, value1,
                        metric2, value2));
    }

    private TopListFilteredCounter<String> createCounterWithMaxSizeTwo(
            Function<String, MetricName> tagToMetricNameFunction) {
        return TopListFilteredCounter.create(
                2,
                INITIAL_DELAY,
                RESET_INTERVAL,
                tagToMetricNameFunction,
                Comparator.naturalOrder(),
                registry,
                scheduler);
    }

    private void tick(Duration duration) {
        // it's important to use millisecond and nothing more granular
        // as the scheduler uses that unit for internal bookkeeping
        scheduler.tick(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static void assertThatAllMetricsAreCounters(Map<MetricName, Metric> metrics) {
        assertThat(metrics).allSatisfy((unused, metric) -> assertThat(metric).isInstanceOf(Counter.class));
    }

    private static Map<MetricName, Long> castAllMetricsToCounters(Map<MetricName, Metric> metrics) {
        return EntryStream.of(metrics)
                .mapValues(Counter.class::cast)
                .mapValues(Counter::getCount)
                .toMap();
    }

    private static MetricName createMetricNameForTag(String tag) {
        return MetricName.builder().safeName("metric").putSafeTags("tag", tag).build();
    }

    private static final class ThrowOnFirstInvocationMetricNameCreator implements Function<String, MetricName> {
        private final AtomicBoolean hasRan = new AtomicBoolean(false);

        @Override
        public MetricName apply(String s) {
            if (hasRan.compareAndSet(false, true)) {
                throw new RuntimeException("Weird stubbed first time failure");
            }
            return createMetricNameForTag(s);
        }
    }
}
