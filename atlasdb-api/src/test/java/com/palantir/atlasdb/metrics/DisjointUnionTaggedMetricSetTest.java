/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class DisjointUnionTaggedMetricSetTest {
    private static final MetricName METRIC_NAME_1 = MetricName.builder()
            .safeName("com.palantir.atlasdb.metrics.lockAndGetHeldMetrics.p99")
            .build();
    private static final MetricName METRIC_NAME_2 = MetricName.builder()
            .safeName("com.palantir.atlasdb.metrics.releaseAllMetrics.p99")
            .build();
    private static final MetricName METRIC_NAME_3 = MetricName.builder()
            .safeName("com.palantir.atlasdb.metrics.logMetricsLogs.p99")
            .build();

    private final TaggedMetricRegistry registry1 = new DefaultTaggedMetricRegistry();
    private final TaggedMetricRegistry registry2 = new DefaultTaggedMetricRegistry();
    private final DisjointUnionTaggedMetricSet disjointUnionTaggedMetricSet =
            new DisjointUnionTaggedMetricSet(registry1, registry2);

    @Test
    public void unionOfEmptyMetricSetsIsEmpty() {
        assertThat(disjointUnionTaggedMetricSet.getMetrics()).isEmpty();
    }

    @Test
    public void metricsInEachSetAreReflectedInUnion() {
        Timer timer = registry1.timer(METRIC_NAME_1);
        Histogram histogram = registry2.histogram(METRIC_NAME_2);
        Meter meter = registry1.meter(METRIC_NAME_3);

        assertThat(disjointUnionTaggedMetricSet.getMetrics())
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(METRIC_NAME_1, timer, METRIC_NAME_2, histogram, METRIC_NAME_3, meter));
    }

    @Test
    public void deregisteringMetricsIsReflectedInUnion() {
        registry1.timer(METRIC_NAME_1);
        Histogram histogram = registry2.histogram(METRIC_NAME_2);
        Meter meter = registry1.meter(METRIC_NAME_3);
        registry1.remove(METRIC_NAME_1);

        assertThat(disjointUnionTaggedMetricSet.getMetrics())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(METRIC_NAME_2, histogram, METRIC_NAME_3, meter));
    }

    @Test
    public void doesNotReplaceIfConflictDetected() {
        Timer timer = registry1.timer(METRIC_NAME_1);
        registry2.timer(METRIC_NAME_1);

        assertThat(disjointUnionTaggedMetricSet.getMetrics())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(METRIC_NAME_1, timer));
    }

    @Test
    public void forEachWorksOnMetricsFromBothRegistries() {
        Timer timer = registry1.timer(METRIC_NAME_1);
        Histogram histogram = registry2.histogram(METRIC_NAME_2);
        Meter meter = registry1.meter(METRIC_NAME_3);

        Map<MetricName, Metric> stateOfTheWorld = new HashMap<>();
        disjointUnionTaggedMetricSet.forEachMetric(stateOfTheWorld::put);
        assertThat(stateOfTheWorld)
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(METRIC_NAME_1, timer, METRIC_NAME_2, histogram, METRIC_NAME_3, meter));
    }
}
