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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.common.streams.KeyedStream;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.junit.Test;

public class MetricPublicationArbiterTest {
    private static final MetricName METRIC_NAME_1 = MetricName.builder()
            .safeName("com.palantir.atlasdb.metrics.metrics.p99")
            .build();
    private static final MetricName METRIC_NAME_2 = MetricName.builder()
            .safeName("com.palantir.atlasdb.metrics.executor.count")
            .safeTags(ImmutableMap.of("tag", "value"))
            .build();

    @Test
    public void metricsWithNoFilterAreAccepted() {
        MetricPublicationArbiter arbiter = new MetricPublicationArbiter(ImmutableMap.of());
        assertThat(arbiter.test(METRIC_NAME_1)).isTrue();
        assertThat(arbiter.test(METRIC_NAME_2)).isTrue();
    }

    @Test
    public void metricsWithOneFilterAreFiltered() {
        MetricPublicationArbiter arbiter = createArbiter(ImmutableMap.of(
                METRIC_NAME_1, ImmutableSet.of(falseFilter()),
                METRIC_NAME_2, ImmutableSet.of(trueFilter())));
        assertThat(arbiter.test(METRIC_NAME_1)).isFalse();
        assertThat(arbiter.test(METRIC_NAME_2)).isTrue();
    }

    @Test
    public void metricsWithMultipleFiltersAreAcceptedOnlyIfAllFiltersPermit() {
        MetricPublicationArbiter arbiter = createArbiter(ImmutableMap.of(
                METRIC_NAME_1, ImmutableSet.of(trueFilter(), falseFilter(), trueFilter()),
                METRIC_NAME_2, ImmutableSet.of(trueFilter(), trueFilter(), trueFilter())));
        assertThat(arbiter.test(METRIC_NAME_1)).isFalse();
        assertThat(arbiter.test(METRIC_NAME_2)).isTrue();
    }

    @Test
    public void canAddFilters() {
        MetricPublicationArbiter arbiter = new MetricPublicationArbiter(new ConcurrentHashMap<>());
        assertThat(arbiter.test(METRIC_NAME_1)).isTrue();

        arbiter.registerMetricsFilter(METRIC_NAME_1, falseFilter());
        assertThat(arbiter.test(METRIC_NAME_1)).isFalse();
    }

    @Test
    public void exceptionTreatedAsNotFiltered() {
        MetricPublicationArbiter arbiter =
                createArbiter(ImmutableMap.of(METRIC_NAME_1, ImmutableSet.of(throwingFilter())));
        assertThat(arbiter.test(METRIC_NAME_1)).isTrue();
    }

    @Test
    public void rejectsMetricWithDefinitivelyFalseFilterEvenWithExceptions() {
        MetricPublicationArbiter arbiter = createArbiter(
                ImmutableMap.of(METRIC_NAME_1, ImmutableSet.of(throwingFilter(), trueFilter(), falseFilter())));
        assertThat(arbiter.test(METRIC_NAME_1)).isFalse();
    }

    @Test
    public void deduplicatesFilters() {
        Map<MetricName, Set<MetricPublicationArbiter.DeduplicatingFilterHolder>> filters = new HashMap<>();
        MetricPublicationArbiter arbiter = new MetricPublicationArbiter(filters);
        for (int i = 0; i < 100; i++) {
            int index = i;
            // not using pre-defined filters because otherwise Object.equals would work without effort
            arbiter.registerMetricsFilter(METRIC_NAME_1, () -> index == 1);
            arbiter.registerMetricsFilter(METRIC_NAME_1, () -> index == 2);
        }
        assertThat(filters.get(METRIC_NAME_1)).hasSize(2);
    }

    private static MetricPublicationFilter trueFilter() {
        return () -> true;
    }

    private static MetricPublicationFilter falseFilter() {
        return () -> false;
    }

    private static MetricPublicationFilter throwingFilter() {
        return () -> {
            throw new RuntimeException("boo");
        };
    }

    private static MetricPublicationArbiter createArbiter(Map<MetricName, Set<MetricPublicationFilter>> filters) {
        return new MetricPublicationArbiter(KeyedStream.stream(filters)
                .map(filtersForMetric -> filtersForMetric.stream()
                        .<MetricPublicationArbiter.DeduplicatingFilterHolder>map(ImmutableDeduplicatingFilterHolder::of)
                        .collect(Collectors.toSet()))
                .collectToMap());
    }
}
