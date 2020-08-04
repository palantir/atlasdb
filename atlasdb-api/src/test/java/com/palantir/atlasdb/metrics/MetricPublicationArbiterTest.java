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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.palantir.common.streams.KeyedStream;
import com.palantir.tritium.metrics.registry.MetricName;

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
                METRIC_NAME_1, ImmutableSet.of(() -> false),
                METRIC_NAME_2, ImmutableSet.of(() -> true)));
        assertThat(arbiter.test(METRIC_NAME_1)).isFalse();
        assertThat(arbiter.test(METRIC_NAME_2)).isTrue();
    }

    @Test
    public void metricsWithMultipleFiltersAreAcceptedOnlyIfAllFiltersPermit() {
        MetricPublicationArbiter arbiter = createArbiter(ImmutableMap.of(
                METRIC_NAME_1, ImmutableSet.of(() -> true, () -> false, () -> true),
                METRIC_NAME_2, ImmutableSet.of(() -> true, () -> true, () -> true)));
        assertThat(arbiter.test(METRIC_NAME_1)).isFalse();
        assertThat(arbiter.test(METRIC_NAME_2)).isTrue();
    }

    @Test
    public void canAddFilters() {
        MetricPublicationArbiter arbiter = new MetricPublicationArbiter(Maps.newConcurrentMap());
        assertThat(arbiter.test(METRIC_NAME_1)).isTrue();

        arbiter.registerMetricsFilter(METRIC_NAME_1, () -> false);
        assertThat(arbiter.test(METRIC_NAME_1)).isFalse();
    }

    @Test
    public void exceptionTreatedAsNotFiltered() {
        MetricPublicationArbiter arbiter = createArbiter(ImmutableMap.of(
                METRIC_NAME_1, ImmutableSet.of(() -> {
                    throw new RuntimeException("boo");
                })));
        assertThat(arbiter.test(METRIC_NAME_1)).isTrue();
    }

    @Test
    public void rejectsMetricWithDefinitivelyFalseFilterEvenWithExceptions() {
        MetricPublicationArbiter arbiter = createArbiter(ImmutableMap.of(
                METRIC_NAME_1, ImmutableSet.of(() -> {
                    throw new RuntimeException("boo");
                },
                        () -> true,
                        () -> false)));
        assertThat(arbiter.test(METRIC_NAME_1)).isFalse();
    }

    private static MetricPublicationArbiter createArbiter(Map<MetricName, Set<MetricPublicationFilter>> filters) {
        return new MetricPublicationArbiter(KeyedStream.stream(filters)
                .map(filtersForMetric -> filtersForMetric.stream()
                        .map(MetricPublicationArbiter.DeduplicatingFilterHolder::new)
                        .collect(Collectors.toSet()))
                .collectToMap());
    }
}
