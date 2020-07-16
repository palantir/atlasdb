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

package com.palantir.atlasdb.transaction.impl.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.tritium.metrics.registry.MetricName;

public class ToplistDeltaFilteringTableLevelMetricsControllerTest {
    private final Clock mockClock = mock(Clock.class);
    private final MetricsManager metricsManager = MetricsManagers.createAlwaysSafeAndFilteringForTests();
    private final ToplistDeltaFilteringTableLevelMetricsController controller
            = new ToplistDeltaFilteringTableLevelMetricsController(metricsManager, 3, mockClock);

    @Before
    public void setUpClock() {
        when(mockClock.getTick()).thenReturn(0L);
    }

    @Test
    public void letsSingularMetricsThrough() {
        Counter counter = controller.createAndRegisterCounter(
                Class.class,
                "metricName",
                TableReference.create(Namespace.create("namespace"), "table"));

        counter.inc();
        assertThat(metricsManager.getPublishableMetrics().getMetrics())
                .hasSize(1)
                .containsKey(getMetricName("table"));
    }

    @Test
    public void selectsOnlyHighestMetricsForPublication() {
        List<Counter> counters = IntStream.range(0, 5)
                .mapToObj(index -> controller.createAndRegisterCounter(
                        Class.class,
                        "metricName",
                        TableReference.create(Namespace.create("namespace"), "table" + index)))
                .collect(Collectors.toList());

        IntStream.range(0, 5)
                .forEach(index -> counters.get(index).inc(index));
        assertThat(metricsManager.getPublishableMetrics().getMetrics())
                .containsOnlyKeys(getMetricName("table2"), getMetricName("table3"), getMetricName("table4"));
    }

    private static MetricName getMetricName(String tableName) {
        return MetricName.builder().safeName(Class.class.getName() + ".metricName")
                .safeTags(ImmutableMap.of("tableName", tableName))
                .build();
    }

}
