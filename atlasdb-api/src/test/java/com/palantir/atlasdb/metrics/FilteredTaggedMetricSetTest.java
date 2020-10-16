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

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Objects;
import org.junit.Test;

public class FilteredTaggedMetricSetTest {
    private static final MetricName METRIC_NAME_1 = MetricName.builder()
            .safeName("com.palantir.atlasdb.metrics.metrics.p99")
            .build();
    private static final MetricName METRIC_NAME_2 = MetricName.builder()
            .safeName("com.palantir.atlasdb.metrics.executor.count")
            .safeTags(ImmutableMap.of("tag", "value"))
            .build();

    private final TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();

    @Test
    public void reportsAllMetricsIfNotPerformingFiltering() {
        Timer timer = taggedMetricRegistry.timer(METRIC_NAME_1);

        FilteredTaggedMetricSet filteredTaggedMetricSet =
                new FilteredTaggedMetricSet(taggedMetricRegistry, $ -> false, Refreshable.only(false));
        assertThat(filteredTaggedMetricSet.getMetrics())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(METRIC_NAME_1, timer));
    }

    @Test
    public void usesFiltersToDecideIfPerformingFiltering() {
        taggedMetricRegistry.timer(METRIC_NAME_1);
        Timer timer = taggedMetricRegistry.timer(METRIC_NAME_2);

        FilteredTaggedMetricSet filteredTaggedMetricSet = new FilteredTaggedMetricSet(
                taggedMetricRegistry, name -> Objects.equals(name, METRIC_NAME_2), Refreshable.only(true));
        assertThat(filteredTaggedMetricSet.getMetrics())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(METRIC_NAME_2, timer));
    }
}
