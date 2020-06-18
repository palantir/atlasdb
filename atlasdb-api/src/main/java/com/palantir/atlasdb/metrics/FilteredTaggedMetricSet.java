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

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import com.codahale.metrics.Metric;
import com.palantir.common.streams.KeyedStream;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricSet;

public class FilteredTaggedMetricSet implements TaggedMetricSet {
    private final TaggedMetricSet unfiltered;
    private final Map<MetricName, MetricPublicationFilter> singleMetricFilters;

    public FilteredTaggedMetricSet(TaggedMetricSet unfiltered,
            Map<MetricName, MetricPublicationFilter> singleMetricFilters) {
        this.unfiltered = unfiltered;
        this.singleMetricFilters = singleMetricFilters;
    }

    @Override
    public Map<MetricName, Metric> getMetrics() {
        return KeyedStream.stream(unfiltered.getMetrics())
                .filterEntries((name, metric) -> Optional.ofNullable(singleMetricFilters.get(name))
                        .map(MetricPublicationFilter::shouldPublish)
                        .orElse(true))
                .collectToMap();
    }

    @Override
    public void forEachMetric(BiConsumer<MetricName, Metric> consumer) {
        unfiltered.forEachMetric((name, metric) -> {
            MetricPublicationFilter relevantFilter = singleMetricFilters.get(name);
            if (relevantFilter == null || relevantFilter.shouldPublish()) {
                consumer.accept(name, metric);
            }
        });
    }
}
