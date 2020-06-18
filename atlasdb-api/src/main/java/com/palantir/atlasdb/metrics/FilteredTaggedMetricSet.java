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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import com.codahale.metrics.Metric;
import com.palantir.common.streams.KeyedStream;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricSet;

public class FilteredTaggedMetricSet implements TaggedMetricSet {
    private final TaggedMetricSet unfiltered;
    private final Map<MetricName, List<MetricPublicationFilter>> singleMetricFilters;
    private final Refreshable<Boolean> performFiltering;

    public FilteredTaggedMetricSet(
            TaggedMetricSet unfiltered,
            Map<MetricName, List<MetricPublicationFilter>> singleMetricFilters,
            Refreshable<Boolean> performFiltering) {
        this.unfiltered = unfiltered;
        this.singleMetricFilters = singleMetricFilters;
        this.performFiltering = performFiltering;
    }

    @Override
    public Map<MetricName, Metric> getMetrics() {
        if (performFiltering.get()) {
            return KeyedStream.stream(unfiltered.getMetrics())
                    .filterEntries((name, metric) -> Optional.ofNullable(singleMetricFilters.get(name))
                            .map(FilteredTaggedMetricSet::allFiltersMatch)
                            .orElse(true))
                    .collectToMap();
        }
        return unfiltered.getMetrics();
    }

    @Override
    public void forEachMetric(BiConsumer<MetricName, Metric> consumer) {
        boolean filter = performFiltering.get();
        if (filter) {
            unfiltered.forEachMetric((name, metric) -> {
                List<MetricPublicationFilter> relevantFilters = singleMetricFilters.get(name);
                if (relevantFilters == null || allFiltersMatch(relevantFilters)) {
                    consumer.accept(name, metric);
                }
            });
        } else {
            unfiltered.forEachMetric(consumer);
        }
    }

    private static boolean allFiltersMatch(List<MetricPublicationFilter> relevantFilters) {
        return relevantFilters.stream().allMatch(MetricPublicationFilter::shouldPublish);
    }
}
