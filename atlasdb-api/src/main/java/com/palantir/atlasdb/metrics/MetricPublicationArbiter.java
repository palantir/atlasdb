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
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.MetricName;

/**
 * Indicates whether metrics should be published. If {@link #test(MetricName)} returns true, that means we think
 * the metric involved SHOULD be published.
 */
public class MetricPublicationArbiter implements Predicate<MetricName> {
    private static final Logger log = LoggerFactory.getLogger(MetricPublicationArbiter.class);

    private final Map<MetricName, List<MetricPublicationFilter>> singleMetricFilters;

    public MetricPublicationArbiter(
            Map<MetricName, List<MetricPublicationFilter>> singleMetricFilters) {
        this.singleMetricFilters = singleMetricFilters;
    }

    @Override
    public boolean test(MetricName metricName) {
        return Optional.ofNullable(singleMetricFilters.get(metricName))
                .map(filters -> allFiltersMatch(metricName, filters))
                .orElse(true);
    }

    public void registerMetricsFilter(MetricName metricName, MetricPublicationFilter filter) {
        singleMetricFilters.merge(metricName, ImmutableList.of(filter), (oldFilters, newFilter)
                -> ImmutableList.<MetricPublicationFilter>builder().addAll(oldFilters).addAll(newFilter).build());
    }

    private static boolean allFiltersMatch(MetricName metricName, List<MetricPublicationFilter> relevantFilters) {
        return relevantFilters.stream().allMatch(filter -> safeShouldPublish(metricName, filter));
    }

    private static boolean safeShouldPublish(MetricName metricName, MetricPublicationFilter filter) {
        try {
            return filter.shouldPublish();
        } catch (Exception e) {
            log.warn("Exception thrown when attempting to determine whether a metric {} should be published."
                            + " In this case we don't filter out the metric.",
                    SafeArg.of("metricName", metricName),
                    e);
            return true;
        }
    }
}
