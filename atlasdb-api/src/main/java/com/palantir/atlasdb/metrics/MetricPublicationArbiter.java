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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.immutables.value.Value;

/**
 * Indicates whether metrics should be published. If {@link #test(MetricName)} returns true, that means we think
 * the metric involved SHOULD be published.
 */
public class MetricPublicationArbiter implements Predicate<MetricName> {
    private static final SafeLogger log = SafeLoggerFactory.get(MetricPublicationArbiter.class);

    private final Map<MetricName, Set<DeduplicatingFilterHolder>> singleMetricFilters;

    @VisibleForTesting
    MetricPublicationArbiter(Map<MetricName, Set<DeduplicatingFilterHolder>> singleMetricFilters) {
        this.singleMetricFilters = singleMetricFilters;
    }

    public static MetricPublicationArbiter create() {
        return new MetricPublicationArbiter(new ConcurrentHashMap<>());
    }

    @Override
    public boolean test(MetricName metricName) {
        return Optional.ofNullable(singleMetricFilters.get(metricName))
                .map(filters -> allFiltersMatch(metricName, filters))
                .orElse(true);
    }

    public void registerMetricsFilter(MetricName metricName, MetricPublicationFilter filter) {
        singleMetricFilters.merge(
                metricName,
                ImmutableSet.of(ImmutableDeduplicatingFilterHolder.of(filter)),
                (oldFilters, newFilter) -> ImmutableSet.<DeduplicatingFilterHolder>builder()
                        .addAll(oldFilters)
                        .addAll(newFilter)
                        .build());
    }

    private static boolean allFiltersMatch(MetricName metricName, Set<DeduplicatingFilterHolder> relevantFilters) {
        return relevantFilters.stream().allMatch(filter -> safeShouldPublish(metricName, filter.filter()));
    }

    private static boolean safeShouldPublish(MetricName metricName, MetricPublicationFilter filter) {
        try {
            return filter.shouldPublish();
        } catch (RuntimeException e) {
            log.warn(
                    "Exception thrown when attempting to determine whether a metric {} should be published."
                            + " In this case we don't filter out the metric.",
                    SafeArg.of("metricName", metricName),
                    e);
            return true;
        }
    }

    /**
     * Most users will want to define filters without having to think about deduplicating
     * filters across calls (similar to how MetricsRegistry methods are all registerOrGet)
     * so in order to keep the existing ability to define filters with lambdas,
     * define a default method on {@link MetricPublicationFilter} with a reasonable deduplicator
     * and then wrap it here.
     */
    @Value.Immutable
    interface DeduplicatingFilterHolder {
        @Value.Parameter
        @Value.Auxiliary
        MetricPublicationFilter filter();

        @Value.Derived
        default String label() {
            return filter().getLabel();
        }
    }
}
