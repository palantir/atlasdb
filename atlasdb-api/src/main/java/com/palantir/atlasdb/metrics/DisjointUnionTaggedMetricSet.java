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

import com.codahale.metrics.Metric;
import com.google.common.collect.Maps;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricSet;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Combines two {@link TaggedMetricSet}s. It is expected that the metric names present from the two sets are disjoint.
 */
public class DisjointUnionTaggedMetricSet implements TaggedMetricSet {

    private static final SafeLogger log = SafeLoggerFactory.get(DisjointUnionTaggedMetricSet.class);

    private final TaggedMetricSet first;
    private final TaggedMetricSet second;

    public DisjointUnionTaggedMetricSet(TaggedMetricSet first, TaggedMetricSet second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public Map<MetricName, Metric> getMetrics() {
        Map<MetricName, Metric> firstMetrics = first.getMetrics();
        Map<MetricName, Metric> secondMetrics = second.getMetrics();

        Map<MetricName, Metric> metrics = Maps.newHashMapWithExpectedSize(firstMetrics.size() + secondMetrics.size());
        firstMetrics.forEach(metrics::putIfAbsent);
        secondMetrics.forEach((metricName, metric) -> {
            if (metrics.putIfAbsent(metricName, metric) != null) {
                log.warn("Detected duplicate metric name", SafeArg.of("metricName", metricName));
            }
        });

        return Collections.unmodifiableMap(metrics);
    }

    @Override
    public void forEachMetric(BiConsumer<MetricName, Metric> consumer) {
        first.forEachMetric(consumer);
        second.forEachMetric(consumer);
    }
}
