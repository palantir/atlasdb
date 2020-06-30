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
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Metric;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricSet;

/**
 * Combines two {@link TaggedMetricSet}s. It is expected that the metric names present from the two sets are disjoint.
 */
public class DisjointUnionTaggedMetricSet implements TaggedMetricSet {
    private static final Logger log = LoggerFactory.getLogger(DisjointUnionTaggedMetricSet.class);

    private final TaggedMetricSet first;
    private final TaggedMetricSet second;

    public DisjointUnionTaggedMetricSet(TaggedMetricSet first, TaggedMetricSet second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public Map<MetricName, Metric> getMetrics() {
        Map<MetricName, Metric> metrics = Maps.newHashMap();
        first.forEachMetric(metrics::put);
        second.forEachMetric((metricName, metric) -> {
            if (metrics.containsKey(metricName)) {
                log.warn("Found duplicate metrics in a disjoint-union set. This is likely to be a product bug."
                        + " In this case, which metric will actually be reported is nondeterministic.",
                        SafeArg.of("metricName", metricName));
            }
            metrics.put(metricName, metric);
        });
        return metrics;
    }

    @Override
    public void forEachMetric(BiConsumer<MetricName, Metric> consumer) {
        first.forEachMetric(consumer);
        second.forEachMetric(consumer);
    }

}
