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
import com.google.common.collect.ImmutableMap;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricSet;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Combines two {@link TaggedMetricSet}s. It is expected that the metric names present from the two sets are disjoint.
 */
public class DisjointUnionTaggedMetricSet implements TaggedMetricSet {
    private final TaggedMetricSet first;
    private final TaggedMetricSet second;

    public DisjointUnionTaggedMetricSet(TaggedMetricSet first, TaggedMetricSet second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public Map<MetricName, Metric> getMetrics() {
        return ImmutableMap.<MetricName, Metric>builder()
                .putAll(first.getMetrics())
                .putAll(second.getMetrics())
                .build();
    }

    @Override
    public void forEachMetric(BiConsumer<MetricName, Metric> consumer) {
        first.forEachMetric(consumer);
        second.forEachMetric(consumer);
    }

}
