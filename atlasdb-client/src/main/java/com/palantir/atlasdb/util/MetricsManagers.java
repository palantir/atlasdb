/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.util;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class MetricsManagers {
    private MetricsManagers() {}

    public static MetricsManager of(MetricRegistry metrics, TaggedMetricRegistry taggedMetrics) {
        return new MetricsManager(metrics, taggedMetrics, LoggingArgs::isSafe);
    }

    public static MetricsManager of(
            MetricRegistry metrics, TaggedMetricRegistry taggedMetrics, Refreshable<Boolean> performMetricFiltering) {
        return new MetricsManager(metrics, taggedMetrics, performMetricFiltering, LoggingArgs::isSafe);
    }

    public static MetricsManager createForTests() {
        return MetricsManagers.of(new MetricRegistry(), new DefaultTaggedMetricRegistry());
    }

    public static MetricsManager createAlwaysSafeAndFilteringForTests() {
        return new MetricsManager(
                new MetricRegistry(), new DefaultTaggedMetricRegistry(), Refreshable.only(true), tableRef -> true);
    }
}
