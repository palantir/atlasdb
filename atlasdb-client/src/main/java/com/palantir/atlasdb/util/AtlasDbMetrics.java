/**
 * Copyright 2017 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public final class AtlasDbMetrics {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbMetrics.class);
    @VisibleForTesting
    static final String DEFAULT_REGISTRY_NAME = "AtlasDb";

    @VisibleForTesting
    static volatile MetricRegistry metrics;

    private AtlasDbMetrics() {}

    public static synchronized void setMetricRegistry(MetricRegistry metricRegistry) {
        AtlasDbMetrics.metrics = Preconditions.checkNotNull(metricRegistry, "Metric registry cannot be null");
    }

    // Using this means that all atlasdb clients will report to the same registry, which may give confusing stats
    public static MetricRegistry getMetricRegistry() {
        if (AtlasDbMetrics.metrics == null) {
            synchronized (AtlasDbMetrics.class) {
                if (AtlasDbMetrics.metrics == null) {
                    AtlasDbMetrics.metrics = SharedMetricRegistries.getOrCreate(DEFAULT_REGISTRY_NAME);
                    log.info("Metric Registry was not set, setting to default registry name of "
                            + DEFAULT_REGISTRY_NAME);
                }
            }
        }

        return AtlasDbMetrics.metrics;
    }
}
