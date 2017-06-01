/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.leader;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

public class LeaderLog {

    private static final Logger log = LoggerFactory.getLogger(LeaderLog.class);

    private static final String DEFAULT_REGISTRY_NAME = "AtlasDb-Leader";
    private static final AtomicReference<MetricRegistry> metrics = new AtomicReference<>(null);

    public static final Logger logger = LoggerFactory.getLogger("leadership");

    // TODO(nziebart): consolidate this with AtlasDBMetrics - can we move that class into a common library?
    public static MetricRegistry metrics() {
        return metrics.updateAndGet(registry -> {
            if (registry == null) {
                return createDefaultMetrics();
            }
            return registry;
        });
    }

    public static void setMetricRegistry(MetricRegistry metricRegistry) {
        metrics.set(metricRegistry);
    }

    private static MetricRegistry createDefaultMetrics() {
        MetricRegistry registry = SharedMetricRegistries.getOrCreate(DEFAULT_REGISTRY_NAME);
        log.warn("Metric Registry was not set, setting to shared default registry name of "
                + DEFAULT_REGISTRY_NAME);
        return registry;
    }

}
