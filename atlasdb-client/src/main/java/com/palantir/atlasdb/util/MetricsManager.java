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

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

public class MetricsManager {

    private static final Logger log = LoggerFactory.getLogger(MetricsManager.class);

    private final MetricRegistry metricRegistry;
    private final Set<String> registeredMetrics;

    public MetricsManager() {
        this.metricRegistry = AtlasDbMetrics.getMetricRegistry();
        this.registeredMetrics = new HashSet<>();
    }

    public void registerMetric(Class clazz, String metricPrefix, String metricName, Gauge gauge) {
        registerMetricWithFqn(MetricRegistry.name(clazz, metricPrefix, metricName), gauge);
    }

    public void registerMetric(Class clazz, String metricName, Gauge gauge) {
        registerMetricWithFqn(MetricRegistry.name(clazz, metricName), gauge);
    }

    private synchronized void registerMetricWithFqn(String fullyQualifiedMetricName, Gauge gauge) {
        try {
            metricRegistry.register(fullyQualifiedMetricName, gauge);
            registeredMetrics.add(fullyQualifiedMetricName);
        } catch (Exception e) {
            // Primarily to handle integration tests that instantiate this class multiple times in a row
            log.error("Unable to register metric {}", fullyQualifiedMetricName, e);
        }
    }

    public synchronized void deregisterMetrics() {
        registeredMetrics.forEach(metricRegistry::remove);
        registeredMetrics.clear();
    }

    private synchronized void deregisterMetric(String fullyQualifiedMetricName) {
        metricRegistry.remove(fullyQualifiedMetricName);
        registeredMetrics.remove(fullyQualifiedMetricName);
    }

    public void deregisterMetricsWithPrefix(Class clazz, String prefix) {
        String fqnPrefix = MetricRegistry.name(clazz, prefix);
        registeredMetrics.stream()
                .filter(metricName -> metricName.startsWith(fqnPrefix))
                .forEach(this::deregisterMetric);
    }
}
