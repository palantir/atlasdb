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
package com.palantir.atlasdb.util;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

public class MetricsManager {

    private static final Logger log = LoggerFactory.getLogger(MetricsManager.class);

    private final MetricRegistry metricRegistry;
    private final Set<String> registeredMetrics;

    public MetricsManager() {
        this.metricRegistry = AtlasDbMetrics.getMetricRegistry();
        this.registeredMetrics = new HashSet<>();
    }

    public MetricRegistry getRegistry() {
        return metricRegistry;
    }

    public void registerMetric(Class clazz, String metricPrefix, String metricName, Gauge gauge) {
        registerMetric(clazz, metricPrefix, metricName, (Metric) gauge);
    }

    public void registerMetric(Class clazz, String metricPrefix, String metricName, Metric metric) {
        registerMetricWithFqn(MetricRegistry.name(clazz, metricPrefix, metricName), metric);
    }

    public void registerMetric(Class clazz, String metricName, Gauge gauge) {
        registerMetric(clazz, metricName, (Metric) gauge);
    }

    public void registerMetric(Class clazz, String metricName, Metric metric) {
        registerMetricWithFqn(MetricRegistry.name(clazz, metricName), metric);
    }

    private synchronized void registerMetricWithFqn(String fullyQualifiedMetricName, Metric metric) {
        try {
            if (!metricRegistry.getNames().contains(fullyQualifiedMetricName)) {
                metricRegistry.register(fullyQualifiedMetricName, metric);
            }
            registeredMetrics.add(fullyQualifiedMetricName);
        } catch (Exception e) {
            // Do not bubble up exceptions when registering metrics.
            log.error("Unable to register metric {}", fullyQualifiedMetricName, e);
        }
    }

    public Meter registerMeter(Class clazz, String metricPrefix, String meterName) {
        return registerMeter(MetricRegistry.name(clazz, metricPrefix, meterName));
    }

    private synchronized Meter registerMeter(String fullyQualifiedMeterName) {
        Meter meter = metricRegistry.meter(fullyQualifiedMeterName);
        registeredMetrics.add(fullyQualifiedMeterName);
        return meter;
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
