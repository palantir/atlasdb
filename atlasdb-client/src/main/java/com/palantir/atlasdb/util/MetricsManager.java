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
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.logsafe.SafeArg;

public class MetricsManager {

    private static final Logger log = LoggerFactory.getLogger(MetricsManager.class);

    private final MetricRegistry metricRegistry;
    private final Set<String> registeredMetrics;

    public MetricsManager() {
        this(AtlasDbMetrics.getMetricRegistry());
    }

    @VisibleForTesting
    MetricsManager(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
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
            metricRegistry.register(fullyQualifiedMetricName, metric);
            registeredMetrics.add(fullyQualifiedMetricName);
        } catch (Exception e) {
            // Primarily to handle integration tests that instantiate this class multiple times in a row
            log.warn("Unable to register metric {}."
                    + " This may occur if you are running integration tests that don't clean up completely after "
                    + " themselves, or if you are trying to use multiple TransactionManagers concurrently in the same"
                    + " JVM (e.g. in a KVS migration). If this is not the case, this is likely to be a product and/or"
                    + " an AtlasDB bug. This is no cause for immediate alarm, but it does mean that your telemetry for"
                    + " the aforementioned metric may be reported incorrectly.",
                    SafeArg.of("metricName", fullyQualifiedMetricName),
                    e);
        }
    }

    public Meter registerMeter(Class clazz, String meterName) {
        return registerMeter(MetricRegistry.name(clazz, "", meterName));
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
        // isEmpty() check required because MetricRegistry.name skips missing components.
        // See MetricsManagerTest#doesNotDeregisterMetricsFromOtherClassesEvenIfStringPrefixesMatch.
        String fqnPrefix = prefix.isEmpty() ? clazz.getName() + "." : MetricRegistry.name(clazz, prefix);
        Set<String> relevantMetrics = registeredMetrics.stream()
                .filter(metricName -> metricName.startsWith(fqnPrefix))
                .collect(Collectors.toSet());
        relevantMetrics.forEach(this::deregisterMetric);
    }
}
