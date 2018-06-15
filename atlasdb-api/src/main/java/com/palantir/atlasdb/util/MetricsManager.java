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
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import com.palantir.util.crypto.Sha256Hash;

public class MetricsManager {

    private static final Logger log = LoggerFactory.getLogger(MetricsManager.class);

    private final MetricRegistry metricRegistry;
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final Set<String> registeredMetrics;
    private final Predicate<TableReference> isSafeToLog;

    public MetricsManager(MetricRegistry metricRegistry,
            TaggedMetricRegistry taggedMetricRegistry,
            Predicate<TableReference> isSafeToLog) {
        this(metricRegistry, taggedMetricRegistry, new HashSet<>(), isSafeToLog);
    }

    @VisibleForTesting
    MetricsManager(MetricRegistry metricRegistry, TaggedMetricRegistry taggedMetricRegistry,
            Set<String> registeredMetrics, Predicate<TableReference> isSafeToLog) {
        this.metricRegistry = metricRegistry;
        this.taggedMetricRegistry = taggedMetricRegistry;
        this.registeredMetrics = registeredMetrics;
        this.isSafeToLog = isSafeToLog;
    }

    public MetricRegistry getRegistry() {
        return metricRegistry;
    }

    public TaggedMetricRegistry getTaggedRegistry() {
        return taggedMetricRegistry;
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

    /**
     * Add a new gauge metric of the given name.
     *
     * If the metric already exists, this will REPLACE it with a new metric.
     * Consider using {@link MetricsManager#registerOrAddToMetric} instead.
     */
    public void registerMetric(Class clazz, String metricName, Gauge gauge, Map<String, String> tag) {
        MetricName metricToAdd = MetricName.builder()
                .safeName(MetricRegistry.name(clazz, metricName))
                .safeTags(tag)
                .build();
        if (taggedMetricRegistry.getMetrics().containsKey(metricToAdd)) {
            log.warn("Replacing the metric [ {} ]. This will happen if you are trying to re-register metrics "
                            + "or have two tagged metrics with the same name across the application.",
                    SafeArg.of("metricName", metricName));
            taggedMetricRegistry.remove(metricToAdd);
        }
        taggedMetricRegistry.gauge(metricToAdd, gauge);
    }

    public static void main(String[] args) {
        MetricName metricToAdd = MetricName.builder()
                .safeName(MetricRegistry.name(MetricsManager.class, "test"))
                .safeTags(ImmutableMap.of("strategy", "bsL"))
                .build();
        System.out.println(metricToAdd);
    }

    public void registerGaugeForTable(Class clazz, String metricName, TableReference tableRef, Gauge gauge) {
        Map<String, String> tag = getTableNameTagFor(tableRef);
        registerMetric(clazz, metricName, gauge, tag);
    }

    /**
     * Add a new gauge to a metric (which may already exist).
     */
    public void registerOrAddToMetric(Class clazz, String metricName, Gauge gauge, Map<String, String> tag) {
        MetricName metricToAdd = MetricName.builder()
                .safeName(MetricRegistry.name(clazz, metricName))
                .safeTags(tag)
                .build();

        try {
            taggedMetricRegistry.gauge(metricToAdd, gauge);
        } catch (IllegalArgumentException ex) {
            log.warn("Tried to add a gauge to a metric name {} that has non-gauge metrics associated with it."
                    + " This indicates a product bug.",
                    SafeArg.of("metricName", metricName),
                    ex);
        }
    }

    @VisibleForTesting
    Map<String, String> getTableNameTagFor(TableReference tableRef) {
        String tableName = tableRef.getTablename();
        if (!isSafeToLog.test(tableRef)) {
            tableName = "unsafeTable_" + obfuscate(tableRef);
        }

        return ImmutableMap.of("tableName", tableName);
    }

    private String obfuscate(TableReference tableRef) {
        return Sha256Hash.computeHash(tableRef.getTablename().getBytes()).serializeToHexString().substring(0, 16);
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
                    + " the aforementioned metric may be reported incorrectly. Turn on TRACE logging to see the full"
                    + " exception.",
                    SafeArg.of("metricName", fullyQualifiedMetricName));
            log.trace("Full exception follows:", e);
        }
    }

    public Histogram registerOrGetHistogram(Class clazz, String metricName) {
        return registerOrGetHistogram(MetricRegistry.name(clazz, metricName));
    }

    private Histogram registerOrGetHistogram(String fullyQualifiedHistogramName) {
        Histogram histogram = metricRegistry.histogram(fullyQualifiedHistogramName);
        registeredMetrics.add(fullyQualifiedHistogramName);
        return histogram;
    }

    public Timer registerOrGetTimer(Class clazz, String metricName) {
        return registerOrGetTimer(MetricRegistry.name(clazz, metricName));
    }

    private Timer registerOrGetTimer(String fullyQualifiedHistogramName) {
        Timer timer = metricRegistry.timer(fullyQualifiedHistogramName);
        registeredMetrics.add(fullyQualifiedHistogramName);
        return timer;
    }

    public Meter registerOrGetMeter(Class clazz, String meterName) {
        return registerOrGetMeter(MetricRegistry.name(clazz, "", meterName));
    }

    public Meter registerOrGetMeter(Class clazz, String metricPrefix, String meterName) {
        return registerOrGetMeter(MetricRegistry.name(clazz, metricPrefix, meterName));
    }

    private synchronized Meter registerOrGetMeter(String fullyQualifiedMeterName) {
        Meter meter = metricRegistry.meter(fullyQualifiedMeterName);
        registeredMetrics.add(fullyQualifiedMeterName);
        return meter;
    }

    public Gauge registerOrGetGauge(Class clazz, String metricPrefix, String metricName, MetricRegistry.MetricSupplier<Gauge> supplier) {
        return registerOrGetGauge(MetricRegistry.name(clazz, metricPrefix, metricName), supplier);
    }

    private synchronized Gauge registerOrGetGauge(String name, MetricRegistry.MetricSupplier<Gauge> supplier) {
        Gauge gauge = metricRegistry.gauge(name, supplier);
        registeredMetrics.add(name);
        return gauge;
    }

    public synchronized void deregisterMetrics() {
        registeredMetrics.forEach(metricRegistry::remove);
        registeredMetrics.clear();
    }
}
