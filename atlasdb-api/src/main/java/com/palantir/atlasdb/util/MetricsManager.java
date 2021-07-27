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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.metrics.DisjointUnionTaggedMetricSet;
import com.palantir.atlasdb.metrics.FilteredTaggedMetricSet;
import com.palantir.atlasdb.metrics.MetricPublicationArbiter;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.DropwizardTaggedMetricSet;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class MetricsManager {

    private static final SafeLogger log = SafeLoggerFactory.get(MetricsManager.class);

    private final MetricRegistry metricRegistry;
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final Set<String> registeredMetrics;
    private final Set<MetricName> registeredTaggedMetrics;
    private final MetricPublicationArbiter publicationArbiter;
    private final TaggedMetricSet publishableMetricsView;
    private final Predicate<TableReference> isSafeToLog;
    private final ReadWriteLock lock;
    private final MetricNameCache metricNameCache;

    public MetricsManager(
            MetricRegistry metricRegistry,
            TaggedMetricRegistry taggedMetricRegistry,
            Predicate<TableReference> isSafeToLog) {
        this(metricRegistry, taggedMetricRegistry, Refreshable.only(false), isSafeToLog);
    }

    public MetricsManager(
            MetricRegistry metricRegistry,
            TaggedMetricRegistry taggedMetricRegistry,
            Refreshable<Boolean> performFiltering,
            Predicate<TableReference> isSafeToLog) {
        this.metricRegistry = metricRegistry;
        this.taggedMetricRegistry = taggedMetricRegistry;
        this.registeredMetrics = ConcurrentHashMap.newKeySet();
        this.registeredTaggedMetrics = ConcurrentHashMap.newKeySet();
        this.isSafeToLog = isSafeToLog;
        this.publicationArbiter = MetricPublicationArbiter.create();
        this.publishableMetricsView = createPublishableMetricsView(
                metricRegistry, taggedMetricRegistry, publicationArbiter, performFiltering);
        this.lock = new ReentrantReadWriteLock();
        this.metricNameCache = new MetricNameCache();
    }

    private static TaggedMetricSet createPublishableMetricsView(
            MetricRegistry metricRegistry,
            TaggedMetricRegistry taggedMetricRegistry,
            MetricPublicationArbiter arbiter,
            Refreshable<Boolean> performFiltering) {
        TaggedMetricSet legacyMetricsAsTaggedSet = new DropwizardTaggedMetricSet(metricRegistry);
        TaggedMetricSet unfilteredUnion =
                new DisjointUnionTaggedMetricSet(taggedMetricRegistry, legacyMetricsAsTaggedSet);
        return new FilteredTaggedMetricSet(unfilteredUnion, arbiter, performFiltering);
    }

    public MetricRegistry getRegistry() {
        return metricRegistry;
    }

    public TaggedMetricRegistry getTaggedRegistry() {
        return taggedMetricRegistry;
    }

    public void addMetricFilter(MetricName metricName, MetricPublicationFilter publicationFilter) {
        publicationArbiter.registerMetricsFilter(metricName, publicationFilter);
    }

    public void addMetricFilter(
            Class clazz, String metricName, Map<String, String> tags, MetricPublicationFilter publicationFilter) {
        addMetricFilter(getTaggedMetricName(clazz, metricName, tags), publicationFilter);
    }

    public void doNotPublish(MetricName metricName) {
        publicationArbiter.registerMetricsFilter(metricName, MetricPublicationFilter.NEVER_PUBLISH);
    }

    public TaggedMetricSet getPublishableMetrics() {
        return publishableMetricsView;
    }

    public void registerMetric(Class clazz, String metricName, Gauge gauge) {
        registerMetricWithFqn(MetricRegistry.name(clazz, metricName), gauge);
    }

    /**
     * Add a new gauge metric of the given name or get the existing gauge if it is already registered.
     *
     * @throws IllegalStateException if a non-gauge metric with the same name already exists.
     */
    public Gauge registerOrGet(Class clazz, String metricName, Gauge gauge, Map<String, String> tag) {
        MetricName metricToAdd = getTaggedMetricName(clazz, metricName, tag);

        try {
            Gauge registeredGauge = taggedMetricRegistry.gauge(metricToAdd, gauge);
            registerTaggedMetricName(metricToAdd);
            return registeredGauge;
        } catch (IllegalArgumentException ex) {
            log.error(
                    "Tried to add a gauge to a metric name {} that has non-gauge metrics associated with it."
                            + " This indicates a product bug.",
                    SafeArg.of("metricName", metricName),
                    ex);
            throw ex;
        }
    }

    private MetricName getTaggedMetricName(Class clazz, String metricName, Map<String, String> tags) {
        return metricNameCache.get(clazz, metricName, tags);
    }

    public Map<String, String> getTableNameTagFor(@Nullable TableReference tableRef) {
        String tableName = tableRef == null ? "unknown" : tableRef.getTablename();
        if (!isSafeToLog.test(tableRef)) {
            tableName = "unsafeTable";
        }

        return ImmutableMap.of("tableName", tableName);
    }

    private void registerMetricWithFqn(String fullyQualifiedMetricName, Metric metric) {
        try {
            metricRegistry.register(fullyQualifiedMetricName, metric);
            registerMetricName(fullyQualifiedMetricName);
        } catch (Exception e) {
            // Primarily to handle integration tests that instantiate this class multiple times in a row
            log.warn(
                    "Unable to register metric {}. This may occur if you are running integration tests that don't"
                        + " clean up completely after  themselves, or if you are trying to use multiple"
                        + " TransactionManagers concurrently in the same JVM (e.g. in a KVS migration). If this is not"
                        + " the case, this is likely to be a product and/or an AtlasDB bug. This is no cause for"
                        + " immediate alarm, but it does mean that your telemetry for the aforementioned metric may be"
                        + " reported incorrectly. Turn on TRACE logging to see the full exception.",
                    SafeArg.of("metricName", fullyQualifiedMetricName));
            log.trace("Full exception follows:", e);
        }
    }

    public <M extends Gauge> M registerOrGetGauge(Class clazz, String metricName, Supplier<M> gaugeSupplier) {
        String fullyQualifiedGaugeName = MetricRegistry.name(clazz, metricName);
        M gauge = (M) metricRegistry.gauge(fullyQualifiedGaugeName, gaugeSupplier::get);
        registerMetricName(fullyQualifiedGaugeName);
        return gauge;
    }

    public Histogram registerOrGetHistogram(Class clazz, String metricName) {
        return registerOrGetHistogram(MetricRegistry.name(clazz, metricName));
    }

    public Histogram registerOrGetHistogram(Class clazz, String metricName, Supplier<Histogram> histogramSupplier) {
        String fullyQualifiedHistogramName = MetricRegistry.name(clazz, metricName);
        Histogram histogram = metricRegistry.histogram(fullyQualifiedHistogramName, histogramSupplier::get);
        registerMetricName(fullyQualifiedHistogramName);
        return histogram;
    }

    private Histogram registerOrGetHistogram(String fullyQualifiedHistogramName) {
        Histogram histogram = metricRegistry.histogram(fullyQualifiedHistogramName);
        registerMetricName(fullyQualifiedHistogramName);
        return histogram;
    }

    public Timer registerOrGetTimer(Class clazz, String metricName) {
        return registerOrGetTimer(MetricRegistry.name(clazz, metricName));
    }

    private Timer registerOrGetTimer(String fullyQualifiedHistogramName) {
        Timer timer = metricRegistry.timer(fullyQualifiedHistogramName);
        registerMetricName(fullyQualifiedHistogramName);
        return timer;
    }

    public Counter registerOrGetCounter(Class clazz, String counterName) {
        return registerOrGetCounter(MetricRegistry.name(clazz, "", counterName));
    }

    private Counter registerOrGetCounter(String fullyQualifiedCounterName) {
        Counter counter = metricRegistry.counter(fullyQualifiedCounterName);
        registerMetricName(fullyQualifiedCounterName);
        return counter;
    }

    public Meter registerOrGetMeter(Class clazz, String meterName) {
        return registerOrGetMeter(MetricRegistry.name(clazz, "", meterName));
    }

    public Meter registerOrGetMeter(Class clazz, String metricPrefix, String meterName) {
        return registerOrGetMeter(MetricRegistry.name(clazz, metricPrefix, meterName));
    }

    private Meter registerOrGetMeter(String fullyQualifiedMeterName) {
        Meter meter = metricRegistry.meter(fullyQualifiedMeterName);
        registerMetricName(fullyQualifiedMeterName);
        return meter;
    }

    public Meter registerOrGetTaggedMeter(Class clazz, String metricName, Map<String, String> tags) {
        MetricName name = getTaggedMetricName(clazz, metricName, tags);
        Meter meter = taggedMetricRegistry.meter(name);
        registerTaggedMetricName(name);
        return meter;
    }

    public Histogram registerOrGetTaggedHistogram(Class clazz, String metricName, Map<String, String> tags) {
        MetricName name = getTaggedMetricName(clazz, metricName, tags);
        Histogram histogram = taggedMetricRegistry.histogram(name);
        registerTaggedMetricName(name);
        return histogram;
    }

    public Histogram registerOrGetTaggedHistogram(
            Class clazz, String metricName, Map<String, String> tags, Supplier<Histogram> supplier) {
        MetricName name = getTaggedMetricName(clazz, metricName, tags);
        Histogram histogram = taggedMetricRegistry.histogram(name, supplier);
        registerTaggedMetricName(name);
        return histogram;
    }

    public Counter registerOrGetTaggedCounter(Class clazz, String metricName, Map<String, String> tags) {
        MetricName name = getTaggedMetricName(clazz, metricName, tags);
        Counter counter = taggedMetricRegistry.counter(name);
        registerTaggedMetricName(name);
        return counter;
    }

    private void registerMetricName(String fullyQualifiedMeterName) {
        lock.readLock().lock();
        try {
            registeredMetrics.add(fullyQualifiedMeterName);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void registerTaggedMetricName(MetricName name) {
        lock.readLock().lock();
        try {
            registeredTaggedMetrics.add(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void deregisterMetrics() {
        lock.writeLock().lock();
        try {
            registeredMetrics.forEach(metricRegistry::remove);
            registeredMetrics.clear();

            registeredTaggedMetrics.forEach(taggedMetricRegistry::remove);
            registeredTaggedMetrics.clear();

            metricNameCache.invalidate();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void deregisterTaggedMetrics(Predicate<MetricName> predicate) {
        lock.writeLock().lock();
        try {
            List<MetricName> metricsToRemove = taggedMetricRegistry.getMetrics().keySet().stream()
                    .filter(predicate)
                    .collect(Collectors.toList());

            metricsToRemove.forEach(taggedMetricRegistry::remove);

            // Don't be clever, invalidate everything. This should be relatively uncommon.
            metricNameCache.invalidate();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
