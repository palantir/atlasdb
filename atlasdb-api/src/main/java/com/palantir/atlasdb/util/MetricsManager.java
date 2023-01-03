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
            Class<?> clazz, String metricName, Map<String, String> tags, MetricPublicationFilter publicationFilter) {
        addMetricFilter(getTaggedMetricName(clazz, metricName, tags), publicationFilter);
    }

    public void doNotPublish(MetricName metricName) {
        publicationArbiter.registerMetricsFilter(metricName, MetricPublicationFilter.NEVER_PUBLISH);
    }

    public TaggedMetricSet getPublishableMetrics() {
        return publishableMetricsView;
    }

    public void registerMetric(Class<?> clazz, String metricName, Gauge<?> gauge) {
        registerOrGet(clazz, metricName, gauge, Map.of());
    }

    /**
     * Add a new gauge metric of the given name or get the existing gauge if it is already registered.
     *
     * @throws IllegalStateException if a non-gauge metric with the same name already exists.
     */
    public <T> Gauge<T> registerOrGet(Class<?> clazz, String metricName, Gauge<T> gauge, Map<String, String> tag) {
        MetricName metricToAdd = getTaggedMetricName(clazz, metricName, tag);

        try {
            Gauge<T> registeredGauge = taggedMetricRegistry.gauge(metricToAdd, gauge);
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

    private MetricName getTaggedMetricName(Class<?> clazz, String metricName, Map<String, String> tags) {
        return metricNameCache.get(clazz, metricName, tags);
    }

    public Map<String, String> getTableNameTagFor(@Nullable TableReference tableRef) {
        String tableName = tableRef == null ? "unknown" : tableRef.getTableName();
        if (!isSafeToLog.test(tableRef)) {
            tableName = "unsafeTable";
        }

        return ImmutableMap.of("tableName", tableName);
    }

    /**
     * @deprecated use {@link #registerOrGet(Class, String, Gauge, Map)}
     */
    @Deprecated
    public <M extends Gauge<?>> M registerOrGetGauge(Class<?> clazz, String metricName, Supplier<M> gaugeSupplier) {
        return (M) registerOrGet(clazz, metricName, (Gauge<?>) gaugeSupplier.get(), Map.of());
    }

    public Histogram registerOrGetHistogram(Class<?> clazz, String metricName) {
        return registerOrGetTaggedHistogram(clazz, metricName, Map.of());
    }

    /**
     * @deprecated use {@link #registerOrGetTaggedHistogram(Class, String, Map, Supplier)}
     */
    @Deprecated
    public Histogram registerOrGetHistogram(Class<?> clazz, String metricName, Supplier<Histogram> histogramSupplier) {
        return registerOrGetTaggedHistogram(clazz, metricName, Map.of(), histogramSupplier);
    }

    public Timer registerOrGetTimer(Class<?> clazz, String metricName) {
        MetricName name = getTaggedMetricName(clazz, metricName, Map.of());
        Timer timer = taggedMetricRegistry.timer(name);
        registerTaggedMetricName(name);
        return timer;
    }

    public Counter registerOrGetCounter(Class<?> clazz, String counterName) {
        return registerOrGetTaggedCounter(clazz, counterName, Map.of());
    }

    public Meter registerOrGetMeter(Class<?> clazz, String meterName) {
        return registerOrGetTaggedMeter(clazz, meterName, Map.of());
    }

    /**
     * @deprecated use {@link #registerOrGetTaggedMeter(Class, String, Map)}
     */
    @Deprecated
    public Meter registerOrGetMeter(Class<?> clazz, String metricPrefix, String meterName) {
        return registerOrGetTaggedMeter(clazz, MetricRegistry.name(metricPrefix, meterName), Map.of());
    }

    public Meter registerOrGetTaggedMeter(Class<?> clazz, String metricName, Map<String, String> tags) {
        MetricName name = getTaggedMetricName(clazz, metricName, tags);
        Meter meter = taggedMetricRegistry.meter(name);
        registerTaggedMetricName(name);
        return meter;
    }

    public Histogram registerOrGetTaggedHistogram(Class<?> clazz, String metricName, Map<String, String> tags) {
        MetricName name = getTaggedMetricName(clazz, metricName, tags);
        Histogram histogram = taggedMetricRegistry.histogram(name);
        registerTaggedMetricName(name);
        return histogram;
    }

    public Histogram registerOrGetTaggedHistogram(
            Class<?> clazz, String metricName, Map<String, String> tags, Supplier<Histogram> supplier) {
        MetricName name = getTaggedMetricName(clazz, metricName, tags);
        Histogram histogram = taggedMetricRegistry.histogram(name, supplier);
        registerTaggedMetricName(name);
        return histogram;
    }

    public Counter registerOrGetTaggedCounter(Class<?> clazz, String metricName, Map<String, String> tags) {
        MetricName name = getTaggedMetricName(clazz, metricName, tags);
        Counter counter = taggedMetricRegistry.counter(name);
        registerTaggedMetricName(name);
        return counter;
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
