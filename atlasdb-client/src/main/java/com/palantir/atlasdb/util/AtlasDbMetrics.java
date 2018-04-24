/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.palantir.tritium.event.InvocationContext;
import com.palantir.tritium.event.log.LoggingInvocationEventHandler;
import com.palantir.tritium.event.log.LoggingLevel;
import com.palantir.tritium.event.metrics.MetricsInvocationEventHandler;
import com.palantir.tritium.metrics.MetricRegistries;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import com.palantir.tritium.proxy.Instrumentation;

public final class AtlasDbMetrics {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbMetrics.class);

    @VisibleForTesting
    static final String DEFAULT_REGISTRY_NAME = "AtlasDb";

    @VisibleForTesting
    static final AtomicReference<MetricRegistry> metrics = new AtomicReference<>(null);

    @VisibleForTesting
    static final AtomicReference<TaggedMetricRegistry> taggedMetrics = new AtomicReference<>(null);

    private AtlasDbMetrics() {}

    public static synchronized void setMetricRegistries(MetricRegistry metricRegistry,
            TaggedMetricRegistry taggedMetricRegistry) {
        if (metricRegistry != metrics.get()) {
            log.warn("The MetricsRegistry was re-set to a different value: the previous registry will be ignored"
                    + " and metrics may be lost.");
        }
        if (taggedMetricRegistry != taggedMetrics.get()) {
            log.warn("The TaggedMetricsRegistry was re-set to a different value: the previous registry will be ignored"
                    + " and metrics may be lost.");
        }

        metrics.set(Preconditions.checkNotNull(metricRegistry, "Metric registry cannot be null"));
        taggedMetrics.set(Preconditions.checkNotNull(taggedMetricRegistry, "Tagged Metric registry cannot be null"));
    }

    // Using this means that all atlasdb clients will report to the same registry, which may give confusing stats
    public static MetricRegistry getMetricRegistry() {
        return metrics.updateAndGet(registry -> {
            if (registry == null) {
                return createDefaultMetrics();
            }
            return registry;
        });
    }

    public static TaggedMetricRegistry getTaggedMetricRegistry() {
        return taggedMetrics.updateAndGet(registry -> {
            if (registry == null) {
                return DefaultTaggedMetricRegistry.getDefault();
            }
            return registry;
        });
    }

    private static MetricRegistry createDefaultMetrics() {
        MetricRegistry registry = SharedMetricRegistries.getOrCreate(DEFAULT_REGISTRY_NAME);
        log.warn("Metric Registry was not set, setting to shared default registry name of "
                + DEFAULT_REGISTRY_NAME);
        return registry;
    }

    public static <T, U extends T> T instrument(Class<T> serviceInterface, U service) {
        return instrument(serviceInterface, service, serviceInterface.getName());
    }

    public static <T, U extends T> T instrument(Class<T> serviceInterface, U service, String name) {
        return Instrumentation.builder(serviceInterface, service)
                .withHandler(new MetricsInvocationEventHandler(getMetricRegistry(), name))
                .withLogging(
                        LoggerFactory.getLogger("performance." + name),
                        LoggingLevel.TRACE,
                        LoggingInvocationEventHandler.LOG_DURATIONS_GREATER_THAN_1_MICROSECOND)
                .build();
    }

    public static <T, U extends T> T instrumentWithTaggedMetrics(
            Class<T> serviceInterface,
            U service,
            String name,
            Function<InvocationContext, Map<String, String>> tagFunction) {
        return Instrumentation.builder(serviceInterface, service)
                .withHandler(new TaggedMetricsInvocationEventHandler(getTaggedMetricRegistry(), name, tagFunction))
                .withLogging(
                        LoggerFactory.getLogger("performance." + name),
                        LoggingLevel.TRACE,
                        LoggingInvocationEventHandler.LOG_DURATIONS_GREATER_THAN_1_MICROSECOND)
                .build();
    }

    public static void registerCache(Cache<?, ?> cache, String metricsPrefix) {
        MetricRegistry metricRegistry = getMetricRegistry();
        Set<String> existingMetrics = metricRegistry.getMetrics().keySet().stream()
                .filter(name -> name.startsWith(metricsPrefix))
                .collect(Collectors.toSet());
        if (existingMetrics.isEmpty()) {
            MetricRegistries.registerCache(metricRegistry, cache, metricsPrefix);
        } else {
            log.info("Not registering cache with prefix '{}' as metric registry already contains metrics: {}",
                    metricsPrefix, existingMetrics);
        }
    }
}
