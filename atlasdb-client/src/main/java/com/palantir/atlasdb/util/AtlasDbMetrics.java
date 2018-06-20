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

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.palantir.tritium.event.InvocationContext;
import com.palantir.tritium.event.log.LoggingInvocationEventHandler;
import com.palantir.tritium.event.log.LoggingLevel;
import com.palantir.tritium.event.metrics.MetricsInvocationEventHandler;
import com.palantir.tritium.metrics.MetricRegistries;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import com.palantir.tritium.proxy.Instrumentation;

public final class AtlasDbMetrics {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbMetrics.class);

    private AtlasDbMetrics() {}

    public static <T, U extends T> T instrument(
            MetricRegistry metricRegistry, Class<T> serviceInterface, U service) {
        return instrument(metricRegistry, serviceInterface, service, serviceInterface.getName());
    }

    public static <T, U extends T> T instrument(
            MetricRegistry metricRegistry, Class<T> serviceInterface, U service, String name) {
        return Instrumentation.builder(serviceInterface, service)
                .withHandler(new MetricsInvocationEventHandler(metricRegistry, name))
                .withLogging(
                        LoggerFactory.getLogger("performance." + name),
                        LoggingLevel.TRACE,
                        LoggingInvocationEventHandler.LOG_DURATIONS_GREATER_THAN_1_MICROSECOND)
                .build();
    }

    public static <T, U extends T> T instrumentWithTaggedMetrics(
            TaggedMetricRegistry taggedMetrics,
            Class<T> serviceInterface,
            U service,
            String name,
            Function<InvocationContext, Map<String, String>> tagFunction) {
        return Instrumentation.builder(serviceInterface, service)
                .withHandler(new TaggedMetricsInvocationEventHandler(taggedMetrics, name, tagFunction))
                .withLogging(
                        LoggerFactory.getLogger("performance." + name),
                        LoggingLevel.TRACE,
                        LoggingInvocationEventHandler.LOG_DURATIONS_GREATER_THAN_1_MICROSECOND)
                .build();
    }

    public static void registerCache(MetricRegistry metricRegistry, Cache<?, ?> cache, String metricsPrefix) {
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
