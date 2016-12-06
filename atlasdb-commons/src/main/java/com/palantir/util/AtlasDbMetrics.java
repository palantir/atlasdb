/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.util;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Slf4jReporter;

public class AtlasDbMetrics {

    private static final String DEFAULT_LOG_DOMAIN = "com.palantir.util";
    private static final String DEFAULT_REGISTRY_NAME = "AtlasDb";
    private static final int DEFAULT_LOG_PERIOD_SECONDS = 60;
    private static volatile AtlasDbMetrics instance = null;

    private MetricRegistry metricRegistry;
    private final Logger metricsLogger;
    private final Slf4jReporter metricsLogReporter;

    private AtlasDbMetrics(String logDomain, String registryName) {
        this.metricRegistry = SharedMetricRegistries.getOrCreate(registryName);
        JmxReporter.forRegistry(metricRegistry).inDomain(logDomain).convertDurationsTo(
                TimeUnit.MILLISECONDS).convertRatesTo(TimeUnit.SECONDS).build().start();
        this.metricsLogger = LoggerFactory.getLogger(logDomain);
        this.metricsLogReporter = Slf4jReporter.forRegistry(metricRegistry).outputTo(metricsLogger).convertRatesTo(
                TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
        metricsLogger.info("Starting metrics recording for AtlasDb metrics");
    }

    public synchronized static AtlasDbMetrics createAndSetInstance(String logDomain, String registryName) {
        if (instance != null) {
            throw new IllegalStateException("AtlasDbMetrics instance already exists!");
        }
        instance = new AtlasDbMetrics(logDomain, registryName);
        return instance;
    }

    private static AtlasDbMetrics getInstance() {
        if (instance == null) {
            throw new IllegalStateException(String.format(
                    "Must call createAndSetInstance before using %s",
                    AtlasDbMetrics.class.getName()));
        }
        return instance;
    }

    public static void startLogging() {
        getInstance().metricsLogReporter.start(DEFAULT_LOG_PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    public static Slf4jReporter getMetricLogReporter() {
        return getInstance().metricsLogReporter;
    }

    public static MetricRegistry getMetricRegistry() {
        return getInstance().metricRegistry;
    }

    public static Logger getMetricsLogger() {
        return getInstance().metricsLogger;
    }

    // Using this means that all atlasdb clients will report to the same registry, which may give confusing stats
    public synchronized static MetricRegistry getOrDefaultMetricRegistry() {
        if (instance == null) {
            getMetricsLogger().info("Metric Registry was not set, setting to default log domain of "
                    + DEFAULT_LOG_DOMAIN + " and default registry name of " + DEFAULT_REGISTRY_NAME);
            instance = new AtlasDbMetrics(DEFAULT_LOG_DOMAIN, DEFAULT_REGISTRY_NAME);
        }
        return getInstance().metricRegistry;
    }

}
