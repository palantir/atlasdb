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
package com.palantir.atlasdb.keyvalue.cassandra.pool;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import com.palantir.atlasdb.util.MetricsManager;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CassandraClientPoolMetrics {
    private final MetricsManager metricsManager;
    private final RequestMetrics aggregateRequestMetrics;
    private final Map<InetSocketAddress, RequestMetrics> metricsByHost = new HashMap<>();
    private final Map<CassandraClientPoolHostLevelMetric, DistributionOutlierController> outlierControllers;

    // Tracks occurrences of client pool exhaustions.
    // Not bundled in with request metrics, as we seek to not produce host-level metrics for economic reasons.
    private final Counter poolExhaustionCounter;

    public CassandraClientPoolMetrics(MetricsManager metricsManager) {
        this.metricsManager = metricsManager;
        this.aggregateRequestMetrics = new RequestMetrics(metricsManager, null);
        this.poolExhaustionCounter =
                metricsManager.registerOrGetCounter(CassandraClientPoolMetrics.class, "pool-exhaustion");
        this.outlierControllers = createOutlierControllers(metricsManager);
    }

    private static Map<CassandraClientPoolHostLevelMetric, DistributionOutlierController> createOutlierControllers(
            MetricsManager metricsManager) {
        ImmutableMap.Builder<CassandraClientPoolHostLevelMetric, DistributionOutlierController> builder =
                ImmutableMap.builder();
        Arrays.stream(CassandraClientPoolHostLevelMetric.values()).forEach(metric -> {
            DistributionOutlierController distributionOutlierController =
                    DistributionOutlierController.create(metric.minimumMeanThreshold, metric.maximumMeanThreshold);
            registerPoolMeanMetrics(metricsManager, metric, distributionOutlierController.getMeanGauge());
            builder.put(metric, distributionOutlierController);
        });
        return builder.build();
    }

    private static void registerPoolMeanMetrics(
            MetricsManager metricsManager, CassandraClientPoolHostLevelMetric metric, Gauge<Double> meanGauge) {
        metricsManager.registerOrGet(
                CassandraClientPoolingContainer.class, metric.metricName, meanGauge, ImmutableMap.of("pool", "mean"));
    }

    public void registerAggregateMetrics(Supplier<Integer> denylistSize) {
        // Keep metrics registered under CassandraClientPool.class rather than move them and potentially break things.
        metricsManager.registerMetric(CassandraClientPool.class, "numBlacklistedHosts", denylistSize::get);
        metricsManager.registerMetric(
                CassandraClientPool.class, "requestFailureProportion", aggregateRequestMetrics::getExceptionProportion);
        metricsManager.registerMetric(
                CassandraClientPool.class,
                "requestConnectionExceptionProportion",
                aggregateRequestMetrics::getConnectionExceptionProportion);
    }

    public void recordRequestOnHost(CassandraClientPoolingContainer hostPool) {
        updateMetricOnAggregateAndHost(hostPool, RequestMetrics::markRequest);
    }

    public void recordExceptionOnHost(CassandraClientPoolingContainer hostPool) {
        updateMetricOnAggregateAndHost(hostPool, RequestMetrics::markRequestException);
    }

    public void recordConnectionExceptionOnHost(CassandraClientPoolingContainer hostPool) {
        updateMetricOnAggregateAndHost(hostPool, RequestMetrics::markRequestConnectionException);
    }

    public void recordPoolExhaustion() {
        poolExhaustionCounter.inc();
    }

    @SuppressWarnings("unchecked") // Guaranteed to have the correct type
    public void registerPoolMetric(CassandraClientPoolHostLevelMetric metric, Gauge<Long> gauge, int poolNumber) {
        MetricPublicationFilter filter = outlierControllers.get(metric).registerAndCreateFilter(gauge);
        registerPoolMetricsToRegistry(metric, gauge, poolNumber, filter);
    }

    private void registerPoolMetricsToRegistry(
            CassandraClientPoolHostLevelMetric metric,
            Gauge<Long> gauge,
            int poolNumber,
            MetricPublicationFilter filter) {
        Map<String, String> poolTag = ImmutableMap.of("pool", "pool" + poolNumber);
        metricsManager.addMetricFilter(CassandraClientPoolingContainer.class, metric.metricName, poolTag, filter);
        metricsManager.registerOrGet(CassandraClientPoolingContainer.class, metric.metricName, gauge, poolTag);
    }

    private void updateMetricOnAggregateAndHost(
            CassandraClientPoolingContainer hostPool, Consumer<RequestMetrics> metricsConsumer) {
        metricsConsumer.accept(aggregateRequestMetrics);
        RequestMetrics requestMetricsForHost = metricsByHost.get(hostPool.getHost());
        if (requestMetricsForHost != null) {
            metricsConsumer.accept(requestMetricsForHost);
        }
    }

    private static class RequestMetrics {
        private final Meter totalRequests;
        private final Meter totalRequestExceptions;
        private final Meter totalRequestConnectionExceptions;

        RequestMetrics(MetricsManager metricsManager, String metricPrefix) {
            totalRequests = metricsManager.registerOrGetMeter(CassandraClientPool.class, metricPrefix, "requests");
            totalRequestExceptions =
                    metricsManager.registerOrGetMeter(CassandraClientPool.class, metricPrefix, "requestExceptions");
            totalRequestConnectionExceptions = metricsManager.registerOrGetMeter(
                    CassandraClientPool.class, metricPrefix, "requestConnectionExceptions");
        }

        void markRequest() {
            totalRequests.mark();
        }

        void markRequestException() {
            totalRequestExceptions.mark();
        }

        void markRequestConnectionException() {
            totalRequestConnectionExceptions.mark();
        }

        // Approximate
        double getExceptionProportion() {
            return ((double) totalRequestExceptions.getCount()) / ((double) totalRequests.getCount());
        }

        // Approximate
        double getConnectionExceptionProportion() {
            return ((double) totalRequestConnectionExceptions.getCount()) / ((double) totalRequests.getCount());
        }
    }
}
