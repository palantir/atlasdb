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

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.codahale.metrics.Meter;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.util.MetricsManager;

public class CassandraClientPoolMetrics {
    private final MetricsManager metricsManager;
    private final RequestMetrics aggregateMetrics;
    private final Map<InetSocketAddress, RequestMetrics> metricsByHost = new HashMap<>();

    public CassandraClientPoolMetrics(MetricsManager metricsManager) {
        this.metricsManager = metricsManager;
        this.aggregateMetrics = new RequestMetrics(metricsManager, null);
    }

    public void registerAggregateMetrics(Supplier<Integer> blacklistSize) {
        // Keep metrics registered under CassandraClientPool.class rather than move them and potentially break things.
        metricsManager.registerMetric(
                CassandraClientPool.class, "numBlacklistedHosts",
                () -> blacklistSize.get());
        metricsManager.registerMetric(
                CassandraClientPool.class, "requestFailureProportion",
                aggregateMetrics::getExceptionProportion);
        metricsManager.registerMetric(
                CassandraClientPool.class, "requestConnectionExceptionProportion",
                aggregateMetrics::getConnectionExceptionProportion);
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

    private void updateMetricOnAggregateAndHost(
            CassandraClientPoolingContainer hostPool,
            Consumer<RequestMetrics> metricsConsumer) {
        metricsConsumer.accept(aggregateMetrics);
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
            totalRequests = metricsManager.registerOrGetMeter(
                    CassandraClientPool.class, metricPrefix, "requests");
            totalRequestExceptions = metricsManager.registerOrGetMeter(
                    CassandraClientPool.class, metricPrefix, "requestExceptions");
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
