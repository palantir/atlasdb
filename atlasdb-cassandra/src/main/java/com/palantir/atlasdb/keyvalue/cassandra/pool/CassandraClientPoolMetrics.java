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
import com.codahale.metrics.Meter;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.util.MetricsManager;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class CassandraClientPoolMetrics {
    private final MetricsManager metricsManager;
    private final RequestMetrics aggregateRequestMetrics;
    private final Map<InetSocketAddress, RequestMetrics> metricsByHost = new HashMap<>();

    // Tracks occurrences of client pool exhaustions.
    // Not bundled in with request metrics, as we seek to not produce host-level metrics for economic reasons.
    private final Counter poolExhaustionCounter;

    public CassandraClientPoolMetrics(MetricsManager metricsManager) {
        this.metricsManager = metricsManager;
        this.aggregateRequestMetrics = new RequestMetrics(metricsManager, null);
        this.poolExhaustionCounter
                = metricsManager.registerOrGetCounter(CassandraClientPoolMetrics.class, "pool-exhaustion");
    }

    public void registerAggregateMetrics(Supplier<Integer> blacklistSize) {
        // Keep metrics registered under CassandraClientPool.class rather than move them and potentially break things.
        metricsManager.registerMetric(
                CassandraClientPool.class, "numBlacklistedHosts",
                blacklistSize::get);
        metricsManager.registerMetric(
                CassandraClientPool.class, "requestFailureProportion",
                aggregateRequestMetrics::getExceptionProportion);
        metricsManager.registerMetric(
                CassandraClientPool.class, "requestConnectionExceptionProportion",
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

    private void updateMetricOnAggregateAndHost(
            CassandraClientPoolingContainer hostPool,
            Consumer<RequestMetrics> metricsConsumer) {
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
