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

package com.palantir.atlasdb.qos.ratelimit;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.palantir.atlasdb.qos.config.CassandraHealthMetric;
import com.palantir.atlasdb.qos.config.CassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.ImmutableCassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.QosCassandraMetricsConfig;
import com.palantir.atlasdb.qos.config.QosPriority;
import com.palantir.atlasdb.qos.config.ThrottlingStrategy;
import com.palantir.cassandra.sidecar.metrics.CassandraMetricsService;
import com.palantir.remoting3.clients.ClientConfigurations;
import com.palantir.remoting3.jaxrs.JaxRsClient;

public final class CassandraMetricsClientLimitMultiplier implements ClientLimitMultiplier {
    private final CassandraMetricsService cassandraMetricClient;
    private Supplier<QosCassandraMetricsConfig> config;
    private ThrottlingStrategy throttlingStrategy;

    private CassandraMetricsClientLimitMultiplier(
            CassandraMetricsService cassandraMetricsService,
            Supplier<QosCassandraMetricsConfig> config,
            ThrottlingStrategy throttlingStrategy) {
        this.cassandraMetricClient = cassandraMetricsService;
        this.config = config;
        this.throttlingStrategy = throttlingStrategy;
    }

    public static ClientLimitMultiplier create(Supplier<QosCassandraMetricsConfig> config) {
        CassandraMetricsService metricsService = JaxRsClient.create(
                CassandraMetricsService.class,
                "qos-service",
                ClientConfigurations.of(config.get().cassandraServiceConfig()));

        return new CassandraMetricsClientLimitMultiplier(metricsService, config,
                config.get().throttlingStrategy().getThrottlingStrategy());
    }

    public double getClientLimitMultiplier(QosPriority qosPriority) {
        List<CassandraHealthMetricMeasurement> cassandraHealthMetricMeasurements =
                getCassandraHealthMetricMeasurements(config.get().cassandraHealthMetrics());

        return throttlingStrategy.clientLimitMultiplier(cassandraHealthMetricMeasurements, qosPriority);
    }

    private List<CassandraHealthMetricMeasurement> getCassandraHealthMetricMeasurements(
            List<CassandraHealthMetric> metricsToMeasure) {
        return metricsToMeasure.stream().map(metric ->
                ImmutableCassandraHealthMetricMeasurement.builder()
                        .currentValue(cassandraMetricClient.getMetric(
                                metric.type(),
                                metric.name(),
                                metric.attribute(),
                                metric.additionalParams()))
                        .lowerLimit(metric.lowerLimit())
                        .upperLimit(metric.upperLimit())
                        .build())
                .collect(Collectors.toList());
    }
}
