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

package com.palantir.atlasdb.qos;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.primitives.Ints;
import com.palantir.atlasdb.qos.config.CassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.ImmutableCassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.ImmutableQosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.QosCassandraMetricsConfig;
import com.palantir.atlasdb.qos.config.QosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.QosPriority;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;
import com.palantir.cassandra.sidecar.metrics.CassandraMetricsService;
import com.palantir.remoting3.clients.ClientConfigurations;
import com.palantir.remoting3.jaxrs.JaxRsClient;

public class QosResource implements QosService {

    private final Optional<CassandraMetricsService> cassandraMetricClient;
    private Supplier<QosServiceRuntimeConfig> config;

    public QosResource(Supplier<QosServiceRuntimeConfig> config) {
        this.config = config;
        this.cassandraMetricClient = config.get().qosCassandraMetricsConfig()
                .map(metricsConfig -> Optional.of(JaxRsClient.create(
                        CassandraMetricsService.class,
                        "qos-service",
                        ClientConfigurations.of(metricsConfig.cassandraServiceConfig()))))
                .orElse(Optional.empty());
    }

    @Override
    public int readLimit(String client) {
        QosClientLimitsConfig qosClientLimitsConfig = config.get().clientLimits().getOrDefault(client,
                ImmutableQosClientLimitsConfig.builder().build());
        return Ints.saturatedCast((long) getClientLimitMultiplier(qosClientLimitsConfig.clientPriority())
                * qosClientLimitsConfig.limits().readBytesPerSecond());
    }

    @Override
    public int writeLimit(String client) {
        QosClientLimitsConfig qosClientLimitsConfig = config.get().clientLimits().getOrDefault(client,
                ImmutableQosClientLimitsConfig.builder().build());
        return Ints.saturatedCast((long) getClientLimitMultiplier(qosClientLimitsConfig.clientPriority())
                * qosClientLimitsConfig.limits().writeBytesPerSecond());
    }

    private double getClientLimitMultiplier(QosPriority qosPriority) {
        Optional<QosCassandraMetricsConfig> qosCassandraMetricsConfig = config.get().qosCassandraMetricsConfig();
        if (qosCassandraMetricsConfig.isPresent() && cassandraMetricClient.isPresent()) {
            QosCassandraMetricsConfig metricsConfig = qosCassandraMetricsConfig.get();

            List<CassandraHealthMetricMeasurement> cassandraHealthMetricMeasurements =
                    metricsConfig.cassandraHealthMetrics().stream().map(metric ->
                            ImmutableCassandraHealthMetricMeasurement.builder()
                                    .currentValue(cassandraMetricClient.get().getMetric(
                                            metric.type(),
                                            metric.name(),
                                            metric.attribute(),
                                            metric.additionalParams()))
                                    .lowerLimit(metric.lowerLimit())
                                    .upperLimit(metric.upperLimit())
                                    .build())
                            .collect(Collectors.toList());

            return metricsConfig
                    .throttlingStrategy()
                    .getThrottlingStrategy()
                    .clientLimitMultiplier(cassandraHealthMetricMeasurements, qosPriority);
        }
        return 1.0;
    }
}
