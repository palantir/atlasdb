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

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import com.palantir.atlasdb.qos.config.CassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.QosCassandraMetricsInstallConfig;
import com.palantir.atlasdb.qos.config.QosCassandraMetricsRuntimeConfig;
import com.palantir.atlasdb.qos.config.QosPriority;
import com.palantir.atlasdb.qos.config.ThrottlingStrategy;
import com.palantir.cassandra.sidecar.metrics.CassandraMetricsService;
import com.palantir.remoting3.clients.ClientConfigurations;
import com.palantir.remoting3.jaxrs.JaxRsClient;

@SuppressWarnings("checkstyle:FinalClass") // Required for testing
public class CassandraMetricsClientLimitMultiplier implements ClientLimitMultiplier {
    private final ThrottlingStrategy throttlingStrategy;
    private final CassandraMetricMeasurementsLoader cassandraHealthMetrics;

    private CassandraMetricsClientLimitMultiplier(
            ThrottlingStrategy throttlingStrategy,
            CassandraMetricMeasurementsLoader cassandraHealthMetrics) {
        this.throttlingStrategy = throttlingStrategy;
        this.cassandraHealthMetrics = cassandraHealthMetrics;
    }

    public static ClientLimitMultiplier create(Supplier<QosCassandraMetricsRuntimeConfig> runtimeConfig,
            QosCassandraMetricsInstallConfig installConfig, ScheduledExecutorService metricsLoaderExecutor) {
        CassandraMetricsService metricsService = JaxRsClient.create(
                CassandraMetricsService.class,
                "qos-service",
                ClientConfigurations.of(installConfig.cassandraServiceConfig()));
        ThrottlingStrategy throttlingStrategy = ThrottlingStrategyFactory.getThrottlingStrategy(
                installConfig.throttlingStrategy());

        CassandraMetricMeasurementsLoader cassandraMetricMeasurementsLoader = new CassandraMetricMeasurementsLoader(
                () -> runtimeConfig.get().cassandraHealthMetrics(), metricsService, metricsLoaderExecutor);
        return new CassandraMetricsClientLimitMultiplier(throttlingStrategy, cassandraMetricMeasurementsLoader);
    }

    public double getClientLimitMultiplier(QosPriority qosPriority) {
        if (qosPriority == QosPriority.HIGH) {
            return 1.0; // don't lower the limit for HIGH priority clients.
        }

        return throttlingStrategy.getClientLimitMultiplier(getCassandraHealthMetricMeasurements());
    }

    private Supplier<Collection<? extends CassandraHealthMetricMeasurement>> getCassandraHealthMetricMeasurements() {
        return cassandraHealthMetrics::getCassandraHealthMetricMeasurements;
    }
}
