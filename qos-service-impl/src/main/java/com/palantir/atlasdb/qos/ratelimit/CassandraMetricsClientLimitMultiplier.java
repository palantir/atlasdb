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
package com.palantir.atlasdb.qos.ratelimit;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import com.palantir.atlasdb.qos.config.CassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.QosCassandraMetricsInstallConfig;
import com.palantir.atlasdb.qos.config.QosCassandraMetricsRuntimeConfig;
import com.palantir.atlasdb.qos.config.ThrottlingStrategy;
import com.palantir.atlasdb.qos.metrics.MetricsService;

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
            QosCassandraMetricsInstallConfig installConfig,
            MetricsService metricsService,
            ScheduledExecutorService metricsLoaderExecutor) {
        ThrottlingStrategy throttlingStrategy = ThrottlingStrategies.getThrottlingStrategy(
                installConfig.throttlingStrategy());

        CassandraMetricMeasurementsLoader cassandraMetricMeasurementsLoader = new CassandraMetricMeasurementsLoader(
                () -> runtimeConfig.get().cassandraHealthMetrics(), metricsService, metricsLoaderExecutor);
        return new CassandraMetricsClientLimitMultiplier(throttlingStrategy, cassandraMetricMeasurementsLoader);
    }

    public double getClientLimitMultiplier() {
        return throttlingStrategy.getClientLimitMultiplier(getCassandraHealthMetricMeasurements());
    }

    private Supplier<Collection<? extends CassandraHealthMetricMeasurement>> getCassandraHealthMetricMeasurements() {
        return cassandraHealthMetrics::getCassandraHealthMetricMeasurements;
    }
}
