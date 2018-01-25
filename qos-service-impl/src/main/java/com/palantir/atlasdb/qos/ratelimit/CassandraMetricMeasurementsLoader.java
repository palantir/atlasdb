/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.qos.config.CassandraHealthMetric;
import com.palantir.atlasdb.qos.config.CassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.ImmutableCassandraHealthMetricMeasurement;
import com.palantir.cassandra.sidecar.SharedSecretHeader;
import com.palantir.cassandra.sidecar.metrics.CassandraMetricsService;

public class CassandraMetricMeasurementsLoader {
    private static final Logger log = LoggerFactory.getLogger(CassandraMetricMeasurementsLoader.class);

    private final Supplier<List<CassandraHealthMetric>> cassandraHealthMetrics;
    private final CassandraMetricsService cassandraMetricClient;
    private List<CassandraHealthMetricMeasurement> cassandraHealthMetricMeasurements;
    private final String header;

    public CassandraMetricMeasurementsLoader(Supplier<List<CassandraHealthMetric>> cassandraHealthMetrics,
            CassandraMetricsService cassandraMetricClient, ScheduledExecutorService scheduledExecutorService,
            String header) {
        this.cassandraHealthMetrics = cassandraHealthMetrics;
        this.cassandraMetricClient = cassandraMetricClient;
        this.header = header;
        this.cassandraHealthMetricMeasurements = new ArrayList<>();
        scheduledExecutorService
                .scheduleWithFixedDelay(() -> {
                    try {
                        loadCassandraMetrics();
                    } catch (Throwable t) {
                        log.error("Failed to refresh the cassandra metrics."
                                + " Extended periods of being unable to refresh will hinder QoS of all clients.", t);
                    }
                }, 0, 5, TimeUnit.SECONDS);
    }

    private void loadCassandraMetrics() {
        cassandraHealthMetricMeasurements = this.cassandraHealthMetrics.get().stream().map(metric ->
                ImmutableCassandraHealthMetricMeasurement.builder()
                        .currentValue(this.cassandraMetricClient.getMetric(
                                SharedSecretHeader.valueOf(header),
                                metric.type(),
                                metric.name(),
                                metric.attribute(),
                                metric.additionalParams()))
                        .lowerLimit(metric.lowerLimit())
                        .upperLimit(metric.upperLimit())
                        .build())
                .collect(Collectors.toList());
    }

    public List<CassandraHealthMetricMeasurement> getCassandraHealthMetricMeasurements() {
        return cassandraHealthMetricMeasurements;
    }
}
