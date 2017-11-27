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
import com.palantir.atlasdb.qos.config.CassandraHealthMetric;
import com.palantir.atlasdb.qos.config.CassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.ImmutableCassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.ImmutableQosClientLimitsConfig;
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
        this.cassandraMetricClient = config.get().cassandraServiceConfig()
                .map(cassandraServiceConfig -> Optional.of(JaxRsClient.create(
                        CassandraMetricsService.class,
                        "qos-service",
                        ClientConfigurations.of(cassandraServiceConfig))))
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
        if (cassandraMetricClient.isPresent()) {
            List<CassandraHealthMetric> cassandraHealthMetrics = config.get().cassandraHealthMetrics();

            List<CassandraHealthMetricMeasurement> cassandraHealthMetricMeasurements =
                    cassandraHealthMetrics.stream().map(metric ->
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

            return config.get()
                    .throttlingStrategy()
                    .getThrottlingStrategy()
                    .clientLimitMultiplier(cassandraHealthMetricMeasurements, qosPriority);
        }
        return 1.0;
    }


    //    private double checkCassandraHealth() {
    //        //        int readTimeoutCounter = getTimeoutCounter("Read");
    //        if (cassandraMetricClient.isPresent()) {
    //            Object numPendingCommitLogTasks = cassandraMetricClient.get().getMetric(
    //                    "CommitLog",
    //                    "PendingTasks",
    //                    GAUGE_ATTRIBUTE,
    //                    ImmutableMap.of());
    //
    //            Preconditions.checkState(numPendingCommitLogTasks instanceof Integer,
    //                    "Expected type Integer, found %s",
    //                    numPendingCommitLogTasks.getClass());
    //
    //            int numPendingCommitLogTasksInt = (int) numPendingCommitLogTasks;
    //
    //            queue.add(ImmutablePendingTaskMetric.builder()
    //                    .numPendingTasks(numPendingCommitLogTasksInt)
    //                    .timetamp(System.currentTimeMillis())
    //                    .build());
    //
    //            double averagePendingCommitLogTasks = queue.stream()
    //                    .mapToInt(PendingTaskMetric::numPendingTasks)
    //                    .average()
    //                    .getAsDouble();
    //
    //            if (Double.compare(averagePendingCommitLogTasks, (double) numPendingCommitLogTasks) < 0) {
    //                return 1.0 -
    //                        ((numPendingCommitLogTasksInt - averagePendingCommitLogTasks) / numPendingCommitLogTasksInt);
    //            }
    //        }
    //        return 1.0;
    //    }
}
