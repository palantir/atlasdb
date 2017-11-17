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

import java.util.function.Supplier;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.cassandra.sidecar.metrics.CassandraMetricsService;
import com.palantir.remoting3.clients.ClientConfigurations;
import com.palantir.remoting3.jaxrs.JaxRsClient;

public class QosResource implements QosService {

    private final CassandraMetricsService cassandraMetricClient;
    private Supplier<QosServiceRuntimeConfig> config;
    private static final String COUNTER_ATTRIBUTE = "Count";
    private static final String GAUGE_ATTRIBUTE = "Value";
    private static MetricsManager metricsManager = new MetricsManager();
    private static EvictingQueue<PendingTaskMetric> queue = EvictingQueue.create(100);

    public QosResource(Supplier<QosServiceRuntimeConfig> config) {
        this.config = config;
        this.cassandraMetricClient = JaxRsClient.create(
                CassandraMetricsService.class,
                "qos-service",
                ClientConfigurations.of(config.get().cassandraServiceConfig()));
    }

    @Override
    public int getLimit(String client) {
        //TODO (hsaraogi): return long once the ratelimiter can handle it.
        int configLimit = config.get().clientLimits().getOrDefault(client, Integer.MAX_VALUE);
        int scaledLimit = (int) (configLimit * checkCassandraHealth());
        //TODO (hsaraogi): add client names as tags
        metricsManager.registerOrGetHistogram(QosResource.class, "scaledLimit").update(scaledLimit);
        return configLimit;
    }

    private double checkCassandraHealth() {
//        int readTimeoutCounter = getTimeoutCounter("Read");

        long numPendingCommitLogTasks = (long) cassandraMetricClient.getMetric(
                "CommitLog",
                "PendingTasks",
                GAUGE_ATTRIBUTE,
                ImmutableMap.of());
        queue.add(ImmutablePendingTaskMetric.builder()
                .numPendingTasks(numPendingCommitLogTasks)
                .timetamp(System.currentTimeMillis())
                .build());

        double averagePendingCommitLogTasks = (long) queue.stream()
                .mapToLong(PendingTaskMetric::numPendingTasks)
                .average()
                .getAsDouble();

        if (averagePendingCommitLogTasks < numPendingCommitLogTasks) {
            return (numPendingCommitLogTasks - averagePendingCommitLogTasks) / averagePendingCommitLogTasks;
        }
        return 1.0;
    }

    private Integer getTimeoutCounter(String operation) {
        return (Integer) cassandraMetricClient
                .getMetric("ClientRequest", "Timeouts", COUNTER_ATTRIBUTE,
                        ImmutableMap.of("scope", operation));
    }
}
