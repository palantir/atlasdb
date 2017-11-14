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

import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;
import com.palantir.cassandra.sidecar.metrics.CassandraMetricsService;
import com.palantir.remoting3.clients.ClientConfigurations;
import com.palantir.remoting3.jaxrs.JaxRsClient;

public class QosResource implements QosService {

    private final CassandraMetricsService cassandraMetricClient;
    private Supplier<QosServiceRuntimeConfig> config;
    private static final String COUNTER_SCOPE = "Count";

    public QosResource(Supplier<QosServiceRuntimeConfig> config) {
        this.config = config;
        this.cassandraMetricClient = JaxRsClient.create(
                CassandraMetricsService.class,
                "qos-service",
                ClientConfigurations.of(config.get().cassandraServiceConfig()));
    }

    @Override
    public long getLimit(String client) {
        checkCassandraHealth();
        return config.get().clientLimits().getOrDefault(client, Long.MAX_VALUE);
    }

    private void checkCassandraHealth() {
        // type=ClientRequest
        // scope=Read,RangeSlice,Write
        // name=Timeouts/Failures//
        Counter readTimeoutCounter = getTimeoutCounter("Read");
        Counter writeTimeoutCounter = getTimeoutCounter("Write");
        Counter rangeSliceTimeoutCounter = getTimeoutCounter("RangeSlice");
        long totalTimeouts = rangeSliceTimeoutCounter.getCount() + writeTimeoutCounter.getCount() + readTimeoutCounter.getCount();

        System.out.println("totalTimeouts :" + totalTimeouts);

    }

    private Counter getTimeoutCounter(String operation) {
        return (Counter) cassandraMetricClient
                .getMetric("ClientRequest", "Timeouts", COUNTER_SCOPE, ImmutableMap.of("scope", operation));
    }
}
