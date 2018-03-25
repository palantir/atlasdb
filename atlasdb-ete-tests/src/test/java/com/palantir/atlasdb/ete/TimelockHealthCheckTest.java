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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.todo.TodoSchema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

public class TimelockHealthCheckTest {
    private static final int CASSANDRA_PORT_NUMBER = 9160;

    private static SerializableTransactionManager serializableTransactionManager;

    @Test
    public void TimelockStatusIsHealthyAfterSuccessfulRequests() {
        ensureTransactionManagerIsCreated();
        serializableTransactionManager.getImmutableTimestamp();

        assertThat(serializableTransactionManager.getTimelockServiceStatus().isHealthy()).isTrue();
    }

    @Test
    public void TimelockStatusIsUnhealthyAfterFailedRequests() {
        ensureTransactionManagerIsCreated();
        try {
            EteSetup.getContainer("timelock").stop();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

        Awaitility.await()
                .ignoreExceptions()
                .atMost(1000, TimeUnit.MILLISECONDS)
                .until(() -> {
                    try {
                        serializableTransactionManager.getImmutableTimestamp();
                        return true;
                    } catch (Throwable t) {
                        return false;
                    }
                });

        assertThat(serializableTransactionManager.getTimelockServiceStatus().isHealthy()).isFalse();
    }

    private static void ensureTransactionManagerIsCreated() {
        serializableTransactionManager = TransactionManagers.builder()
                .config(getAtlasDbConfig())
                .userAgent("ete test")
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(unused -> { })
                .addAllSchemas(ImmutableList.of(TodoSchema.getSchema()))
                .build()
                .serializable();

        Awaitility.await()
                .atMost(Duration.ONE_MINUTE)
                .pollInterval(Duration.ONE_SECOND)
                .until(serializableTransactionManager::isInitialized);
    }

    private static AtlasDbConfig getAtlasDbConfig() {
        DockerPort cassandraPort = EteSetup.getContainer("cassandra")
                .port(CASSANDRA_PORT_NUMBER);

        InetSocketAddress cassandraAddress = new InetSocketAddress(cassandraPort.getIp(),
                cassandraPort.getExternalPort());

        CassandraKeyValueServiceConfig kvsConfig = ImmutableCassandraKeyValueServiceConfig.builder()
                .servers(ImmutableList.of(cassandraAddress))
                .credentials(ImmutableCassandraCredentialsConfig.builder()
                        .username("cassandra")
                        .password("cassandra")
                        .build())
                .ssl(false)
                .replicationFactor(1)
                .autoRefreshNodes(false)
                .build();

        TimeLockClientConfig timeLockClientConfig = ImmutableTimeLockClientConfig.builder()
                .serversList(ImmutableServerListConfig.builder()
                        .addServers("http://timelock:8421").build())
                .build();

        return ImmutableAtlasDbConfig.builder()
                .namespace("qosete")
                .keyValueService(kvsConfig)
                .timelock(timeLockClientConfig)
                .initializeAsync(true)
                .build();
    }
}
