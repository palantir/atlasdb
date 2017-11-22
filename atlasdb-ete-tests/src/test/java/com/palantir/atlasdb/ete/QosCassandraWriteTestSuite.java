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
package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableSweepConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.qos.config.ImmutableQosClientConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosLimitsConfig;
import com.palantir.atlasdb.qos.ratelimit.RateLimitExceededException;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.todo.TodoSchema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.remoting.api.config.service.HumanReadableDuration;

public class QosCassandraWriteTestSuite {
    private static final Random random = new Random();
    private static SerializableTransactionManager serializableTransactionManager;
    private static final int readBytesPerSecond = 10_000;
    private static final int writeBytesPerSecond = 10_000;
    private static final int CASSANDRA_PORT_NUMBER = 9160;

    @ClassRule
    public static DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/cassandra-docker-compose.yml")
            .waitingForService("cassandra", Container::areAllPortsOpen)
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(QosCassandraReadTestSuite.class))
            .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
            .build();

    @Before
    public void setup() {
        AtlasDbMetrics.setMetricRegistry(new MetricRegistry());

        serializableTransactionManager = TransactionManagers.builder()
                .config(getAtlasDbConfig())
                .runtimeConfigSupplier(QosCassandraWriteTestSuite::getAtlasDbRuntimeConfig)
                .schemas(ImmutableList.of(TodoSchema.getSchema()))
                .userAgent("qos-test")
                .buildSerializable();

        Awaitility.await()
                .atMost(Duration.ONE_MINUTE)
                .pollInterval(Duration.ONE_SECOND)
                .until(serializableTransactionManager::isInitialized);
    }

    @Test
    public void shouldBeAbleToWriteSmallAmountOfBytesIfDoesNotExceedLimit() {
        writeNTodosOfSize(1, 100);
    }

    @Test
    public void shouldBeAbleToWriteSmallAmountOfBytesSeriallyIfDoesNotExceedLimit() {
        IntStream.range(0, 50)
                .forEach(i -> writeNTodosOfSize(1, 100));
    }

    @Test
    public void shouldBeAbleToWriteLargeAmountsExceedingTheLimitFirstTime() {
        writeNTodosOfSize(12, 1_000);
    }

    @Test
    public void shouldBeAbleToWriteLargeAmountsExceedingTheLimitSecondTimeWithSoftLimiting() {
        writeNTodosOfSize(12, 1_000);

        writeNTodosOfSize(12, 1_000);
    }

    @Test
    public void shouldNotBeAbleToWriteLargeAmountsIfSoftLimitSleepWillBeMoreThanConfiguredBackoffTime() {
        // Have one limit-exceeding write
        // as the rate-limiter will let anything pass through until the limit is exceeded.
        writeNTodosOfSize(12, 1_000);

        assertThatThrownBy(() -> writeNTodosOfSize(200, 1_000))
                .isInstanceOf(RateLimitExceededException.class)
                .hasMessage("Rate limited. Available capacity has been exhausted.");
    }


    public static void writeNTodosOfSize(int numTodos, int size) {
        serializableTransactionManager.runTaskWithRetry((transaction) -> {
            Map<Cell, byte[]> write = new HashMap<>();
            for (int i = 0; i < numTodos; i++) {
                Cell thisCell = Cell.create(ValueType.FIXED_LONG.convertFromJava(random.nextLong()),
                        TodoSchema.todoTextColumn());
                write.put(thisCell, ValueType.STRING.convertFromJava(getTodoOfSize(size)));
            }
            transaction.put(TodoSchema.todoTable(), write);
            return null;
        });

    }

    private static AtlasDbConfig getAtlasDbConfig() {
        DockerPort cassandraPort = docker.containers()
                .container("cassandra")
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

        return ImmutableAtlasDbConfig.builder()
                .namespace("qosete")
                .keyValueService(kvsConfig)
                .initializeAsync(true)
                .build();
    }

    private static Optional<AtlasDbRuntimeConfig> getAtlasDbRuntimeConfig() {
        return Optional.of(ImmutableAtlasDbRuntimeConfig.builder()
                .sweep(ImmutableSweepConfig.builder().enabled(false).build())
                .qos(ImmutableQosClientConfig.builder()
                        .limits(ImmutableQosLimitsConfig.builder()
                                .readBytesPerSecond(readBytesPerSecond)
                                .writeBytesPerSecond(writeBytesPerSecond)
                                .build())
                        .maxBackoffSleepTime(HumanReadableDuration.seconds(2))
                        .build())
                .build());
    }

    @After
    public void after() {
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
    }

    private static String getTodoOfSize(int size) {
        // Note that the size of the cell for 1000 length text is actually 1050.
        return String.join("", Collections.nCopies(size, "a"));
    }
}
