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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.qos.config.ImmutableQosClientConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosLimitsConfig;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.TodoSchema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.remoting.api.config.service.HumanReadableDuration;

public class QosCassandraReadTestSuite extends EteSetup {
    private static final Random random = new Random();
    private static final int CASSANDRA_PORT_NUMBER = 9160;
    private static SerializableTransactionManager serializableTransactionManager;

    @ClassRule
    public static DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/cassandra-docker-compose.yml")
            .waitingForService("cassandra", Container::areAllPortsOpen)
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(QosCassandraReadTestSuite.class))
            .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
            .build();
    private static Supplier<Integer> readBytesPerSecond = new Supplier<Integer>() {
        int initial = 10_000;
        @Override
        public Integer get() {
            return initial + 1;
        }
    };
    private static Supplier<Integer> writeBytesPerSecond = new Supplier<Integer>() {
        int initial = 10_000;
        @Override
        public Integer get() {
            return initial + 1;
        }
    };

    @BeforeClass
    public static void waitUntilTransactionManagersIsUp() {
        serializableTransactionManager = TransactionManagers.builder()
                .config(getAtlasDbConfig())
                .runtimeConfigSupplier(QosCassandraReadTestSuite::getAtlasDbRuntimeConfig)
                .schemas(ImmutableList.of(TodoSchema.getSchema()))
                .userAgent("qos-test")
                .buildSerializable();

        Awaitility.await()
                .atMost(Duration.ONE_MINUTE)
                .pollInterval(Duration.ONE_SECOND)
                .until(serializableTransactionManager::isInitialized);

        IntStream.range(0, 100).forEach(i -> serializableTransactionManager
                .runTaskWithRetry((transaction) -> {
                    Cell thisCell = Cell.create(ValueType.FIXED_LONG.convertFromJava(random.nextLong()),
                            TodoSchema.todoTextColumn());
                    Map<Cell, byte[]> write = ImmutableMap.of(thisCell,
                            ValueType.STRING.convertFromJava(getTodoOfSize(1_000)));
                    transaction.put(TodoSchema.todoTable(), write);
                    return null;
                }));
    }

    @Test
    public void shouldBeAbleToReadSmallAmountOfBytesThatDoesntExceedLimit() {
        assertThat(readOneBatchOfSize(1)).hasSize(1);
    }

    @Test
    public void shouldBeAbleToReadSmallAmountOfBytesConcurrentlyThatDoesntExceedLimit() {
        assertThat(readOneBatchOfSize(1)).hasSize(1);
    }


    @Test
    public void shouldNotBeAbleToReadLargeAmountsTheFirstTime() {
        assertThatThrownBy(() -> readOneBatchOfSize(100))
                .isInstanceOf(AtlasDbDependencyException.class)
                .hasCause(new RuntimeException("rate limited"));
    }

    private List<ImmutableTodo> readOneBatchOfSize(int batchSize) {
        ImmutableList<RowResult<byte[]>> results = serializableTransactionManager.runTaskWithRetry((transaction) -> {
            BatchingVisitable<RowResult<byte[]>> rowResultBatchingVisitable = transaction.getRange(
                    TodoSchema.todoTable(), RangeRequest.all());
            ImmutableList.Builder<RowResult<byte[]>> rowResults = ImmutableList.builder();

            rowResultBatchingVisitable.batchAccept(batchSize, items -> {
                rowResults.addAll(items);
                return false;
            });

            return rowResults.build();
        });

        return results.stream()
                .map(RowResult::getOnlyColumnValue)
                .map(ValueType.STRING::convertToString)
                .map(ImmutableTodo::of)
                .collect(Collectors.toList());
    }

    @Test
    public void shouldBeAbleToReadSmallAmountsConsecutively() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        readOneBatchOfSize(1);
        long firstLargeReadLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        stopwatch = Stopwatch.createStarted();
        readOneBatchOfSize(1);
        long secondReadLatency = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        assertThat(secondReadLatency).isGreaterThan(firstLargeReadLatency);
    }

    public static AtlasDbConfig getAtlasDbConfig() {
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

    public static Optional<AtlasDbRuntimeConfig> getAtlasDbRuntimeConfig() {

        return Optional.of(ImmutableAtlasDbRuntimeConfig.builder()
                .sweep(ImmutableSweepConfig.builder().enabled(false).build())
                .qos(ImmutableQosClientConfig.builder()
                        .limits(ImmutableQosLimitsConfig.builder()
                                .readBytesPerSecond(readBytesPerSecond.get())
                                .writeBytesPerSecond(writeBytesPerSecond.get())
                                .build())
                        .maxBackoffSleepTime(HumanReadableDuration.seconds(10))
                        .build())
                .build());
    }

    private static String getTodoOfSize(int size) {
        return String.join("", Collections.nCopies(size, "a"));
    }
}
