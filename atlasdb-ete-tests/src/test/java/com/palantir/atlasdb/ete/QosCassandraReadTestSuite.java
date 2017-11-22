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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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
import com.palantir.atlasdb.qos.ratelimit.RateLimitExceededException;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoSchema;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.remoting.api.config.service.HumanReadableDuration;

public class QosCassandraReadTestSuite {
    private static final Random random = new Random();
    private static final int CASSANDRA_PORT_NUMBER = 9160;
    private static SerializableTransactionManager serializableTransactionManager;
    private static final int readBytesPerSecond = 10_000;
    private static final int writeBytesPerSecond = 10_000;
    private static final int ONE_TODO_SIZE_IN_BYTES = 1050;

    @ClassRule
    public static DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/cassandra-docker-compose.yml")
            .waitingForService("cassandra", Container::areAllPortsOpen)
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(QosCassandraReadTestSuite.class))
            .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
            .build();

    @BeforeClass
    public static void createTransactionManagerAndWriteData() {
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

        serializableTransactionManager.runTaskWithRetry((transaction) -> {
                    writeNTodosOfSize(transaction, 200, 1_000);
                    return null;
                });
        serializableTransactionManager.close();
    }

    public static void writeNTodosOfSize(Transaction transaction, int numTodos, int size) {
        Map<Cell, byte[]> write = new HashMap<>();
        for (int i = 0; i < numTodos; i++) {
            Cell thisCell = Cell.create(ValueType.FIXED_LONG.convertFromJava(random.nextLong()),
                    TodoSchema.todoTextColumn());
            write.put(thisCell, ValueType.STRING.convertFromJava(getTodoOfSize(size)));
        }
        transaction.put(TodoSchema.todoTable(), write);
    }

    @Before
    public void setup() {
        AtlasDbMetrics.setMetricRegistry(new MetricRegistry());

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
    }

    @Test
    public void shouldBeAbleToReadSmallAmountOfBytesIfDoesNotExceedLimit() {
        assertThat(readOneBatchOfSize(1)).hasSize(1);
    }

    @Test
    public void shouldBeAbleToReadSmallAmountOfBytesSeriallyIfDoesNotExceedLimit() {
        IntStream.range(0, 50)
                .forEach(i -> assertThat(readOneBatchOfSize(1)).hasSize(1));
    }

    @Test
    public void shouldBeAbleToReadLargeAmountsExceedingTheLimitFirstTime() {
        assertThat(readOneBatchOfSize(12)).hasSize(12);
    }

    @Test
    public void shouldBeAbleToReadLargeAmountsExceedingTheLimitSecondTimeWithSoftLimiting() {
        assertThat(readOneBatchOfSize(12)).hasSize(12);
        // The second read might actually be faster as the transaction/metadata
        assertThat(readOneBatchOfSize(12)).hasSize(12);
    }

    @Test
    public void shouldNotBeAbleToReadLargeAmountsIfSoftLimitSleepWillBeMoreThanConfiguredBackoffTime() {
        assertThatThrownBy(() -> readOneBatchOfSize(200))
                .isInstanceOf(RateLimitExceededException.class)
                .hasMessage("Rate limited. Available capacity has been exhausted.");
    }

    @Test
    public void readRateLimitShouldBeRespectedByConcurrentReadingThreads() throws InterruptedException {
        int numThreads = 5;
        int numReadsPerThread = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<List<Todo>>> futures = new ArrayList<>(numThreads);

        long start = System.nanoTime();
        IntStream.range(0, numThreads)
                .forEach(i ->
                        futures.add(executorService.submit(() -> {
                            List<Todo> results = new ArrayList<>(numReadsPerThread);
                            IntStream.range(0, numReadsPerThread)
                                    .forEach(j -> results.addAll(readOneBatchOfSize(1)));
                            return results;
                        }))
                );
        executorService.shutdown();
        Preconditions.checkState(executorService.awaitTermination(30L, TimeUnit.SECONDS),
                "Read tasks did not finish in 30s");
        long timeTakenToReadInSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);

        assertThatAllReadsWereSuccessful(futures, numReadsPerThread);
        long actualReadRate = (numThreads * numReadsPerThread * ONE_TODO_SIZE_IN_BYTES) / timeTakenToReadInSeconds;
        assertThat(actualReadRate).isLessThan(readBytesPerSecond + (readBytesPerSecond / 10 /* to allow burst time */));
    }

    private void assertThatAllReadsWereSuccessful(List<Future<List<Todo>>> futures, int numReadsPerThread) {
        AtomicInteger exceptionCounter = new AtomicInteger(0);
        futures.forEach(future -> {
            try {
                assertThat(future.get()).hasSize(numReadsPerThread);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RateLimitExceededException) {
                    exceptionCounter.getAndIncrement();
                }
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        });
        assertThat(exceptionCounter.get()).isEqualTo(0);
    }

    private List<Todo> readOneBatchOfSize(int batchSize) {
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
    public void cleanup() {
        serializableTransactionManager.close();
    }

    private static String getTodoOfSize(int size) {
        // Note that the size of the cell for 1000 length text is actually 1050.
        return String.join("", Collections.nCopies(size, "a"));
    }
}
