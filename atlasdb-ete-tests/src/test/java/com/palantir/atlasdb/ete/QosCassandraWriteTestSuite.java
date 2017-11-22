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

import static com.palantir.atlasdb.ete.QosCassandraReadTestSuite.getAtlasDbConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.atlasdb.todo.TodoSchema;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.logging.LogDirectory;

public class QosCassandraWriteTestSuite {

    private static final Random random = new Random();
    private static SerializableTransactionManager serializableTransactionManager;

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
    public void shouldBeAbleToWriteBytesExcee() {
        serializableTransactionManager.runTaskWithRetry((transaction) -> {
                    writeNTodosOfSize(transaction, 200, 1_000);
                    return null;
                });
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

    private void ensureOneWriteHasOccurred(TodoResource todoClient) {
        try {
            todoClient.addTodo(getTodoOfSize(100_000));
            // okay as the first huge write is not rate limited.
        } catch (Exception e) {
            // okay as some other test might have written before
        }
    }

    @Test
    public void canNotWriteLargeNumberOfBytesConcurrentlyIfAllRequestsComeAtTheExactSameTime()
            throws InterruptedException {
        TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);

        CyclicBarrier barrier = new CyclicBarrier(100);
        ForkJoinPool threadPool = new ForkJoinPool(100);
        List<Future<?>> futures = Lists.newArrayList();

        IntStream.range(0, 100).parallel().forEach(i ->
                futures.add(threadPool.submit(
                        () -> {
                            barrier.await();
                            todoClient.addTodo(getTodoOfSize(1_000));
                            return null;
                        })));

        AtomicInteger exceptionCounter = new AtomicInteger(90);
        futures.forEach(future -> {
            try {
                future.get();
            } catch (ExecutionException e) {
                if (e.getCause().getClass().equals(AtlasDbRemoteException.class)) {
                    exceptionCounter.getAndIncrement();
                }
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        });
        assertThat(exceptionCounter.get()).isGreaterThan(90);
    }

    @Test
    public void canNotWriteLargeNumberOfBytesConcurrently() throws InterruptedException {
        TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);

        ForkJoinPool threadPool = new ForkJoinPool(100);
        List<Future<?>> futures = Lists.newArrayList();

        IntStream.range(0, 100).parallel()
                .forEach(i -> futures.add(threadPool.submit(() -> todoClient.addTodo(getTodoOfSize(1_000)))));

        threadPool.shutdown();
        Preconditions.checkState(threadPool.awaitTermination(90, TimeUnit.SECONDS),
                "Not all threads writing data finished in the expected time.");

        AtomicInteger exceptionCounter = new AtomicInteger(0);
        futures.forEach(future -> {
            try {
                future.get();
            } catch (ExecutionException e) {
                if (e.getCause().getClass().equals(AtlasDbRemoteException.class)) {
                    exceptionCounter.getAndIncrement();
                }
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        });
        assertThat(exceptionCounter.get()).isGreaterThan(0);
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
