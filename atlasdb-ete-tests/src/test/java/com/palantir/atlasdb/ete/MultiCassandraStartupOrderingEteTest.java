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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionTimeoutException;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.docker.compose.connection.DockerPort;

@RunWith(Parameterized.class)
public class MultiCassandraStartupOrderingEteTest {

    private final List<String> cassandraNodesToStartOrStop;
    private static final int CASSANDRA_PORT = 9160;

    public MultiCassandraStartupOrderingEteTest(List<String> cassandraNodesToStartorStop) {
        this.cassandraNodesToStartOrStop = cassandraNodesToStartorStop;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testCases() {
        Collection<Object[]> params = Lists.newArrayList();

        params.add(new Object[] {ImmutableList.of("cassandra1")});
        params.add(new Object[] {ImmutableList.of("cassandra1", "cassandra2")});
        return params;
    }

    @Before
    public void randomizeKeyspace() throws IOException, InterruptedException {
        EteSetup.execCliCommand("sed -i 's/keyspace: .*/keyspace: " + UUID.randomUUID().toString().replace("-", "_")
                + "/' var/conf/atlasdb-ete.yml");
    }

    @After
    public void restoreCassandra() throws InterruptedException {
        startCassandraNodes();
    }

    @Test
    public void shouldBeAbleToStartUpIfCassandraIsDownAndRunTransactionsWhenCassandraIsUp()
            throws IOException, InterruptedException {
        restartAtlasWithDegradedCassandra();
        assertNotSatisfiedWithin(40, MultiCassandraStartupOrderingEteTest::canPerformTransaction);

        startCassandraNodes();
        assertSatisfiedWithin(60, MultiCassandraStartupOrderingEteTest::canPerformTransaction);

        restartAtlasWithDegradedCassandra();
        Assert.assertEquals(hasQuorum(), canPerformTransaction());
    }

    private void restartAtlasWithDegradedCassandra() throws IOException, InterruptedException {
        stopAtlasServer();
        stopCassandraNodes();
        startTheAtlasServer();
    }

    private static void stopAtlasServer() throws IOException, InterruptedException {
        EteSetup.execCliCommand("service/bin/init.sh stop");
        assertSatisfiedWithin(10, () -> !serverRunning());
    }

    private static void startTheAtlasServer() throws IOException, InterruptedException {
        EteSetup.execCliCommand("service/bin/init.sh start");
        assertSatisfiedWithin(240, MultiCassandraStartupOrderingEteTest::serverRunning);
    }

    private void stopCassandraNodes() throws InterruptedException {
        runOnCassandraNodes(cassandraNodesToStartOrStop, MultiCassandraTestSuite::killCassandraContainer);
        cassandraNodesToStartOrStop.forEach(node -> {
            DockerPort containerPort = new DockerPort(node, CASSANDRA_PORT, CASSANDRA_PORT);
            assertSatisfiedWithin(10, () -> !containerPort.isListeningNow());
        });
    }

    private void startCassandraNodes() throws InterruptedException {
        runOnCassandraNodes(cassandraNodesToStartOrStop, MultiCassandraTestSuite::startCassandraContainer);
    }

    private static void assertSatisfiedWithin(long time, Callable<Boolean> condition) {
        Awaitility.waitAtMost(time, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until(condition);
    }

    private static void assertNotSatisfiedWithin(long time, Callable<Boolean> condition) {
        Assertions.assertThatThrownBy(
                () -> Awaitility
                        .waitAtMost(time, TimeUnit.SECONDS)
                        .pollInterval(2, TimeUnit.SECONDS)
                        .until(condition))
                .isInstanceOf(ConditionTimeoutException.class);
    }

    private static boolean serverRunning() {
        try {
            canPerformTransaction();
            return true;
        } catch (AtlasDbRemoteException e) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean canPerformTransaction() {
        try {
            addTodo();
            return true;
        } catch (AtlasDbRemoteException e) {
            return false;
        }
    }

    private boolean hasQuorum() {
        return cassandraNodesToStartOrStop.size() < 2;
    }

    private static void addTodo() {
        TodoResource todos = EteSetup.createClientToSingleNode(TodoResource.class);
        Todo todo = getUniqueTodo();

        todos.addTodo(todo);
    }

    private static Todo getUniqueTodo() {
        return ImmutableTodo.of("some unique TODO item with UUID=" + UUID.randomUUID());
    }

    private void runOnCassandraNodes(List<String> nodes, CassandraContainerOperator operator)
            throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(nodes.size());

        executorService.invokeAll(nodes.stream()
                .map(cassandraContainer -> Executors.callable(() -> operator.nodeOperation(cassandraContainer)))
                .collect(Collectors.toList()));
    }

    interface CassandraContainerOperator {
        void nodeOperation(String node);
    }
}
