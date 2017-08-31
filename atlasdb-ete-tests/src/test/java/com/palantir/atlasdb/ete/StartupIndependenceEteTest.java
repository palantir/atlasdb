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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionTimeoutException;
import com.palantir.atlasdb.containers.CassandraEnvironment;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;

public class StartupIndependenceEteTest {

    private static final List<String> ALL_CASSANDRA_NODES = ImmutableList.of("cassandra1", "cassandra2", "cassandra3");
    private static final List<String> QUORUM_OF_CASSANDRA_NODES = ImmutableList.of("cassandra1", "cassandra2");
    private static final List<String> ONE_CASSANDRA_NODE = ImmutableList.of("cassandra1");
    private static final List<String> CLIENTS = ImmutableList.of("ete1");
    private static final int CASSANDRA_PORT = 9160;

    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupWithoutWaiting(
            StartupIndependenceEteTest.class,
            "docker-compose.startup-independence.cassandra.yml",
            CLIENTS,
            CassandraEnvironment.get());

    @Before
    public void randomizeKeyspace() throws IOException, InterruptedException {
        EteSetup.execCliCommand("sed -i 's/keyspace: .*/keyspace: " + UUID.randomUUID().toString().replace("-", "_")
                + "/' var/conf/atlasdb-ete.yml");
    }

    @Test
    public void atlasStartsWithCassandraDownAndOnlyNeedsQuorumAfterTheFirstTime()
            throws IOException, InterruptedException {
        killCassandraNodes(ALL_CASSANDRA_NODES);
        restartAtlasWithChecks();
        assertNotSatisfiedWithin(40, StartupIndependenceEteTest::canPerformTransaction);
        startCassandraNodes(ALL_CASSANDRA_NODES);
        assertSatisfiedWithin(180, StartupIndependenceEteTest::canPerformTransaction);

        killCassandraNodes(ALL_CASSANDRA_NODES);
        restartAtlasWithChecks();
        assertNotSatisfiedWithin(40, StartupIndependenceEteTest::canPerformTransaction);
        startCassandraNodes(QUORUM_OF_CASSANDRA_NODES);
        assertSatisfiedWithin(180, StartupIndependenceEteTest::canPerformTransaction);
    }

    @Test
    public void atlasInitializesSynchronouslyIfCassandraIsInGoodState() throws InterruptedException, IOException {
        startCassandraNodes(ALL_CASSANDRA_NODES);
        restartAtlasWithChecks();
        Assert.assertTrue(canPerformTransaction());

        killCassandraNodes(ONE_CASSANDRA_NODE);
        restartAtlasWithChecks();
        Assert.assertTrue(canPerformTransaction());
    }

    private static void killCassandraNodes(List<String> nodeNames) throws InterruptedException {
        runOnCassandraNodes(nodeNames, StartupIndependenceEteTest::killCassandraContainer);
        nodeNames.forEach(node -> {
            DockerPort containerPort = new DockerPort(node, CASSANDRA_PORT, CASSANDRA_PORT);
            assertSatisfiedWithin(10, () -> !containerPort.isListeningNow());
        });
    }

    private static void startCassandraNodes(List<String> allCassandraNodes) throws InterruptedException {
        runOnCassandraNodes(allCassandraNodes, StartupIndependenceEteTest::startCassandraContainer);
    }

    private static void restartAtlasWithChecks() throws InterruptedException, IOException {
        stopAtlasServerAndAssertSuccess();
        startAtlasServerAndAssertSuccess();
    }

    private static void stopAtlasServerAndAssertSuccess() throws IOException, InterruptedException {
        EteSetup.execCliCommand("service/bin/init.sh stop");
        assertSatisfiedWithin(20, () -> !serverRunning());
    }

    private static void startAtlasServerAndAssertSuccess() throws IOException, InterruptedException {
        EteSetup.execCliCommand("service/bin/init.sh start");
        assertSatisfiedWithin(180, StartupIndependenceEteTest::serverRunning);
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

    private static void addTodo() {
        TodoResource todos = EteSetup.createClientToSingleNode(TodoResource.class);
        Todo todo = getUniqueTodo();

        todos.addTodo(todo);
    }

    private static Todo getUniqueTodo() {
        return ImmutableTodo.of("some unique TODO item with UUID=" + UUID.randomUUID());
    }

    private static void runOnCassandraNodes(List<String> nodes, CassandraContainerOperator operator)
            throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(nodes.size());

        executorService.invokeAll(nodes.stream()
                .map(cassandraContainer -> Executors.callable(() -> operator.nodeOperation(cassandraContainer)))
                .collect(Collectors.toList()));
    }

    private interface CassandraContainerOperator {
        void nodeOperation(String node);
    }

    public static void killCassandraContainer(String containerName) {
        Container container = EteSetup.getContainer(containerName);
        try {
            container.kill();
        } catch (IOException | InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    public static void startCassandraContainer(String containerName) {
        Container container = EteSetup.getContainer(containerName);
        try {
            container.start();
        } catch (IOException | InterruptedException e) {
            throw Throwables.propagate(e);
        }
        waitForCassandraContainer(container);
    }

    private static void waitForCassandraContainer(Container container) {
        DockerPort containerPort = new DockerPort(container.getContainerName(), CASSANDRA_PORT, CASSANDRA_PORT);
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .until(containerPort::isListeningNow);
    }
}
