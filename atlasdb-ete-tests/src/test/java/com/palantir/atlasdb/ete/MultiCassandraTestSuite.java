/**
 * Copyright 2016 Palantir Technologies
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.collect.ImmutableList;
import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        MultiCassandraPerformanceEteTest.class,
        // TODO(@gsheasby) the below tests can be added to the suite, but currently
        // they assume a different docker setup.
        //TodoEteTest.class,
        //DropwizardEteTest.class
    })
public final class MultiCassandraTestSuite {
    private MultiCassandraTestSuite() {
        // Suite class
    }

    private static final List<String> CASSANDRA_NODES = ImmutableList.of("cassandra1", "cassandra2", "cassandra3");

    private static final Gradle GRADLE_PREPARE_TASK =
            Gradle.ensureTaskHasRun(":atlasdb-ete-test-utils:buildCassandraImage");

    private static final int TIMELOCK_SERVER_PORT = 3828;

    private static final String CONTAINER_LOGS_DIRECTORY = "container-logs/cassandra-multinode";

    private static final DockerComposeRule MULTINODE_CASSANDRA_SETUP = DockerComposeRule.builder()
            .file("docker-compose.multiple-cassandra.yml")
            .waitingForService(CASSANDRA_NODES.get(0), Container::areAllPortsOpen)
            .waitingForService(CASSANDRA_NODES.get(1), Container::areAllPortsOpen)
            .waitingForService(CASSANDRA_NODES.get(2), Container::areAllPortsOpen)
            .waitingForService("ete1", toBeReady())
            .saveLogsTo(CONTAINER_LOGS_DIRECTORY)
            .build();

    @ClassRule
    public static final RuleChain PREPARED_DOCKER_SETUP = RuleChain
            .outerRule(GRADLE_PREPARE_TASK)
            .around(MULTINODE_CASSANDRA_SETUP);

    private static HealthCheck<Container> toBeReady() {
        return (container) -> {
            TodoResource todos = createClientFor(TodoResource.class, container.port(TIMELOCK_SERVER_PORT));

            return SuccessOrFailure.onResultOf(() -> {
                todos.isHealthy();
                return true;
            });
        };
    }

    public static TodoResource createTodoResouce() {
        return createClientFor(TodoResource.class, asPort("ete1"));
    }

    private static <T> T createClientFor(Class<T> clazz, DockerPort port) {
        String uri = port.inFormat("http://$HOST:$EXTERNAL_PORT");
        return AtlasDbHttpClients.createProxy(com.google.common.base.Optional.absent(), uri, clazz);
    }

    private static DockerPort asPort(String node) {
        return MULTINODE_CASSANDRA_SETUP.containers().container(node).port(TIMELOCK_SERVER_PORT);
    }

    public static String getRandomCassandraNode() {
        int index = ThreadLocalRandom.current().nextInt(CASSANDRA_NODES.size());
        return CASSANDRA_NODES.get(index);
    }

    public static void killCassandraContainer(String containerName) {
        Container container = MULTINODE_CASSANDRA_SETUP.containers().container(containerName);
        try {
            container.kill();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void startCassandraContainer(String containerName) throws InterruptedException {
        Container container = MULTINODE_CASSANDRA_SETUP.containers().container(containerName);
        try {
            container.start();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        waitForAllPorts(container);
    }

    private static void waitForAllPorts(Container container) {
        Awaitility.await()
                .atMost(50, TimeUnit.SECONDS)
                .until(() -> container.areAllPortsOpen().succeeded());
    }

    public static void waitUntilAllNodesAreUp() {
        for (String containerName : CASSANDRA_NODES) {
            Container container = MULTINODE_CASSANDRA_SETUP.containers().container(containerName);
            waitForAllPorts(container);
        }
    }
}
