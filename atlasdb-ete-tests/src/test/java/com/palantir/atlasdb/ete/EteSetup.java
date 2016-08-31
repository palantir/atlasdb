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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Duration;
import org.junit.rules.RuleChain;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import com.palantir.docker.compose.execution.DockerComposeExecArgument;
import com.palantir.docker.compose.execution.DockerComposeExecOption;
import com.palantir.docker.compose.execution.DockerComposeRunArgument;
import com.palantir.docker.compose.execution.DockerComposeRunOption;

public class EteSetup {
    private static final Gradle GRADLE_PREPARE_TASK = Gradle.ensureTaskHasRun(":atlasdb-ete-tests:prepareForEteTests");
    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();

    private static final String FIRST_ETE_CONTAINER = "ete1";
    private static final int ETE_PORT = 3828;
    public static final int NUM_CASSANDRA_NODES = 3;

    private static DockerComposeRule docker;
    private static final String NODE_UP_STATUS = "UN  ";
    private static final String NODE_DOWN_STATUS = "DN  ";

    protected <T> T createClientToSingleNode(Class<T> clazz) {
        return createClientFor(clazz, asPort(FIRST_ETE_CONTAINER));
    }

    protected <T> T createClientToMultipleNodes(Class<T> clazz, String... nodeNames) {
        Collection<String> uris = ImmutableList.copyOf(nodeNames).stream()
                .map(node -> asPort(node))
                .map(port -> port.inFormat("http://$HOST:$EXTERNAL_PORT"))
                .collect(Collectors.toList());

        return AtlasDbHttpClients.createProxyWithFailover(NO_SSL, uris, clazz);
    }

    protected String runCommand(String command) throws IOException, InterruptedException {
        return docker.run(
                DockerComposeRunOption.options("-T"),
                "ete-cli",
                DockerComposeRunArgument.arguments("bash", "-c", command));
    }

    protected static RuleChain setupComposition(String name, String composeFile) {
        docker = DockerComposeRule.builder()
                .file(composeFile)
                .waitingForService(FIRST_ETE_CONTAINER, toBeReady(), Duration.standardMinutes(3))
                .saveLogsTo("container-logs/" + name)
                .build();

        return RuleChain
                .outerRule(GRADLE_PREPARE_TASK)
                .around(docker);
    }

    private DockerPort asPort(String node) {
        return docker.containers().container(node).port(ETE_PORT);
    }

    private static <T> T createClientFor(Class<T> clazz, Container container) {
        return createClientFor(clazz, container.portMappedInternallyTo(ETE_PORT));
    }

    private static <T> T createClientFor(Class<T> clazz, DockerPort port) {
        String uri = port.inFormat("http://$HOST:$EXTERNAL_PORT");
        return AtlasDbHttpClients.createProxy(NO_SSL, uri, clazz);
    }

    private static HealthCheck<Container> toBeReady() {
        return (container) -> {
            TodoResource todos = createClientFor(TodoResource.class, container);

            return SuccessOrFailure.onResultOf(() -> {
                todos.isHealthy();
                return true;
            });
        };
    }

    protected void stopCassandraContainer(String containerName) {
        Container container = docker.containers().container(containerName);
        try {
            container.kill();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        waitForNodetoolToConfirmStatus(container, NODE_DOWN_STATUS, 1);
    }

    protected void startCassandraContainer(String containerName) {
        Container container = docker.containers().container(containerName);
        try {
            container.start();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        waitForCassandraContainerToBeReady(container);
    }

    private void waitForCassandraContainerToBeReady(Container container) {
        waitForAllPorts(container);
        waitForNodetoolToConfirmStatus(container, NODE_UP_STATUS, NUM_CASSANDRA_NODES);
    }

    private void waitForAllPorts(Container container) {
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .until(() -> container.areAllPortsOpen().succeeded());
    }

    private void waitForNodetoolToConfirmStatus(Container container, String status, int expectedNodeCount) {
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(() -> {
                    try {
                        String nodetoolStatus = docker.exec(DockerComposeExecOption.options("-T"),
                                container.getContainerName(),
                                DockerComposeExecArgument.arguments("bash", "-c", "nodetool status | grep UN"));
                        return StringUtils.countMatches(nodetoolStatus, status) == expectedNodeCount;
                    } catch (Exception e) {
                        return false;
                    }
                });
    }
}
