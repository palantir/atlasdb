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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.net.ssl.SSLSocketFactory;

import org.junit.rules.RuleChain;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import com.palantir.docker.compose.execution.DockerComposeRunArgument;
import com.palantir.docker.compose.execution.DockerComposeRunOption;

public class EteSetup {
    private static final Gradle GRADLE_PREPARE_TASK = Gradle.ensureTaskHasRun(":atlasdb-ete-tests:prepareForEteTests");
    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();

    private static final String FIRST_ETE_CONTAINER = "ete1";
    private static final int ETE_PORT = 3828;

    private static DockerComposeRule docker;
    private static List<String> availableClients;

    protected static <T> T createClientToSingleNode(Class<T> clazz) {
        return createClientFor(clazz, asPort(FIRST_ETE_CONTAINER));
    }

    protected static <T> T createClientToAllNodes(Class<T> clazz) {
        return createClientToMultipleNodes(clazz, availableClients);
    }

    protected static <T> T createClientToMultipleNodes(Class<T> clazz, List<String> nodeNames) {
        Collection<String> uris = ImmutableList.copyOf(nodeNames).stream()
                .map(node -> asPort(node))
                .map(port -> port.inFormat("http://$HOST:$EXTERNAL_PORT"))
                .collect(Collectors.toList());

        return AtlasDbHttpClients.createProxyWithFailover(NO_SSL, uris, clazz);
    }

    protected static String runCliCommand(String command) throws IOException, InterruptedException {
        return docker.run(
                DockerComposeRunOption.options("-T"),
                "ete-cli",
                DockerComposeRunArgument.arguments("bash", "-c", command));
    }

    protected static RuleChain setupComposition(String name, String composeFile, List<String> availableClientNames) {
        availableClients = ImmutableList.copyOf(availableClientNames);

        docker = DockerComposeRule.builder()
                .file(composeFile)
                .waitingForService(FIRST_ETE_CONTAINER, toBeReady())
                .saveLogsTo("container-logs/" + name)
                .build();

        return RuleChain
                .outerRule(GRADLE_PREPARE_TASK)
                .around(docker);
    }

    private static DockerPort asPort(String node) {
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
}
