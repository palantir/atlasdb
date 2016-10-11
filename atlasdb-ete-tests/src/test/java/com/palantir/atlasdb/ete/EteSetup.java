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
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import javax.net.ssl.SSLSocketFactory;

import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.testing.DockerProxyRule;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.execution.DockerComposeRunArgument;
import com.palantir.docker.compose.execution.DockerComposeRunOption;

public class EteSetup {
    private static final Gradle GRADLE_PREPARE_TASK = Gradle.ensureTaskHasRun(":atlasdb-ete-tests:prepareForEteTests");
    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();

    private static final short SERVER_PORT = 3828;

    private static DockerComposeRule docker;
    private static List<String> availableClients;

    static RuleChain setupComposition(String name, String composeFile, List<String> availableClientNames) {
        availableClients = ImmutableList.copyOf(availableClientNames);

        docker = DockerComposeRule.builder()
                .file(composeFile)
                .saveLogsTo("container-logs/" + name)
                .build();

        DockerProxyRule dockerProxyRule = new DockerProxyRule(docker.projectName());

        return RuleChain
                .outerRule(GRADLE_PREPARE_TASK)
                .around(docker)
                .around(dockerProxyRule)
                .around(waitForServersToBeReady());
    }

    static String runCliCommand(String command) throws IOException, InterruptedException {
        return docker.run(
                DockerComposeRunOption.options("-T"),
                "ete-cli",
                DockerComposeRunArgument.arguments("bash", "-c", command));
    }

    static <T> T createClientToSingleNode(Class<T> clazz) {
        return createClientFor(clazz, Iterables.getFirst(availableClients, null), SERVER_PORT);
    }

    static <T> T createClientToAllNodes(Class<T> clazz) {
        return createClientToMultipleNodes(clazz, availableClients, SERVER_PORT);
    }

    private static ExternalResource waitForServersToBeReady() {
        return new ExternalResource() {
            @Override
            protected void before() throws Throwable {
                Awaitility.await()
                        .ignoreExceptions()
                        .atMost(Duration.TWO_MINUTES)
                        .pollInterval(Duration.ONE_SECOND)
                        .until(serversAreReady());
            }
        };
    }

    private static Callable<Boolean> serversAreReady() {
        return () -> {
            for (String client : availableClients) {
                TodoResource todos = createClientFor(TodoResource.class, client, SERVER_PORT);
                todos.isHealthy();
            }
            return true;
        };
    }

    private static <T> T createClientToMultipleNodes(Class<T> clazz, List<String> nodeNames, short port) {
        Collection<String> uris = nodeNames.stream()
                .map(nodeName -> String.format("http://%s:%s", nodeName, port))
                .collect(Collectors.toList());

        return AtlasDbHttpClients.createProxyWithFailover(NO_SSL, uris, clazz);
    }

    private static <T> T createClientFor(Class<T> clazz, String host, short port) {
        String uri = String.format("http://%s:%s", host, port);
        return AtlasDbHttpClients.createProxy(NO_SSL, uri, clazz);
    }
}
