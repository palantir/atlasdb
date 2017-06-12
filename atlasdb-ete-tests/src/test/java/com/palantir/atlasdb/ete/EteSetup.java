/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import javax.net.ssl.SSLSocketFactory;

import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.execution.DockerComposeRunArgument;
import com.palantir.docker.compose.execution.DockerComposeRunOption;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.docker.proxy.DockerProxyRule;

// **** Important: Some internal tests depend on this class,
// please recompile them if any breaking changes are made to the setup ***
public abstract class EteSetup {
    private static final Gradle GRADLE_PREPARE_TASK = Gradle.ensureTaskHasRun(":atlasdb-ete-tests:prepareForEteTests");
    private static final Optional<SSLSocketFactory> NO_SSL = Optional.empty();

    private static final short SERVER_PORT = 3828;

    private static DockerComposeRule docker;
    private static List<String> availableClients;
    private static Duration waitDuration;

    static RuleChain setupComposition(Class<?> eteClass, String composeFile, List<String> availableClientNames) {
        return setupComposition(eteClass, composeFile, availableClientNames, Duration.TWO_MINUTES);
    }

    static RuleChain setupComposition(
            Class<?> eteClass,
            String composeFile,
            List<String> availableClientNames,
            Duration waitTime) {
        return setupComposition(eteClass, composeFile, availableClientNames, waitTime, ImmutableMap.of());
    }

    static RuleChain setupComposition(
            Class<?> eteClass,
            String composeFile,
            List<String> availableClientNames,
            Map<String, String> environment) {
        return setupComposition(eteClass, composeFile, availableClientNames, Duration.TWO_MINUTES, environment);
    }

    static RuleChain setupComposition(
            Class<?> eteClass,
            String composeFile,
            List<String> availableClientNames,
            Duration waitTime,
            Map<String, String> environment) {
        waitDuration = waitTime;
        availableClients = ImmutableList.copyOf(availableClientNames);

        DockerMachine machine = DockerMachine.localMachine().withEnvironment(environment).build();
        String logDirectory = EteSetup.class.getSimpleName() + "-" + eteClass.getSimpleName();

        docker = DockerComposeRule.builder()
                .file(composeFile)
                .machine(machine)
                .saveLogsTo(LogDirectory.circleAwareLogDirectory(logDirectory))
                .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
                .build();

        DockerProxyRule dockerProxyRule = DockerProxyRule.fromProjectName(docker.projectName(), eteClass);

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

    public static Container getContainer(String containerName) {
        return docker.containers().container(containerName);
    }

    private static ExternalResource waitForServersToBeReady() {
        return new ExternalResource() {
            @Override
            protected void before() throws Throwable {
                Awaitility.await()
                        .ignoreExceptions()
                        .atMost(waitDuration)
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
