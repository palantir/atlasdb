/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.ete;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.TestProxyUtils;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.execution.DockerComposeExecArgument;
import com.palantir.docker.compose.execution.DockerComposeExecOption;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.docker.proxy.DockerProxyRule;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.awaitility.Awaitility;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;

// Important: Some internal tests depend on this class.
// Please recompile Oracle internal tests if any breaking changes are made to the setup.
// Please don't make the setup methods private.
public abstract class EteSetup {
    private static final Gradle GRADLE_PREPARE_TASK = Gradle.ensureTaskHasRun(":atlasdb-ete-tests:prepareForEteTests");
    private static final Gradle TIMELOCK_TASK = Gradle.ensureTaskHasRun(":timelock-server-distribution:dockerTag");

    private static final SslConfiguration SSL_CONFIGURATION =
            SslConfiguration.of(Paths.get("var/security/trustStore.jks"));
    public static final TrustContext TRUST_CONTEXT = SslSocketFactories.createTrustContext(SSL_CONFIGURATION);

    private static final short SERVER_PORT = 3828;

    private static DockerComposeRule docker;
    private static List<String> availableClients;
    private static Duration waitDuration;

    public static RuleChain setupComposition(Class<?> eteClass, String composeFile, List<String> availableClientNames) {
        return setupComposition(eteClass, composeFile, availableClientNames, Duration.ofMinutes(2));
    }

    public static RuleChain setupComposition(
            Class<?> eteClass, String composeFile, List<String> availableClientNames, Duration waitTime) {
        return setupComposition(eteClass, composeFile, availableClientNames, waitTime, ImmutableMap.of());
    }

    public static RuleChain setupComposition(
            Class<?> eteClass, String composeFile, List<String> availableClientNames, Map<String, String> environment) {
        return setupComposition(eteClass, composeFile, availableClientNames, Duration.ofMinutes(2), environment);
    }

    public static RuleChain setupComposition(
            Class<?> eteClass,
            String composeFile,
            List<String> availableClientNames,
            Duration waitTime,
            Map<String, String> environment) {
        waitDuration = waitTime;
        return setup(eteClass, composeFile, availableClientNames, environment);
    }

    public static RuleChain setupCompositionWithTimelock(
            Class<?> eteClass, String composeFile, List<String> availableClientNames, Map<String, String> environment) {
        waitDuration = Duration.ofMinutes(2);
        return setup(eteClass, composeFile, availableClientNames, environment, true);
    }

    public static RuleChain setup(
            Class<?> eteClass, String composeFile, List<String> availableClientNames, Map<String, String> environment) {
        return setup(eteClass, composeFile, availableClientNames, environment, false);
    }

    public static RuleChain setup(
            Class<?> eteClass,
            String composeFile,
            List<String> availableClientNames,
            Map<String, String> environment,
            boolean usingTimelock) {
        availableClients = ImmutableList.copyOf(availableClientNames);

        DockerMachine machine =
                DockerMachine.localMachine().withEnvironment(environment).build();
        String logDirectory = EteSetup.class.getSimpleName() + "-" + eteClass.getSimpleName();

        docker = DockerComposeRule.builder()
                .file(composeFile)
                .machine(machine)
                .saveLogsTo(LogDirectory.circleAwareLogDirectory(logDirectory))
                .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
                .nativeServiceHealthCheckTimeout(org.joda.time.Duration.standardMinutes(5))
                .build();

        DockerProxyRule dockerProxyRule = DockerProxyRule.fromProjectName(docker.projectName(), eteClass);

        RuleChain ruleChain = RuleChain.outerRule(GRADLE_PREPARE_TASK);
        if (usingTimelock) {
            ruleChain = ruleChain.around(TIMELOCK_TASK);
        }
        return ruleChain.around(docker).around(dockerProxyRule).around(waitForServersToBeReady());
    }

    public static String execCliCommand(String client, String command) throws IOException, InterruptedException {
        return docker.exec(
                DockerComposeExecOption.noOptions(),
                client,
                DockerComposeExecArgument.arguments("bash", "-c", command));
    }

    public static <T> T createClientToSingleNode(Class<T> clazz) {
        return createClientFor(clazz, Iterables.getFirst(availableClients, null), SERVER_PORT);
    }

    public static <T> T createClientToSingleNodeWithExtendedTimeout(Class<T> clazz) {
        return createClientWithExtendedTimeout(clazz, Iterables.getFirst(availableClients, null), SERVER_PORT);
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
                        .pollInterval(Duration.ofSeconds(1))
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

        return AtlasDbHttpClients.createProxyWithFailover(
                MetricsManagers.createForTests(),
                ImmutableServerListConfig.builder()
                        .addAllServers(uris)
                        .sslConfiguration(SSL_CONFIGURATION)
                        .build(),
                clazz,
                TestProxyUtils.AUXILIARY_REMOTING_PARAMETERS_RETRYING);
    }

    private static <T> T createClientFor(Class<T> clazz, String host, short port) {
        String uri = String.format("http://%s:%s", host, port);
        return AtlasDbHttpClients.createProxy(
                Optional.of(TRUST_CONTEXT), uri, clazz, TestProxyUtils.AUXILIARY_REMOTING_PARAMETERS_RETRYING);
    }

    private static <T> T createClientWithExtendedTimeout(Class<T> clazz, String host, short port) {
        String uri = String.format("http://%s:%s", host, port);
        return AtlasDbHttpClients.createProxy(
                Optional.of(TRUST_CONTEXT), uri, clazz, TestProxyUtils.AUXILIARY_REMOTING_PARAMETERS_EXTENDED_TIMEOUT);
    }
}
