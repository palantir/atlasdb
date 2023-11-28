/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.ete.utilities;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.AtlasDbEteServer;
import com.palantir.atlasdb.containers.CassandraEnvironment;
import com.palantir.atlasdb.ete.GradleExtension;
import com.palantir.atlasdb.ete.suites.MultiClientWithPostgresTimelockAndPostgresTestSuite;
import com.palantir.atlasdb.ete.suites.MultiClientWithTimelockAndCassandraTestSuite;
import com.palantir.atlasdb.ete.suites.SingleClientWithEmbeddedAndCassandraTestSuite;
import com.palantir.atlasdb.ete.suites.SingleClientWithEmbeddedAndOracleTestSuite;
import com.palantir.atlasdb.ete.suites.SingleClientWithEmbeddedAndPostgresTestSuite;
import com.palantir.atlasdb.ete.suites.SingleClientWithEmbeddedAndThreeNodeCassandraTestSuite;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.TestProxyUtils;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.execution.DockerComposeExecArgument;
import com.palantir.docker.compose.execution.DockerComposeExecOption;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.docker.proxy.DockerProxyExtension;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class EteExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static final GradleExtension GRADLE_PREPARE_TASK =
            GradleExtension.ensureTaskHasRun(":atlasdb-ete-tests:prepareForEteTests");
    private static final GradleExtension TIMELOCK_TASK =
            GradleExtension.ensureTaskHasRun(":timelock-server-distribution:dockerTag");
    private static final SslConfiguration SSL_CONFIGURATION =
            SslConfiguration.of(Paths.get("var/security/trustStore.jks"));
    private static final TrustContext TRUST_CONTEXT = SslSocketFactories.createTrustContext(SSL_CONFIGURATION);
    private static final short SERVER_PORT = 3828;

    private static final List<Extension> extensions = new ArrayList<>();

    private static DockerComposeExtension docker;
    private static Duration waitDuration;
    private static List<String> availableClients;

    private static volatile EteExtension instance;
    private static volatile boolean isBeforeAllCalled = false;

    private EteExtension() {}

    public static synchronized EteExtension getInstance(EteExtensionConfiguration eteExtensionConfiguration) {
        if (instance == null) {
            waitDuration = eteExtensionConfiguration.waitDuration();
            setup(
                    eteExtensionConfiguration.eteClass(),
                    eteExtensionConfiguration.composeFile(),
                    eteExtensionConfiguration.clients(),
                    eteExtensionConfiguration.environment(),
                    eteExtensionConfiguration.usingTimelock());
            instance = new EteExtension();
        }
        return instance;
    }

    private static void setup(
            Class<?> eteClass,
            String composeFile,
            Clients clients,
            Map<String, String> environment,
            boolean usingTimelock) {
        availableClients = clients.getClients();

        DockerMachine machine =
                DockerMachine.localMachine().withEnvironment(environment).build();
        String logDirectory = EteExtension.class.getSimpleName() + "-" + eteClass.getSimpleName();

        docker = DockerComposeExtension.builder()
                .file(composeFile)
                .machine(machine)
                .saveLogsTo(LogDirectory.circleAwareLogDirectory(logDirectory))
                .nativeServiceHealthCheckTimeout(org.joda.time.Duration.standardSeconds(
                        AtlasDbEteServer.CREATE_TRANSACTION_MANAGER_MAX_WAIT_TIME_SECS))
                .build();

        DockerProxyExtension dockerProxyExtension =
                DockerProxyExtension.fromProjectName(docker.projectName(), eteClass);

        extensions.add(GRADLE_PREPARE_TASK);
        if (usingTimelock) {
            extensions.add(TIMELOCK_TASK);
        }
        extensions.add(docker);
        extensions.add(dockerProxyExtension);
        extensions.add(waitForServersToBeReady());
    }

    @Override
    public synchronized void beforeAll(ExtensionContext extensionContext) throws Exception {
        if (!isBeforeAllCalled) {
            isBeforeAllCalled = true;
            for (Extension extension : extensions) {
                if (extension instanceof BeforeAllCallback) {
                    ((BeforeAllCallback) extension).beforeAll(extensionContext);
                }
            }
            extensionContext.getRoot().getStore(GLOBAL).put("EteExtension", this);
        }
    }

    @Override
    public void close() {
        for (Extension extension : Lists.reverse(extensions)) {
            // Only extensions that implement afterAll() are these and after() is equivalent to afterAll().
            if (extension instanceof DockerComposeExtension) {
                ((DockerComposeExtension) extension).after();
            }
            if (extension instanceof DockerProxyExtension) {
                ((DockerProxyExtension) extension).after();
            }
        }
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

    public static Container getContainer(String containerName) {
        return docker.containers().container(containerName);
    }

    private static Extension waitForServersToBeReady() {
        return (BeforeAllCallback) extensionContext -> Awaitility.await()
                .ignoreExceptions()
                .atMost(waitDuration)
                .pollInterval(Duration.ofSeconds(1))
                .until(serversAreReady());
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

    public enum Clients {
        SINGLE(ImmutableList.of("ete1")),
        MULTI(ImmutableList.of("ete1", "ete2"));

        private final ImmutableList<String> clients;

        Clients(ImmutableList<String> clients) {
            this.clients = clients;
        }

        List<String> getClients() {
            return clients;
        }
    }

    public interface EteExtensionConfiguration {
        Class<?> eteClass();

        String composeFile();

        Clients clients();

        Map<String, String> environment();

        boolean usingTimelock();

        Duration waitDuration();
    }

    private static final class MultiClientWithPostgresTimelockAndPostgresEteExtensionConfiguration
            implements EteExtensionConfiguration {
        @Override
        public Class<?> eteClass() {
            return MultiClientWithPostgresTimelockAndPostgresTestSuite.class;
        }

        @Override
        public String composeFile() {
            return "docker-compose.multi-client-with-postgres-timelock-and-postgres.yml";
        }

        @Override
        public Clients clients() {
            return Clients.MULTI;
        }

        @Override
        public Map<String, String> environment() {
            return ImmutableMap.of();
        }

        @Override
        public boolean usingTimelock() {
            return true;
        }

        @Override
        public Duration waitDuration() {
            return Duration.ofMinutes(2);
        }
    }

    private static final class MultiClientWithTimelockAndCassandraEteExtensionConfiguration
            implements EteExtensionConfiguration {
        @Override
        public Class<?> eteClass() {
            return MultiClientWithTimelockAndCassandraTestSuite.class;
        }

        @Override
        public String composeFile() {
            return "docker-compose.multi-client-with-timelock-and-cassandra.yml";
        }

        @Override
        public Clients clients() {
            return Clients.MULTI;
        }

        @Override
        public Map<String, String> environment() {
            return CassandraEnvironment.get();
        }

        @Override
        public boolean usingTimelock() {
            return true;
        }

        @Override
        public Duration waitDuration() {
            return Duration.ofMinutes(2);
        }
    }

    private static final class SingleClientWithEmbeddedAndCassandraEteExtensionConfiguration
            implements EteExtensionConfiguration {
        @Override
        public Class<?> eteClass() {
            return SingleClientWithEmbeddedAndCassandraTestSuite.class;
        }

        @Override
        public String composeFile() {
            return "docker-compose.single-client-with-embedded-and-cassandra.yml";
        }

        @Override
        public Clients clients() {
            return Clients.SINGLE;
        }

        @Override
        public Map<String, String> environment() {
            return CassandraEnvironment.get();
        }

        @Override
        public boolean usingTimelock() {
            return false;
        }

        @Override
        public Duration waitDuration() {
            return Duration.ofMinutes(2);
        }
    }

    private static final class SingleClientWithEmbeddedAndOracleEteExtensionConfiguration
            implements EteExtensionConfiguration {
        @Override
        public Class<?> eteClass() {
            return SingleClientWithEmbeddedAndOracleTestSuite.class;
        }

        @Override
        public String composeFile() {
            return "docker-compose.single-client-with-embedded-and-oracle.yml";
        }

        @Override
        public Clients clients() {
            return Clients.SINGLE;
        }

        @Override
        public Map<String, String> environment() {
            return ImmutableMap.of();
        }

        @Override
        public boolean usingTimelock() {
            return false;
        }

        @Override
        public Duration waitDuration() {
            return Duration.ofMinutes(10);
        }
    }

    private static final class SingleClientWithEmbeddedAndPostgresEteExtensionConfiguration
            implements EteExtensionConfiguration {
        @Override
        public Class<?> eteClass() {
            return SingleClientWithEmbeddedAndPostgresTestSuite.class;
        }

        @Override
        public String composeFile() {
            return "docker-compose.single-client-with-embedded-and-postgres.yml";
        }

        @Override
        public Clients clients() {
            return Clients.SINGLE;
        }

        @Override
        public Map<String, String> environment() {
            return ImmutableMap.of();
        }

        @Override
        public boolean usingTimelock() {
            return false;
        }

        @Override
        public Duration waitDuration() {
            return Duration.ofMinutes(2);
        }
    }

    private static final class SingleClientWithEmbeddedAndThreeNodeCassandra implements EteExtensionConfiguration {
        @Override
        public Class<?> eteClass() {
            return SingleClientWithEmbeddedAndThreeNodeCassandraTestSuite.class;
        }

        @Override
        public String composeFile() {
            return "docker-compose.single-client-with-embedded-and-three-node-cassandra.yml";
        }

        @Override
        public Clients clients() {
            return Clients.SINGLE;
        }

        @Override
        public Map<String, String> environment() {
            return CassandraEnvironment.get();
        }

        @Override
        public boolean usingTimelock() {
            return false;
        }

        @Override
        public Duration waitDuration() {
            return Duration.ofMinutes(2);
        }
    }

    public static final EteExtensionConfiguration
            MULTI_CLIENT_WITH_POSTGRES_TIMELOCK_AND_POSTGRES_ETE_EXTENSION_CONFIGURATION =
                    new MultiClientWithPostgresTimelockAndPostgresEteExtensionConfiguration();

    public static final EteExtensionConfiguration MULTI_CLIENT_WITH_TIMELOCK_AND_CASSANDRA_ETE_EXTENSION_CONFIGURATION =
            new MultiClientWithTimelockAndCassandraEteExtensionConfiguration();

    public static final EteExtensionConfiguration
            SINGLE_CLIENT_WITH_EMBEDDED_AND_CASSANDRA_ETE_EXTENSION_CONFIGURATION =
                    new SingleClientWithEmbeddedAndCassandraEteExtensionConfiguration();

    public static final EteExtensionConfiguration SINGLE_CLIENT_WITH_EMBEDDED_AND_ORACLE_ETE_EXTENSION_CONFIGURATION =
            new SingleClientWithEmbeddedAndOracleEteExtensionConfiguration();

    public static final EteExtensionConfiguration SINGLE_CLIENT_WITH_EMBEDDED_AND_POSTGRES_ETE_EXTENSION_CONFIGURATION =
            new SingleClientWithEmbeddedAndPostgresEteExtensionConfiguration();

    public static final EteExtensionConfiguration
            SINGLE_CLIENT_WITH_EMBEDDED_AND_THREE_NODE_CASSANDRA_ETE_EXTENSION_CONFIGURATION =
                    new SingleClientWithEmbeddedAndThreeNodeCassandra();
}
