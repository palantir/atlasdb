/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SimpleTimedSqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.ReentrantManagedConnectionSupplier;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.OracleConnectionConfig;
import java.io.IOException;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExecutableInvoker;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstances;
import org.junit.jupiter.api.parallel.ExecutionMode;

public final class DbKvsOracleExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static final String LOCALHOST = "0.0.0.0";
    private static final int ORACLE_PORT_NUMBER = 1521;
    private static volatile boolean isInitialized = false;

    public static final DockerComposeExtension docker = DockerComposeExtension.builder()
            .file("src/test/resources/docker-compose.oracle.yml")
            .waitingForService("oracle", Container::areAllPortsOpen)
            .nativeServiceHealthCheckTimeout(org.joda.time.Duration.standardMinutes(5))
            .saveLogsTo("container-logs")
            .build();

    @Override
    public void beforeAll(ExtensionContext var1) throws IOException, InterruptedException {
        if (!isInitialized) {
            isInitialized = true;
            docker.beforeAll(var1);
            Awaitility.await()
                    .atMost(Duration.ofMinutes(5))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(canCreateKeyValueService());
            var1.getRoot().getStore(GLOBAL).put("NodesDownTestSetup", this);
        }
    }

    @Override
    public void close() {
        docker.afterAll(new ExtensionContext() {
            @Override
            public Optional<ExtensionContext> getParent() {
                return Optional.empty();
            }

            @Override
            public ExtensionContext getRoot() {
                return null;
            }

            @Override
            public String getUniqueId() {
                return null;
            }

            @Override
            public String getDisplayName() {
                return null;
            }

            @Override
            public Set<String> getTags() {
                return null;
            }

            @Override
            public Optional<AnnotatedElement> getElement() {
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> getTestClass() {
                return Optional.empty();
            }

            @Override
            public Optional<Lifecycle> getTestInstanceLifecycle() {
                return Optional.empty();
            }

            @Override
            public Optional<Object> getTestInstance() {
                return Optional.empty();
            }

            @Override
            public Optional<TestInstances> getTestInstances() {
                return Optional.empty();
            }

            @Override
            public Optional<Method> getTestMethod() {
                return Optional.empty();
            }

            @Override
            public Optional<Throwable> getExecutionException() {
                return Optional.empty();
            }

            @Override
            public Optional<String> getConfigurationParameter(String s) {
                return Optional.empty();
            }

            @Override
            public <T> Optional<T> getConfigurationParameter(String s, Function<String, T> function) {
                return Optional.empty();
            }

            @Override
            public void publishReportEntry(Map<String, String> map) {}

            @Override
            public Store getStore(Namespace namespace) {
                return null;
            }

            @Override
            public ExecutionMode getExecutionMode() {
                return null;
            }

            @Override
            public ExecutableInvoker getExecutableInvoker() {
                return null;
            }
        });
    }

    public static KeyValueService createKvs() {
        return ConnectionManagerAwareDbKvs.create(getKvsConfig());
    }

    public static DbKeyValueServiceConfig getKvsConfig() {
        DockerPort port = docker.containers().container("oracle").port(ORACLE_PORT_NUMBER);

        InetSocketAddress oracleAddress = InetSocketAddress.createUnresolved(LOCALHOST, port.getExternalPort());

        ConnectionConfig connectionConfig = new OracleConnectionConfig.Builder()
                .dbLogin("palantir")
                .dbPassword(ImmutableMaskedValue.of("7_SeeingStones_7"))
                .sid("palantir")
                .host(oracleAddress.getHostString())
                .port(oracleAddress.getPort())
                .build();

        return ImmutableDbKeyValueServiceConfig.builder()
                .connection(connectionConfig)
                .ddl(ImmutableOracleDdlConfig.builder()
                        .overflowMigrationState(OverflowMigrationState.FINISHED)
                        .build())
                .build();
    }

    public static ConnectionSupplier getConnectionSupplier() {
        KeyValueService kvs = ConnectionManagerAwareDbKvs.create(getKvsConfig());
        return getConnectionSupplier(kvs);
    }

    public static ConnectionSupplier getConnectionSupplier(KeyValueService kvs) {
        ReentrantManagedConnectionSupplier connSupplier =
                new ReentrantManagedConnectionSupplier(getConnectionManager(kvs));
        return new ConnectionSupplier(getSimpleTimedSqlConnectionSupplier(connSupplier));
    }

    public static ConnectionManager getConnectionManager(KeyValueService kvs) {
        ConnectionManagerAwareDbKvs castKvs = (ConnectionManagerAwareDbKvs) kvs;
        return castKvs.getConnectionManager();
    }

    private static SqlConnectionSupplier getSimpleTimedSqlConnectionSupplier(
            ReentrantManagedConnectionSupplier connectionSupplier) {
        return new SimpleTimedSqlConnectionSupplier(connectionSupplier);
    }

    private static Callable<Boolean> canCreateKeyValueService() {
        return () -> {
            try (ConnectionManagerAwareDbKvs kvs = ConnectionManagerAwareDbKvs.create(getKvsConfig());
                    Connection conn = kvs.getConnectionManager().getConnection()) {
                return conn.isValid(5);
            } catch (Exception e) {
                return false;
            }
        };
    }
}
