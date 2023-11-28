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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;
import com.palantir.nexus.db.pool.config.PostgresConnectionConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Callable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class DbKvsPostgresExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static final int POSTGRES_PORT_NUMBER = 5432;

    private static volatile boolean isInitialized = false;

    public static final DockerComposeExtension docker = DockerComposeExtension.builder()
            .file("src/test/resources/docker-compose.postgres.yml")
            .waitingForService("postgres-dbkvs", Container::areAllPortsOpen)
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(DbKvsPostgresExtension.class))
            .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
            .build();

    @Override
    public synchronized void beforeAll(ExtensionContext extensionContext) throws InterruptedException, IOException {
        if (!isInitialized) {
            isInitialized = true;
            docker.beforeAll(extensionContext);
            Awaitility.await()
                    .atMost(Duration.ofMinutes(1))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(canCreateKeyValueService());
            extensionContext.getRoot().getStore(GLOBAL).put("DbKvsOracleExtension", this);
        }
    }

    @Override
    public void close() {
        docker.after();
    }

    public static PostgresConnectionConfig getConnectionConfig() {
        DockerPort port = docker.containers().container("postgres-dbkvs").port(POSTGRES_PORT_NUMBER);
        InetSocketAddress postgresAddress = InetSocketAddress.createUnresolved(port.getIp(), port.getExternalPort());
        return ImmutablePostgresConnectionConfig.builder()
                .dbName("atlas")
                .dbLogin("palantir")
                .dbPassword(ImmutableMaskedValue.of("palantir"))
                .host(postgresAddress.getHostString())
                .port(postgresAddress.getPort())
                .build();
    }

    public static DbKeyValueServiceConfig getKvsConfig() {
        return ImmutableDbKeyValueServiceConfig.builder()
                .connection(getConnectionConfig())
                .ddl(ImmutablePostgresDdlConfig.builder()
                        .compactInterval(HumanReadableDuration.days(2))
                        .build())
                .build();
    }

    private static Callable<Boolean> canCreateKeyValueService() {
        return () -> {
            ConnectionManagerAwareDbKvs kvs = null;
            try {
                kvs = createKvs();
                return kvs.getConnectionManager().getConnection().isValid(5);
            } catch (Exception ex) {
                if (ex.getMessage().contains("The connection attempt failed.")
                        || ex.getMessage().contains("the database system is starting up")) {
                    return false;
                } else {
                    throw ex;
                }
            } finally {
                if (kvs != null) {
                    kvs.close();
                }
            }
        };
    }

    public static ConnectionManagerAwareDbKvs createKvs() {
        return ConnectionManagerAwareDbKvs.create(getKvsConfig());
    }
}
