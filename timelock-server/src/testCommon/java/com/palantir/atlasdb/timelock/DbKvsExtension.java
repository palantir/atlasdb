/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.Callable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class DbKvsExtension implements BeforeAllCallback, AfterAllCallback {
    private static final int POSTGRES_PORT = 5432;
    private static final int FIVE_SECONDS = 5;

    private final DockerComposeExtension docker = DockerComposeExtension.builder()
            .file("src/testCommon/resources/docker-compose.yml")
            .waitingForService("postgres", Container::areAllPortsOpen)
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(DbKvsExtension.class))
            .build();

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        docker.beforeAll(extensionContext);
        waitUntilDbkvsIsUp();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        docker.afterAll(extensionContext);
    }

    private void waitUntilDbkvsIsUp() {
        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .pollInterval(Duration.ofSeconds(1))
                .until(canCreateKeyValueService());
    }

    private DbKeyValueServiceConfig getKvsConfig() {
        DockerPort port = docker.containers().container("postgres").port(POSTGRES_PORT);

        InetSocketAddress postgresAddress = InetSocketAddress.createUnresolved(port.getIp(), port.getExternalPort());

        ConnectionConfig connectionConfig = ImmutablePostgresConnectionConfig.builder()
                .dbName("atlas")
                .dbLogin("palantir")
                .dbPassword(ImmutableMaskedValue.of("palantir"))
                .host(postgresAddress.getHostString())
                .port(postgresAddress.getPort())
                .build();

        return ImmutableDbKeyValueServiceConfig.builder()
                .connection(connectionConfig)
                .ddl(ImmutablePostgresDdlConfig.builder()
                        .compactInterval(HumanReadableDuration.days(2))
                        .build())
                .build();
    }

    private Callable<Boolean> canCreateKeyValueService() {
        return () -> {
            try (ConnectionManagerAwareDbKvs kvs = createKvs()) {
                try (Connection connection = kvs.getConnectionManager().getConnection()) {
                    return connection.isValid(FIVE_SECONDS);
                }
            } catch (Exception ex) {
                if (ex.getMessage().contains("The connection attempt failed.")
                        || ex.getMessage().contains("the database system is starting up")) {
                    return false;
                } else {
                    throw ex;
                }
            }
        };
    }

    private ConnectionManagerAwareDbKvs createKvs() {
        return ConnectionManagerAwareDbKvs.create(getKvsConfig());
    }
}
