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

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.util.concurrent.Callable;

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;

public class DbKvsRule implements TestRule {
    private final DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/testCommon/resources/docker-compose.yml")
            .waitingForService("postgres", Container::areAllPortsOpen)
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(DbKvsRule.class))
            .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
            .build();

    @Override
    public Statement apply(Statement base, Description description) {
        return RuleChain.outerRule(docker)
                .around(new ExternalResource() {
                    @Override
                    protected void before() throws Throwable {
                        waitUntilDbkvsIsUp();
                    }

                    @Override
                    protected void after() throws Throwable {
                        // no op
                    }
                }).apply(base, description);
    }

    public void waitUntilDbkvsIsUp() {
        Awaitility.await()
                .atMost(Duration.ONE_MINUTE)
                .pollInterval(Duration.ONE_SECOND)
                .until(canCreateKeyValueService());
    }

    public DbKeyValueServiceConfig getKvsConfig() {
        DockerPort port = docker.containers()
                .container("postgres")
                .port(5432); // TODO (jkong): wat

        InetSocketAddress postgresAddress = new InetSocketAddress(port.getIp(), port.getExternalPort());

        ConnectionConfig connectionConfig = ImmutablePostgresConnectionConfig.builder()
                .dbName("atlas")
                .dbLogin("palantir")
                .dbPassword(ImmutableMaskedValue.of("palantir"))
                .host(postgresAddress.getHostName())
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
            ConnectionManagerAwareDbKvs kvs = null;
            try {
                kvs = createKvs();
                try (Connection connection = kvs.getConnectionManager().getConnection()) {
                    return connection.isValid(5);
                }
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

    public ConnectionManagerAwareDbKvs createKvs() {
        return ConnectionManagerAwareDbKvs.create(getKvsConfig());
    }
}
