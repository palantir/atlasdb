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

import static com.palantir.atlasdb.timelock.TemplateVariables.generateThreeNodeTimelockCluster;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.Callable;

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import com.github.peterwippermann.junit4.parameterizedsuite.ParameterizedSuite;
import com.google.common.collect.ImmutableSet;
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
import com.palantir.timelock.config.PaxosInstallConfiguration;

@RunWith(ParameterizedSuite.class)
@Suite.SuiteClasses({
        DbKvsMNPTLIT.class
})
public class DbTimeLockSuite {
    private static final int POSTGRES_PORT_NUMBER = 5432;

    @ClassRule
    public static final DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.yml")
            .waitingForService("postgres", Container::areAllPortsOpen)
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(DbTimeLockSuite.class))
            .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
            .build();

    @BeforeClass
    public static void waitUntilDbkvsIsUp() throws InterruptedException {
        Awaitility.await()
                .atMost(Duration.ONE_MINUTE)
                .pollInterval(Duration.ONE_SECOND)
                .until(canCreateKeyValueService());
    }

    public static DbKeyValueServiceConfig getKvsConfig() {
        DockerPort port = docker.containers()
                .container("postgres")
                .port(POSTGRES_PORT_NUMBER);

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

    public static TemplateVariables.DbKvsConnectionConfig getKvsTemplateVariables() {
        DockerPort port = docker.containers()
                .container("postgres")
                .port(POSTGRES_PORT_NUMBER);

        InetSocketAddress postgresAddress = new InetSocketAddress(port.getIp(), port.getExternalPort());

        return ImmutableTemplateVariables.DbKvsConnectionConfig.builder()
                .dbName("atlas")
                .dbLogin("palantir")
                .dbPassword("palantir")
                .host(postgresAddress.getHostName())
                .port(postgresAddress.getPort())
                .build();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<ClusterSupplier> params() {
        return ImmutableSet.of(ClusterSupplier.of(() -> new TestableTimelockCluster(
                "db TimeLock",
                "dbKvsServer.ftl",
                generateThreeNodeTimelockCluster(9080, builder ->
                        builder.clientPaxosBuilder(builder.clientPaxosBuilder().isUseBatchPaxosTimestamp(false))
                                .leaderMode(PaxosInstallConfiguration.PaxosLeaderMode.SINGLE_LEADER)
                                .dbConfig(getKvsTemplateVariables())))));
    }

    @Rule
    @Parameterized.Parameter
    public ClusterSupplier clusterSupplier;
}
