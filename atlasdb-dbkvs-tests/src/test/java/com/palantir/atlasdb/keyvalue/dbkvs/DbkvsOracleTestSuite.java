/**
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs;

import static com.palantir.logsafe.Preconditions.checkState;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.nexus.db.monitoring.timer.SqlTimer;
import com.palantir.nexus.db.monitoring.timer.SqlTimers;
import com.palantir.nexus.db.pool.ReentrantManagedConnectionSupplier;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.OracleConnectionConfig;
import com.palantir.nexus.db.sql.ConnectionBackedSqlConnectionImpl;
import com.palantir.nexus.db.sql.SQL;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.nexus.db.sql.SqlConnectionHelper;

/**
 * In order to run these CI tests, you need to provide your own Docker image, and place it in the environment variable
 * 'ORACLE_DOCKER_IMAGE'. If you are a Palantir developer and need to develop locally, this value can be found in
 * the AtlasDB CI configuration.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        DbkvsOracleTargetedSweepIntegrationTest.class,
        DbKvsOracleKeyValueServiceTest.class,
        DbKvsOracleSerializableTransactionTest.class,
        DbKvsOracleSweepTaskRunnerTest.class,
        DbKvsOracleGetCandidateCellsForSweepingTest.class,
        OverflowSequenceSupplierEteTest.class,
        OracleTableNameMapperEteTest.class,
        OracleDbTimestampBoundStoreTest.class
})
public final class DbkvsOracleTestSuite {
    private static final String DOCKER_COMPOSE_TEMPLATE = "version: '2'\n"
            + "\n"
            + "services:\n"
            + "  oracle:\n"
            + "    image: $ORACLE_DOCKER_IMAGE\n"
            + "    environment:\n"
            + "      SQLPLUS_INIT_00: startup nomount\n"
            + "      SQLPLUS_INIT_01: alter system set processes=1000 scope=spfile\n"
            + "      SQLPLUS_INIT_02: alter system set open_cursors=1000 scope=spfile\n"
            + "      SQLPLUS_INIT_03: alter system set sessions=1000 scope=spfile\n"
            + "      SQLPLUS_INIT_04: alter system set max_shared_servers=4000 scope=spfile\n"
            + "      SQLPLUS_INIT_05: alter system set sga_max_size=1000M scope=spfile\n"
            + "      SQLPLUS_INIT_06: alter system set sga_target=1000M scope=spfile\n"
            + "      SQLPLUS_INIT_08: alter system set pga_aggregate_target=160M scope=spfile\n"
            + "      SQLPLUS_INIT_09: shutdown immediate\n"
            + "      SQLPLUS_INIT_10: startup\n"
            + "    ports:\n"
            + "      - \"2080\"\n"
            + "      - \"1521\"";
    private static final String ORACLE_DOCKER_IMAGE = "ORACLE_DOCKER_IMAGE";
    private static final String ORACLE_DOCKER_COMPOSE_FILE = "src/test/resources/docker-compose.oracle.yml";
    private static final int ORACLE_PORT_NUMBER = 1521;

    private DbkvsOracleTestSuite() {
        // Test suite
    }

    static {
        String dockerImage = System.getenv(ORACLE_DOCKER_IMAGE);
        checkState(dockerImage != null, "docker compose file variable must be set in order to run Oracle tests");
        String dockerComposeFile = DOCKER_COMPOSE_TEMPLATE.replace("$" + ORACLE_DOCKER_IMAGE, dockerImage);
        try {
            Files.write(dockerComposeFile.getBytes(StandardCharsets.UTF_8), new File(ORACLE_DOCKER_COMPOSE_FILE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @ClassRule
    public static final DockerComposeRule docker = DockerComposeRule.builder()
            .file(ORACLE_DOCKER_COMPOSE_FILE)
            .waitingForService("oracle", Container::areAllPortsOpen)
            .saveLogsTo("container-logs")
            .build();

    @BeforeClass
    public static void waitUntilDbKvsIsUp() throws InterruptedException {
        Awaitility.await()
                .atMost(Duration.FIVE_MINUTES)
                .pollInterval(Duration.TEN_SECONDS)
                .until(canCreateKeyValueService());
    }

    public static KeyValueService createKvs() {
        return ConnectionManagerAwareDbKvs.create(getKvsConfig());
    }

    public static DbKeyValueServiceConfig getKvsConfig() {
        DockerPort port = docker.containers()
                .container("oracle")
                .port(ORACLE_PORT_NUMBER);

        InetSocketAddress oracleAddress = new InetSocketAddress(port.getIp(), port.getExternalPort());

        ConnectionConfig connectionConfig = new OracleConnectionConfig.Builder()
                .dbLogin("palantir")
                .dbPassword(ImmutableMaskedValue.of("palpal"))
                .sid("palantir")
                .host(oracleAddress.getHostName())
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
        ConnectionManagerAwareDbKvs castKvs = (ConnectionManagerAwareDbKvs) kvs;
        ReentrantManagedConnectionSupplier connSupplier =
                new ReentrantManagedConnectionSupplier(castKvs.getConnectionManager());
        return new ConnectionSupplier(getSimpleTimedSqlConnectionSupplier(connSupplier));
    }

    private static SqlConnectionSupplier getSimpleTimedSqlConnectionSupplier(
            ReentrantManagedConnectionSupplier connectionSupplier) {
        SQL sql = new SQL() {
            @Override
            protected SqlConfig getSqlConfig() {
                return new SqlConfig() {
                    @Override
                    public boolean isSqlCancellationDisabled() {
                        return false;
                    }

                    protected Iterable<SqlTimer> getSqlTimers() {
                        return ImmutableList.of(
                                SqlTimers.createDurationSqlTimer(),
                                SqlTimers.createSqlStatsSqlTimer());
                    }

                    @Override
                    public SqlTimer getSqlTimer() {
                        return SqlTimers.createCombinedSqlTimer(getSqlTimers());
                    }
                };
            }
        };

        return new SqlConnectionSupplier() {
            @Override
            public SqlConnection get() {
                return new ConnectionBackedSqlConnectionImpl(
                        ((Supplier<Connection>) connectionSupplier).get(),
                        () -> {
                            throw new UnsupportedOperationException(
                                    "This SQL connection does not provide reliable timestamp.");
                        },
                        new SqlConnectionHelper(sql));
            }

            @Override
            public void close() {
                connectionSupplier.close();
            }
        };
    }

    private static Callable<Boolean> canCreateKeyValueService() {
        return () -> {
            ConnectionManagerAwareDbKvs kvs = null;
            try {
                kvs = ConnectionManagerAwareDbKvs.create(getKvsConfig());
                return kvs.getConnectionManager().getConnection().isValid(5);
            } catch (Exception e) {
                return false;
            } finally {
                if (kvs != null) {
                    kvs.close();
                }
            }
        };
    }
}
