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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.util.concurrent.Callable;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.db.oracle.NativeOracleJdbcHandler;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.nexus.db.monitoring.timer.SqlTimer;
import com.palantir.nexus.db.monitoring.timer.SqlTimers;
import com.palantir.nexus.db.pool.ReentrantManagedConnectionSupplier;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutableOracleConnectionConfig;
import com.palantir.nexus.db.sql.ConnectionBackedSqlConnectionImpl;
import com.palantir.nexus.db.sql.SQL;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.nexus.db.sql.SqlConnectionHelper;

@RunWith(Suite.class)
@Suite.SuiteClasses(DbKvsOracleGetCandidateCellsForSweepingTest.class)
public final class DbkvsOracleTestSuite {
    private static final int ORACLE_PORT_NUMBER = 1521;

    private DbkvsOracleTestSuite() {
        // Test suite
    }

    @ClassRule
    public static final DockerComposeRule docker = DockerComposeRule.builder()
            .file("/Users/hsaraogi/code/atlasdb/atlasdb-dbkvs-tests/src/test/resources/docker-compose.oracle.yml")
            .waitingForService("oracle", Container::areAllPortsOpen)
            .saveLogsTo("container-logs")
            .shutdownStrategy(ShutdownStrategy.SKIP)
            .build();

    @BeforeClass
    public static void waitUntilDbKvsIsUp() throws InterruptedException {
        Awaitility.await()
                .atMost(Duration.TEN_MINUTES)
                .pollInterval(Duration.TEN_SECONDS)
                .until(canCreateKeyValueService());
    }

    public static DbKeyValueServiceConfig getKvsConfig() {
        DockerPort port = docker.containers()
                .container("oracle")
                .port(ORACLE_PORT_NUMBER);

        InetSocketAddress oracleAddress = new InetSocketAddress(port.getIp(), port.getExternalPort());

        ConnectionConfig connectionConfig = ImmutableOracleConnectionConfig.builder()
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
                        .jdbcHandler(new NativeOracleJdbcHandler())
                        .build())
                .build();
    }

    public static ConnectionSupplier getConnectionSupplier() {
        ConnectionManagerAwareDbKvs kvs = ConnectionManagerAwareDbKvs.create(getKvsConfig());
        ReentrantManagedConnectionSupplier connSupplier =
                new ReentrantManagedConnectionSupplier(kvs.getConnectionManager());
        return new ConnectionSupplier(getSimpleTimedSqlConnectionSupplier(connSupplier));
    }

    private static SqlConnectionSupplier getSimpleTimedSqlConnectionSupplier(
            ReentrantManagedConnectionSupplier connectionSupplier) {
        Supplier<Connection> supplier = () -> connectionSupplier.get();
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
                        supplier.get(),
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