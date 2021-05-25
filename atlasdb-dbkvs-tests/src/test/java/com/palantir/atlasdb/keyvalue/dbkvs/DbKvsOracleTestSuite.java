/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OverflowSequenceSupplierEteTest;
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
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.Callable;
import org.awaitility.Awaitility;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
    // DbkvsOracleTargetedSweepIntegrationTest.class,
    // DbKvsOracleKeyValueServiceTest.class,
    // DbKvsOracleSerializableTransactionTest.class,
    // DbKvsOracleSweepTaskRunnerTest.class,
    // DbKvsOracleGetCandidateCellsForSweepingTest.class,
    OverflowSequenceSupplierEteTest.class,
    // OracleTableNameMapperEteTest.class,
    // OracleDbTimestampBoundStoreTest.class
})
public final class DbKvsOracleTestSuite {
    private static final int ORACLE_PORT_NUMBER = 1521;

    private DbKvsOracleTestSuite() {
        // Test suite
    }

    @ClassRule
    public static final DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.oracle.yml")
            .waitingForService("oracle", Container::areAllPortsOpen)
            .saveLogsTo("container-logs")
            .build();

    @BeforeClass
    public static void waitUntilDbKvsIsUp() {
        Awaitility.await()
                .atMost(Duration.ofMinutes(5))
                .pollInterval(Duration.ofSeconds(1))
                .until(canCreateKeyValueService());
    }

    public static KeyValueService createKvs() {
        return ConnectionManagerAwareDbKvs.create(getKvsConfig());
    }

    public static DbKeyValueServiceConfig getKvsConfig() {
        DockerPort port = docker.containers().container("oracle").port(ORACLE_PORT_NUMBER);

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
                        return ImmutableList.of(SqlTimers.createDurationSqlTimer(), SqlTimers.createSqlStatsSqlTimer());
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
