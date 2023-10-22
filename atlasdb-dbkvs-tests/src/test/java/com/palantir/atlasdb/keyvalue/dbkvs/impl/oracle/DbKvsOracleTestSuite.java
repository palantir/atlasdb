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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SimpleTimedSqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.ReentrantManagedConnectionSupplier;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.OracleConnectionConfig;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.Callable;
import org.awaitility.Awaitility;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
    DbKvsOracleTargetedSweepIntegrationTest.class,
    DbKvsOracleKeyValueServiceTest.class,
    DbKvsOracleSerializableTransactionTest.class,
    DbKvsOracleSweepTaskRunnerTest.class,
    DbKvsOracleGetCandidateCellsForSweepingTest.class,
    OverflowSequenceSupplierEteTest.class,
    OracleTableNameMapperEteTest.class,
    OracleEmbeddedDbTimestampBoundStoreTest.class,
    OracleNamespaceDeleterIntegrationTest.class,
    OracleAlterTableIntegrationTest.class
})
public final class DbKvsOracleTestSuite {
    private static final String LOCALHOST = "0.0.0.0";
    private static final int ORACLE_PORT_NUMBER = 1521;

    private DbKvsOracleTestSuite() {
        // Test suite
    }

    @ClassRule
    public static final DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.oracle.yml")
            .waitingForService("oracle", Container::areAllPortsOpen)
            .nativeServiceHealthCheckTimeout(org.joda.time.Duration.standardMinutes(5))
            .saveLogsTo("container-logs")
            .build();

    @BeforeAll
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

        InetSocketAddress oracleAddress = InetSocketAddress.createUnresolved(LOCALHOST, port.getExternalPort());

        ConnectionConfig connectionConfig = new OracleConnectionConfig.Builder()
                .dbLogin("palantir")
                .dbPassword(ImmutableMaskedValue.of("palpal"))
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
