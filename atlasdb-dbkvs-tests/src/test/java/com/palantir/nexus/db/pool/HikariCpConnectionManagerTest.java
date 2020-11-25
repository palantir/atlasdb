/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.nexus.db.pool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HikariCpConnectionManagerTest {

    private static final int POSTGRES_PORT_NUMBER = 5432;

    @ClassRule
    public static final DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.yml")
            .waitingForService("postgres", Container::areAllPortsOpen)
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(HikariCpConnectionManagerTest.class))
            .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
            .build();

    private ConnectionManager manager;

    @Before
    public void initConnectionManager() {
        manager = new HikariCPConnectionManager(createConnectionConfig(3));
    }

    @After
    public void closeConnectionManager() throws SQLException {
        manager.close();
    }

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Test
    public void testCanGetConnection() throws SQLException {
        try (Connection conn = manager.getConnection()) {
            checkConnection(conn);
        }
    }

    @Test
    public void testCantGetConnectionIfClosed() throws SQLException {
        manager.close();
        thrown.expect(SQLException.class);
        thrown.expectMessage("Hikari connection pool already closed!");
        try (Connection conn = manager.getConnection()) {
            fail("fail");
        }
    }

    @Test
    public void testCantGetConnectionIfPoolExhausted() throws SQLException {
        try (Connection conn1 = manager.getConnection();
                Connection conn2 = manager.getConnection();
                Connection conn3 = manager.getConnection()) {
            thrown.expect(SQLTransientConnectionException.class);
            thrown.expectMessage("Connection is not available, request timed out after");
            try (Connection conn4 = manager.getConnection()) {
                fail("fail");
            }
        }
    }

    @SuppressWarnings("checkstyle:NestedTryDepth")
    @Test
    public void testConnectionsAreReturnedToPoolWhenClosedAndOverAllocationsAreStillRejected() throws SQLException {
        try (Connection conn1 = manager.getConnection();
                Connection conn2 = manager.getConnection()) {
            try (Connection conn3 = manager.getConnection()) {
                checkConnection(conn3);
                // Make sure we exhausted the pool
                boolean caught = false;
                try (Connection conn4 = manager.getConnection()) {
                    fail("fail");
                } catch (SQLTransientConnectionException e) {
                    caught = true;
                }
                assertThat(caught).isTrue();
            }
            // Try getting a connection again after we returned the last one: should succeed
            try (Connection conn3 = manager.getConnection()) {
                checkConnection(conn3);
            }
        }
    }

    @Test
    public void testConnectionsAreReturnedToPoolWhenClosed() throws SQLException {
        try (Connection conn1 = manager.getConnection();
                Connection conn2 = manager.getConnection()) {
            try (Connection conn3 = manager.getConnection()) {
                checkConnection(conn3);
            }
            // Try getting a connection again after we returned the last one: should succeed
            try (Connection conn3 = manager.getConnection()) {
                checkConnection(conn3);
            }
        }
    }

    @Test
    public void testCloseIdempotent() throws SQLException {
        manager.init();
        manager.close();
        manager.close(); // shouldn't throw
    }

    private static void checkConnection(Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement()) {
            try (ResultSet result = statement.executeQuery("SELECT 123")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getInt(1)).isEqualTo(123);
                assertThat(result.next()).isFalse();
            }
        }
    }

    private static ConnectionConfig createConnectionConfig(int maxConnections) {
        DockerPort port = docker.containers().container("postgres").port(POSTGRES_PORT_NUMBER);
        InetSocketAddress postgresAddress = new InetSocketAddress(port.getIp(), port.getExternalPort());
        return ImmutablePostgresConnectionConfig.builder()
                .dbName("atlas")
                .dbLogin("palantir")
                .dbPassword(ImmutableMaskedValue.of("palantir"))
                .host(postgresAddress.getHostName())
                .port(postgresAddress.getPort())
                .maxConnections(maxConnections)
                .checkoutTimeout(2000)
                .build();
    }
}
