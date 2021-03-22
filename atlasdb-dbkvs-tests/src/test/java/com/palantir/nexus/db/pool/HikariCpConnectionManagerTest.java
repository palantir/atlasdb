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

import com.google.common.base.Throwables;
import com.palantir.atlasdb.keyvalue.dbkvs.DbkvsPostgresTestSuite;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;
import com.palantir.nexus.db.pool.config.PostgresConnectionConfig;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Random;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HikariCpConnectionManagerTest {

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

    @Test
    public void testRuntimePasswordChange() throws SQLException {
        // create a new user to avoid messing up the main user for other tests
        // make username random in case concurrent runs makes things bad
        String testUsername = "testuser" + new Random().nextInt(1 << 16);
        String password1 = "password1";
        String password2 = "password2";
        String password3 = "password3";

        // initial config intentionally has the wrong password for the test user
        PostgresConnectionConfig testConfig = createConnectionConfig(testUsername, password3, 0, 3);

        try (Connection conn = manager.getConnection()) {
            try (Statement statement = conn.createStatement()) {
                statement.execute(String.format("CREATE USER %s WITH PASSWORD '%s'", testUsername, password1));
            }
            try (Statement statement = conn.createStatement()) {
                statement.execute(String.format(
                        "GRANT ALL PRIVILEGES ON DATABASE %s TO %s", testConfig.getDbName(), testUsername));
            }
        }

        ConnectionManager testManager = new HikariCPConnectionManager(testConfig);
        // fixing the password before init should work
        testManager.setPassword(password1);

        try (Connection conn = testManager.getConnection()) {
            changePassword(conn, testUsername, password2);
            // existing connection should still work
            checkConnection(conn);

            // trying to get a new connection should fail (times out with wrong password)
            assertPasswordWrong(testManager);

            // fix the password on the pool
            testManager.setPassword(password2);
            // original connection should still work
            checkConnection(conn);

            // new connection should also work
            try (Connection conn2 = testManager.getConnection()) {
                checkConnection(conn2);
            }

            // changing the pool password again and one connection should work (conn still in the pool)
            testManager.setPassword(password3);
            try (Connection conn2 = testManager.getConnection()) {
                checkConnection(conn2);

                // getting one more connection should see the password error
                assertPasswordWrong(testManager);

                // use existing conn to fix the password
                changePassword(conn2, testUsername, password3);

                // now a new connection should work
                try (Connection conn3 = testManager.getConnection()) {
                    checkConnection(conn3);
                }
            }
        } finally {
            testManager.close();
        }
    }

    private static void changePassword(Connection conn, String username, String newPassword) throws SQLException {
        // for some reason things seem to sometimes be unreliable in CI, so try setting the password twice
        for (int i = 0; i < 2; i++) {
            try (Statement statement = conn.createStatement()) {
                statement.execute(String.format("ALTER USER %s WITH PASSWORD '%s'", username, newPassword));
            }
        }
    }

    private static void assertPasswordWrong(ConnectionManager testManager) {
        // This is needed because it appears that sometimes postgres password changes do not take effect immediately.
        // If the password change happens too late, we end up with a connection in the pool that we did not expect.
        // In that case we need to wait the configured time (30 seconds) for hikari to remove the idle connection from
        // the pool before we can detect that the password is wrong. Note that I cannot reproduce this case locally,
        // but it appears to almost always happen on circle.
        Awaitility.await("assertPasswordWrong")
                .atMost(Duration.ofMinutes(2))
                .pollInterval(Duration.ofSeconds(2))
                .pollDelay(Duration.ofMillis(100))
                .until(() -> isPasswordWrong(testManager));
    }

    private static boolean isPasswordWrong(ConnectionManager testManager) {
        try (Connection ignored = testManager.getConnection()) {
            return false;
        } catch (SQLException e) {
            // if "password authentication failed" is in the cause chain, the password is wrong
            // otherwise this is an unexpected exception
            for (Throwable t : Throwables.getCausalChain(e)) {
                String message = t.getMessage();
                if (message != null && message.contains("password authentication failed")) {
                    return true;
                }
            }
            throw new SafeRuntimeException("unexpected exception checking password on ConnectionManager", e);
        }
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
        return createConnectionConfig("palantir", "palantir", maxConnections, maxConnections);
    }

    private static PostgresConnectionConfig createConnectionConfig(
            String username, String password, int minConnections, int maxConnections) {
        PostgresConnectionConfig suiteConfig = DbkvsPostgresTestSuite.getConnectionConfig();
        return ImmutablePostgresConnectionConfig.builder()
                .dbName(suiteConfig.getDbName())
                .host(suiteConfig.getHost())
                .port(suiteConfig.getPort())
                .dbLogin(username)
                .dbPassword(ImmutableMaskedValue.of(password))
                .minConnections(minConnections)
                .maxConnections(maxConnections)
                .checkoutTimeout(2000)
                // hikari doesn't allow this to be lower than 30 seconds
                .maxConnectionAge(30)
                .build();
    }
}
