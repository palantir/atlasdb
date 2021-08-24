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

import com.google.common.base.Stopwatch;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.sql.ExceptionCheck;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HikariCP Connection Manager.
 *
 * @author dstipp
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class HikariCPConnectionManager extends BaseConnectionManager {
    private static final Logger log = LoggerFactory.getLogger(HikariCPConnectionManager.class);

    private final ConnectionConfig connConfig;
    private final HikariConfig hikariConfig;

    private enum StateType {
        // Base state at construction.  Nothing is set.
        ZERO,

        // "Normal" state.  dataSourcePool and poolProxy are set.
        NORMAL,

        // Closed state.  closeTrace is set.
        CLOSED;
    }

    private static class State {
        final StateType type;
        final HikariDataSource dataSourcePool;
        final HikariPoolMXBean poolProxy;
        final Throwable closeTrace;

        State(StateType type, HikariDataSource dataSourcePool, HikariPoolMXBean poolProxy, Throwable closeTrace) {
            this.type = type;
            this.dataSourcePool = dataSourcePool;
            this.poolProxy = poolProxy;
            this.closeTrace = closeTrace;
        }
    }

    private volatile State state = new State(StateType.ZERO, null, null, null);

    public HikariCPConnectionManager(ConnectionConfig connConfig) {
        this.connConfig = Preconditions.checkNotNull(connConfig, "ConnectionConfig must not be null");
        this.hikariConfig = connConfig.getHikariConfig();
    }

    @Override
    public Connection getConnection() throws SQLException {
        ConnectionAcquisitionProfiler profiler = new ConnectionAcquisitionProfiler();
        try {
            return acquirePooledConnection(profiler);
        } finally {
            profiler.logQueryDuration();
        }
    }

    /**
     * Attempts to acquire a valid database connection from the pool.
     * @param profiler Connection acquisition profiler for this attempt to acquire a connection
     * @return valid pooled connection
     * @throws SQLException if a connection cannot be acquired within the {@link ConnectionConfig#getCheckoutTimeout()}
     */
    private Connection acquirePooledConnection(ConnectionAcquisitionProfiler profiler) throws SQLException {
        while (true) {
            HikariDataSource dataSourcePool = checkAndGetDataSourcePool();
            Connection conn = dataSourcePool.getConnection();
            profiler.logAcquisitionAndRestart();

            try {
                testConnection(conn);
                profiler.logConnectionTest();
                return conn;
            } catch (SQLException e) {
                log.error(
                        "[{}] Dropping connection which failed validation",
                        SafeArg.of("connectionPoolName", connConfig.getConnectionPoolName()),
                        e);
                dataSourcePool.evictConnection(conn);

                if (profiler.globalStopwatch.elapsed(TimeUnit.MILLISECONDS) > connConfig.getCheckoutTimeout()) {
                    // It's been long enough that had hikari been
                    // validating internally it would have given up rather
                    // than retry
                    throw e;
                }
            }
        }
    }

    private void logPoolStats() {
        if (log.isWarnEnabled()) {
            State stateLocal = state;
            HikariPoolMXBean poolProxy = stateLocal.poolProxy;
            if (poolProxy != null) {
                try {
                    log.warn(
                            "[{}] HikariCP: "
                                    + "numBusyConnections = {}, "
                                    + "numIdleConnections = {}, "
                                    + "numConnections = {}, "
                                    + "numThreadsAwaitingCheckout = {}",
                            SafeArg.of("connectionPoolName", connConfig.getConnectionPoolName()),
                            SafeArg.of("activeConnections", poolProxy.getActiveConnections()),
                            SafeArg.of("idleConnections", poolProxy.getIdleConnections()),
                            SafeArg.of("totalConnections", poolProxy.getTotalConnections()),
                            SafeArg.of("threadsAwaitingConnection", poolProxy.getThreadsAwaitingConnection()));
                } catch (Exception e) {
                    log.error(
                            "[{}] Unable to log pool statistics.",
                            SafeArg.of("connectionPoolName", connConfig.getConnectionPoolName()),
                            e);
                }
            }
        }
    }

    private HikariDataSource checkAndGetDataSourcePool() throws SQLException {
        while (true) {
            // Volatile read state to see if we can get through here without locking.
            State stateLocal = state;

            switch (stateLocal.type) {
                case ZERO:
                    // Not yet initialized, no such luck.
                    synchronized (this) {
                        if (state == stateLocal) {
                            // The state hasn't changed on us, we can perform
                            // the initialization and start over again.
                            state = normalState();
                        } else {
                            // Someone else changed the state on us, just start over.
                        }
                    }
                    break;

                case NORMAL:
                    // Normal state, good to go
                    return stateLocal.dataSourcePool;

                case CLOSED:
                    throw new SQLException("Hikari connection pool already closed!", stateLocal.closeTrace);
            }

            // fall throughs are spins
        }
    }

    private void logConnectionFailure() {
        log.error(
                "Failed to get connection from the datasource "
                        + "{}. Please check the jdbc url ({}), the password, "
                        + "and that the secure server key is correct for the hashed password.",
                SafeArg.of("connectionPoolName", connConfig.getConnectionPoolName()),
                UnsafeArg.of("url", connConfig.getUrl()));
    }

    /**
     * Test an initialized dataSourcePool by running a simple query.
     * @param dataSourcePool data source to validate
     * @throws SQLException if data source is invalid and cannot be used.
     */
    private void testDataSource(HikariDataSource dataSourcePool) throws SQLException {
        try {
            try (Connection conn = dataSourcePool.getConnection()) {
                testConnection(conn);
            }
        } catch (SQLException e) {
            logConnectionFailure();
            throw e;
        }
    }

    /**
     * Test a database connection to determine if it is valid for use.
     * @param connection connection to validate
     * @throws SQLException if connection is invalid and cannot be used.
     */
    private void testConnection(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(connConfig.getTestQuery())) {
            if (!rs.next()) {
                throw new SQLException(String.format(
                        "Connection %s could not be validated as it did not return any results for test query %s",
                        connection, connConfig.getTestQuery()));
            }
        }
    }

    @Override
    public synchronized void close() {
        try {
            switch (state.type) {
                case ZERO:
                    break;

                case NORMAL:
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Closing connection pool",
                                UnsafeArg.of("connConfig", connConfig),
                                new SafeRuntimeException("Closing connection pool"));
                    }

                    state.dataSourcePool.close();
                    break;

                case CLOSED:
                    break;
            }
        } finally {
            state = new State(StateType.CLOSED, null, null, new Throwable("Hikari pool closed here"));
        }
    }

    @Override
    public boolean isClosed() {
        return state.type == StateType.CLOSED;
    }

    /**
     * Initializes a connection to the provided database.
     */
    @Override
    public synchronized void init() throws SQLException {
        if (state.type == StateType.ZERO) {
            state = normalState();
        }
    }

    private State normalState() throws SQLException {
        HikariDataSource dataSourcePool = getDataSourcePool();
        boolean keep = false;

        try {
            // Setup monitoring
            HikariPoolMXBean poolProxy = initPoolMbeans();
            testDataSource(dataSourcePool);
            keep = true;
            return new State(StateType.NORMAL, dataSourcePool, poolProxy, null);
        } finally {
            if (!keep) {
                IOUtils.closeQuietly(dataSourcePool);
            }
        }
    }

    private HikariDataSource getDataSourcePool() {
        // Print a stack trace whenever we initialize a pool
        if (log.isDebugEnabled()) {
            log.debug(
                    "Initializing connection pool",
                    UnsafeArg.of("connConfig", connConfig),
                    new SafeRuntimeException("Initializing connection pool"));
        }

        HikariDataSource dataSourcePool;

        try {
            try {
                dataSourcePool = new HikariDataSource(hikariConfig);
            } catch (IllegalArgumentException e) {
                // allow multiple pools on same JVM (they need unique names / endpoints)
                if (e.getMessage().contains("A metric named")) {
                    String poolName = connConfig.getConnectionPoolName();

                    hikariConfig.setPoolName(
                            poolName + "-" + ThreadLocalRandom.current().nextInt());
                    dataSourcePool = new HikariDataSource(hikariConfig);
                } else {
                    throw e;
                }
            }
        } catch (PoolInitializationException e) {
            log.error(
                    "Failed to initialize hikari data source",
                    SafeArg.of("connectionPoolName", connConfig.getConnectionPoolName()),
                    UnsafeArg.of("url", connConfig.getUrl()),
                    e);

            if (ExceptionCheck.isTimezoneInvalid(e)) {
                String tzname = TimeZone.getDefault().getID();

                final String errorPreamble = "Invalid system timezone " + tzname + " detected.";
                final String errorAfterward = "This can be corrected at the system level or overridden in the JVM "
                        + "startup flags by appending -Duser.timezone=UTC. Contact support for more information.";

                if (tzname.equals("Etc/Universal")) {
                    // Really common failure case. UTC is both a name AND an abbreviation.
                    log.error("{} The timezone *name* should be UTC. {}", errorPreamble, errorAfterward);
                } else {
                    log.error(
                            "{} This is caused by using non-standard or unsupported timezone names. {}",
                            errorPreamble,
                            errorAfterward);
                }
            }

            // This exception gets thrown by HikariCP and is useless outside of ConnectionManagers.
            RuntimeException e2 = new RuntimeException(ExceptionUtils.getMessage(e), e.getCause());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
        }
        return dataSourcePool;
    }

    /**
     * Setup JMX client.  This is only used for trace logging.
     */
    private HikariPoolMXBean initPoolMbeans() {
        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        ObjectName poolName = null;
        try {
            poolName = new ObjectName("com.zaxxer.hikari:type=Pool (" + connConfig.getConnectionPoolName() + ")");
        } catch (MalformedObjectNameException e) {
            log.error(
                    "Unable to setup mBean monitoring for pool {}.",
                    SafeArg.of("connectionPoolName", connConfig.getConnectionPoolName()),
                    e);
        }
        return JMX.newMBeanProxy(mbeanServer, poolName, HikariPoolMXBean.class);
    }

    @Override
    public DBType getDbType() {
        return connConfig.getDbType();
    }

    @Override
    public void setPassword(String newPassword) {
        Preconditions.checkNotNull(newPassword, "password cannot be null");
        State localState = state;
        if (localState.type == StateType.ZERO) {
            synchronized (this) {
                // need to check again in case init just happened
                localState = state;
                if (localState.type == StateType.ZERO) {
                    // the pool is not initialized, need to set password on the config object
                    internalSetPassword(newPassword, hikariConfig);
                    return;
                }
            }
        }
        if (localState.type == StateType.NORMAL) {
            // pool is initialized, need to set password directly on the pool
            internalSetPassword(newPassword, localState.dataSourcePool);
        } else if (localState.type == StateType.CLOSED) {
            throw new SafeRuntimeException("Hikari pool is closed", localState.closeTrace);
        }
    }

    /**
     * Sets the password (and username) on the HikariConfig (or HikariDataSource pool) if it is necessary.
     *
     * <p>Note that when the password matches static config, performance is better when setting the username and
     * password to null so that hikari uses the original JDBC parameters and avoids making new copies for every
     * new connection. See {@link com.zaxxer.hikari.pool.PoolBase#newConnection}. Also see
     * {@link com.palantir.nexus.db.pool.config.OracleConnectionConfig#getHikariProperties} for the JDBC properties.
     *
     * <p>Hikari checks the username field for being null to determine if it needs to copy the JDBC parameters, so
     * the username *must* be set for it to respect the password change.
     */
    private void internalSetPassword(String newPassword, HikariConfig configOrPool) {
        if (newPassword.equals(connConfig.getDbPassword().unmasked())) {
            configOrPool.setUsername(null);
            configOrPool.setPassword(null);
        } else {
            configOrPool.setPassword(newPassword);
            configOrPool.setUsername(connConfig.getDbLogin());
        }
    }

    private final class ConnectionAcquisitionProfiler {
        private final Stopwatch globalStopwatch;
        private final Stopwatch trialStopwatch;

        private long acquisitionMillis;
        private long connectionTestMillis;

        private ConnectionAcquisitionProfiler() {
            this(Stopwatch.createStarted(), Stopwatch.createStarted());
        }

        private ConnectionAcquisitionProfiler(Stopwatch globalStopwatch, Stopwatch trialStopwatch) {
            this.globalStopwatch = globalStopwatch;
            this.trialStopwatch = trialStopwatch;
        }

        private void logAcquisitionAndRestart() {
            trialStopwatch.stop();
            acquisitionMillis = trialStopwatch.elapsed(TimeUnit.MILLISECONDS);
            trialStopwatch.reset().start();
        }

        private void logConnectionTest() {
            trialStopwatch.stop();
            connectionTestMillis = trialStopwatch.elapsed(TimeUnit.MILLISECONDS);
        }

        private void logQueryDuration() {
            long elapsedMillis = globalStopwatch.elapsed(TimeUnit.MILLISECONDS);
            if (elapsedMillis > 1000) {
                log.warn(
                        "[{}] Waited {}ms for connection (acquisition: {}, connectionTest: {})",
                        SafeArg.of("connectionPoolName", connConfig.getConnectionPoolName()),
                        SafeArg.of("elapsedMillis", elapsedMillis),
                        SafeArg.of("acquisitionMillis", acquisitionMillis),
                        SafeArg.of("connectionTestMillis", connectionTestMillis));
                logPoolStats();
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "[{}] Waited {}ms for connection (acquisition: {}, connectionTest: {})",
                            SafeArg.of("connectionPoolName", connConfig.getConnectionPoolName()),
                            SafeArg.of("elapsedMillis", elapsedMillis),
                            SafeArg.of("acquisitionMillis", acquisitionMillis),
                            SafeArg.of("connectionTestMillis", connectionTestMillis));
                }
            }
        }
    }
}
