package com.palantir.atlasdb.rdbms.impl.service;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.rdbms.api.config.AtlasRdbmsConfiguration;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbms;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsConnection;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsConnectionCallable;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsConnectionCheckedCallable;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsConstants;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsExecutionException;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsIllegalMigrationException;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsSchemaVersion;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsSchemaVersionMismatchException;
import com.palantir.atlasdb.rdbms.impl.util.TempTableDescriptor;
import com.palantir.atlasdb.rdbms.impl.util.TempTableDescriptorProvider;
import com.palantir.common.base.Throwables;
import com.palantir.util.Nullable;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.DriverDataSource;

public final class AtlasRdbmsImpl implements AtlasRdbms {
    private static final Logger log = LoggerFactory.getLogger(AtlasRdbmsImpl.class);

    private final String systemPropertiesTableName;
    private final HikariDataSource dataSource;
    private final QueryRunner queryRunner;

    public static AtlasRdbmsImpl create(AtlasRdbmsConfiguration dbConfiguration,
                                        TempTableDescriptorProvider tempTableProvider) {
        HikariDataSource dataSource = initializeDataSource(dbConfiguration, tempTableProvider.getTempTableDescriptors());
        QueryRunner queryRunner = initializeQueryRunner(dataSource);
        AtlasRdbmsImpl db = new AtlasRdbmsImpl(dbConfiguration, dataSource, queryRunner);
        db.ensureDbSeeded();
        db.registerShutdownHook();
        return db;
    }

    private static QueryRunner initializeQueryRunner(DataSource dataSource) {
        return new QueryRunner(dataSource);
    }

    private static HikariDataSource initializeDataSource(AtlasRdbmsConfiguration dbConfig,
                                                         Collection<TempTableDescriptor> tempTables) {
        HikariConfig config = new HikariConfig();

        // Load JDBC driver.
        try {
            config.setDriverClassName(dbConfig.getJdbcDriverClassName());
        } catch (Exception e) { // CHECKSTYLE IGNORE: ok to catch Exception here
            throw new RuntimeException("Failed to load DB driver: " + dbConfig.getJdbcDriverClassName());
        }

        // Configure data source.
        config.setJdbcUrl(dbConfig.getJdbcUrl());

        config.setMaximumPoolSize(dbConfig.getMaxConnections());
        config.setConnectionTestQuery("SELECT 1");
        config.setRegisterMbeans(true);

        DriverDataSource underlying = new DriverDataSource(config.getJdbcUrl(), config.getDriverClassName(),
                config.getDataSourceProperties(), null, null);
        DataSource customized = AtlasRdbmsConnectionCustomizer.customize(underlying, tempTables);
        config.setDataSource(customized);
        return new HikariDataSource(config);
    }

    private AtlasRdbmsImpl(AtlasRdbmsConfiguration dbConfig,
                           HikariDataSource dataSource,
                           QueryRunner queryRunner) {
        this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
        this.queryRunner = Preconditions.checkNotNull(queryRunner, "queryRunner");
        this.systemPropertiesTableName = Preconditions.checkNotNull(
                dbConfig.getSystemPropertiesTableName(), "systemPropertiesTableName");
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                shutdown();
            }
        }));
    }

    /** <b>NOTE:</b> All connections returned by this method have auto-commit enabled. */
    private Connection getConnection() throws SQLException {
        long start = System.currentTimeMillis();
        Connection connection = dataSource.getConnection();
        long totalMs = System.currentTimeMillis() - start;
        if (log.isDebugEnabled()) {
            log.debug("Waited " + totalMs + "ms waiting to get a connection.");
        }
        connection = ConnectionAssertionProxy.newInstance(connection);
        connection.setAutoCommit(true);

        return connection;
    }

    /* package */ static final String DB_ERROR_MESSAGE = "DB error occurred";

    public static SQLException chainSqlExceptions(SQLException e) {
        List<SQLException> chain = Lists.newArrayList();
        SQLException next = e.getNextException();
        while (next != null) {
            chain.add(next);
            next = next.getNextException();
        }
        if (chain.size() == 0) {
            return e;
        }

        SQLException ret = chain.get(chain.size() - 1);
        for (int i = chain.size() - 2; i >= 0; i--) {
            ret = Throwables.chain(chain.get(i), ret);
        }
        return ret;
    }

    @CheckForNull
    @Override
    public <T> T runWithDbConnection(AtlasRdbmsConnectionCallable<T> dbCallable) throws AtlasRdbmsExecutionException {
        Connection c = null;
        try {
            c = getConnection();
            final T result = dbCallable.call(new AtlasRdbmsConnectionImpl(c, queryRunner));
            return result;
        } catch (SQLException e) {
            throw new AtlasRdbmsExecutionException(DB_ERROR_MESSAGE, chainSqlExceptions(e));
        } catch (Exception e) { // CHECKSTYLE IGNORE: ok to catch Exception here
            Throwables.throwIfUncheckedException(e);
            throw new AtlasRdbmsExecutionException(e);
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException e) {
                    throw new AtlasRdbmsExecutionException("Error trying to close connection", e);
                }
            }
        }
    }

    @Override
    public <T, K extends Exception> T runWithDbConnection(AtlasRdbmsConnectionCheckedCallable<T, K> dbCallable)
            throws AtlasRdbmsExecutionException, K {
        try {
            return runWithDbConnection((AtlasRdbmsConnectionCallable<T>) dbCallable);
        } catch (AtlasRdbmsExecutionException e) {
            Throwables.throwIfInstance(e.getCause(), dbCallable.getThrowableClass());
            throw e;
        }
    }

    @CheckForNull
    @Override
    public <T, K extends Exception> T runWithDbConnectionInTransaction(AtlasRdbmsConnectionCheckedCallable<T, K> dbCallable)
            throws AtlasRdbmsExecutionException, K {
        try {
            return runWithDbConnectionInTransaction((AtlasRdbmsConnectionCallable<T>) dbCallable);
        } catch (AtlasRdbmsExecutionException e) {
            Throwables.throwIfInstance(e.getCause(), dbCallable.getThrowableClass());
            throw e;
        }
    }

    @CheckForNull
    @Override
    public <T> T runWithDbConnectionInTransaction(AtlasRdbmsConnectionCallable<T> dbCallable) throws AtlasRdbmsExecutionException {
        return runInTransaction(wrap(dbCallable));
    }

    private <T> ConnectionCallable<T> wrap(final AtlasRdbmsConnectionCallable<T> callable) {
        return new ConnectionCallable<T>() {
            @Override
            public T call(Connection c) throws Exception {
                AtlasRdbmsConnection atlasConnection = new AtlasRdbmsConnectionImpl(c, queryRunner);
                T result = callable.call(atlasConnection);
                if (atlasConnection.getError().isPresent()) {
                    log.error("An error occurred during a DB operation, but no exception was thrown."
                            + "  The following is the last exception that occurred.", atlasConnection.getError().get());
                    throw atlasConnection.getError().get();
                }
                return result;
            }
        };
    }

    private static interface ConnectionCallable<T> {
        T call(Connection c) throws Exception;
    }

    private <T> T runInTransaction(ConnectionCallable<T> callable) throws AtlasRdbmsExecutionException {
        Connection c = null;
        boolean success = false;
        try {
            c = getConnection();
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            c.setAutoCommit(false);
            final T result = callable.call(c);
            c.commit();
            success = true;
            return result;
        } catch (SQLException e) {
            throw new AtlasRdbmsExecutionException(DB_ERROR_MESSAGE, chainSqlExceptions(e));
        } catch (Exception e) { // CHECKSTYLE IGNORE: ok to catch Exception here
            Throwables.throwIfUncheckedException(e);
            throw new AtlasRdbmsExecutionException(e);
        } finally {
            if (!success) {
                // Error occurred. Rollback.
                try {
                    DbUtils.rollback(c);
                } catch (Exception e) { // CHECKSTYLE IGNORE: ok to catch Exception here
                    log.error("Rollback failed", e);
                }
            }

            DbUtils.closeQuietly(c);
        }
    }

    @Override
    public AtlasRdbmsSchemaVersion getDbSchemaVersion() throws SQLException {
        try {
            return runInTransaction(new ConnectionCallable<AtlasRdbmsSchemaVersion>() {
                @Override
                public AtlasRdbmsSchemaVersion call(Connection c) throws Exception {
                    return getDbSchemaVersion(c);
                }
            });
        } catch (AtlasRdbmsExecutionException e) {
            Throwables.throwIfInstance(e.getCause(), SQLException.class);
            throw Throwables.throwUncheckedException(e.getCause());
        }
    }

    @Override
    public Nullable<String> getDbSystemProperty(final String property) throws SQLException {
        try {
            return runInTransaction(new ConnectionCallable<Nullable<String>>() {
                @Override
                public Nullable<String> call(Connection c) throws Exception {
                    return getDbSystemProperty(c, property);
                }
            });
        } catch (AtlasRdbmsExecutionException e) {
            Throwables.throwIfInstance(e.getCause(), SQLException.class);
            throw Throwables.throwUncheckedException(e.getCause());
        }
    }

    @Override
    public Nullable<String> setDbSystemProperty(final String property, final String value) throws SQLException {
        if (AtlasRdbmsConstants.PROPERTY_DB_SCHEMA_VERSION.equals(property) ||
                AtlasRdbmsConstants.PROPERTY_DB_SCHEMA_HOTFIX_VERSION.equals(property)) {
            throw new IllegalArgumentException("Cannot directly set schema version.  Run a schema migration instead.");
        }
        try {
            return runInTransaction(new ConnectionCallable<Nullable<String>>() {
                @Override
                public Nullable<String> call(Connection c) throws Exception {
                    return setDbSystemProperty(c, property, value);
                }
            });
        } catch (AtlasRdbmsExecutionException e) {
            Throwables.throwIfInstance(e.getCause(), SQLException.class);
            throw Throwables.throwUncheckedException(e.getCause());
        }
    }

    @Override
    public void performSchemaMigration(final AtlasRdbmsSchemaVersion expectedCurrentVersion,
                                       final AtlasRdbmsSchemaVersion versionAfterMigration,
                                       final List<String> migrationSqlStatements)
                                               throws AtlasRdbmsSchemaVersionMismatchException, AtlasRdbmsIllegalMigrationException,
                                               SQLException {
        try {
            runInTransaction(new ConnectionCallable<Void>() {
                @Override
                public Void call(Connection c) throws Exception {
                    AtlasRdbmsSchemaVersion currentVersion = getDbSchemaVersion(c);
                    checkVersions(expectedCurrentVersion, versionAfterMigration, currentVersion);
                    for (String sql : migrationSqlStatements) {
                        executeSqlStatement(c, sql);
                    }
                    AtlasRdbmsSchemaVersion currentVersionAgain = setDbSchemaVersion(c, versionAfterMigration);
                    // this should not fail if we are using good transaction semantics, but if not it's worth checking again
                    checkVersions(expectedCurrentVersion, versionAfterMigration, currentVersionAgain);
                    return null;
                }
            });
        } catch (AtlasRdbmsExecutionException e) {
            Throwables.throwIfInstance(e.getCause(), AtlasRdbmsSchemaVersionMismatchException.class);
            Throwables.throwIfInstance(e.getCause(), AtlasRdbmsIllegalMigrationException.class);
            Throwables.throwIfInstance(e.getCause(), SQLException.class);
            throw Throwables.throwUncheckedException(e.getCause());
        }

    }

    private static void checkVersions(AtlasRdbmsSchemaVersion expectedCurrentVersion,
                                      AtlasRdbmsSchemaVersion versionAfterMigration,
                                      AtlasRdbmsSchemaVersion currentVersion)
                                              throws AtlasRdbmsSchemaVersionMismatchException,
                                              AtlasRdbmsIllegalMigrationException {
        if (!currentVersion.equals(expectedCurrentVersion)) {
            throw new AtlasRdbmsSchemaVersionMismatchException(expectedCurrentVersion, currentVersion);
        } else if (currentVersion.compareTo(versionAfterMigration) >= 0) {
            throw new AtlasRdbmsIllegalMigrationException(currentVersion, versionAfterMigration);
        }
    }

    private AtlasRdbmsSchemaVersion getDbSchemaVersion(Connection c) throws SQLException {
        String schemaVersion = getDbSystemProperty(c, AtlasRdbmsConstants.PROPERTY_DB_SCHEMA_VERSION).get();
        String schemaHotfixVersion = getDbSystemProperty(c, AtlasRdbmsConstants.PROPERTY_DB_SCHEMA_HOTFIX_VERSION).get();
        return new AtlasRdbmsSchemaVersion(Long.parseLong(schemaVersion), Long.parseLong(schemaHotfixVersion));
    }

    private AtlasRdbmsSchemaVersion setDbSchemaVersion(Connection c, AtlasRdbmsSchemaVersion newVersion) throws SQLException {
        String oldVersion = setDbSystemProperty(c, AtlasRdbmsConstants.PROPERTY_DB_SCHEMA_VERSION,
                String.valueOf(newVersion.getMajorVersion())).get();
        String oldHotfixVersion = setDbSystemProperty(c, AtlasRdbmsConstants.PROPERTY_DB_SCHEMA_HOTFIX_VERSION,
                String.valueOf(newVersion.getHotfixVersion())).get();
        return new AtlasRdbmsSchemaVersion(Long.parseLong(oldVersion), Long.parseLong(oldHotfixVersion));
    }

    public Nullable<String> getDbSystemProperty(Connection c, String property) throws SQLException {
        if (property == null) {
            throw new IllegalArgumentException("property == null");
        }
        return queryRunner.query(c, "SELECT value FROM " + systemPropertiesTableName + " WHERE property = ?",
                new ResultSetHandler<Nullable<String>>() {
            @Override
            public Nullable<String> handle(ResultSet rs) throws SQLException {
                if (rs.next()) {
                    return Nullable.of(rs.getString("value"));
                } else {
                    return Nullable.absent();
                }
            }
        }, property);
    }

    private Nullable<String> setDbSystemProperty(Connection c, String property, String value) throws SQLException {
        if (property == null) {
            throw new IllegalArgumentException("property == null");
        }

        final String query;
        String[] variables;
        Nullable<String> oldValue = getDbSystemProperty(c, property);
        if (value == null) {
            query = "DELETE FROM " + systemPropertiesTableName + " WHERE property = ?";
            variables = new String[] {property };
        } else if (!oldValue.isPresent()) {
            query = "INSERT INTO " + systemPropertiesTableName + " (property, value) VALUES (?, ?)";
            variables = new String[] {property, value };
        } else {
            query = "UPDATE " + systemPropertiesTableName + " SET value = ? WHERE property = ?";
            variables = new String[] {value, property };
        }

        PreparedStatement ps = null;
        try {
            ps = c.prepareStatement(query);
            for (int i = 1; i <= variables.length; i++) {
                ps.setString(i, variables[i - 1]);
            }
            ps.executeUpdate();
        } finally {
            if (ps != null) {
                ps.close();
            }
        }

        return oldValue;
    }

    public void shutdown() {
        closeConnections();

        log.info("Shut down Database Service");
    }

    private void closeConnections() {
        // close outstanding connections in the C390 connection pool to free server resources
        if (dataSource != null) {
            dataSource.close();
        }
    }

    private void ensureDbSeeded() {
        if (!isDbSeeded()) {
            try {
                runInTransaction(new ConnectionCallable<Void>() {
                    @Override
                    public Void call(Connection c) throws Exception {
                        executeSqlStatement(c, "CREATE TABLE " + systemPropertiesTableName +
                                "(property VARCHAR(512) NOT NULL, value VARCHAR(4096) NOT NULL, "
                                + "CONSTRAINT pk_" + systemPropertiesTableName + " PRIMARY KEY (property));");
                        setDbSystemProperty(c, AtlasRdbmsConstants.PROPERTY_DB_SCHEMA_VERSION, "1");
                        setDbSystemProperty(c, AtlasRdbmsConstants.PROPERTY_DB_SCHEMA_HOTFIX_VERSION, "0");
                        return null;
                    }

                });
            } catch (AtlasRdbmsExecutionException e) {
                log.error("Error trying to set up system properties table: ", e.getCause());
            }
        }
    }

    private void executeSqlStatement(Connection c, String sqlStatement) throws SQLException {
        Statement s = null;
        try {
            s = c.createStatement();
            s.execute(sqlStatement);
        } finally {
            DbUtils.closeQuietly(s);
        }
    }

    private boolean isDbSeeded() {
        Connection c = null;
        try {
            c = getConnection();
            boolean isDbSeeded = false;
            String [] tables = {systemPropertiesTableName, systemPropertiesTableName.toUpperCase() };
            DatabaseMetaData dbMetaData = c.getMetaData();
            for (String table : tables) {
                ResultSet rs = null;
                try {
                    rs = dbMetaData.getTables(null, null, table, null);
                    isDbSeeded = rs.next();
                    if (isDbSeeded) {
                        break;
                    }
                } finally {
                    if (rs != null) {
                        rs.close();
                    }
                }
            }

            if (isDbSeeded) {
                log.info("The Data Integration Server DB is seeded");
            } else {
                log.info("The Data Integration Server DB is NOT seeded");
            }

            return isDbSeeded;
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Error occurred while checking if Data Integration Server DB is seeded", e);
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException e) {
                    throw new RuntimeException("Error closing connection");
                }
            }
        }
    }

    private static final class ConnectionAssertionProxy implements InvocationHandler {
        private static final Logger log = LoggerFactory.getLogger(ConnectionAssertionProxy.class);

        private final Connection connection;
        private final AtomicReference<Thread> firstThread;
        private final Exception e;

        private boolean hasClosed;

        public static Connection newInstance(final Connection connection) {
            return (Connection)
                    Proxy.newProxyInstance(
                            connection.getClass().getClassLoader(),
                            connection.getClass().getInterfaces(),
                            new ConnectionAssertionProxy(connection));
        }

        private ConnectionAssertionProxy(Connection connection) {
            this.connection = connection;
            firstThread = new AtomicReference<Thread>(null);
            e = new Exception("The assertion proxy was created here");
            hasClosed = false;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {
            try {
                firstThread.compareAndSet(null, Thread.currentThread());
                if (Thread.currentThread() != firstThread.get()) {
                    log.error("BUG: DB connections aren't thread safe.  " +
                            "A DB connection was accessed from multiple threads: " +
                            firstThread.get() + " and " + Thread.currentThread() + ".  " +
                            "The stack trace is from when the assertion proxy was created.", e);
                    assert false : "DB connections should only be used by one thread at a time";
                }

                if ("close".equals(method.getName())) {
                    hasClosed = true;
                }

                return method.invoke(connection, arguments);
            } catch (InvocationTargetException ite) {
                throw ite.getTargetException();
            }
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            if (!hasClosed) {
                log.error("BUG: A DB connection has been leaked.  " +
                        "The stack trace is from when the assertion proxy was created.", e);
                assert false : "DB connections should be closed";
            }
        }
    }
}