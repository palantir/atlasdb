package com.palantir.atlasdb.sql.jdbc.statement;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.sql.grammar.SelectQuery;
import com.palantir.atlasdb.sql.jdbc.connection.AtlasJdbcConnection;
import com.palantir.atlasdb.sql.jdbc.results.AtlasJdbcResultSet;

public class AtlasJdbcStatement implements Statement {

    private static final Logger log = LoggerFactory.getLogger(AtlasJdbcStatement.class);
    private final AtlasJdbcConnection conn;
    private boolean isClosed = false;

    /**
     * The warnings chain
     */
    private SQLWarning warnings = null;

    /**
     * Maximum number of rows to return, 0 = unlimited
     */
    private int maxRows = 0;

    public AtlasJdbcStatement(AtlasJdbcConnection conn) {
        this.conn = conn;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        SelectQuery select = SelectQuery.create(sql);
        return AtlasJdbcResultSet.create(conn.getService(), conn.getTransactionToken(), select, this);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return 0;
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private void assertNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is not allowed to be closed at this time.");
        }
    }

    private void methodNotSupported() throws SQLException {
        StringWriter errors = new StringWriter();
        final Throwable ignored = new Throwable("Method not supported:");
        ignored.printStackTrace(new PrintWriter(errors));
        throw new SQLException("Method not supported:\n" + errors.toString(), ignored);
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        assertNotClosed();
        if (max < 0) {
            throw new SQLException("Max field size < 0");
        }
        methodNotSupported();
    }

    @Override
    public int getMaxRows() throws SQLException {
        assertNotClosed();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        assertNotClosed();
        if (max < 0) {
            throw new SQLException("Max rows < 0 not allowed");
        }
        maxRows = max;
        log.trace("Max rows set to {}", max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        assertNotClosed();
        methodNotSupported();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        assertNotClosed();
        if (seconds < 0) {
            throw new SQLException("Query timeout < 0 not allowed");
        }
        methodNotSupported();
    }

    @Override
    public void cancel() throws SQLException {
        assertNotClosed();
        methodNotSupported();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        assertNotClosed();
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        assertNotClosed();
        warnings = null;
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        assertNotClosed();
        methodNotSupported();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        // TODO
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        assertNotClosed();
        switch (direction) {
            case ResultSet.FETCH_FORWARD:
            case ResultSet.FETCH_REVERSE:
            case ResultSet.FETCH_UNKNOWN:
                methodNotSupported();
            default:
                throw new SQLException("Invalid direction provided");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        assertNotClosed();
        if (rows < 0) {
            throw new SQLException("Fetch size < 0 not allowed");
        }
        methodNotSupported();
    }

    @Override
    public int getFetchSize() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        assertNotClosed();
        methodNotSupported();
    }

    @Override
    public void clearBatch() throws SQLException {
        assertNotClosed();
        methodNotSupported();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        assertNotClosed();
        methodNotSupported();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        assertNotClosed();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        assertNotClosed();
        methodNotSupported();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
