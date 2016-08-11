package com.palantir.atlasdb.sql.jdbc.statement;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.palantir.atlasdb.sql.grammar.SelectQuery;
import com.palantir.atlasdb.sql.jdbc.connection.AtlasJdbcConnection;
import com.palantir.atlasdb.sql.jdbc.results.AtlasJdbcResultSet;

public class AtlasJdbcStatement implements Statement {

    private final AtlasJdbcConnection conn;
    private SqlExecutionResult sqlExecutionResult;

    private boolean isClosed = false;

    public AtlasJdbcStatement(AtlasJdbcConnection conn) {
        this.conn = conn;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        execute(sql);
        if (getResultSet() == null) {
            throw new SQLException("query did not produce a result set: " + sql);
        }
        return getResultSet();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        execute(sql);
        if (getUpdateCount() == SqlExecutionResult.NO_UPDATES) {
            throw new SQLException("query did not produce an update: " + sql);
        }
        return getUpdateCount();
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
    }

    //----------------------------------------------------------------------

    @Override
    public int getMaxFieldSize() throws SQLException {
        assertNotClosed();
        return 0; // unlimited
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        assertNotClosed();
        methodNotSupported();
    }

    @Override
    public int getMaxRows() throws SQLException {
        assertNotClosed();
        return 0; // unlimited
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        assertNotClosed();
        methodNotSupported();
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
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        assertNotClosed();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        assertNotClosed();
        methodNotSupported();
    }

    //----------------------- Multiple Results --------------------------

    @Override
    public boolean execute(String sql) throws SQLException {
        SelectQuery select = SelectQuery.create(sql);
        ResultSet rset = AtlasJdbcResultSet.create(conn.getService(), conn.getTransactionToken(), select, this);
        sqlExecutionResult = SqlExecutionResult.fromResult(rset);
        return getResultSet() != null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return sqlExecutionResult.resultSet().orElse(null);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        assertNotClosed();
        return sqlExecutionResult.updateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        assertNotClosed();
        sqlExecutionResult = SqlExecutionResult.empty();
        return false;
    }

    //-------------------------- JDBC 2.0 -----------------------------

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        assertNotClosed();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("results sets are only TYPE_FORWARD_ONLY");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        assertNotClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        assertNotClosed();
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
        assertNotClosed();
        methodNotSupported();
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return conn;
    }

    //-------------------------- JDBC 3.0 -----------------------------

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        assertNotClosed();
        methodNotSupported();
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
        assertNotClosed();
        methodNotSupported();
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return 0;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        assertNotClosed();
        methodNotSupported();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return false;
    }

    //-------------------------- JDBC 4.1 -----------------------------

    @Override
    public void closeOnCompletion() throws SQLException {
        assertNotClosed();
        methodNotSupported();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        assertNotClosed();
        methodNotSupported();
        return false;
    }

    //------------------------- WRAPPERS ------------------------------

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

    //------------------------- WRAPPERS ------------------------------

    private void assertNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is not allowed to be closed at this time.");
        }
    }

    private void methodNotSupported() throws SQLException {
        StringWriter errors = new StringWriter();
        new Throwable().printStackTrace(new PrintWriter(errors));
        throw new SQLFeatureNotSupportedException("Method not supported:\n" + errors.toString());
    }

}
