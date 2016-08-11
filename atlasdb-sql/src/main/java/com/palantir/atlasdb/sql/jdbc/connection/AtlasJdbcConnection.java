package com.palantir.atlasdb.sql.jdbc.connection;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.palantir.atlasdb.api.AtlasDbService;
import com.palantir.atlasdb.api.TransactionToken;
import com.palantir.atlasdb.sql.jdbc.statement.AtlasJdbcPreparedStatement;
import com.palantir.atlasdb.sql.jdbc.statement.AtlasJdbcStatement;

public class AtlasJdbcConnection implements Connection {

    private final AtlasDbService service;
    private final String jdbcUrl;

    private TransactionToken token = null;
    private boolean isClosed = false;

    public AtlasJdbcConnection(AtlasDbService service, String jdbcUrl) {
        this.service = service;
        this.jdbcUrl = jdbcUrl;
        this.token = TransactionToken.autoCommit();
    }

    public AtlasDbService getService() {
        return service;
    }

    /**  Transaction token is either a sentinel, an autocommit or is in an open transaction.
         The first two mean connection is <b>not</b> in transaction.
     */
    public TransactionToken getTransactionToken() {
        return token;
    }

    private TransactionToken resetTransactionToken() {
        return token.shouldAutoCommit() ? TransactionToken.autoCommit() : emptyTransactionToken();
    }

    private TransactionToken emptyTransactionToken() {
        return null; // TODO implement a sentinel transaction token
    }

    private void assertConnectionNotInTransaction() throws SQLException {
        if (token == emptyTransactionToken()) {
            throw new SQLException("connection is not allowed to be in transaction at this time.");
        }
    }

    String getURL() {
        return jdbcUrl;
    }

    @Override
    public AtlasJdbcStatement createStatement() throws SQLException {
        return new AtlasJdbcStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (token == emptyTransactionToken()) {
            token = service.startTransaction();
        }
        return new AtlasJdbcPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit == token.shouldAutoCommit()) {
            return;
        }
        this.token = autoCommit ? TransactionToken.autoCommit() : emptyTransactionToken();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return token.shouldAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        service.commit(token);
        this.token = resetTransactionToken();
    }

    @Override
    public void rollback() throws SQLException {
        service.abort(token);
        this.token = resetTransactionToken();
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    private void assertConnectionNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is not allowed to be closed at this time.");
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new AtlasJdbcDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        assertConnectionNotClosed();
        assertConnectionNotInTransaction();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
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
