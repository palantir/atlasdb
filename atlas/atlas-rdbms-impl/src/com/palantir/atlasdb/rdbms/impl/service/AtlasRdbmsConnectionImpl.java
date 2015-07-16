package com.palantir.atlasdb.rdbms.impl.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsConnection;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsResultSetHandler;
import com.palantir.util.Nullable;

public class AtlasRdbmsConnectionImpl implements AtlasRdbmsConnection {
    private static final Logger log = LoggerFactory.getLogger(AtlasRdbmsConnectionImpl.class);

    private final Connection connection;
    private final QueryRunner queryRunner;

    private Nullable<SQLException> error = Nullable.absent();

    public AtlasRdbmsConnectionImpl(Connection connection,
                                    QueryRunner queryRunner) {
        this.connection = connection;
        this.queryRunner = queryRunner;
    }

    // postgres has this nice...uh..."feature"...where if anything goes wrong
    // with any query, it auto-aborts your transaction.  Which means if you
    // want to go back and see what went wrong, you are SOL.  So we do the
    // next best thing and roll back automatically so you can continue
    // using the connection.
    private void rollbackAfterQueryFailure(SQLException e) {
        error = Nullable.of(e);
        try {
            connection.rollback();
        } catch (SQLException rollbackException) {
            log.error("Error while rolling back connection", rollbackException);
        }
    }

    private SQLException handleSqlException(SQLException e) throws SQLException {
        SQLException chained = AtlasRdbmsImpl.chainSqlExceptions(e);
        rollbackAfterQueryFailure(chained);
        throw chained;
    }

    private static <T> ResultSetHandler<T> wrap(final AtlasRdbmsResultSetHandler<T> handler) {
        return new ResultSetHandler<T>() {
            @Override
            public T handle(ResultSet rs) throws SQLException {
                return handler.handle(rs);
            }
        };
    }

    @Override
    public <T> T query(String sql, AtlasRdbmsResultSetHandler<T> rsh, Object... params) throws SQLException {
        try {
            return queryRunner.query(connection, sql, wrap(rsh), params);
        } catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public int update(String sql, Object... params) throws SQLException {
        try {
            return queryRunner.update(connection, sql, params);
        } catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public int[] batch(String sql, Object[][] params) throws SQLException {
        try {
            return queryRunner.batch(connection, sql, params);
        } catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean isAutoCommit() throws SQLException {
        return connection.getAutoCommit();
    }

    @Override
    public Nullable<SQLException> getError() {
        return error;
    }
}
