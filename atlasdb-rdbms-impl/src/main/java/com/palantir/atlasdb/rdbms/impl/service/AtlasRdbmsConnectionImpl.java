package com.palantir.atlasdb.rdbms.impl.service;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.commons.dbutils.DbUtils;
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
            return internalBatch(connection, sql, params);
        } catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    // Borrowed from QueryRunner
    private void rethrow(SQLException cause, String sql, Object... params) throws SQLException {
        String causeMessage = cause.getMessage();
        if (causeMessage == null) {
            causeMessage = "";
        }
        StringBuffer msg = new StringBuffer(causeMessage);

        msg.append(" Query: ");
        msg.append(sql);
        msg.append(" Parameters: ");

        if (params == null) {
            msg.append("[]");
        } else {
            msg.append(Arrays.deepToString(params));
        }

        SQLException e = new SQLException(msg.toString(), cause.getSQLState(), cause.getErrorCode());
        e.setNextException(cause);

        throw e;
    }

    private void fillStatement(PreparedStatement stmt, ParameterMetaData pmd, Object... params) throws SQLException {
        if (params == null) {
            return;
        }

        if (pmd.getParameterCount() < params.length) {
            throw new SQLException("Too many parameters: expected "
                    + pmd.getParameterCount() + ", was given " + params.length);
        }

        for (int i = 0; i < params.length; i++) {
            if (params[i] != null) {
                stmt.setObject(i + 1, params[i]);
            } else {
                stmt.setNull(i + 1, pmd.getParameterType(i + 1));
            }
        }
    }

    // Borrowed from QueryRunner but we don't call getParameterMetaData inside the loop, since it would hit the underlying DB
    private int[] internalBatch(Connection conn, String sql, Object[][] params) throws SQLException {
        PreparedStatement stmt = null;
        int[] rows = null;
        try {
            stmt = conn.prepareStatement(sql);
            ParameterMetaData pmd = stmt.getParameterMetaData();

            for (int i = 0; i < params.length; i++) {
                fillStatement(stmt, pmd, params[i]);
                stmt.addBatch();
            }
            rows = stmt.executeBatch();
        } catch (SQLException e) {
            rethrow(e, sql, (Object[]) params);
        } finally {
            DbUtils.close(stmt);
        }

        return rows;
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
