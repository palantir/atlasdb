package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Callable;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.Throwables;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.BasicSQLUtils;
import com.palantir.nexus.db.sql.PalantirSqlConnection;
import com.palantir.sql.Connections;

public final class SingleConnectionTransactionService implements TransactionService {
    private final PalantirSqlConnection connection;

    public SingleConnectionTransactionService(PalantirSqlConnection connection) {
        this.connection = connection;
    }

    @Override
    public Long get(long startTimestamp) {
        throw new UnsupportedOperationException(
                "Single connection transaction service for commit putUnlessExists only.");
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        throw new UnsupportedOperationException(
                "Single connection transaction service for commit putUnlessExists only.");
    }

    private static final String SQL_MET_INSERT_ONE_TRANSACTION =
            "/* SQL_MET_INSERT_ONE ("+ TransactionConstants.TRANSACTION_TABLE.getQualifiedName() + ") */" +
            " INSERT INTO pt_met_" + TransactionConstants.TRANSACTION_TABLE.getQualifiedName() +
            " (row_name, col_name, ts, val) " +
            " VALUES (?, ?, ?, ?) ";

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) {
        Cell cell = getTransactionCell(startTimestamp);
        byte[] value = TransactionConstants.getValueForTimestamp(commitTimestamp);

        try {
            assert connection.getUnderlyingConnection().getAutoCommit() == false;
        } catch (PalantirSqlException e) {
            throw Throwables.throwUncheckedException(e);
        } catch (SQLException e) {
            throw Throwables.throwUncheckedException(e);
        }
        connection.insertOneUnregisteredQuery(SQL_MET_INSERT_ONE_TRANSACTION,
                cell.getRowName(), cell.getColumnName(), 0L, value);
        BasicSQLUtils.runUninterruptably(new Callable<Void>() {
            @Override
            public Void call() throws PalantirSqlException  {
                Connections.commit(connection.getUnderlyingConnection());
                return null;
            }
        }, "atlas SQL commit"); //$NON-NLS-1$
    }

    private Cell getTransactionCell(long startTimestamp) {
        return Cell.create(
                TransactionConstants.getValueForTimestamp(startTimestamp),
                TransactionConstants.COMMIT_TS_COLUMN);
    }
}
