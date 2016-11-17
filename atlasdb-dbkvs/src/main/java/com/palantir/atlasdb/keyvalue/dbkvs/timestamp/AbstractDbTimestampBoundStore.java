/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.timestamp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.dbutils.QueryRunner;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.RetriableTransactions;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

abstract class AbstractDbTimestampBoundStore implements TimestampBoundStore {
    ConnectionManager connManager;
    TableReference timestampTable;

    @GuardedBy("this") // lazy init to avoid db connections in constructors
    private DBType dbType;

    @GuardedBy("this")
    private Long currentLimit = null;

    abstract void createTimestampTable(Connection connection) throws SQLException;


    protected AbstractDbTimestampBoundStore(ConnectionManager connManager, TableReference timestampTable) {
        this.connManager = Preconditions.checkNotNull(connManager, "connectionManager is required");
        this.timestampTable = Preconditions.checkNotNull(timestampTable, "timestampTable is required");
    }

    protected void init() {
        try {
            createTimestampTable(connManager.getConnection());
        } catch (SQLException error) {
            throw PalantirSqlException.create(error);
        }
    }

    private interface Operation {
        long run(Connection connection, @Nullable Long oldLimit) throws SQLException;
    }

    private long runOperation(final Operation operation) {
        RetriableTransactions.TransactionResult<Long> result = RetriableTransactions.run(connManager, connection -> {
            Long oldLimit = readLimit(connection);
            if (currentLimit != null) {
                if (oldLimit != null) {
                    if (currentLimit.equals(oldLimit)) {
                        // match, good
                    } else {
                        // mismatch
                        throw new MultipleRunningTimestampServiceError(
                                "Timestamp limit changed underneath us (limit in memory: " + currentLimit
                                        + ", limit in DB: " + oldLimit + "). This may indicate that "
                                        + "another timestamp service is running against this database!  "
                                        + "This may indicate that your services are not properly configured "
                                        + "to run in an HA configuration, or to have a CLI run against them.");
                    }
                } else {
                    // disappearance
                    throw new IllegalStateException(
                            "Unable to retrieve a timestamp when expected. "
                                    + "This service is in a dangerous state and should be taken down "
                                    + "until a new safe timestamp value can be established in the KVS. "
                                    + "Please contact support.");
                }
            } else {
                // first read, no check to be done
            }
            return operation.run(connection, oldLimit);
        });
        switch (result.getStatus()) {
            case SUCCESSFUL:
                currentLimit = result.getResultValue();
                return currentLimit;
            case UNKNOWN:
            case FAILED:
                if (result.getStatus() == RetriableTransactions.TransactionStatus.UNKNOWN) {
                    // Since the DB's state is unknown, the in-memory state can't be confirmed, so get rid of it.
                    currentLimit = null;
                }

                Throwable error = result.getError();
                if (error instanceof SQLException) {
                    throw PalantirSqlException.create((SQLException) error);
                }
                throw Throwables.rewrapAndThrowUncheckedException(error);
            default:
                throw new IllegalStateException("Unrecognized transaction status " + result.getStatus());
        }
    }

    @Override
    public synchronized long getUpperLimit() {
        return runOperation((connection, oldLimit) -> {
            if (oldLimit != null) {
                return oldLimit;
            }

            final long startVal = 10000;
            createLimit(connection, startVal);
            return startVal;
        });
    }

    @Override
    public synchronized void storeUpperLimit(final long limit) {
        runOperation((connection, oldLimit) -> {
            if (oldLimit != null) {
                writeLimit(connection, limit);
            } else {
                createLimit(connection, limit);
            }

            return limit;
        });
    }

    private Long readLimit(Connection connection) throws SQLException {
        String sql = "SELECT last_allocated FROM " + timestampTable.getQualifiedName() + " FOR UPDATE";
        QueryRunner run = new QueryRunner();
        return run.query(connection, sql, rs -> {
            if (rs.next()) {
                return rs.getLong("last_allocated");
            } else {
                return null;
            }
        });
    }

    private void writeLimit(Connection connection, long limit) throws SQLException {
        String updateTs = "UPDATE " + timestampTable.getQualifiedName() + " SET last_allocated = ?";
        try (PreparedStatement statement = connection.prepareStatement(updateTs)) {
            statement.setLong(1, limit);
            statement.executeUpdate();
        }
    }

    private void createLimit(Connection connection, long limit) throws SQLException {
        QueryRunner run = new QueryRunner();
        run.update(connection,
                String.format("INSERT INTO %s (last_allocated) VALUES (?)", timestampTable.getQualifiedName()),
                limit);
    }

    private DBType getDbType(Connection connection) {
        if (dbType == null) {
            dbType = ConnectionDbTypes.getDbType(connection);
        }
        return dbType;
    }
}
