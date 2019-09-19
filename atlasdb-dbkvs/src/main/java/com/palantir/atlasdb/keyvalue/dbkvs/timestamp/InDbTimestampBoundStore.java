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
package com.palantir.atlasdb.keyvalue.dbkvs.timestamp;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleErrorConstants;
import com.palantir.common.base.Throwables;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.RetriableTransactions;
import com.palantir.nexus.db.pool.RetriableTransactions.TransactionResult;
import com.palantir.nexus.db.pool.RetriableWriteTransaction;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO(hsaraogi): switch to using ptdatabase sql running, which more gracefully supports multiple db types.
public class InDbTimestampBoundStore implements TimestampBoundStore {
    private static final Logger log = LoggerFactory.getLogger(InDbTimestampBoundStore.class);

    private static final String EMPTY_TABLE_PREFIX = "";

    private final ConnectionManager connManager;
    private final TableReference timestampTable;
    private final String tablePrefix;

    @GuardedBy("this") // lazy init to avoid db connections in constructors
    private DBType dbType;

    @GuardedBy("this")
    private Long currentLimit = null;

    /**
     * Use only if you have already initialized the timestamp table. This exists for legacy support.
     */
    public InDbTimestampBoundStore(ConnectionManager connManager, TableReference timestampTable) {
        this(connManager, timestampTable, EMPTY_TABLE_PREFIX);
    }

    public static InDbTimestampBoundStore create(ConnectionManager connManager, TableReference timestampTable) {
        return InDbTimestampBoundStore.create(connManager, timestampTable, EMPTY_TABLE_PREFIX);
    }

    public static InDbTimestampBoundStore create(
            ConnectionManager connManager,
            TableReference timestampTable,
            String tablePrefixString) {
        InDbTimestampBoundStore inDbTimestampBoundStore = new InDbTimestampBoundStore(
                connManager,
                timestampTable,
                tablePrefixString);

        inDbTimestampBoundStore.init();

        return inDbTimestampBoundStore;
    }

    private InDbTimestampBoundStore(ConnectionManager connManager, TableReference timestampTable, String tablePrefix) {
        this.connManager = Preconditions.checkNotNull(connManager, "connectionManager is required");
        this.timestampTable = Preconditions.checkNotNull(timestampTable, "timestampTable is required");
        this.tablePrefix = tablePrefix;
    }

    private void init() {
        try (Connection conn = connManager.getConnection()) {
            createTimestampTable(conn);
        } catch (SQLException error) {
            throw PalantirSqlException.create(error);
        }
    }

    private interface Operation {
        long run(Connection connection, @Nullable Long oldLimit) throws SQLException;
    }

    @GuardedBy("this")
    private long runOperation(final Operation operation) {
        TransactionResult<Long> result = RetriableTransactions.run(connManager, new RetriableWriteTransaction<Long>() {
            @GuardedBy("InDbTimestampBoundStore.this")
            @Override
            public Long run(Connection connection) throws SQLException {
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
                        throw new SafeIllegalStateException(
                                "Unable to retrieve a timestamp when expected. "
                                        + "This service is in a dangerous state and should be taken down "
                                        + "until a new safe timestamp value can be established in the KVS. "
                                        + "Please contact support.");
                    }
                } else {
                    // first read, no check to be done
                }
                return operation.run(connection, oldLimit);
            }
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
        String sql = "SELECT last_allocated FROM " + prefixedTimestampTableName() + " FOR UPDATE";
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
        String updateTs = "UPDATE " + prefixedTimestampTableName() + " SET last_allocated = ?";
        try (PreparedStatement statement = connection.prepareStatement(updateTs)) {
            statement.setLong(1, limit);
            statement.executeUpdate();
        }
    }

    private void createLimit(Connection connection, long limit) throws SQLException {
        QueryRunner run = new QueryRunner();
        run.update(connection,
                String.format("INSERT INTO %s (last_allocated) VALUES (?)", prefixedTimestampTableName()),
                limit);
    }

    private void createTimestampTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            if (getDbType(connection).equals(DBType.ORACLE)) {
                createTimestampTableIgnoringAlreadyExistsError(statement);
            } else {
                statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s ( last_allocated int8 NOT NULL )",
                        prefixedTimestampTableName()));
            }
        }
    }

    private void createTimestampTableIgnoringAlreadyExistsError(Statement statement) throws SQLException {
        try {
            statement.execute(String.format("CREATE TABLE %s ( last_allocated NUMBER(38) NOT NULL )",
                    prefixedTimestampTableName()));
        } catch (SQLException e) {
            if (!e.getMessage().contains(OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR)) {
                log.error("Error occurred creating the Oracle timestamp table", e);
                throw e;
            }
        }
    }

    private String prefixedTimestampTableName() {
        return tablePrefix + timestampTable.getQualifiedName();
    }

    @GuardedBy("this")
    private DBType getDbType(Connection connection) {
        if (dbType == null) {
            dbType = ConnectionDbTypes.getDbType(connection);
        }
        return dbType;
    }
}
