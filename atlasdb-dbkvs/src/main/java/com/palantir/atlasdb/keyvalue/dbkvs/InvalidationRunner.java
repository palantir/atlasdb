/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.ConnectionDbTypes;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.CreateTimestampTableQueries;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.PhysicalBoundStoreDatabaseUtils;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.common.base.Throwables;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.RetriableTransactions;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import org.apache.commons.dbutils.QueryRunner;
import org.immutables.value.Value;

public class InvalidationRunner {
    private static final String LAST_ALLOCATED = "last_allocated";
    private static final String LEGACY_LAST_ALLOCATED = "legacy_last_allocated";

    private final ConnectionManager connManager;
    private final TableReference timestampTable;
    private final String tablePrefix;

    public InvalidationRunner(ConnectionManager connManager, TableReference timestampTable, String tablePrefixString) {
        this.connManager = connManager;
        this.timestampTable = timestampTable;
        this.tablePrefix = tablePrefixString;
    }

    public void createTableIfDoesNotExist() {
        try (Connection conn = connManager.getConnection()) {
            createTimestampTable(conn);
        } catch (SQLException error) {
            throw PalantirSqlException.create(error);
        }
    }

    public void createTimestampTable(Connection conn) throws SQLException {
        PhysicalBoundStoreDatabaseUtils.createTimestampTable(
                conn,
                ConnectionDbTypes::getDbType,
                CreateTimestampTableQueries.getCreateTableQueriesForLegacyStore(prefixedTimestampTableName()));
    }

    public long ensureInDbStoreIsPoisonedAndGetLastAllocatedTimestamp() {
        RetriableTransactions.TransactionResult<Long> result = RetriableTransactions.run(connManager, connection -> {
            Limits limits = getLimits(connection);
            TableStatus tableStatus = checkTableStatus(limits);

            if (tableStatus == TableStatus.POISONED) {
                return limits.legacyUpperLimit().get().value();
            }
            return poisonStoreAndGetLastAllocatedTimestamp(connection, limits, tableStatus);
        });

        switch (result.getStatus()) {
            case SUCCESSFUL:
                return result.getResultValue();
            case UNKNOWN:
            case FAILED:
                Throwable error = result.getError();
                if (error instanceof SQLException) {
                    throw PalantirSqlException.create((SQLException) error);
                }
                throw Throwables.rewrapAndThrowUncheckedException(error);
            default:
                throw new SafeIllegalStateException(
                        "Unrecognized transaction status.", SafeArg.of("status", result.getStatus()));
        }
    }

    private Long poisonStoreAndGetLastAllocatedTimestamp(Connection connection, Limits limits, TableStatus tableStatus)
            throws SQLException {
        long lastAllocated = tableStatus == TableStatus.NO_DATA
                ? AtlasDbFactory.NO_OP_FAST_FORWARD_TIMESTAMP
                : limits.upperLimit().get().value();
        poisonTable(connection);
        return lastAllocated;
    }

    private void poisonTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            if (ConnectionDbTypes.getDbType(connection) == DBType.ORACLE) {
                poisonOracleTable(connection, statement);
            } else {
                poisonPostgresTable(connection, statement);
            }
        }
    }

    private void poisonOracleTable(Connection connection, Statement statement) throws SQLException {
        try {
            statement.execute(String.format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s",
                    prefixedTimestampTableName(), LAST_ALLOCATED, LEGACY_LAST_ALLOCATED));
        } catch (SQLException e) {
            if (!PhysicalBoundStoreDatabaseUtils.isOracleDuplicateColumnError(e)) {
                throw e;
            }
            connection.rollback();
        }
    }

    private void poisonPostgresTable(Connection connection, Statement statement) throws SQLException {
        try {
            statement.execute(String.format(
                    "ALTER TABLE %s RENAME %s TO %s",
                    prefixedTimestampTableName(), LAST_ALLOCATED, LEGACY_LAST_ALLOCATED));
        } catch (SQLException e) {
            if (!PhysicalBoundStoreDatabaseUtils.isPostgresColumnDoesNotExistError(e)) {
                throw e;
            }
            connection.rollback();
        }
    }

    private Limits getLimits(Connection connection) throws SQLException {
        String sql = String.format("SELECT * FROM %s FOR UPDATE", prefixedTimestampTableName());
        QueryRunner run = new QueryRunner();
        return run.query(connection, sql, rs -> {
            if (rs.next()) {
                return ImmutableLimits.builder()
                        .upperLimit(getColumnStatusFromResultSet(rs, LAST_ALLOCATED))
                        .legacyUpperLimit(getColumnStatusFromResultSet(rs, LEGACY_LAST_ALLOCATED))
                        .build();
            } else {
                return ImmutableLimits.builder().build();
            }
        });
    }

    private Optional<ColumnStatus> getColumnStatusFromResultSet(ResultSet rs, String colName) throws SQLException {
        try {
            return ColumnStatus.columnStatusWithValue(rs.getLong(colName));
        } catch (SQLException e) {
            if (PhysicalBoundStoreDatabaseUtils.isOracleInvalidColumnError(e)
                    || PhysicalBoundStoreDatabaseUtils.isPostgresColumnDoesNotExistError(e)) {
                return Optional.empty();
            } else {
                throw e;
            }
        }
    }

    private TableStatus checkTableStatus(Limits limits) {
        TableStatus status = getTableStatus(limits);

        Preconditions.checkState(
                status != TableStatus.BOTH_COLUMNS,
                "We detected the table has been poisoned but last_allocated column still exists."
                        + "This is unexpected. Please contact support.");
        return status;
    }

    private TableStatus getTableStatus(Limits limits) {
        boolean upperLimitExists = limits.upperLimit().isPresent();
        boolean legacyUpperLimitExists = limits.legacyUpperLimit().isPresent();

        if (upperLimitExists) {
            return legacyUpperLimitExists ? TableStatus.BOTH_COLUMNS : TableStatus.HEALTHY;
        }
        return legacyUpperLimitExists ? TableStatus.POISONED : TableStatus.NO_DATA;
    }

    private String prefixedTimestampTableName() {
        return tablePrefix + timestampTable.getQualifiedName();
    }

    @Value.Immutable
    interface Limits {
        Optional<ColumnStatus> upperLimit();

        Optional<ColumnStatus> legacyUpperLimit();
    }

    @Value.Immutable
    interface ColumnStatus {
        @Value.Default
        default long value() {
            return AtlasDbFactory.NO_OP_FAST_FORWARD_TIMESTAMP;
        }

        static Optional<ColumnStatus> columnStatusWithValue(long value) {
            return Optional.of(ImmutableColumnStatus.builder().value(value).build());
        }
    }

    private enum TableStatus {
        NO_DATA,
        POISONED,
        HEALTHY,
        BOTH_COLUMNS, // Both last_allocated and poisoned columns exist
    }
}
