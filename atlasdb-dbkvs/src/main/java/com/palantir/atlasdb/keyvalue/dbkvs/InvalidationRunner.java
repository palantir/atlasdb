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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbutils.QueryRunner;
import org.immutables.value.Value;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.ConnectionDbTypes;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStoreInitializer;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.common.base.Throwables;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.Preconditions;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.RetriableTransactions;

public class InvalidationRunner {
    private static final String LAST_ALLOCATED = "last_allocated";
    private static final String LEGACY_LAST_ALLOCATED = "legacy_last_allocated";

    private final ConnectionManager connManager;
    private final InDbTimestampBoundStoreInitializer helper;

    public InvalidationRunner(ConnectionManager connManager) {
        this.connManager = connManager;
        this.helper = new InDbTimestampBoundStoreInitializer(connManager);
    }

    public void createTableIfDoesNotExist() {
        helper.createTableIfDoesNotExist(prefixedTimestampTableName());
    }

    public long getLastAllocatedTimestampAndPoisonInDbStore() {
        RetriableTransactions.TransactionResult<Long> result = RetriableTransactions.run(connManager,
                connection -> {
                    Limits limits = getLimits(connection);
                    TableStatus tableStatus = checkTableStatus(limits);

                    if (tableStatus == TableStatus.POISONED) {
                        return limits.legacyUpperLimit().value();
                    }

                    long lastAllocated;
                    if (tableStatus == TableStatus.NO_DATA) {
                        lastAllocated = AtlasDbFactory.NO_OP_FAST_FORWARD_TIMESTAMP;
                    } else {
                        lastAllocated = limits.upperLimit().value();
                    }

                    poisonTable(connection);
                    return lastAllocated;
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
                throw new IllegalStateException("Unrecognized transaction status " + result.getStatus());
        }
    }

    private void poisonTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            if (ConnectionDbTypes.getDbType(connection).equals(DBType.ORACLE)) {
                statement.execute(String.format("ALTER TABLE %s RENAME COLUMN last_allocated TO LEGACY_last_allocated",
                        prefixedTimestampTableName()));
            } else {
                statement.execute(String.format("ALTER TABLE %s RENAME last_allocated TO LEGACY_last_allocated",
                        prefixedTimestampTableName()));
            }
        }
    }

    private Limits getLimits(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet res = metaData.getTables(null, null, prefixedTimestampTableName(), null);

        Preconditions.checkState(res.next(), "We are in the process of invalidating the "
                + "InDbTimestampBoundStore but the data table does not exist. "
                + "We should never reach here. Please contact support.");

        return ImmutableLimits.builder()
                .upperLimit(getColumnStatus(LAST_ALLOCATED, connection))
                .legacyUpperLimit(getColumnStatus(LEGACY_LAST_ALLOCATED, connection))
                .build();
    }

    private ColumnStatus getColumnStatus(String colName, Connection connection) throws SQLException {
        if (hasColumn(connection, colName)) {
            String sql = String.format("SELECT %s FROM %s FOR UPDATE", colName, prefixedTimestampTableName());
            QueryRunner run = new QueryRunner();
            return run.query(connection, sql, rs -> {
                if (rs.next()) {
                    return ColumnStatus.columnStatusWithValue(rs.getLong(colName));
                }
                return ColumnStatus.columnStatusWithoutValue();
            });

        } else {
            return ColumnStatus.voidColumnStatus();
        }
    }

    private boolean hasColumn(Connection connection, String colName) throws SQLException {
        return connection.getMetaData()
                .getColumns(null, null, prefixedTimestampTableName(), colName)
                .next();
    }

    private String prefixedTimestampTableName() {
        return AtlasDbConstants.TIMELOCK_TIMESTAMP_TABLE.getQualifiedName();
    }

    private TableStatus checkTableStatus(Limits limits) {
        TableStatus status = getTableStatus(limits);

        Preconditions.checkState(status != TableStatus.ILLEGAL_COLUMNS,
                "We detected the table has both current as well as legacy columns."
                        + "This is unexpected. Please contact support.");
        return status;
    }

    private TableStatus getTableStatus(Limits limits) {
        boolean upperLimitExists = limits.upperLimit().exists();
        boolean legacyUpperLimitExists = limits.legacyUpperLimit().exists();

        if (upperLimitExists) {
            return legacyUpperLimitExists ? TableStatus.ILLEGAL_COLUMNS : TableStatus.HEALTHY;
        }
        return legacyUpperLimitExists ? TableStatus.POISONED : TableStatus.NO_DATA; // no data in table
    }

    @Value.Immutable
    interface Limits {
        ColumnStatus upperLimit();
        ColumnStatus legacyUpperLimit();
    }

    @Value.Immutable
    interface ColumnStatus {
        @Value.Default
        default Boolean exists() {
            return false;
        }

        @Value.Default
        default long value() {
            return AtlasDbFactory.NO_OP_FAST_FORWARD_TIMESTAMP;
        }

        static ColumnStatus columnStatusWithValue(long value) {
            return ImmutableColumnStatus.builder().exists(true).value(value).build();
        }

        static ColumnStatus columnStatusWithoutValue() {
            return ImmutableColumnStatus.builder().exists(true).build();
        }

        static ColumnStatus voidColumnStatus() {
            return ImmutableColumnStatus.builder().build();
        }
    }

    private enum TableStatus {
        NO_DATA,
        POISONED,
        HEALTHY,
        ILLEGAL_COLUMNS,
    }
}
