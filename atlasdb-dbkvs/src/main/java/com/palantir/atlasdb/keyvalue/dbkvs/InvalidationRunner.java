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

import static com.palantir.atlasdb.keyvalue.dbkvs.timestamp.ConnectionDbTypes.getDbType;
import static com.palantir.atlasdb.spi.AtlasDbFactory.NO_OP_FAST_FORWARD_TIMESTAMP;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.OptionalLong;

import org.apache.commons.dbutils.QueryRunner;
import org.immutables.value.Value;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStoreHelper;
import com.palantir.common.base.Throwables;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.Preconditions;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.RetriableTransactions;
import com.palantir.nexus.db.pool.RetriableWriteTransaction;

public class InvalidationRunner {
    private final ConnectionManager connManager;
    private final InDbTimestampBoundStoreHelper helper;

    public InvalidationRunner(ConnectionManager connManager) {
        this.connManager = connManager;
        this.helper = new InDbTimestampBoundStoreHelper(connManager);
    }

    public void createTableIfDoesNotExist() {
        helper.createTableIfDoesNotExist(prefixedTimestampTableName());
    }

    public long getLastAllocatedAndPoison() {
        RetriableTransactions.TransactionResult<Long> result = RetriableTransactions.run(connManager,
                new RetriableWriteTransaction<Long>() {
            @Override
            public Long run(Connection connection) throws SQLException {
                Limits limits = readLimits(connection);
                TableStatus tableStatus = checkTableStatus(limits);

                if (tableStatus == TableStatus.POISONED) {
                    return limits.legacyUpperLimit().value().orElse(NO_OP_FAST_FORWARD_TIMESTAMP);
                }

                long lastAllocated;

                if (tableStatus == TableStatus.NO_DATA) {
                    lastAllocated = NO_OP_FAST_FORWARD_TIMESTAMP;
                } else {
                    lastAllocated = limits.upperLimit().value().orElse(NO_OP_FAST_FORWARD_TIMESTAMP);
                }

                poisonTable(connection);
                return lastAllocated;
            }
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
            if (getDbType(connection).equals(DBType.ORACLE)) {
                statement.execute(String.format("ALTER TABLE %s RENAME COLUMN last_allocated TO LEGACY_last_allocated",
                        prefixedTimestampTableName()));
            } else {
                statement.execute(String.format("ALTER TABLE %s RENAME last_allocated TO LEGACY_last_allocated",
                        prefixedTimestampTableName()));
            }
        }
    }

    private Limits readLimits(Connection connection) throws SQLException {
        ImmutableLimits.Builder limitsBuilder = ImmutableLimits.builder();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet res = metaData.getTables(null, null, prefixedTimestampTableName(), null);

        if (!res.next()) {
            return limitsBuilder.build(); // Illegal - table does not exist;
        }
        QueryRunner run = new QueryRunner();
        String sql = String.format("SELECT * FROM %s", prefixedTimestampTableName());
        return run.query(connection, sql, rs -> {
            ImmutableColumnStatus.Builder lastAllocatedStatusBuilder = ImmutableColumnStatus.builder();
            ImmutableColumnStatus.Builder legacyAllocatedStatusBuilder = ImmutableColumnStatus.builder();

            lastAllocatedStatusBuilder.exists(getColumnStatus(rs, "last_allocated"));
            legacyAllocatedStatusBuilder.exists(getColumnStatus(rs, "LEGACY_last_allocated"));

            if (rs.next()) {
                return limitsBuilder
                        .upperLimit(lastAllocatedStatusBuilder.value(
                                getUpperLimitForColumn(rs, "last_allocated")).build())

                        .legacyUpperLimit(lastAllocatedStatusBuilder.value(
                                getUpperLimitForColumn(rs, "LEGACY_last_allocated")).build())
                        .build();
            }
            return limitsBuilder
                    .upperLimit(getVoidColumnStatus())
                    .legacyUpperLimit(getVoidColumnStatus())
                    .build();
        });
    }

    private ImmutableColumnStatus getVoidColumnStatus() {
        return ImmutableColumnStatus.builder().build();
    }

    private boolean getColumnStatus(ResultSet rs, String columnLabel) {
        try {
            rs.findColumn(columnLabel);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private OptionalLong getUpperLimitForColumn(ResultSet rs, String columnLabel) {
        try {
            return OptionalLong.of(rs.getLong(columnLabel));
        } catch (SQLException e) {
            return OptionalLong.empty();
        }
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
        OptionalLong value();
    }

    private enum TableStatus {
        NO_DATA,
        POISONED,
        HEALTHY,
        ILLEGAL_COLUMNS,
    }
}
