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
                    return limits.legacyUpperLimit().getAsLong();
                }

                long lastAllocated;

                if (tableStatus == TableStatus.NO_DATA) {
                    lastAllocated = NO_OP_FAST_FORWARD_TIMESTAMP;
                } else {
                    lastAllocated = limits.upperLimit().getAsLong();
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
        String sql = "SELECT * FROM " + prefixedTimestampTableName();
        return run.query(connection, sql, rs -> {
            if (rs.next()) {
                limitsBuilder.upperLimit(getUpperLimit(rs, "last_allocated"));
                limitsBuilder.legacyUpperLimit(getUpperLimit(rs, "LEGACY_last_allocated"));
            }
            return limitsBuilder.build();
        });
    }

    private OptionalLong getUpperLimit(ResultSet rs, String columnLabel) {
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
        boolean upperLimitExists = limits.upperLimit().isPresent();
        boolean legacyUpperLimitExists = limits.legacyUpperLimit().isPresent();

        if (upperLimitExists) {
            return legacyUpperLimitExists ? TableStatus.ILLEGAL_COLUMNS : TableStatus.HEALTHY;
        }
        return legacyUpperLimitExists ? TableStatus.POISONED : TableStatus.NO_DATA; // no data in table
    }

    @Value.Immutable
    interface Limits {
        OptionalLong upperLimit();
        OptionalLong legacyUpperLimit();
    }

    private enum TableStatus {
        NO_DATA,
        POISONED,
        HEALTHY,
        ILLEGAL_COLUMNS,
    }
}
