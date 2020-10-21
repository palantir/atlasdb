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

package com.palantir.atlasdb.keyvalue.dbkvs.timestamp;

import com.palantir.atlasdb.keyvalue.dbkvs.OracleErrorConstants;
import com.palantir.nexus.db.DBType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.OptionalLong;
import java.util.function.Function;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PhysicalBoundStoreDatabaseUtils {
    private static final Logger log = LoggerFactory.getLogger(PhysicalBoundStoreDatabaseUtils.class);

    private PhysicalBoundStoreDatabaseUtils() {
        // utilities
    }

    public static void createTimestampTable(
            Connection connection, Function<Connection, DBType> dbTypeExtractor, CreateTimestampTableQueries queries)
            throws SQLException {
        try (Statement statement = connection.createStatement()) {
            if (dbTypeExtractor.apply(connection).equals(DBType.ORACLE)) {
                createTimestampTableIgnoringAlreadyExistsError(statement, queries.oracleQuery());
            } else {
                statement.execute(queries.postgresQuery());
            }
        }
    }

    private static void createTimestampTableIgnoringAlreadyExistsError(Statement statement, String oracleQuery)
            throws SQLException {
        try {
            statement.execute(oracleQuery);
        } catch (SQLException e) {
            if (!isTableAlreadyExistsError(e)) {
                log.error("Error occurred creating the Oracle timestamp table", e);
                throw e;
            }
            // The table already exists and we thus couldn't create it.
            // Note that in Postgres this is taken care of by the query itself.
        }
    }

    public static boolean isTableAlreadyExistsError(SQLException exception) {
        return exception.getMessage().contains(OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR);
    }

    public static boolean isOracleDuplicateColumnError(SQLException exception) {
        return exception.getMessage().contains(OracleErrorConstants.ORACLE_DUPLICATE_COLUMN_ERROR);
    }

    public static boolean isOracleInvalidColumnError(SQLException exception) {
        return exception.getMessage().contains("Invalid column name");
    }

    public static boolean isPostgresColumnDoesNotExistError(SQLException exception) {
        return exception.getSQLState().equals(PSQLState.UNDEFINED_COLUMN.getState());
    }

    public static OptionalLong getLastAllocatedColumn(ResultSet rs) throws SQLException {
        if (rs.next()) {
            return OptionalLong.of(rs.getLong("last_allocated"));
        } else {
            return OptionalLong.empty();
        }
    }
}
