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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Function;

import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleErrorConstants;
import com.palantir.logsafe.Preconditions;
import com.palantir.nexus.db.DBType;

public class SingleTablePhysicalBoundStoreStrategy implements PhysicalBoundStoreStrategy {
    private static final Logger log = LoggerFactory.getLogger(SingleTablePhysicalBoundStoreStrategy.class);

    private final TableReference timestampTable;
    private final String tablePrefix;

    public SingleTablePhysicalBoundStoreStrategy(TableReference timestampTable, String tablePrefix) {
        this.timestampTable = Preconditions.checkNotNull(timestampTable, "timestampTable cannot be null");
        this.tablePrefix = tablePrefix;
    }

    @Override
    public Long readLimit(Connection connection) throws SQLException {
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

    @Override
    public void writeLimit(Connection connection, long limit) throws SQLException {
        String updateTs = "UPDATE " + prefixedTimestampTableName() + " SET last_allocated = ?";
        try (PreparedStatement statement = connection.prepareStatement(updateTs)) {
            statement.setLong(1, limit);
            statement.executeUpdate();
        }
    }

    @Override
    public void createLimit(Connection connection, long limit) throws SQLException {
        QueryRunner run = new QueryRunner();
        run.update(connection,
                String.format("INSERT INTO %s (last_allocated) VALUES (?)", prefixedTimestampTableName()),
                limit);
    }

    @Override
    public void createTimestampTable(Connection connection, Function<Connection, DBType> dbTypeExtractor)
            throws SQLException {
        try (Statement statement = connection.createStatement()) {
            if (dbTypeExtractor.apply(connection).equals(DBType.ORACLE)) {
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
}
