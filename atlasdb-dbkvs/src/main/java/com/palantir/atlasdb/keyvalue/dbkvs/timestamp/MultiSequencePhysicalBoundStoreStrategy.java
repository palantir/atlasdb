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
import com.palantir.nexus.db.DBType;

public class MultiSequencePhysicalBoundStoreStrategy implements PhysicalBoundStoreStrategy {
    private static final Logger log = LoggerFactory.getLogger(MultiSequencePhysicalBoundStoreStrategy.class);

    private final TableReference timestampTable;
    private final String series;

    public MultiSequencePhysicalBoundStoreStrategy(TableReference timestampTable, String series) {
        this.timestampTable = timestampTable;
        this.series = series;
    }

    @Override
    public void createTimestampTable(Connection connection, Function<Connection, DBType> dbTypeExtractor)
            throws SQLException {
        try (Statement statement = connection.createStatement()) {
            if (dbTypeExtractor.apply(connection).equals(DBType.ORACLE)) {
                createTimestampTableIgnoringAlreadyExistsError(statement);
            } else {
                statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s ("
                                + " client VARCHAR(2000) NOT NULL,"
                                + " last_allocated int8 NOT NULL,"
                                + " PRIMARY KEY (client))",
                        timestampTable.getQualifiedName()));
            }
        }
    }

    private void createTimestampTableIgnoringAlreadyExistsError(Statement statement) throws SQLException {
        try {
            statement.execute(String.format("CREATE TABLE %s ("
                            + " client VARCHAR(2000) NOT NULL,"
                            + " last_allocated NUMBER(38) NOT NULL,"
                            + " CONSTRAINT %s_pk PRIMARY KEY (client))",
                    timestampTable.getQualifiedName(),
                    timestampTable.getQualifiedName()));
        } catch (SQLException e) {
            if (!e.getMessage().contains(OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR)) {
                log.error("Error occurred creating the Oracle timestamp table", e);
                throw e;
            }
        }
    }

    @Override
    public Long readLimit(Connection connection) throws SQLException {
        String sql = String.format("SELECT last_allocated FROM %s WHERE client = ? FOR UPDATE",
                timestampTable.getQualifiedName());
        QueryRunner run = new QueryRunner();
        return run.query(connection, sql, rs -> {
            if (rs.next()) {
                return rs.getLong("last_allocated");
            } else {
                return null;
            }
        }, series);
    }

    @Override
    public void writeLimit(Connection connection, long limit) throws SQLException {
        String updateTs = String.format("UPDATE %s SET last_allocated = ? WHERE client = ?",
                timestampTable.getQualifiedName());
        try (PreparedStatement statement = connection.prepareStatement(updateTs)) {
            statement.setLong(1, limit);
            statement.setString(2, series);
            statement.executeUpdate();
        }
    }

    @Override
    public void createLimit(Connection connection, long limit) throws SQLException {
        QueryRunner run = new QueryRunner();
        run.update(connection,
                String.format("INSERT INTO %s (client, last_allocated) VALUES (?, ?)",
                        timestampTable.getQualifiedName()),
                series,
                limit);
    }
}
