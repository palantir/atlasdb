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

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.nexus.db.DBType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.OptionalLong;
import java.util.function.Function;
import org.apache.commons.dbutils.QueryRunner;

public class MultiSequencePhysicalBoundStoreStrategy implements PhysicalBoundStoreStrategy {
    private final TableReference timestampTable;
    private final TimestampSeries series;

    public MultiSequencePhysicalBoundStoreStrategy(TableReference timestampTable, TimestampSeries series) {
        this.timestampTable = timestampTable;
        this.series = series;
    }

    @Override
    public void createTimestampTable(Connection connection, Function<Connection, DBType> dbTypeExtractor)
            throws SQLException {
        PhysicalBoundStoreDatabaseUtils.createTimestampTable(
                connection,
                dbTypeExtractor,
                ImmutableCreateTimestampTableQueries.builder()
                        .postgresQuery(String.format(
                                "CREATE TABLE IF NOT EXISTS %s ("
                                        + " client VARCHAR(2000) NOT NULL,"
                                        + " last_allocated int8 NOT NULL,"
                                        + " PRIMARY KEY (client))",
                                timestampTable.getQualifiedName()))
                        .oracleQuery(String.format(
                                "CREATE TABLE %s ("
                                        + " client VARCHAR(2000) NOT NULL,"
                                        + " last_allocated NUMBER(38) NOT NULL,"
                                        + " CONSTRAINT %s_pk PRIMARY KEY (client))",
                                timestampTable.getQualifiedName(), timestampTable.getQualifiedName()))
                        .build());
    }

    @Override
    public OptionalLong readLimit(Connection connection) throws SQLException {
        String sql = String.format(
                "SELECT last_allocated FROM %s WHERE client = ? FOR UPDATE", timestampTable.getQualifiedName());
        QueryRunner run = new QueryRunner();
        return run.query(connection, sql, PhysicalBoundStoreDatabaseUtils::getLastAllocatedColumn, series.series());
    }

    @Override
    public void writeLimit(Connection connection, long limit) throws SQLException {
        String updateTs =
                String.format("UPDATE %s SET last_allocated = ? WHERE client = ?", timestampTable.getQualifiedName());
        try (PreparedStatement statement = connection.prepareStatement(updateTs)) {
            statement.setLong(1, limit);
            statement.setString(2, series.series());
            statement.executeUpdate();
        }
    }

    @Override
    public void createLimit(Connection connection, long limit) throws SQLException {
        QueryRunner run = new QueryRunner();
        run.update(
                connection,
                String.format(
                        "INSERT INTO %s (client, last_allocated) VALUES (?, ?)", timestampTable.getQualifiedName()),
                series.series(),
                limit);
    }
}
