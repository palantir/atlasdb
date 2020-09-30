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
import java.sql.SQLException;
import java.sql.Statement;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.dbkvs.OracleErrorConstants;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.ConnectionManager;

public class InDbTimestampBoundStoreHelper {
    private static final Logger log = LoggerFactory.getLogger(InDbTimestampBoundStore.class);
    private final ConnectionManager connManager;

    @GuardedBy("this") // lazy init to avoid db connections in constructors
    private DBType dbType;

    public InDbTimestampBoundStoreHelper(ConnectionManager connManager) {this.connManager = connManager;}

    public void createTableIfDoesNotExist(String prefixedTimestampTableName) {
        try (Connection conn = connManager.getConnection()) {
            createTimestampTable(conn, prefixedTimestampTableName);
        } catch (SQLException error) {
            throw PalantirSqlException.create(error);
        }
    }

    private void createTimestampTable(Connection connection, String prefixedTimestampTableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            if (getDbType(connection).equals(DBType.ORACLE)) {
                createTimestampTableIgnoringAlreadyExistsError(statement, prefixedTimestampTableName);
            } else {
                statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s ( last_allocated int8 NOT NULL )",
                        prefixedTimestampTableName));
            }
        }
    }

    private void createTimestampTableIgnoringAlreadyExistsError(Statement statement, String prefixedTimestampTableName)
            throws SQLException {
        try {
            statement.execute(String.format("CREATE TABLE %s ( last_allocated NUMBER(38) NOT NULL )",
                    prefixedTimestampTableName));
        } catch (SQLException e) {
            if (!e.getMessage().contains(OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR)) {
                log.error("Error occurred creating the Oracle timestamp table", e);
                throw e;
            }
        }
    }

    @GuardedBy("this")
    private DBType getDbType(Connection connection) {
        if (dbType == null) {
            dbType = ConnectionDbTypes.getDbType(connection);
        }
        return dbType;
    }
}