/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleErrorConstants;
import com.palantir.nexus.db.pool.ConnectionManager;

public final class OracleDbTimestampBoundStore extends AbstractDbTimestampBoundStore {
    private static final Logger log = LoggerFactory.getLogger(OracleDbTimestampBoundStore.class);
    public static final String TIMESTAMP_COLUMN_NAME = "last_allocated";

    public static OracleDbTimestampBoundStore create(ConnectionManager connManager, String tablePrefix) {
        OracleDbTimestampBoundStore oracleDbTimestampBoundStore = new OracleDbTimestampBoundStore(
                connManager,
                tablePrefix,
                AtlasDbConstants.TIMESTAMP_TABLE);
        oracleDbTimestampBoundStore.init();
        return oracleDbTimestampBoundStore;
    }

    private OracleDbTimestampBoundStore(
            ConnectionManager connManager,
            String tablePrefix,
            TableReference timestampTable) {
        super(connManager, tablePrefix, timestampTable);
    }

    @Override
    protected void createTimestampTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            createTimestampTableIgnoringAlreadyExistsError(statement);
        }
    }

    private void createTimestampTableIgnoringAlreadyExistsError(Statement statement) throws SQLException {
        try {
            statement.execute(String.format("CREATE TABLE %s ( " + TIMESTAMP_COLUMN_NAME + " NUMBER(38) NOT NULL )",
                    prefixedTimestampTableName()));
        } catch (SQLException e) {
            if (!e.getMessage().contains(OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR)) {
                log.error("Error occurred creating the Oracle timestamp table", e);
                throw e;
            }
        }
    }
}
