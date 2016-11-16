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

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.nexus.db.pool.ConnectionManager;

public final class OracleDbTimestampBoundStore extends AbstractDbTimestampBoundStore {
    private static final Logger log = LoggerFactory.getLogger(OracleDbTimestampBoundStore.class);

    private static final String ORACLE_ALREADY_EXISTS_ERROR = "ORA-00955";

    public static OracleDbTimestampBoundStore create(ConnectionManager connManager) {
        OracleDbTimestampBoundStore oracleDbTimestampBoundStore = new OracleDbTimestampBoundStore(
                connManager,
                AtlasDbConstants.ORACLE_TIMESTAMP_TABLE);
        oracleDbTimestampBoundStore.init();
        return oracleDbTimestampBoundStore;
    }

    private OracleDbTimestampBoundStore(ConnectionManager connManager, TableReference timestampTable) {
        this.connManager = Preconditions.checkNotNull(connManager, "connectionManager is required");
        this.timestampTable = Preconditions.checkNotNull(timestampTable, "timestampTable is required");
    }

    @Override
    protected void createTimestampTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            createTimestampTableIgnoringAlreadyExistsError(statement);
        }
    }

    private void createTimestampTableIgnoringAlreadyExistsError(Statement statement) throws SQLException {
        try {
            statement.execute(String.format("CREATE TABLE %s ( last_allocated NUMBER(38) NOT NULL )",
                    timestampTable.getQualifiedName()));
        } catch (SQLException e) {
            if (!e.getMessage().contains(ORACLE_ALREADY_EXISTS_ERROR)) {
                log.error("Error occurred creating the Oracle timestamp table", e);
                throw e;
            }
        }
    }
}
