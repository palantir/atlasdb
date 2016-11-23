/**
 * Copyright 2015 Palantir Technologies
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

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.nexus.db.pool.ConnectionManager;

// TODO: switch to using ptdatabase sql running, which more gracefully
// supports multiple db types.
public final class PostgresDbTimestampBoundStore extends AbstractDbTimestampBoundStore {
    public static final String TIMESTAMP_COLUMN_NAME = "last_allocated";

    public static PostgresDbTimestampBoundStore create(ConnectionManager connManager, String tablePrefix) {
        PostgresDbTimestampBoundStore postgresDbTimestampBoundStore = new PostgresDbTimestampBoundStore(
                connManager,
                tablePrefix,
                AtlasDbConstants.TIMESTAMP_TABLE);
        postgresDbTimestampBoundStore.init();
        return postgresDbTimestampBoundStore;
    }

    private PostgresDbTimestampBoundStore(
            ConnectionManager connManager,
            String tablePrefix,
            TableReference timestampTable) {
        super(connManager, tablePrefix, timestampTable);
    }

    @Override
    protected void createTimestampTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("CREATE TABLE IF NOT EXISTS %s ( %s int8 NOT NULL )",
                    prefixedTimestampTableName(),
                    TIMESTAMP_COLUMN_NAME));
        }
    }
}
