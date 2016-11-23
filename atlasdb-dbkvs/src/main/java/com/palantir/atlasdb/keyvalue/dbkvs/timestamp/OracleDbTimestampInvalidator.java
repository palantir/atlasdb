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

import com.google.common.base.Throwables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleErrorConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.timestamp.TimestampInvalidator;

public class OracleDbTimestampInvalidator implements TimestampInvalidator {
    private static final Logger log = LoggerFactory.getLogger(OracleDbTimestampInvalidator.class);

    private final ConnectionManager connectionManager;
    private final String tablePrefix;
    private final TableReference timestampTable;

    public OracleDbTimestampInvalidator(KeyValueService rawKvs) {
        ConnectionManagerAwareDbKvs dbkvs = (ConnectionManagerAwareDbKvs) rawKvs;
        this.connectionManager = dbkvs.getConnectionManager();
        this.tablePrefix = dbkvs.getTablePrefix();
        this.timestampTable = AtlasDbConstants.TIMESTAMP_TABLE;
    }

    @Override
    public void invalidateTimestampTable() {
        try {
            Connection connection = connectionManager.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format(
                        "ALTER TABLE %s RENAME COLUMN %s TO %s",
                        tablePrefix + timestampTable.getQualifiedName(),
                        OracleDbTimestampBoundStore.TIMESTAMP_COLUMN_NAME,
                        "LEGACY_" + OracleDbTimestampBoundStore.TIMESTAMP_COLUMN_NAME));
            }
        } catch (SQLException e) {
            if (e.getMessage().contains(OracleErrorConstants.ORACLE_DUPLICATE_COLUMNS_ERROR)) {
                // This is fine. The table was already upgraded.
                log.info("Tried to invalidate the Oracle timestamp table a second time.");
            }
            throw Throwables.propagate(e);
        }
    }
}
