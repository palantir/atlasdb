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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.PostgresKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableSize;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.util.VersionStrings;

public class PostgresDdlTable implements DbDdlTable {
    private static final Logger log = LoggerFactory.getLogger(PostgresDdlTable.class);
    private final String tableName;
    private final ConnectionSupplier conns;
    private final PostgresKeyValueServiceConfig config;

    public PostgresDdlTable(String tableName,
                            ConnectionSupplier conns,
                            PostgresKeyValueServiceConfig config) {
        this.tableName = tableName;
        this.conns = conns;
        this.config = config;
    }

    @Override
    public void create(byte[] tableMetadata) {
        if (conns.get().selectExistsUnregisteredQuery(
                "SELECT 1 FROM " + AtlasDbConstants.METADATA_TABLE.getQualifiedName() + " WHERE table_name = ?",
                tableName)) {
            return;
        }
        executeIgnoringError(
                "CREATE TABLE " + prefixedTableName() + " (" +
                "  row_name   BYTEA NOT NULL," +
                "  col_name   BYTEA NOT NULL," +
                "  ts         INT8 NOT NULL," +
                "  val        BYTEA," +
                "  CONSTRAINT pk_" + prefixedTableName() + " PRIMARY KEY (row_name, col_name, ts) " +
                ")",
                "already exists");
        conns.get().insertOneUnregisteredQuery(
                "INSERT INTO " + AtlasDbConstants.METADATA_TABLE.getQualifiedName() + " (table_name, table_size) VALUES (?, ?)",
                tableName,
                TableSize.RAW.getId());
    }

    @Override
    public void drop() {
        executeIgnoringError("DROP TABLE " + prefixedTableName(), "does not exist");
        conns.get().executeUnregisteredQuery(
                "DELETE FROM " + AtlasDbConstants.METADATA_TABLE.getQualifiedName() + " WHERE table_name = ?", tableName);
    }

    @Override
    public void truncate() {
        executeIgnoringError("TRUNCATE TABLE " + prefixedTableName(), "does not exist");
    }

    @Override
    public void checkDatabaseVersion() {
        String MIN_POSTGRES_VERSION = "9.2";
        AgnosticResultSet result = conns.get().selectResultSetUnregisteredQuery("SHOW server_version");
        String version = result.get(0).getString("server_version");
        if (!version.matches("^[\\.0-9]+$") || VersionStrings.compareVersions(version, MIN_POSTGRES_VERSION) < 0) {
            log.error("Your key value service currently uses version " + version +
                    " of postgres. The minimum supported version is " + MIN_POSTGRES_VERSION +
                    ". If you absolutely need to use an older version of postgres, please contact Palantir support for assistance.");
        }
    }

    private void executeIgnoringError(String sql, String errorToIgnore) {
        try {
            conns.get().executeUnregisteredQuery(sql);
        } catch (PalantirSqlException e) {
            if (!e.getMessage().contains(errorToIgnore)) {
                log.error(e.getMessage(), e);
                throw e;
            }
        }
    }

    @Override
    public void compactInternally() {
        // VACUUM FULL is /really/ what we want here, but it takes out a table lock
        conns.get().executeUnregisteredQuery("VACUUM ANALYZE " + prefixedTableName());
    }

    private String prefixedTableName() {
        return config.shared().tablePrefix() + tableName;
    }
}
