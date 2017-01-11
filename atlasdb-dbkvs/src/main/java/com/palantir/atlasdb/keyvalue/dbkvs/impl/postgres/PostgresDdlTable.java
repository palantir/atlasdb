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

import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.PostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.TableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyle;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.ExceptionCheck;
import com.palantir.util.VersionStrings;

public class PostgresDdlTable implements DbDdlTable {
    private static final Logger log = LoggerFactory.getLogger(PostgresDdlTable.class);
    private static final String MIN_POSTGRES_VERSION = "9.2";

    private final TableReference tableRef;
    private final ConnectionSupplier conns;
    private final PostgresDdlConfig config;
    private final TableNameGetter tableNameGetter;

    public PostgresDdlTable(TableReference tableRef,
                            ConnectionSupplier conns,
                            PostgresDdlConfig config,
                            TableNameGetter tableNameGetter) {
        this.tableRef = tableRef;
        this.conns = conns;
        this.config = config;
        this.tableNameGetter = tableNameGetter;
    }

    @Override
    public void create(byte[] tableMetadata) {
        if (conns.get().selectExistsUnregisteredQuery(
                "SELECT 1 FROM " + config.metadataTable().getQualifiedName() + " WHERE table_name = ?",
                tableRef.getQualifiedName())) {
            return;
        }

        executeIgnoringError(
                String.format("CREATE TABLE %s ("
                        + "  row_name   BYTEA NOT NULL,"
                        + "  col_name   BYTEA NOT NULL,"
                        + "  ts         INT8 NOT NULL,"
                        + "  val        BYTEA,"
                        + "  CONSTRAINT pk_%s PRIMARY KEY (row_name, col_name, ts) ",
                        prefixedTableName(), prefixedTableName())
                + ")",
                "already exists");

        ignoringError(() -> {
            conns.get().insertOneUnregisteredQuery(
                    String.format(
                            "INSERT INTO %s (table_name, table_size) VALUES (?, ?)",
                            config.metadataTable().getQualifiedName()),
                    tableRef.getQualifiedName(),
                    TableValueStyle.RAW.getId());
        }, ExceptionCheck::isUniqueConstraintViolation);
    }

    @Override
    public void drop() {
        executeIgnoringError("DROP TABLE " + prefixedTableName(), "does not exist");
        conns.get().executeUnregisteredQuery(
                String.format("DELETE FROM %s WHERE table_name = ?", config.metadataTable().getQualifiedName()),
                tableRef.getQualifiedName());
    }

    @Override
    public void truncate() {
        conns.get().executeUnregisteredQuery("TRUNCATE TABLE " + prefixedTableName());
    }

    @Override
    public void checkDatabaseVersion() {
        AgnosticResultSet result = conns.get().selectResultSetUnregisteredQuery("SHOW server_version");
        String version = result.get(0).getString("server_version");
        if (!version.matches("^[\\.0-9]+$") || VersionStrings.compareVersions(version, MIN_POSTGRES_VERSION) < 0) {
            log.error("Your key value service currently uses version {} of postgres."
                    + " The minimum supported version is {}."
                    + " If you absolutely need to use an older version of postgres,"
                    + " please contact Palantir support for assistance.", version, MIN_POSTGRES_VERSION);
        }
    }

    @Override
    public void compactInternally() {
        // VACUUM FULL is /really/ what we want here, but it takes out a table lock
        conns.get().executeUnregisteredQuery("VACUUM ANALYZE " + prefixedTableName());
    }

    private String prefixedTableName() {
        return tableNameGetter.generateShortTableName(conns, tableRef);
    }

    private void executeIgnoringError(String sql, String errorToIgnore) {
        ignoringError(() -> conns.get().executeUnregisteredQuery(sql), e -> e.getMessage().contains(errorToIgnore));
    }

    private static void ignoringError(Runnable fn, Predicate<PalantirSqlException> shouldIgnoreException) {
        try {
            fn.run();
        } catch (PalantirSqlException e) {
            if (!shouldIgnoreException.test(e)) {
                log.error("Error occurred trying to run function", e);
                throw e;
            }
        }
    }
}
