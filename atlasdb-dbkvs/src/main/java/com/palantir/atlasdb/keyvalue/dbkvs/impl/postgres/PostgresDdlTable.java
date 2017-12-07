/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.sql.Timestamp;
import java.util.concurrent.Semaphore;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.PostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyle;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.PrimaryKeyConstraintNames;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.ExceptionCheck;

public class PostgresDdlTable implements DbDdlTable {
    private static final Logger log = LoggerFactory.getLogger(PostgresDdlTable.class);
    private static final int POSTGRES_NAME_LENGTH_LIMIT = 63;
    public static final int ATLASDB_POSTGRES_TABLE_NAME_LIMIT = POSTGRES_NAME_LENGTH_LIMIT
            - AtlasDbConstants.PRIMARY_KEY_CONSTRAINT_PREFIX.length();
    private static final String FAILED_TO_CREATE_TABLE_MESSAGE = "Failed to create table {}."
            + " The table name is longer than the postgres limit of {} characters."
            + " Attempted to truncate the name but the truncated table name or truncated primary"
            + " key constraint name already exists. Please ensure all your table names have unique"
            + " first {} characters.";

    private final TableReference tableName;
    private final ConnectionSupplier conns;
    private final PostgresDdlConfig config;
    private final Semaphore compactionSemaphore = new Semaphore(1);

    public PostgresDdlTable(TableReference tableName,
            ConnectionSupplier conns,
            PostgresDdlConfig config) {
        this.tableName = tableName;
        this.conns = conns;
        this.config = config;
    }

    @Override
    public void create(byte[] tableMetadata) {
        if (conns.get().selectExistsUnregisteredQuery(
                "SELECT 1 FROM " + config.metadataTable().getQualifiedName() + " WHERE table_name = ?",
                tableName.getQualifiedName())) {
            return;
        }

        String prefixedTableName = prefixedTableName();
        try {
            conns.get().executeUnregisteredQuery(
                    String.format("CREATE TABLE %s ("
                                    + "  row_name   BYTEA NOT NULL,"
                                    + "  col_name   BYTEA NOT NULL,"
                                    + "  ts         INT8 NOT NULL,"
                                    + "  val        BYTEA,"
                                    + "  CONSTRAINT %s PRIMARY KEY (row_name, col_name, ts) ",
                            prefixedTableName, PrimaryKeyConstraintNames.get(prefixedTableName)) + ")");
        } catch (PalantirSqlException e) {
            if (!e.getMessage().contains("already exists")) {
                log.error("Error occurred trying to create the table", e);
                throw e;
            } else if (prefixedTableName.length() > ATLASDB_POSTGRES_TABLE_NAME_LIMIT) {
                log.error(FAILED_TO_CREATE_TABLE_MESSAGE, prefixedTableName, ATLASDB_POSTGRES_TABLE_NAME_LIMIT,
                        ATLASDB_POSTGRES_TABLE_NAME_LIMIT, e);
                String exceptionMsg = MessageFormatter.arrayFormat(FAILED_TO_CREATE_TABLE_MESSAGE, new Object[] {
                        prefixedTableName, ATLASDB_POSTGRES_TABLE_NAME_LIMIT, ATLASDB_POSTGRES_TABLE_NAME_LIMIT})
                        .getMessage();
                throw new RuntimeException(exceptionMsg, e);
            }
        }

        ignoringError(() -> conns.get().insertOneUnregisteredQuery(
                String.format(
                        "INSERT INTO %s (table_name, table_size) VALUES (?, ?)",
                        config.metadataTable().getQualifiedName()),
                tableName.getQualifiedName(),
                TableValueStyle.RAW.getId()), ExceptionCheck::isUniqueConstraintViolation);
    }

    @Override
    public void drop() {
        executeIgnoringError("DROP TABLE " + prefixedTableName(), "does not exist");
        conns.get().executeUnregisteredQuery(
                String.format("DELETE FROM %s WHERE table_name = ?", config.metadataTable().getQualifiedName()),
                tableName.getQualifiedName());
    }

    @Override
    public void truncate() {
        conns.get().executeUnregisteredQuery("TRUNCATE TABLE " + prefixedTableName());
    }

    @Override
    public void checkDatabaseVersion() {
        AgnosticResultSet result = conns.get().selectResultSetUnregisteredQuery("SHOW server_version");
        String version = result.get(0).getString("server_version");
        PostgresVersionCheck.checkDatabaseVersion(version, log);
    }

    @Override
    public void compactInternally() {
        try {
            if (compactionSemaphore.tryAcquire()) {
                if (config.compactIntervalMillis() == 0) {
                    runCompactOnTable();
                } else if (config.compactIntervalMillis() > 0
                        && checkIfTableHasNotBeenCompactedForCompactIntervalMillis()) {
                    runCompactOnTable();
                }
            }
        } finally {
            compactionSemaphore.release();
        }
    }

    @VisibleForTesting
    boolean checkIfTableHasNotBeenCompactedForCompactIntervalMillis() {
        return getCurrentDatabaseTimestamp().getTime() - getLastVacuumTimestamp().getTime()
                > config.compactIntervalMillis();
    }

    private Timestamp getLastVacuumTimestamp() {
        AgnosticResultSet vaccumTimestamps = conns.get().selectResultSetUnregisteredQuery(
                "SELECT relname, "
                        + "last_vacuum, "
                        + "last_autovacuum, "
                        + "last_analyze, "
                        + "last_autoanalyze "
                        + "from pg_stat_user_tables where relname = ?",
                prefixedTableName());

        AgnosticResultRow vacuumTimestampsForTable = Iterables.getOnlyElement(vaccumTimestamps.rows());

        return ImmutableList.of("last_vacuum", "last_autovacuum", "last_analyze", "last_autoanalyze")
                .stream()
                .map(str -> getTimestamp(vacuumTimestampsForTable.getObject(str)))
                .max(Timestamp::compareTo)
                .orElse(new Timestamp(0));
    }

    private Timestamp getCurrentDatabaseTimestamp() {
        AgnosticResultSet currentTimestamp = conns.get().selectResultSetUnregisteredQuery("SELECT CURRENT_TIMESTAMP");
        return getTimestamp(Iterables.getOnlyElement(currentTimestamp.rows()).getObject("now"));
    }

    private void runCompactOnTable() {
        // VACUUM FULL is /really/ what we want here, but it takes out a table lock
        conns.get().executeUnregisteredQuery("VACUUM ANALYZE " + prefixedTableName());
    }

    private Timestamp getTimestamp(Object timestampFromResultSet) {
        if (timestampFromResultSet == null) {
            return new Timestamp(0);
        } else {
            Preconditions.checkState(timestampFromResultSet instanceof Timestamp,
                    "Expected the column type to be timestamp");
            return (Timestamp) timestampFromResultSet;
        }
    }

    private String prefixedTableName() {
        return config.tablePrefix() + DbKvs.internalTableName(tableName);
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
