/*
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

import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
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
import com.palantir.util.VersionStrings;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class PostgresDdlTable implements DbDdlTable {
    private static final int POSTGRES_NAME_LENGTH_LIMIT = 63;
    public static final int ATLASDB_POSTGRES_TABLE_NAME_LIMIT = POSTGRES_NAME_LENGTH_LIMIT
            - AtlasDbConstants.PRIMARY_KEY_CONSTRAINT_PREFIX.length();
    private static final Logger log = LoggerFactory.getLogger(PostgresDdlTable.class);
    private static final String MIN_POSTGRES_VERSION = "9.2";

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
    @SuppressFBWarnings("SLF4J_FORMAT_SHOULD_BE_CONST")
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
                String msg = String.format("The table name is longer than the postgres limit of %d characters. "
                                + "Attempted to truncate the name but the truncated table name or truncated primary "
                                + "key constraint name already exists. Please ensure all your table names have unique "
                                + "first %d characters.",
                        ATLASDB_POSTGRES_TABLE_NAME_LIMIT,
                        ATLASDB_POSTGRES_TABLE_NAME_LIMIT);

                String logMessage = "Failed to create the table {}. " + msg;
                log.error(logMessage, prefixedTableName, e);

                throw new RuntimeException("Failed to create the table" + prefixedTableName + "." + msg, e);
            }
        }

        ignoringError(() -> {
            conns.get().insertOneUnregisteredQuery(
                    String.format(
                            "INSERT INTO %s (table_name, table_size) VALUES (?, ?)",
                            config.metadataTable().getQualifiedName()),
                    tableName.getQualifiedName(),
                    TableValueStyle.RAW.getId());
        }, ExceptionCheck::isUniqueConstraintViolation);
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
        if (!version.matches("^[\\.0-9]+$") || VersionStrings.compareVersions(version, MIN_POSTGRES_VERSION) < 0) {
            log.error("Your key value service currently uses version {} of postgres."
                    + " The minimum supported version is {}."
                    + " If you absolutely need to use an older version of postgres,"
                    + " please contact Palantir support for assistance.", version, MIN_POSTGRES_VERSION);
        } else if (VersionStrings.compareVersions(version, "9.5") >= 0
                && VersionStrings.compareVersions(version, "9.5.2") < 0) {
            throw new RuntimeException(
                      "You are running Postgres " + version + ". Versions 9.5.0 and 9.5.1 contain a known bug "
                    + "that causes incorrect results to be returned for certain queries. "
                    + "Please update your Postgres distribution.");
        }
    }

    @Override
    public void compactInternally(boolean unused) {
        if (compactionSemaphore.tryAcquire()) {
            try {
                if (shouldRunCompaction()) {
                    runCompactOnTable();
                }
            } finally {
                compactionSemaphore.release();
            }
        }
    }

    @VisibleForTesting
    boolean shouldRunCompaction() {
        long compactIntervalMillis = config.compactInterval().toMilliseconds();

        if (compactIntervalMillis <= 0) {
            return true;
        }

        long millisSinceLastCompact = getMillisSinceLastCompact();
        boolean noOverlyRecentCompaction = millisSinceLastCompact >= compactIntervalMillis;
        if (noOverlyRecentCompaction) {
            log.info("Compacting table {} because there wasn't an overly recent compaction (was {} ms ago).",
                    prefixedTableName(),
                    millisSinceLastCompact);
        } else {
            log.info("Not compacting table {} because it was recently compacted (was {} ms ago).",
                    prefixedTableName(),
                    millisSinceLastCompact);
        }
        return noOverlyRecentCompaction;
    }

    /**
     * Returns the number of milliseconds since the last compaction, or Long.MAX_VALUE if
     * compaction has never run.
     */
    private long getMillisSinceLastCompact() {
        AgnosticResultSet rs = conns.get().selectResultSetUnregisteredQuery(
                "SELECT FLOOR(EXTRACT(EPOCH FROM GREATEST( "
                        + "  last_vacuum, last_autovacuum, last_analyze, last_autoanalyze"
                        + "))*1000) AS last, "
                        + "FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)*1000) AS current "
                        + "FROM pg_stat_user_tables WHERE relname = ?",
                prefixedTableName());

        AgnosticResultRow row = Iterables.getOnlyElement(rs.rows());

        Optional<Long> lastVacuumTime = getLastVacuumTime(row);
        if (vacuumWasNeverRun(lastVacuumTime)) {
            return Long.MAX_VALUE;
        }
        long currentVacuumTime = row.getLong("current");
        long timeSinceLastCompact = currentVacuumTime - lastVacuumTime.orElseThrow(
                () -> new IllegalStateException("Should not calculate time since last compact if it doesn't exist!"));
        log.info("For table {}, we compacted {} ms ago", prefixedTableName(), timeSinceLastCompact);
        return timeSinceLastCompact;
    }

    private boolean vacuumWasNeverRun(Optional<Long> lastVacuumTime) {
        return !lastVacuumTime.isPresent();
    }

    private Optional<Long> getLastVacuumTime(AgnosticResultRow resultRow) {
        return Optional.ofNullable(resultRow.getLongObject("last"));
    }

    private void runCompactOnTable() {
        // VACUUM FULL is /really/ what we want here, but it takes out a table lock
        conns.get().executeUnregisteredQuery("VACUUM ANALYZE " + prefixedTableName());
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
