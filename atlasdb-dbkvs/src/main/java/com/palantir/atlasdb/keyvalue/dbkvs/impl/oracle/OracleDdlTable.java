/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import com.google.common.base.Stopwatch;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleErrorConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.CaseSensitivity;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyle;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCache;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.base.RunnableCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.util.VersionStrings;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

public final class OracleDdlTable implements DbDdlTable {
    private static final SafeLogger log = SafeLoggerFactory.get(OracleDdlTable.class);
    private static final String MIN_ORACLE_VERSION = "11.2.0.2.3";

    private final OracleDdlConfig config;
    private final ConnectionSupplier conns;
    private final TableReference tableRef;
    private final OracleTableNameGetter oracleTableNameGetter;
    private final TableValueStyleCache valueStyleCache;
    private final ExecutorService compactionTimeoutExecutor;

    private OracleDdlTable(
            OracleDdlConfig config,
            ConnectionSupplier conns,
            TableReference tableRef,
            OracleTableNameGetter oracleTableNameGetter,
            TableValueStyleCache valueStyleCache,
            ExecutorService compactionTimeoutExecutor) {
        this.config = config;
        this.conns = conns;
        this.tableRef = tableRef;
        this.oracleTableNameGetter = oracleTableNameGetter;
        this.valueStyleCache = valueStyleCache;
        this.compactionTimeoutExecutor = compactionTimeoutExecutor;
    }

    public static OracleDdlTable create(
            TableReference tableRef,
            ConnectionSupplier conns,
            OracleDdlConfig config,
            OracleTableNameGetter oracleTableNameGetter,
            TableValueStyleCache valueStyleCache,
            ExecutorService compactionTimeoutExecutor) {
        return new OracleDdlTable(
                config, conns, tableRef, oracleTableNameGetter, valueStyleCache, compactionTimeoutExecutor);
    }

    @Override
    public void create(byte[] tableMetadata) {
        boolean needsOverflow = needsOverflow(tableMetadata);

        if (tableExists()) {
            if (needsOverflow) {
                TableValueStyle existingStyle = valueStyleCache.getTableType(conns, tableRef, config.metadataTable());
                if (existingStyle != TableValueStyle.OVERFLOW) {
                    throwForMissingOverflowTable();
                }
                maybeAlterTableToHaveOverflowColumn();
            }
            return;
        }

        String shortTableName = createTable(needsOverflow);

        if (needsOverflow && !overflowColumnExists(shortTableName)) {
            throwForMissingOverflowTable();
        }

        if (needsOverflow && config.overflowMigrationState() != OverflowMigrationState.UNSTARTED) {
            createOverflowTable();
        }

        insertIgnoringConstraintViolation(
                needsOverflow,
                "INSERT INTO " + config.metadataTable().getQualifiedName() + " (table_name, table_size) VALUES (?, ?)");
    }

    private void maybeAlterTableToHaveOverflowColumn() {
        if (config.alterTablesOrMetadataToMatch().contains(tableRef)) {
            try {
                String shortTableName = oracleTableNameGetter.getInternalShortTableName(conns, tableRef);
                if (overflowTableHasMigrated() && overflowTableExists() && !overflowColumnExists(shortTableName)) {
                    alterTableToHaveOverflowColumn(shortTableName);
                }
            } catch (TableMappingNotFoundException e) {
                Throwables.rewrapAndThrowUncheckedException(
                        "Unable to alter table to have overflow column due to a table mapping error.", e);
            }
        }
    }

    private void alterTableToHaveOverflowColumn(String shortTableName) {
        log.info(
                "Altering table to have overflow column to match metadata.",
                LoggingArgs.tableRef(tableRef),
                UnsafeArg.of("shortTableName", shortTableName));
        executeIgnoringError(
                "ALTER TABLE " + shortTableName + " ADD (overflow NUMBER(38))",
                OracleErrorConstants.ORACLE_COLUMN_ALREADY_EXISTS);
    }

    private boolean overflowTableHasMigrated() {
        return config.overflowMigrationState() == OverflowMigrationState.FINISHED;
    }

    private boolean needsOverflow(byte[] tableMetadata) {
        TableMetadata metadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(tableMetadata);
        return (metadata != null)
                && (metadata.getColumns().getMaxValueSize() > AtlasDbConstants.ORACLE_OVERFLOW_THRESHOLD);
    }

    private boolean overflowColumnExists(String shortTableName) {
        // All table names in user_tab_cols are upper case
        return conns.get()
                .selectExistsUnregisteredQuery(
                        "SELECT 1 FROM user_tab_cols WHERE TABLE_NAME = ? AND COLUMN_NAME = 'OVERFLOW'",
                        shortTableName.toUpperCase());
    }

    private boolean tableExists() {
        return conns.get()
                .selectExistsUnregisteredQuery(
                        "SELECT 1 FROM " + config.metadataTable().getQualifiedName() + " WHERE table_name = ?",
                        tableRef.getQualifiedName());
    }

    private boolean overflowTableExists() throws TableMappingNotFoundException {
        String shortTableName = oracleTableNameGetter.getInternalShortOverflowTableName(conns, tableRef);
        return conns.get()
                .selectExistsUnregisteredQuery(
                        "SELECT 1 FROM user_tables WHERE TABLE_NAME = ?", shortTableName.toUpperCase());
    }

    private void throwForMissingOverflowTable() {
        throw new SafeIllegalArgumentException(
                "Unsupported table change from raw to overflow, likely due to a schema change. "
                        + "Changing the table type requires manual intervention. Please roll back the change or "
                        + "contact support for help with the change.",
                LoggingArgs.tableRef(tableRef));
    }

    /**
     * Creates the table for the given reference. If the schema is modified here, please also update it in {@link #alterTableToHaveOverflowColumn}.
     */
    private String createTable(boolean needsOverflow) {
        String shortTableName = oracleTableNameGetter.generateShortTableName(conns, tableRef);
        executeIgnoringError(
                "CREATE TABLE " + shortTableName + " ("
                        + "  row_name   RAW(" + Cell.MAX_NAME_LENGTH + ") NOT NULL,"
                        + "  col_name   RAW(" + Cell.MAX_NAME_LENGTH + ") NOT NULL,"
                        + "  ts         NUMBER(20) NOT NULL,"
                        + "  val        RAW(" + AtlasDbConstants.ORACLE_OVERFLOW_THRESHOLD + "), "
                        + (needsOverflow ? "overflow   NUMBER(38), " : "")
                        + "  CONSTRAINT " + PrimaryKeyConstraintNames.get(shortTableName)
                        + " PRIMARY KEY (row_name, col_name, ts) "
                        + ") organization index compress overflow",
                OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR);
        putTableNameMapping(oracleTableNameGetter.getPrefixedTableName(tableRef), shortTableName);
        return shortTableName;
    }

    private void createOverflowTable() {
        final String shortOverflowTableName = oracleTableNameGetter.generateShortOverflowTableName(conns, tableRef);
        executeIgnoringError(
                "CREATE TABLE " + shortOverflowTableName + " ("
                        + "  id  NUMBER(38) NOT NULL, "
                        + "  val BLOB NOT NULL,"
                        + "  CONSTRAINT " + PrimaryKeyConstraintNames.get(shortOverflowTableName) + " PRIMARY KEY (id)"
                        + ")",
                OracleErrorConstants.ORACLE_ALREADY_EXISTS_ERROR);
        putTableNameMapping(oracleTableNameGetter.getPrefixedOverflowTableName(tableRef), shortOverflowTableName);
    }

    private void putTableNameMapping(String fullTableName, String shortTableName) {
        if (config.useTableMapping()) {
            insertTableMappingIgnoringPrimaryKeyViolation(fullTableName, shortTableName);
        }
    }

    private void insertTableMappingIgnoringPrimaryKeyViolation(String fullTableName, String shortTableName) {
        try {
            conns.get()
                    .insertOneUnregisteredQuery(
                            "INSERT INTO " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE
                                    + " (table_name, short_table_name) VALUES (?, ?)",
                            fullTableName,
                            shortTableName);
        } catch (PalantirSqlException ex) {
            if (!isPrimaryKeyViolation(ex)) {
                log.error(
                        "Error occurred trying to create table mapping {} -> {}",
                        UnsafeArg.of("fullTableName", fullTableName),
                        UnsafeArg.of("shortTableName", shortTableName),
                        ex);
                dropTableInternal(fullTableName, shortTableName);
                throw ex;
            }
        }
    }

    private boolean isPrimaryKeyViolation(PalantirSqlException ex) {
        return StringUtils.containsIgnoreCase(ex.getMessage(), AtlasDbConstants.ORACLE_NAME_MAPPING_PK_CONSTRAINT);
    }

    @Override
    public void drop() {
        drop(CaseSensitivity.CASE_SENSITIVE);
    }

    @Override
    public void drop(CaseSensitivity referenceCaseSensitivity) {
        executeIgnoringTableMappingNotFound(() -> dropTableInternal(
                oracleTableNameGetter.getPrefixedTableName(tableRef),
                oracleTableNameGetter.getInternalShortTableName(conns, tableRef)));

        // It's possible to end up in a situation where the base table was deleted (above), but we failed to delete
        // the corresponding overflow table due to some transient failure.
        // To ensure we fully clean up, we delete each table in a separate block so the overflow table is deleted even
        // if the base table was deleted in a previous call.
        executeIgnoringTableMappingNotFound(() -> dropTableInternal(
                oracleTableNameGetter.getPrefixedOverflowTableName(tableRef),
                oracleTableNameGetter.getInternalShortOverflowTableName(conns, tableRef)));

        switch (referenceCaseSensitivity) {
            case CASE_SENSITIVE:
                clearTableSizeCacheAndDropTableMetadataCaseSensitive();
                break;
            case CASE_INSENSITIVE:
                clearTableSizeCacheAndDropTableMetadataCaseInsensitive();
                break;
            default:
                throw new SafeIllegalStateException(
                        "Unknown Case Sensitivity value", SafeArg.of("caseSensitivity", referenceCaseSensitivity));
        }
    }

    private static void executeIgnoringTableMappingNotFound(
            RunnableCheckedException<TableMappingNotFoundException> runnable) {
        try {
            runnable.run();
        } catch (TableMappingNotFoundException ex) {
            // Do nothing
        }
    }

    private void clearTableSizeCacheAndDropTableMetadataCaseInsensitive() {
        valueStyleCache.clearCacheForTable(tableRef);
        long numberOfMatchingTableReferences = conns.get()
                .selectCount(
                        config.metadataTable().getQualifiedName(),
                        "LOWER(table_name) = LOWER(?)",
                        tableRef.getQualifiedName());

        Preconditions.checkState(
                numberOfMatchingTableReferences <= 1,
                "There are multiple tables that have the same case insensitive table reference. Throwing to avoid"
                        + " accidentally deleting the wrong table reference. Please contact support to delete the"
                        + " metadata, which will involve deleting the row from the DB manually.",
                SafeArg.of("numberOfMatchingTableReferences", numberOfMatchingTableReferences),
                UnsafeArg.of("tableReference", tableRef));

        // There is a race condition here - if a new table was created after the above count, and has a table reference
        // with a case-insensitive match of this table reference, then that table reference will be deleted.
        // We explicitly are not supporting that case, and acknowledge the need for DB surgery if it ever happens.
        conns.get()
                .executeUnregisteredQuery(
                        "DELETE FROM " + config.metadataTable().getQualifiedName() + " WHERE LOWER(table_name) ="
                                + " LOWER(?)",
                        tableRef.getQualifiedName());
    }

    private void clearTableSizeCacheAndDropTableMetadataCaseSensitive() {
        valueStyleCache.clearCacheForTable(tableRef);
        conns.get()
                .executeUnregisteredQuery(
                        "DELETE FROM " + config.metadataTable().getQualifiedName() + " WHERE table_name = ?",
                        tableRef.getQualifiedName());
    }

    private void dropTableInternal(String fullTableName, String shortTableName) {
        executeIgnoringError("DROP TABLE " + shortTableName + " PURGE", OracleErrorConstants.ORACLE_NOT_EXISTS_ERROR);
        if (config.useTableMapping()) {
            conns.get()
                    .executeUnregisteredQuery(
                            "DELETE FROM " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE + " WHERE table_name = ?",
                            fullTableName);
            oracleTableNameGetter.clearCacheForTable(fullTableName);
        }
    }

    @Override
    public void truncate() {
        try {
            conns.get()
                    .executeUnregisteredQuery("TRUNCATE TABLE "
                            + oracleTableNameGetter.getInternalShortTableName(conns, tableRef) + " DROP STORAGE");
        } catch (TableMappingNotFoundException | RuntimeException e) {
            throw new IllegalStateException(
                    String.format(
                            "Truncate called on a table (%s) that did not exist",
                            oracleTableNameGetter.getPrefixedTableName(tableRef)),
                    e);
        }
        truncateOverflowTableIfItExists();
    }

    private void truncateOverflowTableIfItExists() {
        TableValueStyle tableValueStyle = valueStyleCache.getTableType(conns, tableRef, config.metadataTable());
        if (tableValueStyle.equals(TableValueStyle.OVERFLOW)
                && config.overflowMigrationState() != OverflowMigrationState.UNSTARTED) {
            try {
                conns.get()
                        .executeUnregisteredQuery("TRUNCATE TABLE "
                                + oracleTableNameGetter.getInternalShortOverflowTableName(conns, tableRef));
            } catch (TableMappingNotFoundException | RuntimeException e) {
                throw new IllegalStateException(
                        String.format(
                                "Truncate called on a table (%s) that was supposed to have an overflow table (%s),"
                                        + " but that overflow table appears to not exist",
                                oracleTableNameGetter.getPrefixedTableName(tableRef),
                                oracleTableNameGetter.getPrefixedOverflowTableName(tableRef)),
                        e);
            }
        }
    }

    @Override
    public void checkDatabaseVersion() {
        AgnosticResultSet result = conns.get()
                .selectResultSetUnregisteredQuery(
                        "SELECT version FROM product_component_version where lower(product) like '%oracle%'");
        String version = result.get(0).getString("version");
        if (VersionStrings.compareVersions(version, MIN_ORACLE_VERSION) < 0) {
            log.error(
                    "Your key value service currently uses version {}"
                            + " of oracle. The minimum supported version is {}"
                            + ". If you absolutely need to use an older version of oracle,"
                            + " please contact Palantir support for assistance.",
                    SafeArg.of("version", version),
                    SafeArg.of("minVersion", MIN_ORACLE_VERSION));
        }
    }

    private void insertIgnoringConstraintViolation(boolean needsOverflow, String sql) {
        try {
            conns.get()
                    .insertOneUnregisteredQuery(
                            sql,
                            tableRef.getQualifiedName(),
                            needsOverflow ? TableValueStyle.OVERFLOW.getId() : TableValueStyle.RAW.getId());
        } catch (PalantirSqlException e) {
            if (!e.getMessage().contains(OracleErrorConstants.ORACLE_CONSTRAINT_VIOLATION_ERROR)) {
                log.error("Error occurred trying to execute the Oracle query {}.", UnsafeArg.of("sql", sql), e);
                throw e;
            }
        }
    }

    private void executeIgnoringError(String sql, String errorToIgnore) {
        try {
            conns.get().executeUnregisteredQuery(sql);
        } catch (PalantirSqlException e) {
            if (!e.getMessage().contains(errorToIgnore)) {
                log.error("Error occurred trying to execute the Oracle query {}.", UnsafeArg.of("sql", sql), e);
                throw e;
            }
        }
    }

    @Override
    public void compactInternally(boolean inMaintenanceHours) {
        final String compactionFailureTemplate = "Tried to clean up {} bloat,"
                + " but underlying Oracle database or configuration does not support this {} feature online. "
                + " Good practice could be do to occasional offline manual maintenance of rebuilding"
                + " IOT tables to compensate for bloat. You can contact Palantir Support if you'd"
                + " like more information. Underlying error was: {}";

        if (config.enableOracleEnterpriseFeatures()) {
            try {
                getCompactionConnection()
                        .executeUnregisteredQuery("ALTER TABLE "
                                + oracleTableNameGetter.getInternalShortTableName(conns, tableRef) + " MOVE ONLINE");
            } catch (PalantirSqlException e) {
                log.error(
                        compactionFailureTemplate,
                        LoggingArgs.tableRef(tableRef),
                        SafeArg.of(
                                "auxiliary message",
                                "(Enterprise Edition that requires this user to be able to perform DDL operations)."
                                        + " Please change the `enableOracleEnterpriseFeatures` config to false."),
                        UnsafeArg.of("exception message", e.getMessage()),
                        e);
            } catch (TableMappingNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (config.enableShrinkOnOracleStandardEdition()) {
            Stopwatch timer = Stopwatch.createStarted();
            try {
                if (inMaintenanceHours) {
                    Stopwatch shrinkTimer = Stopwatch.createStarted();
                    getCompactionConnection()
                            .executeUnregisteredQuery(
                                    "ALTER TABLE " + oracleTableNameGetter.getInternalShortTableName(conns, tableRef)
                                            + " SHRINK SPACE");
                    log.info(
                            "Call to SHRINK SPACE on table {} took {} ms."
                                    + " This implies that locks on the entire table were held for this period.",
                            LoggingArgs.tableRef(tableRef),
                            SafeArg.of("elapsed time", shrinkTimer.elapsed(TimeUnit.MILLISECONDS)));
                } else {
                    Stopwatch shrinkAndCompactTimer = Stopwatch.createStarted();
                    getCompactionConnection()
                            .executeUnregisteredQuery(
                                    "ALTER TABLE " + oracleTableNameGetter.getInternalShortTableName(conns, tableRef)
                                            + " SHRINK SPACE COMPACT");
                    log.info(
                            "Call to SHRINK SPACE COMPACT on table {} took {} ms.",
                            LoggingArgs.tableRef(tableRef),
                            SafeArg.of("elapsed time", shrinkAndCompactTimer.elapsed(TimeUnit.MILLISECONDS)));
                }
            } catch (PalantirSqlException e) {
                log.error(
                        compactionFailureTemplate,
                        LoggingArgs.tableRef(tableRef),
                        SafeArg.of(
                                "auxiliary message",
                                "(If you are running against Enterprise Edition,"
                                        + " you can set enableOracleEnterpriseFeatures to true in the configuration.)"),
                        UnsafeArg.of("exception message", e.getMessage()),
                        e);
            } catch (TableMappingNotFoundException e) {
                throw new RuntimeException(e);
            } finally {
                log.info(
                        "Call to KVS.compactInternally on table {} took {} ms.",
                        LoggingArgs.tableRef(tableRef),
                        SafeArg.of("elapsed time", timer.elapsed(TimeUnit.MILLISECONDS)));
            }
        }
    }

    private SqlConnection getCompactionConnection() {
        SqlConnection sqlConnection = conns.get();

        try {
            int originalNetworkTimeout = sqlConnection.getUnderlyingConnection().getNetworkTimeout();
            int newNetworkMillis = Ints.saturatedCast(config.compactionConnectionTimeout());

            log.info(
                    "Increased sql socket read timeout from {} to {}",
                    SafeArg.of("originalNetworkTimeoutMillis", originalNetworkTimeout),
                    SafeArg.of("newNetworkTimeoutMillis", newNetworkMillis));
            sqlConnection.getUnderlyingConnection().setNetworkTimeout(compactionTimeoutExecutor, newNetworkMillis);
        } catch (SQLException e) {
            log.warn("Failed to increase socket read timeout for the connection. Encountered an exception:", e);
        }

        return sqlConnection;
    }
}
