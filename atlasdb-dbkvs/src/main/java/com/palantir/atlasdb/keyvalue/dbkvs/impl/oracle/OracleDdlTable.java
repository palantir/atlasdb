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
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyle;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCache;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.util.VersionStrings;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OracleDdlTable implements DbDdlTable {
    private static final Logger log = LoggerFactory.getLogger(OracleDdlTable.class);
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

        if (conns.get()
                .selectExistsUnregisteredQuery(
                        "SELECT 1 FROM " + config.metadataTable().getQualifiedName() + " WHERE table_name = ?",
                        tableRef.getQualifiedName())) {
            if (needsOverflow) {
                TableValueStyle existingStyle = valueStyleCache.getTableType(conns, tableRef, config.metadataTable());
                if (existingStyle != TableValueStyle.OVERFLOW) {
                    throwForMissingOverflowTable();
                }
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

    private void throwForMissingOverflowTable() {
        throw new SafeIllegalArgumentException(
                "Unsupported table change from raw to overflow for table {}, likely due to a schema change. "
                        + "Changing the table type requires manual intervention. Please roll back the change or "
                        + "contact support for help with the change.",
                LoggingArgs.tableRef(tableRef));
    }

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
                log.error("Error occurred trying to create table mapping {} -> {}", fullTableName, shortTableName, ex);
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
        try {
            dropTableInternal(
                    oracleTableNameGetter.getPrefixedTableName(tableRef),
                    oracleTableNameGetter.getInternalShortTableName(conns, tableRef));
            dropTableInternal(
                    oracleTableNameGetter.getPrefixedOverflowTableName(tableRef),
                    oracleTableNameGetter.getInternalShortOverflowTableName(conns, tableRef));
        } catch (TableMappingNotFoundException ex) {
            // If table does not exist, do nothing
        }

        clearTableSizeCacheAndDropTableMetadata();
    }

    private void clearTableSizeCacheAndDropTableMetadata() {
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
                    .executeUnregisteredQuery(
                            "TRUNCATE TABLE " + oracleTableNameGetter.getInternalShortTableName(conns, tableRef));
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
                    version,
                    MIN_ORACLE_VERSION);
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
                log.error("Error occurred trying to execute the Oracle query {}.", sql, e);
                throw e;
            }
        }
    }

    private void executeIgnoringError(String sql, String errorToIgnore) {
        try {
            conns.get().executeUnregisteredQuery(sql);
        } catch (PalantirSqlException e) {
            if (!e.getMessage().contains(errorToIgnore)) {
                log.error("Error occurred trying to execute the Oracle query {}.", sql, e);
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
