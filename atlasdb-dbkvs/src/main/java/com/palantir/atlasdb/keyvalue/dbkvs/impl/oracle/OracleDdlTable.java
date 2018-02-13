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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
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
import com.palantir.atlasdb.keyvalue.impl.TableMappingNotFoundException;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.util.VersionStrings;

public final class OracleDdlTable implements DbDdlTable {
    private static final Logger log = LoggerFactory.getLogger(OracleDdlTable.class);
    private static final String MIN_ORACLE_VERSION = "11.2.0.2.3";

    private final OracleDdlConfig config;
    private final ConnectionSupplier conns;
    private final TableReference tableRef;
    private final OracleTableNameGetter oracleTableNameGetter;
    private final TableValueStyleCache valueStyleCache;

    private OracleDdlTable(
            OracleDdlConfig config,
            ConnectionSupplier conns,
            TableReference tableRef,
            OracleTableNameGetter oracleTableNameGetter,
            TableValueStyleCache valueStyleCache) {
        this.config = config;
        this.conns = conns;
        this.tableRef = tableRef;
        this.oracleTableNameGetter = oracleTableNameGetter;
        this.valueStyleCache = valueStyleCache;
    }

    public static OracleDdlTable create(
            TableReference tableRef,
            ConnectionSupplier conns,
            OracleDdlConfig config,
            OracleTableNameGetter oracleTableNameGetter,
            TableValueStyleCache valueStyleCache) {
        return new OracleDdlTable(config, conns, tableRef, oracleTableNameGetter, valueStyleCache);
    }

    @Override
    public void create(byte[] tableMetadata) {
        if (conns.get().selectExistsUnregisteredQuery(
                "SELECT 1 FROM " + config.metadataTable().getQualifiedName() + " WHERE table_name = ?",
                tableRef.getQualifiedName())) {
            return;
        }

        boolean needsOverflow = false;
        TableMetadata metadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(tableMetadata);
        if (metadata != null) {
            needsOverflow = metadata.getColumns().getMaxValueSize() > AtlasDbConstants.ORACLE_OVERFLOW_THRESHOLD;
        }

        createTable(needsOverflow);
        if (needsOverflow && config.overflowMigrationState() != OverflowMigrationState.UNSTARTED) {
            createOverflowTable();
        }

        conns.get().insertOneUnregisteredQuery(
                "INSERT INTO " + config.metadataTable().getQualifiedName() + " (table_name, table_size) VALUES (?, ?)",
                tableRef.getQualifiedName(),
                needsOverflow ? TableValueStyle.OVERFLOW.getId() : TableValueStyle.RAW.getId());
    }

    private void createTable(boolean needsOverflow) {
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
            conns.get().insertOneUnregisteredQuery(
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
            dropTableInternal(oracleTableNameGetter.getPrefixedTableName(tableRef),
                    oracleTableNameGetter.getInternalShortTableName(conns, tableRef));
            dropTableInternal(oracleTableNameGetter.getPrefixedOverflowTableName(tableRef),
                    oracleTableNameGetter.getInternalShortOverflowTableName(conns, tableRef));
        } catch (TableMappingNotFoundException ex) {
            // If table does not exist, do nothing
        }

        clearTableSizeCacheAndDropTableMetadata();
    }

    private void clearTableSizeCacheAndDropTableMetadata() {
        valueStyleCache.clearCacheForTable(tableRef);
        conns.get().executeUnregisteredQuery(
                "DELETE FROM " + config.metadataTable().getQualifiedName() + " WHERE table_name = ?",
                tableRef.getQualifiedName());
    }

    private void dropTableInternal(String fullTableName, String shortTableName) {
        executeIgnoringError("DROP TABLE " + shortTableName + " PURGE", OracleErrorConstants.ORACLE_NOT_EXISTS_ERROR);
        if (config.useTableMapping()) {
            conns.get().executeUnregisteredQuery(
                    "DELETE FROM " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE + " WHERE table_name = ?",
                    fullTableName);
            oracleTableNameGetter.clearCacheForTable(fullTableName);
        }
    }

    @Override
    public void truncate() {
        try {
            conns.get().executeUnregisteredQuery(
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
                conns.get().executeUnregisteredQuery(
                        "TRUNCATE TABLE " + oracleTableNameGetter.getInternalShortOverflowTableName(conns, tableRef));
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
        AgnosticResultSet result = conns.get().selectResultSetUnregisteredQuery(
                "SELECT version FROM product_component_version where lower(product) like '%oracle%'");
        String version = result.get(0).getString("version");
        if (VersionStrings.compareVersions(version, MIN_ORACLE_VERSION) < 0) {
            log.error("Your key value service currently uses version {}"
                    + " of oracle. The minimum supported version is {}"
                    + ". If you absolutely need to use an older version of oracle,"
                    + " please contact Palantir support for assistance.", version, MIN_ORACLE_VERSION);
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
    public void compactInternally(boolean inSafeHours) {
        final String compactionFailureTemplate = "Tried to clean up {} bloat after a sweep operation,"
                + " but underlying Oracle database or configuration does not support this {} feature online. "
                + " Since this can't be automated in your configuration,"
                + " good practice would be do to occasional offline manual maintenance of rebuilding"
                + " IOT tables to compensate for bloat. You can contact Palantir Support if you'd"
                + " like more information. Underlying error was: {}";

        if (config.enableOracleEnterpriseFeatures()) {
            try {
                conns.get().executeUnregisteredQuery(
                        "ALTER TABLE " + oracleTableNameGetter.getInternalShortTableName(conns, tableRef)
                                + " MOVE ONLINE");
            } catch (PalantirSqlException e) {
                log.error(compactionFailureTemplate,
                        tableRef,
                        "(Enterprise Edition that requires this user to be able to perform DDL operations)",
                        e.getMessage());
            } catch (TableMappingNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (config.enableShrinkOnOracleStandardEdition()) {
            Stopwatch timer = Stopwatch.createStarted();
            try {
                if (inSafeHours) {
                    Stopwatch shrinkTimer = Stopwatch.createStarted();
                    conns.get().executeUnregisteredQuery(
                            "ALTER TABLE " + oracleTableNameGetter.getInternalShortTableName(conns, tableRef)
                                    + " SHRINK SPACE");
                    log.info("Call to SHRINK SPACE on table {} took {} ms."
                                    + " This implies that locks on the entire table were held for this period.",
                            tableRef, shrinkTimer.elapsed(TimeUnit.MILLISECONDS));
                } else {
                    Stopwatch shrinkAndCompactTimer = Stopwatch.createStarted();
                    conns.get().executeUnregisteredQuery(
                            "ALTER TABLE " + oracleTableNameGetter.getInternalShortTableName(conns, tableRef)
                                    + " SHRINK SPACE COMPACT");
                    log.info("Call to SHRINK SPACE COMPACT on table {} took {} ms.",
                            tableRef, shrinkAndCompactTimer.elapsed(TimeUnit.MILLISECONDS));
                }
            } catch (PalantirSqlException e) {
                log.error(compactionFailureTemplate,
                        tableRef,
                        "(If you are running against Enterprise Edition,"
                                + " you can set enableOracleEnterpriseFeatures to true in the configuration.)",
                        e.getMessage());
            } catch (TableMappingNotFoundException e) {
                throw new RuntimeException(e);
            } finally {
                log.info("Call to KVS.compactInternally on table {} took {} ms.",
                        tableRef, timer.elapsed(TimeUnit.MILLISECONDS));
            }
        }
    }
}
