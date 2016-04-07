package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.AtlasSystemPropertyManager;
import com.palantir.atlasdb.DeclaredAtlasSystemProperty;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableSize;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.util.VersionStrings;

public class OracleDdlTable implements DbDdlTable {
    private static final Logger log = LoggerFactory.getLogger(OracleDdlTable.class);
    private final String tableName;
    private final ConnectionSupplier conns;
    private final OverflowMigrationState migrationState;
    private final AtlasSystemPropertyManager systemProperties;



    public OracleDdlTable(String tableName,
                                  ConnectionSupplier conns,
                                  OverflowMigrationState migrationState,
                                  AtlasSystemPropertyManager systemProperties) {
        this.tableName = tableName;
        this.conns = conns;
        this.migrationState = migrationState;
        this.systemProperties = systemProperties;
    }

    @Override
    public void create(byte[] tableMetadata) {
        if (conns.get().selectExistsUnregisteredQuery(
                "SELECT 1 FROM pt_metropolis_table_meta WHERE table_name = ?",
                tableName)) {
            return;
        }

        boolean needsOverflow = false;
        TableMetadata metadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(tableMetadata);
        if (metadata != null) {
            needsOverflow = metadata.getColumns().getMaxValueSize() > 2000;
        }

        executeIgnoringError(
                "CREATE TABLE pt_met_" + tableName + " (" +
                "  row_name   RAW(" + Cell.MAX_NAME_LENGTH + ") NOT NULL," +
                "  col_name   RAW(" + Cell.MAX_NAME_LENGTH + ") NOT NULL," +
                "  ts         NUMBER(20) NOT NULL," +
                "  val        RAW(2000), " +
                (needsOverflow ? "  overflow   NUMBER(38), " : "") +
                "  CONSTRAINT pk_pt_met_" + tableName + " PRIMARY KEY (row_name, col_name, ts) " +
                ") organization index compress overflow",
                "ORA-00955");
        if (needsOverflow && migrationState != OverflowMigrationState.UNSTARTED) {
            executeIgnoringError(
                    "CREATE TABLE pt_mo_" + tableName + " (" +
                    "  id  NUMBER(38) NOT NULL, " +
                    "  val BLOB NOT NULL," +
                    "  CONSTRAINT pk_pt_mo_" + tableName + " PRIMARY KEY (id)" +
                    ")",
                    "ORA-00955");
        }
        conns.get().insertOneUnregisteredQuery(
                "INSERT INTO pt_metropolis_table_meta (table_name, table_size) VALUES (?, ?)",
                tableName,
                (needsOverflow ? TableSize.OVERFLOW.getId() : TableSize.RAW.getId()));
    }

    @Override
    public void drop() {
        executeIgnoringError("DROP TABLE pt_met_" + tableName + " PURGE", "ORA-00942");
        executeIgnoringError("DROP TABLE pt_mo_" + tableName + " PURGE", "ORA-00942");
        conns.get().executeUnregisteredQuery(
                "DELETE FROM pt_metropolis_table_meta WHERE table_name = ?", tableName);
    }

    @Override
    public void truncate() {
        executeIgnoringError("TRUNCATE TABLE pt_met_" + tableName, "ORA-00942");
        executeIgnoringError("TRUNCATE TABLE pt_mo_" + tableName, "ORA-00942");
    }

    @Override
    public void checkDatabaseVersion() {
        String MIN_ORACLE_VERSION = "11.2.0.2.3";
        AgnosticResultSet result = conns.get().selectResultSetUnregisteredQuery(
                "SELECT version FROM product_component_version where lower(product) like '%oracle%'");
        String version = result.get(0).getString("version");
        if (VersionStrings.compareVersions(version, MIN_ORACLE_VERSION) < 0) {
            log.error("Your key value service currently uses version "
                    + version
                    + " of oracle. The minimum supported version is "
                    + MIN_ORACLE_VERSION
                    + ". If you absolutely need to use an older version of oracle, please contact Palantir support for assistance.");
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
        if (systemProperties.getCachedSystemPropertyBoolean(DeclaredAtlasSystemProperty.ORACLEDB_ENABLE_EE_FEATURES)) {
            try {
                conns.get().executeUnregisteredQuery("ALTER TABLE pt_met_" + tableName + " MOVE ONLINE");
            } catch (PalantirSqlException e) {
                log.warn("Tried to clean up " + tableName + " bloat after a sweep operation, "
                        + "but underlying Oracle database or configuration does not support this "
                        + "(Enterprise Edition that requires this user to be able to perform DDL operations) feature online. "
                        + "Since this can't be automated in your configuration, good practice would be do to occasional offline manual maintenance "
                        + "of rebuilding IOT tables to compensate for bloat. You can contact Palantir Support if you'd like more information. "
                        + "Underlying error was: " + e.getMessage());
            }
        }
    }
}
