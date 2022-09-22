/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SimpleTimedSqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.ConnectionSupplier;
import com.palantir.nexus.db.sql.SqlConnection;
import com.palantir.util.file.TempFileUtils;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value;

final class SqliteOracleAdapter implements ConnectionSupplier {
    public static final TableReference METADATA_TABLE = AtlasDbConstants.DEFAULT_ORACLE_METADATA_TABLE;
    private final File sqliteFileLocation;

    SqliteOracleAdapter() throws IOException {
        sqliteFileLocation = TempFileUtils.createTempFile("sqlite", ".db");
    }

    @Override
    public Connection get() throws PalantirSqlException {
        try {
            // As with all connection supplier things, clients are expected to close connections.
            return applyPragma(DriverManager.getConnection(getConnectionUrl()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        sqliteFileLocation.delete();
    }

    public <T> T runWithConnection(FunctionCheckedException<Connection, T, SQLException> task) {
        try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
            return task.apply(applyPragma(connection));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public SqlConnectionSupplier createSqlConnectionSupplier() {
        return new SqliteSqlConnectionSupplier(this);
    }

    public void initializeMetadataAndMappingTables() {
        runWithConnection(connection -> {
            Statement statement = connection.createStatement();

            statement.executeUpdate("CREATE TABLE IF NOT EXISTS all_tables (table_name VARCHAR(128) NOT NULL,"
                    + " owner VARCHAR(32) NOT NULL, PRIMARY KEY (table_name, owner))");

            // Creating our own equivalent table is much simpler than trying to wrangle the OracleTableInitializer
            // create types and tables queries into sqlite.
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE
                    + " (table_name VARCHAR(128) NOT NULL, "
                    + " short_table_name VARCHAR(128) NOT NULL, PRIMARY KEY (table_name))");

            return null;
        });

        // OracleTableInitializer requires a config for initialization, but creating the metadata table requires no
        // configuration elements. Thus, the config is just to satisfy the initializer.
        OracleDdlConfig unusedConfig = ImmutableOracleDdlConfig.builder()
                .overflowMigrationState(OverflowMigrationState.UNSTARTED)
                .build();

        // We don't need to close the newly created sqlconnectionsupplier, since that just closes the sqlite oracle
        // adapter!
        OracleTableInitializer tableInitializer = new OracleTableInitializer(
                new com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier(createSqlConnectionSupplier()),
                unusedConfig);
        tableInitializer.createMetadataTable(METADATA_TABLE.getQualifiedName());
    }

    public TableDetails createTable(String prefix, String tableName, String owner) {
        // This is so that unique table metadata is created for different prefixes and owners
        // While Oracle doesn't handle the first case (it creates tables then throws when adding metadata!), tables
        // are scoped to owners. The below attempts to mimic that.
        Namespace namespace = Namespace.create(prefix + owner);
        TableReference tableReference = TableReference.create(namespace, tableName);

        // Equivalent to prefixed table name. Since we want control over
        String reversibleTableName = generatePrefixedTableName(prefix, tableReference);

        // We don't actually shrink it, but we do add a unique identifier to mimic the counting that ddltable does
        String physicalTableName =
                reversibleTableName + UUID.randomUUID().toString().replace("-", "");

        // We don't use ddlTable createTable since we want the table name to insert into allTables, and extracting
        // that is quite painful
        return runWithConnection(connection -> {
            PreparedStatement insertAllTables =
                    connection.prepareStatement("INSERT INTO all_tables VALUES (upper(?), " + "upper(?))");
            insertAllTables.setString(1, physicalTableName);
            insertAllTables.setString(2, owner);
            insertAllTables.executeUpdate();

            String createTableSql = "CREATE TABLE %s (k VARCHAR(128) PRIMARY KEY, value VARCHAR(32))";
            Statement createNewTable = connection.createStatement();
            createNewTable.executeUpdate(String.format(createTableSql, physicalTableName));

            PreparedStatement insertMetadata = connection.prepareStatement(
                    "INSERT INTO " + METADATA_TABLE.getQualifiedName() + " (table_name, table_size) VALUES (?, ?)");
            insertMetadata.setString(1, tableReference.getQualifiedName());
            insertMetadata.setInt(2, 1);
            insertMetadata.executeUpdate();

            PreparedStatement insertTableMapping = connection.prepareStatement("INSERT INTO "
                    + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE + " (table_name, short_table_name) VALUES (?, ?)");
            insertTableMapping.setString(1, reversibleTableName);
            insertTableMapping.setString(2, physicalTableName);
            insertTableMapping.executeUpdate();
            return TableDetails.builder()
                    .tableReference(tableReference)
                    .physicalTableName(physicalTableName)
                    .reversibleTableName(reversibleTableName)
                    .build();
        });
    }

    public Set<String> listAllTables() {
        refreshTableList();
        return runWithConnection(connection -> {
            PreparedStatement statement = connection.prepareStatement("SELECT name FROM sqlite_schema WHERE"
                    + " type ='table' AND name NOT LIKE 'sqlite_%' AND name != 'all_tables' AND"
                    + " name != ? AND name != ?");
            statement.setString(1, METADATA_TABLE.getQualifiedName());
            statement.setString(2, AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE);
            ResultSet resultSet = statement.executeQuery();

            ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("name"));
            }
            return tableNames.build();
        });
    }

    // all_tables is not a native sqlite concept, unlike Oracle. Thus, if we perform deletes, we don't cascade and
    // delete from all_tables. This method does not insert tables created outside the create_table method
    private void refreshTableList() {
        runWithConnection(connection -> {
            connection
                    .createStatement()
                    .executeUpdate("DELETE FROM all_tables WHERE lower(table_name) NOT IN (SELECT lower(name) as"
                            + " table_name FROM sqlite_schema WHERE type = 'table')");
            return null;
        });
    }

    public Set<TableReference> listAllTablesWithMetadata() {
        return runWithConnection(connection -> {
            PreparedStatement statement = connection.prepareStatement("SELECT table_name FROM " + METADATA_TABLE);
            ResultSet resultSet = statement.executeQuery();

            ImmutableSet.Builder<TableReference> tableNames = ImmutableSet.builder();
            while (resultSet.next()) {
                tableNames.add(TableReference.createFromFullyQualifiedName(resultSet.getString("table_name")));
            }
            return tableNames.build();
        });
    }

    public Set<String> listAllTablesWithMapping() {
        return runWithConnection(connection -> {
            PreparedStatement statement =
                    connection.prepareStatement("SELECT table_name FROM " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE);
            ResultSet resultSet = statement.executeQuery();

            ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("table_name"));
            }
            return tableNames.build();
        });
    }

    private Connection applyPragma(Connection connection) throws SQLException {
        // Oracle likes are case-sensitive. This pragma ensures that we mimic that behaviour with our sqlite queries
        connection.createStatement().executeUpdate("PRAGMA case_sensitive_like = true;");
        return connection;
    }

    private String getConnectionUrl() {
        return "jdbc:sqlite:" + sqliteFileLocation.getAbsolutePath();
    }

    private String generatePrefixedTableName(String prefix, TableReference tableReference) {
        OracleDdlConfig config = ImmutableOracleDdlConfig.builder()
                .tablePrefix(prefix)
                .overflowMigrationState(OverflowMigrationState.UNSTARTED)
                .build();

        OracleTableNameGetter tableNameGetter = OracleTableNameGetter.createDefault(config);
        return tableNameGetter.getPrefixedTableName(tableReference);
    }

    /**
     * A lightweight wrapper around {@link SimpleTimedSqlConnectionSupplier} that marshals some queries containing
     * Oracle specific keywords such as PURGE to sqlite compatible queries
     */
    private static class SqliteSqlConnectionSupplier implements SqlConnectionSupplier {
        private final SqlConnectionSupplier delegate;

        SqliteSqlConnectionSupplier(ConnectionSupplier delegate) {
            this.delegate = new SimpleTimedSqlConnectionSupplier(delegate);
        }

        @Override
        public SqlConnection get() {
            return marshalOracleQueryToSqlite(delegate.get());
        }

        @Override
        public void close() throws PalantirSqlException {
            delegate.close();
        }

        private SqlConnection marshalOracleQueryToSqlite(SqlConnection sqlConnection) {
            SqlConnection spy = spy(sqlConnection);
            doAnswer(invocation -> {
                        String sql = invocation.getArgument(0);
                        String strippedSql = StringUtils.removeEnd(sql, "PURGE");
                        sqlConnection.executeUnregisteredQuery(strippedSql);
                        return null;
                    })
                    .when(spy)
                    .executeUnregisteredQuery(endsWith("PURGE"), any());
            return spy;
        }
    }

    /**
     * TableDetails enables us to provide tests with sufficient physical information such that they can verify that
     * everything that _should_ be cleaned up has been cleaned up.
     *
     * Generally, tests will be working on a collection of tables and will want to compare against the currently
     * persisted table names. Thus, this also provides some utility methods for mapping from a collection of
     * TableDetails down to a set of a particular constituent part
     */
    @Value.Immutable
    interface TableDetails {
        String reversibleTableName();

        String physicalTableName();

        TableReference tableReference();

        static ImmutableTableDetails.Builder builder() {
            return ImmutableTableDetails.builder();
        }

        static Set<String> mapToReversibleTableNames(Collection<TableDetails> tableDetails) {
            return tableDetails.stream().map(TableDetails::reversibleTableName).collect(Collectors.toSet());
        }

        static Set<String> mapToPhysicalTableNames(Collection<TableDetails> tableDetails) {
            return tableDetails.stream().map(TableDetails::physicalTableName).collect(Collectors.toSet());
        }

        static Set<TableReference> mapToTableReferences(Collection<TableDetails> tableDetails) {
            return tableDetails.stream().map(TableDetails::tableReference).collect(Collectors.toSet());
        }
    }
}
