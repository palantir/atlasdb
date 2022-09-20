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
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
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

public final class SqliteOracleAdapter implements ConnectionSupplier {
    public static final TableReference METADATA_TABLE = AtlasDbConstants.DEFAULT_ORACLE_METADATA_TABLE;

    private final File sqliteFileLocation;

    SqliteOracleAdapter() throws IOException {
        sqliteFileLocation = TempFileUtils.createTempFile("sqlite", "db");
    }

    @Override
    public Connection get() throws PalantirSqlException {
        try {
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

            statement.executeUpdate("CREATE TABLE IF NOT EXISTS all_tables (table_name VARCHAR(128) NOT NULL, "
                    + "owner VARCHAR(32) NOT NULL, PRIMARY KEY (table_name, owner))");

            statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE
                    + "(table_name VARCHAR(128) NOT NULL, "
                    + "short_table_name VARCHAR(128) NOT NULL, PRIMARY KEY (table_name))");

            return null;
        });
        OracleTableInitializer oti = new OracleTableInitializer(
                new com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier(createSqlConnectionSupplier()), null);
        oti.createMetadataTable(METADATA_TABLE.getQualifiedName());
    }

    public TableDetails createTable(String prefix, String tableName, String owner) {
        Namespace namespace = Namespace.create(prefix + owner);
        TableReference tableReference = TableReference.create(namespace, tableName);

        String reversibleTableName = prefix + DbKvs.internalTableName(tableReference);

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

            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO " + METADATA_TABLE.getQualifiedName() + " (table_name, table_size) VALUES (?, ?)");
            ps.setString(1, tableReference.getQualifiedName());
            ps.setInt(2, 1);
            ps.executeUpdate();

            PreparedStatement tableNameMapping = connection.prepareStatement("INSERT INTO "
                    + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE + "(table_name, short_table_name) VALUES (?, ?)");
            tableNameMapping.setString(1, reversibleTableName);
            tableNameMapping.setString(2, physicalTableName);
            tableNameMapping.executeUpdate();
            return TableDetails.builder()
                    .tableReference(tableReference)
                    .physicalTableName(physicalTableName)
                    .reversibleTableName(reversibleTableName)
                    .build();
        });
    }

    public void refreshTableList() {
        runWithConnection(connection -> {
            connection
                    .createStatement()
                    .executeUpdate("DELETE FROM all_tables WHERE lower(table_name) "
                            + "NOT IN"
                            + " (SELECT lower(name) as table_name FROM sqlite_schema WHERE type = 'table')");
            return null;
        });
    }

    public Set<String> listAllTables() {
        return runWithConnection(connection -> {
            PreparedStatement statement = connection.prepareStatement("SELECT name FROM "
                    + "sqlite_schema WHERE "
                    + "type ='table' AND name NOT LIKE 'sqlite_%' AND name != 'all_tables' AND name != ? AND name != "
                    + "?");
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
        connection.createStatement().executeUpdate("PRAGMA case_sensitive_like = true;");
        return connection;
    }

    private String getConnectionUrl() {
        return "jdbc:sqlite:" + sqliteFileLocation.getAbsolutePath();
    }

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
