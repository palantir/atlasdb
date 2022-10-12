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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetterImpl;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceDeleter;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceDeleter.OracleNamespaceDeleterParameters;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCache;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleter;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.impl.TransactionTestSetup;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.nexus.db.pool.ConnectionManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class OracleNamespaceDeleterIntegrationTest extends TransactionTestSetup {
    private static final String LIST_ALL_TABLES =
            "SELECT COUNT(table_name) AS total FROM all_tables WHERE owner = upper(?) AND table_name LIKE upper(?)"
                    + " ESCAPE '\\'";
    private static final String TABLE_NAME_ONE = "tablenameone";
    private static final String TABLE_NAME_ONE_CASE_INSENSITIVE_MATCH = "tableNameOne";
    private static final String TABLE_NAME_TWO = "tablenametwo";

    private static final String NON_DEFAULT_TABLE_PREFIX = "diff_";
    private static final String NON_DEFAULT_OVERFLOW_TABLE_PREFIX = "over_";

    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(DbKvsOracleTestSuite::createKvs);

    private DbKeyValueServiceConfig dbKeyValueServiceConfig;
    private OracleDdlConfig oracleDdlConfig;
    private NamespaceDeleter namespaceDeleter;
    private ConnectionManager connectionManager;
    private ExecutorService executorService;

    private KeyValueService keyValueServiceWithAnotherPrefix;
    private NamespaceDeleter namespaceDeleterWithAnotherPrefix;

    public OracleNamespaceDeleterIntegrationTest() {
        super(TRM, TRM);
    }

    @Before
    public void before() {
        dbKeyValueServiceConfig = DbKvsOracleTestSuite.getKvsConfig();
        oracleDdlConfig = (OracleDdlConfig) dbKeyValueServiceConfig.ddl();
        executorService = MoreExecutors.newDirectExecutorService();
        connectionManager = DbKvsOracleTestSuite.getConnectionManager(keyValueService);
        ConnectionSupplier connectionSupplier = DbKvsOracleTestSuite.getConnectionSupplier(keyValueService);

        OracleTableNameGetter oracleTableNameGetter = OracleTableNameGetterImpl.createDefault(oracleDdlConfig);
        Function<TableReference, OracleDdlTable> ddlTableFactory = tableReference -> OracleDdlTable.create(
                tableReference,
                connectionSupplier,
                oracleDdlConfig,
                oracleTableNameGetter,
                new TableValueStyleCache(),
                executorService);

        namespaceDeleter = new OracleNamespaceDeleter(OracleNamespaceDeleterParameters.builder()
                .tablePrefix(oracleDdlConfig.tablePrefix())
                .overflowTablePrefix(oracleDdlConfig.overflowTablePrefix())
                .connectionSupplier(connectionSupplier)
                .tableNameGetter(oracleTableNameGetter)
                .oracleDdlTableFactory(ddlTableFactory)
                .userId(dbKeyValueServiceConfig.connection().getDbLogin())
                .build());

        OracleDdlConfig ddlConfigForAnotherPrefix = ImmutableOracleDdlConfig.builder()
                .from(oracleDdlConfig)
                .tablePrefix(NON_DEFAULT_TABLE_PREFIX)
                .overflowTablePrefix(NON_DEFAULT_OVERFLOW_TABLE_PREFIX)
                .build();

        OracleTableNameGetter oracleTableNameGetterForAnotherPrefix =
                OracleTableNameGetterImpl.createDefault(ddlConfigForAnotherPrefix);

        Function<TableReference, OracleDdlTable> ddlTableFactoryForAnotherPrefix =
                tableReference -> OracleDdlTable.create(
                        tableReference,
                        connectionSupplier,
                        ddlConfigForAnotherPrefix,
                        oracleTableNameGetterForAnotherPrefix,
                        new TableValueStyleCache(),
                        executorService);

        keyValueServiceWithAnotherPrefix = ConnectionManagerAwareDbKvs.create(ImmutableDbKeyValueServiceConfig.builder()
                .from(dbKeyValueServiceConfig)
                .ddl(ddlConfigForAnotherPrefix)
                .build());

        namespaceDeleterWithAnotherPrefix = new OracleNamespaceDeleter(OracleNamespaceDeleterParameters.builder()
                .tablePrefix(ddlConfigForAnotherPrefix.tablePrefix())
                .overflowTablePrefix(ddlConfigForAnotherPrefix.overflowTablePrefix())
                .connectionSupplier(connectionSupplier)
                .tableNameGetter(oracleTableNameGetterForAnotherPrefix)
                .oracleDdlTableFactory(ddlTableFactoryForAnotherPrefix)
                .userId(dbKeyValueServiceConfig.connection().getDbLogin())
                .build());
    }

    @After
    public void after() {
        namespaceDeleter.deleteAllDataFromNamespace();
        namespaceDeleterWithAnotherPrefix.deleteAllDataFromNamespace();
        executorService.shutdown();
    }

    @Test
    public void dropNamespaceDropsAllTablesAndOverflowTablesWithConfigPrefix() {
        int kvsTables = getNumberOfTables();
        createTableAndOverflowTable(TABLE_NAME_ONE);
        createTableAndOverflowTable(TABLE_NAME_TWO);
        assertThat(getNumberOfTables() - kvsTables).isEqualTo(4);

        namespaceDeleter.deleteAllDataFromNamespace();

        assertThat(getNumberOfTables()).isEqualTo(0);
        assertThat(areMetadataAndMappingTablesAreEmpty()).isTrue();
    }

    @Test
    public void dropNamespaceDropsAllTablesAndOverflowTablesDespiteCaseInsensitiveMatchingNames() {
        int kvsTables = getNumberOfTables();
        createTableAndOverflowTable(TABLE_NAME_ONE);
        // Creating a table with the same name works in that we create the tables and the table mapping, but we fail
        // when adding to the metadata table. We must still clear this up!
        assertThatThrownBy(() -> createTableAndOverflowTable(TABLE_NAME_ONE_CASE_INSENSITIVE_MATCH));
        assertThat(getNumberOfTables() - kvsTables).isEqualTo(4);

        namespaceDeleter.deleteAllDataFromNamespace();

        assertThat(getNumberOfTables()).isEqualTo(0);
        assertThat(areMetadataAndMappingTablesAreEmpty()).isTrue();
    }

    @Test
    public void dropAllTablesDoesNotThrowWhenNoTablesToDelete() {
        // Oracle throws on an empty IN () clause, where as sqlite does not.
        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(getNumberOfTables()).isEqualTo(0);

        assertThatNoException().isThrownBy(namespaceDeleter::deleteAllDataFromNamespace);
    }

    @Test
    public void dropAllTablesDoesNotDropTablesFromADifferentPrefix() {
        createTableAndOverflowTable(TABLE_NAME_ONE);

        keyValueServiceWithAnotherPrefix.createTable(
                TableReference.create(Namespace.create("other"), TABLE_NAME_ONE),
                TableMetadata.builder()
                        .nameLogSafety(LogSafety.SAFE)
                        .columns(new ColumnMetadataDescription())
                        .build()
                        .persistToBytes());

        int numberOfTables =
                getNumberOfTables(NON_DEFAULT_TABLE_PREFIX) + getNumberOfTables(NON_DEFAULT_OVERFLOW_TABLE_PREFIX);
        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(getNumberOfTables(NON_DEFAULT_TABLE_PREFIX) + getNumberOfTables(NON_DEFAULT_OVERFLOW_TABLE_PREFIX))
                .isEqualTo(numberOfTables);
    }

    @Test
    public void hasNamespaceSuccessfullyDroppedReturnsFalseIfTablesRemain() {
        createTableAndOverflowTable(TABLE_NAME_ONE);
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isFalse();
    }

    @Test
    public void hasNamespaceSuccessfullyDroppedReturnsTrueIfNoTablesRemain() {
        int kvsTables = getNumberOfTables();
        createTableAndOverflowTable(TABLE_NAME_ONE);
        createTableAndOverflowTable(TABLE_NAME_TWO);
        assertThat(getNumberOfTables() - kvsTables).isEqualTo(4);

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    @Test
    public void hasNamespaceSuccessfullyDroppedReturnsTrueEvenIfOtherPrefixExists() {
        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
        keyValueServiceWithAnotherPrefix.createTable(
                TableReference.create(Namespace.create("other"), TABLE_NAME_ONE),
                TableMetadata.builder()
                        .nameLogSafety(LogSafety.SAFE)
                        .columns(new ColumnMetadataDescription())
                        .build()
                        .persistToBytes());
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    private void createTableAndOverflowTable(String tableName) {
        keyValueService.createTable(
                getTableReference(tableName),
                TableMetadata.builder()
                        .nameLogSafety(LogSafety.SAFE)
                        .columns(new ColumnMetadataDescription())
                        .build()
                        .persistToBytes());
    }

    private TableReference getTableReference(String tableName) {
        return dbKeyValueServiceConfig
                .namespace()
                .map(namespace -> TableReference.create(Namespace.create(namespace), tableName))
                .orElseGet(() -> TableReference.createWithEmptyNamespace(tableName));
    }

    private int getNumberOfTables() {
        return getNumberOfTables(oracleDdlConfig.tablePrefix())
                + getNumberOfTables(oracleDdlConfig.overflowTablePrefix());
    }
    // TODO: List the tables and compare sets (since equal number of tables doesn't mean the right tables are deleted)
    private int getNumberOfTables(String prefix) {
        String escapedPrefix = escapeString(prefix);
        return runWithConnection(connection -> {
            PreparedStatement statement = connection.prepareStatement(LIST_ALL_TABLES);
            statement.setString(1, dbKeyValueServiceConfig.connection().getDbLogin());
            statement.setString(2, escapedPrefix + "%");
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("total");
            } else {
                return 0;
            }
        });
    }

    private String escapeString(String s) {
        return s.replace("_", "\\_");
    }

    private boolean areMetadataAndMappingTablesAreEmpty() {
        return numberOfMetadataEntries() == 0 && numberOfMappingEntries() == 0;
    }

    private int numberOfMetadataEntries() {
        return runWithConnection(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT COUNT(table_name) as total from " + AtlasDbConstants.DEFAULT_ORACLE_METADATA_TABLE);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("total");
            } else {
                return 0;
            }
        });
    }

    private int numberOfMappingEntries() {
        return runWithConnection(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT COUNT(table_name) as total from " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("total");
            } else {
                return 0;
            }
        });
    }

    private <T> T runWithConnection(FunctionCheckedException<Connection, T, SQLException> task) {
        try (Connection connection = connectionManager.getConnection()) {
            return task.apply(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
