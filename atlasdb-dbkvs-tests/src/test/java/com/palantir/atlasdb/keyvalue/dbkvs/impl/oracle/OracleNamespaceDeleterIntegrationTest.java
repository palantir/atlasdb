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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
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
import com.palantir.common.base.Throwables;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class OracleNamespaceDeleterIntegrationTest extends TransactionTestSetup {
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

        namespaceDeleter = createNamespaceDeleter(
                oracleDdlConfig,
                dbKeyValueServiceConfig.connection(),
                DbKvsOracleTestSuite.getConnectionSupplier(keyValueService),
                executorService);

        OracleDdlConfig ddlConfigForAnotherPrefix = ImmutableOracleDdlConfig.builder()
                .from(oracleDdlConfig)
                .tablePrefix(NON_DEFAULT_TABLE_PREFIX)
                .overflowTablePrefix(NON_DEFAULT_OVERFLOW_TABLE_PREFIX)
                .build();

        keyValueServiceWithAnotherPrefix = ConnectionManagerAwareDbKvs.create(ImmutableDbKeyValueServiceConfig.builder()
                .from(dbKeyValueServiceConfig)
                .ddl(ddlConfigForAnotherPrefix)
                .build());

        namespaceDeleterWithAnotherPrefix = createNamespaceDeleter(
                ddlConfigForAnotherPrefix,
                dbKeyValueServiceConfig.connection(),
                DbKvsOracleTestSuite.getConnectionSupplier(keyValueServiceWithAnotherPrefix),
                executorService);
    }

    private static NamespaceDeleter createNamespaceDeleter(
            OracleDdlConfig ddlConfig,
            ConnectionConfig connectionConfig,
            ConnectionSupplier connectionSupplier,
            ExecutorService executorService) {
        OracleTableNameGetter oracleTableNameGetterForAnotherPrefix =
                OracleTableNameGetterImpl.createDefault(ddlConfig);

        Function<TableReference, OracleDdlTable> ddlTableFactoryForAnotherPrefix =
                tableReference -> OracleDdlTable.create(
                        tableReference,
                        connectionSupplier,
                        ddlConfig,
                        oracleTableNameGetterForAnotherPrefix,
                        new TableValueStyleCache(),
                        executorService);

        return new OracleNamespaceDeleter(OracleNamespaceDeleterParameters.builder()
                .tablePrefix(ddlConfig.tablePrefix())
                .overflowTablePrefix(ddlConfig.overflowTablePrefix())
                .connectionSupplier(connectionSupplier)
                .tableNameGetter(oracleTableNameGetterForAnotherPrefix)
                .oracleDdlTableFactory(ddlTableFactoryForAnotherPrefix)
                .userId(connectionConfig.getDbLogin())
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
        createTwoLogicalTablesWithDefaultPrefix();

        namespaceDeleter.deleteAllDataFromNamespace();

        assertSnapshotIsEmpty(getTableDetailsForDefaultPrefixes());
    }

    @Test
    public void dropNamespaceDropsAllTablesAndOverflowTablesDespiteCaseInsensitiveMatchingNames() {
        AllTableDetailsSnapshot kvsTablesSnapshot = getTableDetailsForDefaultPrefixes();

        createTableAndOverflowTable(TABLE_NAME_ONE);
        // Creating a table with the same name works in that we create the tables and the table mapping, but we fail
        // when adding to the metadata table. We must still clear this up!
        assertThatThrownBy(() -> createTableAndOverflowTable(TABLE_NAME_ONE_CASE_INSENSITIVE_MATCH));
        AllTableDetailsSnapshot newTables =
                AllTableDetailsSnapshot.difference(kvsTablesSnapshot, getTableDetailsForDefaultPrefixes());
        assertThat(newTables.physicalTableNames()).hasSize(2);
        assertThat(newTables.prefixedTableNames()).hasSize(2);
        assertThat(newTables.tableReferences()).hasSize(1);

        namespaceDeleter.deleteAllDataFromNamespace();

        assertSnapshotIsEmpty(getTableDetailsForDefaultPrefixes());
    }

    @Test
    public void dropAllTablesDoesNotThrowWhenNoTablesToDelete() {
        // Oracle throws on an empty IN () clause, whereas sqlite does not.
        namespaceDeleter.deleteAllDataFromNamespace();
        assertSnapshotIsEmpty(getTableDetailsForDefaultPrefixes());

        assertThatNoException().isThrownBy(namespaceDeleter::deleteAllDataFromNamespace);
    }

    @Test
    public void dropAllTablesDoesNotDropTablesFromADifferentPrefix() {
        createTableAndOverflowTable(TABLE_NAME_ONE);
        createTableAndOverflowTableForAnotherPrefix(TABLE_NAME_ONE);

        Set<String> allTablesWithNonDefaultPrefix =
                listAllPhysicalTableNames(NON_DEFAULT_TABLE_PREFIX, NON_DEFAULT_OVERFLOW_TABLE_PREFIX);

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(listAllPhysicalTableNames(NON_DEFAULT_TABLE_PREFIX, NON_DEFAULT_OVERFLOW_TABLE_PREFIX))
                .hasSameElementsAs(allTablesWithNonDefaultPrefix);
    }

    @Test
    public void hasNamespaceSuccessfullyDroppedReturnsFalseIfTablesRemain() {
        createTableAndOverflowTable(TABLE_NAME_ONE);
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isFalse();
    }

    @Test
    public void hasNamespaceSuccessfullyDroppedReturnsTrueIfNoTablesRemain() {
        createTwoLogicalTablesWithDefaultPrefix();

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    @Test
    public void hasNamespaceSuccessfullyDroppedReturnsTrueEvenIfOtherPrefixExists() {
        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
        createTableAndOverflowTableForAnotherPrefix(TABLE_NAME_ONE);

        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    private void createTwoLogicalTablesWithDefaultPrefix() {
        AllTableDetailsSnapshot kvsTablesSnapshot = getTableDetailsForDefaultPrefixes();
        assertSnapshotIsNotEmpty(kvsTablesSnapshot);
        createTableAndOverflowTable(TABLE_NAME_ONE);
        createTableAndOverflowTable(TABLE_NAME_TWO);
        assertThatSnapshotContainsEqualNumberOfElementsFromEachInternalTable(
                AllTableDetailsSnapshot.difference(kvsTablesSnapshot, getTableDetailsForDefaultPrefixes()), 2);
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

    private void createTableAndOverflowTableForAnotherPrefix(String tableName) {
        keyValueServiceWithAnotherPrefix.createTable(
                TableReference.create(Namespace.create("other"), tableName),
                TableMetadata.builder()
                        .nameLogSafety(LogSafety.SAFE)
                        .columns(new ColumnMetadataDescription())
                        .build()
                        .persistToBytes());
    }

    private AllTableDetailsSnapshot getTableDetailsForDefaultPrefixes() {
        return AllTableDetailsSnapshot.builder()
                .addAllPhysicalTableNames(
                        listAllPhysicalTableNames(oracleDdlConfig.tablePrefix(), oracleDdlConfig.overflowTablePrefix()))
                .addAllPrefixedTableNames(listAllTablesWithMapping())
                .addAllTableReferences(listAllTablesWithMetadata())
                .build();
    }

    private Set<String> listAllPhysicalTableNames(String tablePrefix, String overflowTablePrefix) {
        String escapedTablePrefix = withWildcardSuffix(withEscapedUnderscores(tablePrefix));
        String escapedOverflowTablePrefix = withWildcardSuffix(withEscapedUnderscores(overflowTablePrefix));

        return runWithConnection(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT table_name FROM all_tables WHERE owner = upper(?) AND (table_name LIKE upper(?)"
                            + " ESCAPE '\\' OR table_name LIKE upper(?) ESCAPE '\\')");
            statement.setString(1, dbKeyValueServiceConfig.connection().getDbLogin());
            statement.setString(2, escapedTablePrefix);
            statement.setString(3, escapedOverflowTablePrefix);

            ResultSet resultSet = statement.executeQuery();
            return getTableNamesFromResultSet(resultSet);
        });
    }

    private Set<TableReference> listAllTablesWithMetadata() {
        return runWithConnection(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT table_name from " + AtlasDbConstants.DEFAULT_ORACLE_METADATA_TABLE);
            ResultSet resultSet = statement.executeQuery();
            return getTableNamesFromResultSet(resultSet).stream()
                    .map(TableReference::createFromFullyQualifiedName)
                    .collect(Collectors.toSet());
        });
    }

    private Set<String> listAllTablesWithMapping() {
        return runWithConnection(connection -> {
            PreparedStatement statement =
                    connection.prepareStatement("SELECT table_name from " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE);
            ResultSet resultSet = statement.executeQuery();
            return getTableNamesFromResultSet(resultSet);
        });
    }

    private <T> T runWithConnection(FunctionCheckedException<Connection, T, SQLException> task) {
        try (Connection connection = connectionManager.getConnection()) {
            return task.apply(connection);
        } catch (SQLException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private static Set<String> getTableNamesFromResultSet(ResultSet resultSet) throws SQLException {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        while (resultSet.next()) {
            builder.add(resultSet.getString("table_name"));
        }
        return builder.build();
    }

    private static String withEscapedUnderscores(String s) {
        return s.replace("_", "\\_");
    }

    private static String withWildcardSuffix(String s) {
        return s + "%";
    }

    private static void assertThatSnapshotContainsEqualNumberOfElementsFromEachInternalTable(
            AllTableDetailsSnapshot snapshot, int expectedSize) {
        assertThat(snapshot.prefixedTableNames())
                .hasSameSizeAs(snapshot.physicalTableNames())
                .hasSameSizeAs(snapshot.tableReferences())
                .hasSize(expectedSize);
    }

    private static void assertSnapshotIsEmpty(AllTableDetailsSnapshot snapshot) {
        assertThat(snapshot.prefixedTableNames())
                .hasSameSizeAs(snapshot.physicalTableNames())
                .hasSameSizeAs(snapshot.tableReferences())
                .isEmpty();
    }

    private static void assertSnapshotIsNotEmpty(AllTableDetailsSnapshot snapshot) {
        assertThat(snapshot.prefixedTableNames())
                .hasSameSizeAs(snapshot.physicalTableNames())
                .hasSameSizeAs(snapshot.tableReferences())
                .isNotEmpty();
    }

    @Value.Immutable
    interface AllTableDetailsSnapshot {
        Set<String> prefixedTableNames();

        Set<String> physicalTableNames();

        Set<TableReference> tableReferences();

        static ImmutableAllTableDetailsSnapshot.Builder builder() {
            return ImmutableAllTableDetailsSnapshot.builder();
        }

        static AllTableDetailsSnapshot difference(AllTableDetailsSnapshot initial, AllTableDetailsSnapshot secondary) {
            return builder()
                    .addAllTableReferences(Sets.difference(initial.tableReferences(), secondary.tableReferences()))
                    .addAllPrefixedTableNames(
                            Sets.difference(initial.prefixedTableNames(), secondary.prefixedTableNames()))
                    .addAllPhysicalTableNames(
                            Sets.difference(initial.physicalTableNames(), secondary.physicalTableNames()))
                    .build();
        }
    }
}
