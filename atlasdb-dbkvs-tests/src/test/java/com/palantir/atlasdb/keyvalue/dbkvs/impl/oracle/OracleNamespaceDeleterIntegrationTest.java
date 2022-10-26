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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.DbKvsNamespaceDeleterFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.LegacyPhysicalBoundStoreStrategy;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleter;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleterFactory;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.impl.TransactionTestSetup;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.refreshable.Refreshable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public final class OracleNamespaceDeleterIntegrationTest extends TransactionTestSetup {

    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(DbKvsOracleTestSuite::createKvs);

    private static final Refreshable<Optional<KeyValueServiceRuntimeConfig>> RUNTIME_CONFIG =
            Refreshable.only(Optional.empty());
    private static final String TABLE_NAME_ONE = "tablenameone";
    private static final String TABLE_NAME_ONE_CASE_INSENSITIVE_MATCH = "tableNameOne";
    private static final String TABLE_NAME_TWO = "tablenametwo";
    private static final String NON_DEFAULT_TABLE_PREFIX = "diff_";
    private static final String NON_DEFAULT_OVERFLOW_TABLE_PREFIX = "over_";

    private DbKeyValueServiceConfig dbKeyValueServiceConfig;
    private OracleDdlConfig oracleDdlConfig;
    private NamespaceDeleter namespaceDeleter;
    private ConnectionManager connectionManager;

    private KeyValueService keyValueServiceWithNonDefaultPrefix;
    private NamespaceDeleter namespaceDeleterWithNonDefaultPrefix;

    private KeyValueService keyValueServiceWithDefaultPrefixNoMapping;
    private NamespaceDeleter namespaceDeleterWithDefaultPrefixNoMapping;

    private String timestampTableName;

    public OracleNamespaceDeleterIntegrationTest() {
        super(TRM, TRM);
    }

    @Before
    public void before() {
        NamespaceDeleterFactory factory = new DbKvsNamespaceDeleterFactory();
        dbKeyValueServiceConfig = DbKvsOracleTestSuite.getKvsConfig();
        oracleDdlConfig = (OracleDdlConfig) dbKeyValueServiceConfig.ddl();
        connectionManager = DbKvsOracleTestSuite.getConnectionManager(keyValueService);
        namespaceDeleter = factory.createNamespaceDeleter(dbKeyValueServiceConfig, RUNTIME_CONFIG);

        DbKeyValueServiceConfig kvsConfigWithNonDefaultPrefix = ImmutableDbKeyValueServiceConfig.builder()
                .from(dbKeyValueServiceConfig)
                .ddl(ImmutableOracleDdlConfig.builder()
                        .from(oracleDdlConfig)
                        .tablePrefix(NON_DEFAULT_TABLE_PREFIX)
                        .overflowTablePrefix(NON_DEFAULT_OVERFLOW_TABLE_PREFIX)
                        .build())
                .build();

        keyValueServiceWithNonDefaultPrefix =
                ConnectionManagerAwareDbKvs.create(kvsConfigWithNonDefaultPrefix);

        namespaceDeleterWithNonDefaultPrefix = factory.createNamespaceDeleter(kvsConfigWithNonDefaultPrefix,
                RUNTIME_CONFIG);

        DbKeyValueServiceConfig kvsConfigWithDefaultPrefixNoMapping = ImmutableDbKeyValueServiceConfig.builder()
                .from(dbKeyValueServiceConfig)
                .ddl(ImmutableOracleDdlConfig.builder()
                        .from(oracleDdlConfig)
                        .useTableMapping(false)
                        .build())
                .build();

        keyValueServiceWithDefaultPrefixNoMapping =
                ConnectionManagerAwareDbKvs.create(kvsConfigWithDefaultPrefixNoMapping);

        namespaceDeleterWithDefaultPrefixNoMapping =
                factory.createNamespaceDeleter(kvsConfigWithDefaultPrefixNoMapping, RUNTIME_CONFIG);

        timestampTableName = oracleDdlConfig.tablePrefix() + AtlasDbConstants.TIMESTAMP_TABLE.getQualifiedName();
    }

    @After
    public void after() throws IOException, SQLException {
        namespaceDeleter.deleteAllDataFromNamespace();
        namespaceDeleterWithNonDefaultPrefix.deleteAllDataFromNamespace();
        namespaceDeleterWithDefaultPrefixNoMapping.deleteAllDataFromNamespace();
        namespaceDeleter.close();
        namespaceDeleterWithDefaultPrefixNoMapping.close();
        namespaceDeleterWithNonDefaultPrefix.close();
        connectionManager.close();
    }

    @Test
    public void deleteAllDataFromNamespaceDropsAllTablesAndOverflowTablesWithConfigPrefix() {
        createTwoLogicalTablesWithDefaultPrefix();

        namespaceDeleter.deleteAllDataFromNamespace();

        assertSnapshotIsEmptyExcludingTimestampTable(getTableDetailsForDefaultPrefixes());
    }

    @Test
    public void deleteAllDataFromNamespaceDropsAllTablesAndOverflowTablesDespiteCaseInsensitiveMatchingNames() {
        AllTableDetailsSnapshot kvsTablesSnapshot = getTableDetailsForDefaultPrefixes();

        createTableAndOverflowTable(TABLE_NAME_ONE);
        // Creating a table with the same name works in that we create the tables and the table mapping, but we fail
        // when adding to the metadata table. We must still clear this up!
        assertThatThrownBy(() -> createTableAndOverflowTable(TABLE_NAME_ONE_CASE_INSENSITIVE_MATCH));
        AllTableDetailsSnapshot newTables =
                AllTableDetailsSnapshot.difference(getTableDetailsForDefaultPrefixes(), kvsTablesSnapshot);
        assertThat(newTables.physicalTableNames()).hasSize(4);
        assertThat(newTables.prefixedTableNames()).hasSize(4);
        assertThat(newTables.tableReferences()).hasSize(1);

        namespaceDeleter.deleteAllDataFromNamespace();

        assertSnapshotIsEmptyExcludingTimestampTable(getTableDetailsForDefaultPrefixes());
    }

    @Test
    public void deleteAllDataFromNamespaceDoesNotThrowWhenNoTablesToDelete() {
        // Oracle throws on an empty IN () clause, whereas sqlite does not.
        namespaceDeleter.deleteAllDataFromNamespace();
        assertSnapshotIsEmptyExcludingTimestampTable(getTableDetailsForDefaultPrefixes());

        assertThatCode(namespaceDeleter::deleteAllDataFromNamespace).doesNotThrowAnyException();
    }

    @Test
    public void deleteAllDataFromNamespaceDoesNotDropTablesFromADifferentPrefix() {
        createTableAndOverflowTable(TABLE_NAME_ONE);
        createTableAndOverflowTableWithNonDefaultPrefix(TABLE_NAME_ONE);

        Set<String> allTablesWithNonDefaultPrefix =
                listAllPhysicalTableNames(NON_DEFAULT_TABLE_PREFIX, NON_DEFAULT_OVERFLOW_TABLE_PREFIX);

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(listAllPhysicalTableNames(NON_DEFAULT_TABLE_PREFIX, NON_DEFAULT_OVERFLOW_TABLE_PREFIX))
                .hasSameElementsAs(allTablesWithNonDefaultPrefix);
    }

    @Test
    public void deleteAllDataFromNamespaceDoesNotDropTablesFromUnmappedTablesForMappedUser() {
        namespaceDeleter.deleteAllDataFromNamespace();
        createTableAndOverflowTableWithDefaultPrefixNoMapping(TABLE_NAME_ONE);
        AllTableDetailsSnapshot snapshot = getTableDetailsForDefaultPrefixes();

        createTableAndOverflowTable(TABLE_NAME_ONE);
        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(getTableDetailsForDefaultPrefixes()).isEqualTo(snapshot);
    }

    @Test
    public void deleteAllDataFromNamespaceDoesNotDropTablesFromMappedTablesForUnmappedUser() {
        namespaceDeleterWithDefaultPrefixNoMapping.deleteAllDataFromNamespace();
        createTwoLogicalTablesWithDefaultPrefix();
        AllTableDetailsSnapshot snapshot = getTableDetailsForDefaultPrefixes();

        createTableAndOverflowTableWithDefaultPrefixNoMapping(TABLE_NAME_ONE);
        namespaceDeleterWithDefaultPrefixNoMapping.deleteAllDataFromNamespace();
        assertThat(getTableDetailsForDefaultPrefixes()).isEqualTo(snapshot);
    }

    @Test
    public void deleteAllDataFromNamespaceDoesNotDropTimestampTable() throws SQLException {
        new LegacyPhysicalBoundStoreStrategy(AtlasDbConstants.TIMESTAMP_TABLE, oracleDdlConfig.tablePrefix())
                .createTimestampTable(connectionManager.getConnection(), _conn -> DBType.ORACLE);

        createTableAndOverflowTableWithDefaultPrefixNoMapping(TABLE_NAME_ONE);
        createTableAndOverflowTable(TABLE_NAME_ONE);
        namespaceDeleter.deleteAllDataFromNamespace();
        namespaceDeleterWithDefaultPrefixNoMapping.deleteAllDataFromNamespace();

        assertSnapshotIsEmptyExcludingTimestampTable(getTableDetailsForDefaultPrefixes());
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsFalseIfTablesRemain() {
        createTableAndOverflowTable(TABLE_NAME_ONE);
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isFalse();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfNoTablesRemain() {
        createTwoLogicalTablesWithDefaultPrefix();

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueEvenIfOtherPrefixExists() {
        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
        createTableAndOverflowTableWithNonDefaultPrefix(TABLE_NAME_ONE);

        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfUnmappedTablesExistForMappedUser() {
        namespaceDeleter.deleteAllDataFromNamespace();
        createTableAndOverflowTableWithDefaultPrefixNoMapping(TABLE_NAME_ONE);

        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfMappedTablesExistForUnmappedUser() {
        namespaceDeleterWithDefaultPrefixNoMapping.deleteAllDataFromNamespace();
        createTableAndOverflowTable(TABLE_NAME_ONE);

        assertThat(namespaceDeleterWithDefaultPrefixNoMapping.isNamespaceDeletedSuccessfully())
                .isTrue();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfTimestampTableExists() throws SQLException {
        namespaceDeleter.deleteAllDataFromNamespace();
        namespaceDeleterWithDefaultPrefixNoMapping.deleteAllDataFromNamespace();
        new LegacyPhysicalBoundStoreStrategy(AtlasDbConstants.TIMESTAMP_TABLE, oracleDdlConfig.tablePrefix())
                .createTimestampTable(connectionManager.getConnection(), _conn -> DBType.ORACLE);

        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
        assertThat(namespaceDeleterWithDefaultPrefixNoMapping.isNamespaceDeletedSuccessfully())
                .isTrue();
    }

    private void createTwoLogicalTablesWithDefaultPrefix() {
        AllTableDetailsSnapshot kvsTablesSnapshot = getTableDetailsForDefaultPrefixes();
        assertSnapshotIsNotEmpty(kvsTablesSnapshot);
        createTableAndOverflowTable(TABLE_NAME_ONE);
        createTableAndOverflowTable(TABLE_NAME_TWO);
        assertSnapshotContainsNewTableReferences(
                AllTableDetailsSnapshot.difference(getTableDetailsForDefaultPrefixes(), kvsTablesSnapshot), 2);
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

    private void createTableAndOverflowTableWithDefaultPrefixNoMapping(String tableName) {
        keyValueServiceWithDefaultPrefixNoMapping.createTable(
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

    private void createTableAndOverflowTableWithNonDefaultPrefix(String tableName) {
        keyValueServiceWithNonDefaultPrefix.createTable(
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
                    .map(TableReference::createUnsafe)
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

    private static String withEscapedUnderscores(String value) {
        return value.replace("_", "\\_");
    }

    private static String withWildcardSuffix(String prefix) {
        return prefix + "%";
    }

    private static void assertSnapshotContainsNewTableReferences(
            AllTableDetailsSnapshot snapshot, int expectedNumberOfNewTableReferences) {
        assertThat(snapshot.prefixedTableNames()).hasSize(expectedNumberOfNewTableReferences * 2);
        assertThat(snapshot.physicalTableNames()).hasSize(expectedNumberOfNewTableReferences * 2);
        assertThat(snapshot.tableReferences()).hasSize(expectedNumberOfNewTableReferences);
    }

    private void assertSnapshotIsEmptyExcludingTimestampTable(AllTableDetailsSnapshot snapshot) {
        assertThat(snapshot.prefixedTableNames())
                .hasSameSizeAs(snapshot.tableReferences())
                .isEmpty();
        if (snapshot.physicalTableNames().size() == 1) {
            assertThat(Iterables.getOnlyElement(snapshot.physicalTableNames()).toLowerCase(Locale.ROOT))
                    .isEqualTo(timestampTableName);
        } else {
            assertThat(snapshot.physicalTableNames()).isEmpty();
        }
    }

    private static void assertSnapshotIsNotEmpty(AllTableDetailsSnapshot snapshot) {
        assertThat(snapshot.prefixedTableNames()).isNotEmpty();
        assertThat(snapshot.physicalTableNames()).isNotEmpty();
        assertThat(snapshot.tableReferences()).isNotEmpty();
    }

    @Value.Immutable
    interface AllTableDetailsSnapshot {
        Set<String> prefixedTableNames();

        Set<String> physicalTableNames();

        Set<TableReference> tableReferences();

        static ImmutableAllTableDetailsSnapshot.Builder builder() {
            return ImmutableAllTableDetailsSnapshot.builder();
        }

        static AllTableDetailsSnapshot difference(AllTableDetailsSnapshot newest, AllTableDetailsSnapshot initial) {
            return builder()
                    .addAllTableReferences(Sets.difference(newest.tableReferences(), initial.tableReferences()))
                    .addAllPrefixedTableNames(
                            Sets.difference(newest.prefixedTableNames(), initial.prefixedTableNames()))
                    .addAllPhysicalTableNames(
                            Sets.difference(newest.physicalTableNames(), initial.physicalTableNames()))
                    .build();
        }
    }
}
