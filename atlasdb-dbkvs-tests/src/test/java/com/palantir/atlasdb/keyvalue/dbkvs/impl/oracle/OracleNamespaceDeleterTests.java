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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetterImpl;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.ImmutableOracleNamespaceDeleterParameters;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceDeleter;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceDeleter.OracleNamespaceDeleterParameters;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCacheImpl;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.SqliteOracleAdapter.TableDetails;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleter;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class OracleNamespaceDeleterTests {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String TABLE_PREFIX = "a_";
    private static final String OVERFLOW_TABLE_PREFIX = "ao_";
    private static final String ANOTHER_TABLE_PREFIX = "test_";
    private static final String ANOTHER_OVERFLOW_TABLE_PREFIX = "over_";
    private static final String TABLE_NAME_1 = "abc";
    private static final String TABLE_NAME_2 = "hello";
    private static final String TABLE_NAME_3 = "world";
    private static final String TEST_USER = "testuser";
    private static final String TEST_USER_2 = "testuser2";

    private SqliteOracleAdapter sqliteOracleAdapter;

    @Before
    public void before() throws IOException {
        sqliteOracleAdapter = new SqliteOracleAdapter(temporaryFolder.newFile());
        sqliteOracleAdapter.initializeMetadataAndMappingTables();
    }

    @After
    public void after() {
        sqliteOracleAdapter.close();
    }

    @Test
    public void deleteAllDataFromNamespaceOnlyDropsTablesWithConfigPrefixesForCurrentUser() {
        NamespaceDeleter namespaceDeleter = createDefaultNamespaceDeleter();
        Set<TableDetails> tablesToDelete = Sets.union(
                createTablesWithDefaultPrefixes(TABLE_NAME_1), createTablesWithDefaultPrefixes(TABLE_NAME_2));

        Set<TableDetails> tablesThatShouldNotBeDeleted = Set.of(
                createTable(ANOTHER_TABLE_PREFIX, TABLE_NAME_1, TEST_USER),
                createTable(ANOTHER_OVERFLOW_TABLE_PREFIX, TABLE_NAME_2, TEST_USER),
                createTable(TABLE_PREFIX, TABLE_NAME_3, TEST_USER_2),
                // In most SQL implementations, _ is the single character wildcard (so a_ matches ab too).
                // We obviously do not want to drop a table named abc when the prefix is a_, so the following explicitly
                // tests that case.
                createTable("ab_", TABLE_NAME_1, TEST_USER));

        assertThatTableDetailsMatchPersistedData(Sets.union(tablesToDelete, tablesThatShouldNotBeDeleted));

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThatTableDetailsMatchPersistedData(tablesThatShouldNotBeDeleted);
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfNoTablesWithConfigPrefixAndUser() {
        NamespaceDeleter namespaceDeleter = createDefaultNamespaceDeleter();
        createTable(ANOTHER_TABLE_PREFIX, TABLE_NAME_1, TEST_USER);
        createTable(ANOTHER_OVERFLOW_TABLE_PREFIX, TABLE_NAME_2, TEST_USER);
        createTable(TABLE_PREFIX, TABLE_NAME_3, TEST_USER_2);

        createTable("ab_", TABLE_NAME_1, TEST_USER);

        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfNoTablesExist() {
        NamespaceDeleter namespaceDeleter = createDefaultNamespaceDeleter();
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsFalseIfTablePrefixExistsForConfigUser() {
        NamespaceDeleter namespaceDeleter = createDefaultNamespaceDeleter();
        createTable(TABLE_PREFIX, TABLE_NAME_1, TEST_USER);

        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isFalse();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsFalseIfTableOverflowPrefixExistsForConfigUser() {
        NamespaceDeleter namespaceDeleter = createDefaultNamespaceDeleter();
        createTable(OVERFLOW_TABLE_PREFIX, TABLE_NAME_1, TEST_USER);

        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isFalse();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsFalseIfTablePrefixAndOverflowTablePrefixExistsForConfigUser() {
        NamespaceDeleter namespaceDeleter = createDefaultNamespaceDeleter();
        createTablesWithDefaultPrefixes(TABLE_NAME_1);

        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isFalse();
    }

    @Test
    public void deleteAllDataFromNamespaceIsRetryable() {
        NamespaceDeleter namespaceDeleter =
                new OracleNamespaceDeleter(createNamespaceDeleterParametersWithUnstableFactory());

        for (int i = 0; i < 10; i++) {
            createTablesWithDefaultPrefixes(TABLE_NAME_1 + i);
        }

        assertThat(listAllPhysicalTableNames()).hasSize(20);
        assertThatThrownBy(namespaceDeleter::deleteAllDataFromNamespace);
        assertThat(listAllPhysicalTableNames()).hasSizeBetween(1, 19);

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(listAllPhysicalTableNames()).isEmpty();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyDoesNotExecuteArbitrarySqlOnOwner() {
        NamespaceDeleter namespaceDeleter = new OracleNamespaceDeleter(createDefaultNamespaceDeleterParameters()
                .userId("1'; CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --")
                .build());
        Set<TableDetails> tables = createTablesWithDefaultPrefixes(TABLE_NAME_1);

        assertThatTableDetailsMatchPersistedData(tables);

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThatTableDetailsMatchPersistedData(tables);
    }

    @Test
    public void isNamespaceDeletedSuccessfullyDoesNotExecuteArbitrarySqlOnTablePrefix() {
        NamespaceDeleter namespaceDeleter = new OracleNamespaceDeleter(createDefaultNamespaceDeleterParameters()
                // The config doesn't actually let you have such a tablePrefix, so we can't build this in the
                // DDL config. However, in the interest of safety (i.e., not relying on the config changing
                // its validation, we're explicitly forcing the tablePrefix here)
                .tablePrefix("1'); CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --_")
                .build());
        Set<TableDetails> tables = createTablesWithDefaultPrefixes(TABLE_NAME_1);

        assertThatTableDetailsMatchPersistedData(tables);

        namespaceDeleter.deleteAllDataFromNamespace();
        // deleteAllDataFromNamespace will not clean up the table with the standard table prefix
        assertThatTableDetailsMatchPersistedData(tables.stream()
                .filter(tableDetails -> tableDetails.prefixedTableName().startsWith(TABLE_PREFIX))
                .collect(Collectors.toSet()));
    }

    @Test
    public void isNamespaceDeletedSuccessfullyDoesNotExecuteArbitrarySqlOnOverflowTablePrefix() {
        NamespaceDeleter namespaceDeleter = new OracleNamespaceDeleter(createDefaultNamespaceDeleterParameters()
                .overflowTablePrefix("1'); CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --_")
                .build());
        Set<TableDetails> tables = createTablesWithDefaultPrefixes(TABLE_NAME_1);

        assertThatTableDetailsMatchPersistedData(tables);

        namespaceDeleter.deleteAllDataFromNamespace();
        // deleteAllDataFromNamespace will not clean up the table with the standard overflow table prefix
        assertThatTableDetailsMatchPersistedData(tables.stream()
                .filter(tableDetails -> tableDetails.prefixedTableName().startsWith(OVERFLOW_TABLE_PREFIX))
                .collect(Collectors.toSet()));
    }

    @Test
    public void deleteAllDataFromNamespaceDoesNotExecuteArbitrarySqlOnOwner() {
        NamespaceDeleter namespaceDeleter = new OracleNamespaceDeleter(createDefaultNamespaceDeleterParameters()
                .userId("1'; CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --")
                .build());
        Set<TableDetails> tables = createTablesWithDefaultPrefixes(TABLE_NAME_1);

        assertThatTableDetailsMatchPersistedData(tables);

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThatTableDetailsMatchPersistedData(tables);
    }

    @Test
    public void deleteAllDataFromNamespaceDoesNotExecuteArbitrarySqlOnTablePrefix() {
        NamespaceDeleter namespaceDeleter =
                new OracleNamespaceDeleter(createNamespaceDeleterParameters(getDefaultDdlConfig()
                                .overflowTablePrefix(ANOTHER_OVERFLOW_TABLE_PREFIX)
                                .build())
                        .tablePrefix("1'); CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --_")
                        .build());
        Set<TableDetails> tables = createTablesWithDefaultPrefixes(TABLE_NAME_1);

        assertThatTableDetailsMatchPersistedData(tables);

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThatTableDetailsMatchPersistedData(tables);
    }

    @Test
    public void deleteAllDataFromNamespaceDoesNotExecuteArbitrarySqlOnOverflowTablePrefix() {
        NamespaceDeleter namespaceDeleter = new OracleNamespaceDeleter(createNamespaceDeleterParameters(
                        getDefaultDdlConfig().tablePrefix(ANOTHER_TABLE_PREFIX).build())
                .overflowTablePrefix("1'); CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --_")
                .build());
        Set<TableDetails> tables = createTablesWithDefaultPrefixes(TABLE_NAME_1);

        assertThatTableDetailsMatchPersistedData(tables);

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThatTableDetailsMatchPersistedData(tables);
    }

    @Test
    public void deleteAllDataFromNamespaceIgnoresTimestampTable() {
        NamespaceDeleter namespaceDeleter = createNonMappingNamespaceDeleter();

        String physicalTimestampTableName = createTimestampTable();

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(listAllPhysicalTableNames()).containsExactly(physicalTimestampTableName);
        assertThat(listAllTablesWithMapping()).isEmpty();
        assertThat(listAllTablesWithMetadata()).isEmpty();
    }

    @Test
    public void deleteAllDataFromNamespaceWithMappingIgnoresUnmappedTables() {
        NamespaceDeleter namespaceDeleter = createDefaultNamespaceDeleter();

        createTablesWithDefaultPrefixes(TABLE_NAME_1);

        Set<TableDetails> tableDetails = createTablesWithDefaultPrefixesWithoutMapping(TABLE_NAME_1);

        namespaceDeleter.deleteAllDataFromNamespace();
        assertThat(listAllPhysicalTableNames())
                .containsExactlyInAnyOrderElementsOf(TableDetails.mapToPhysicalTableNames(tableDetails));
        assertThat(listAllTablesWithMapping()).isEmpty();
        assertThat(listAllTablesWithMetadata())
                .containsExactlyInAnyOrderElementsOf(TableDetails.mapToTableReferences(tableDetails));
    }

    @Test
    public void deleteAllDataFromNamespaceWithNoMappingIgnoresMappedTables() {
        NamespaceDeleter namespaceDeleter = createNonMappingNamespaceDeleter();
        Set<TableDetails> tableDetails = createTablesWithDefaultPrefixes(TABLE_NAME_1);
        createTablesWithDefaultPrefixesWithoutMapping(TABLE_NAME_1);
        namespaceDeleter.deleteAllDataFromNamespace();
        assertThatTableDetailsMatchPersistedData(tableDetails);
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfUnmappedTablesExistForMappedUser() {
        NamespaceDeleter namespaceDeleter = createDefaultNamespaceDeleter();
        createTablesWithDefaultPrefixesWithoutMapping(TABLE_NAME_1);
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfMappedTablesExistForUnmappedUser() {
        NamespaceDeleter namespaceDeleter = createNonMappingNamespaceDeleter();
        createTablesWithDefaultPrefixes(TABLE_NAME_1);
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    @Test
    public void isNamespaceDeletedSuccessfullyReturnsTrueIfTimestampTableExists() {
        NamespaceDeleter namespaceDeleter = createNonMappingNamespaceDeleter();
        createTimestampTable();
        assertThat(namespaceDeleter.isNamespaceDeletedSuccessfully()).isTrue();
    }

    @Test
    public void closeClosesConnectionSupplier() throws IOException {
        ConnectionSupplier mockConnectionSupplier = mock(ConnectionSupplier.class);
        NamespaceDeleter namespaceDeleter = new OracleNamespaceDeleter(createDefaultNamespaceDeleterParameters()
                .connectionSupplier(mockConnectionSupplier)
                .build());

        namespaceDeleter.close();
        verify(mockConnectionSupplier).close();
    }

    private Set<TableDetails> createTablesWithDefaultPrefixes(String tableName) {
        return Set.of(
                createTable(TABLE_PREFIX, tableName, TEST_USER),
                createTable(OVERFLOW_TABLE_PREFIX, tableName, TEST_USER));
    }

    private Set<TableDetails> createTablesWithDefaultPrefixesWithoutMapping(String tableName) {
        TableReference arbitraryTableReference = TableReference.create(Namespace.DEFAULT_NAMESPACE, tableName);
        String physicalTableName = TABLE_PREFIX + DbKvs.internalTableName(arbitraryTableReference);
        sqliteOracleAdapter.createTableWithoutMapping(physicalTableName, arbitraryTableReference, TEST_USER, true);

        String overflowPhysicalTableName = OVERFLOW_TABLE_PREFIX + DbKvs.internalTableName(arbitraryTableReference);
        sqliteOracleAdapter.createTableWithoutMapping(
                overflowPhysicalTableName, arbitraryTableReference, TEST_USER, false);

        return Set.of(
                TableDetails.builder()
                        .tableReference(arbitraryTableReference)
                        .physicalTableName(physicalTableName)
                        .prefixedTableName(physicalTableName)
                        .build(),
                TableDetails.builder()
                        .tableReference(arbitraryTableReference)
                        .physicalTableName(overflowPhysicalTableName)
                        .prefixedTableName(overflowPhysicalTableName)
                        .build());
    }

    private String createTimestampTable() {
        TableReference timestampTable = AtlasDbConstants.TIMESTAMP_TABLE;
        String physicalTimestampTable = TABLE_PREFIX + timestampTable.getQualifiedName();
        sqliteOracleAdapter.createTableWithoutMapping(physicalTimestampTable, timestampTable, TEST_USER, false);
        return physicalTimestampTable;
    }

    private TableDetails createTable(String prefix, String tableName, String owner) {
        return sqliteOracleAdapter.createTable(prefix, tableName, owner);
    }

    private void assertThatTableDetailsMatchPersistedData(Set<TableDetails> tableDetails) {
        assertThat(listAllPhysicalTableNames())
                .containsExactlyInAnyOrderElementsOf(TableDetails.mapToPhysicalTableNames(tableDetails));
        assertThat(listAllTablesWithMapping())
                .containsExactlyInAnyOrderElementsOf(TableDetails.mapToPrefixedTableNames(tableDetails));
        assertThat(listAllTablesWithMetadata())
                .containsExactlyInAnyOrderElementsOf(TableDetails.mapToTableReferences(tableDetails));
    }

    private Set<String> listAllPhysicalTableNames() {
        return sqliteOracleAdapter.listAllPhysicalTableNames();
    }

    private Set<String> listAllTablesWithMapping() {
        return sqliteOracleAdapter.listAllTablesWithMapping();
    }

    private Set<TableReference> listAllTablesWithMetadata() {
        return sqliteOracleAdapter.listAllTablesWithMetadata();
    }

    private NamespaceDeleter createDefaultNamespaceDeleter() {
        return new OracleNamespaceDeleter(
                createDefaultNamespaceDeleterParameters().build());
    }

    private NamespaceDeleter createNonMappingNamespaceDeleter() {
        return new OracleNamespaceDeleter(createNamespaceDeleterParameters(
                        getDefaultDdlConfig().useTableMapping(false).build())
                .build());
    }

    private ImmutableOracleDdlConfig.Builder getDefaultDdlConfig() {
        return ImmutableOracleDdlConfig.builder()
                .tablePrefix(TABLE_PREFIX)
                .overflowTablePrefix(OVERFLOW_TABLE_PREFIX)
                .overflowMigrationState(OverflowMigrationState.FINISHED)
                .useTableMapping(true)
                .metadataTable(SqliteOracleAdapter.METADATA_TABLE);
    }

    private ImmutableOracleNamespaceDeleterParameters.Builder createDefaultNamespaceDeleterParameters() {
        return createNamespaceDeleterParameters(getDefaultDdlConfig().build());
    }

    private ImmutableOracleNamespaceDeleterParameters.Builder createNamespaceDeleterParameters(
            OracleDdlConfig ddlConfig) {
        OracleTableNameGetter tableNameGetter = OracleTableNameGetterImpl.createDefault(ddlConfig);
        ConnectionSupplier connectionSupplier =
                new ConnectionSupplier(sqliteOracleAdapter.createSqlConnectionSupplier());
        Function<TableReference, DbDdlTable> ddlTableFactory = tableReference -> OracleDdlTable.create(
                tableReference,
                connectionSupplier,
                ddlConfig,
                tableNameGetter,
                new TableValueStyleCacheImpl(),
                MoreExecutors.newDirectExecutorService());
        return OracleNamespaceDeleterParameters.builder()
                .tablePrefix(ddlConfig.tablePrefix())
                .overflowTablePrefix(ddlConfig.overflowTablePrefix())
                .userId(TEST_USER)
                .ddlTableFactory(ddlTableFactory)
                .connectionSupplier(connectionSupplier)
                .tableNameGetter(tableNameGetter);
    }

    private OracleNamespaceDeleterParameters createNamespaceDeleterParametersWithUnstableFactory() {
        OracleNamespaceDeleterParameters parameters =
                createDefaultNamespaceDeleterParameters().build();
        return OracleNamespaceDeleterParameters.builder()
                .from(parameters)
                .ddlTableFactory(new OnceFailingOracleDdlFactory(parameters.ddlTableFactory()))
                .build();
    }

    /**
     * An OracleDdlTable factory that throws an exception every 5 + (2^32)n calls.
     *
     * This is intended to be used for testing resilience to failures for operations that work across multiple
     * OracleDDLTables.
     */
    private static final class OnceFailingOracleDdlFactory implements Function<TableReference, DbDdlTable> {
        private static final int NUMBER_OF_TABLES_TO_CREATE_BEFORE_THROWING_ONCE = 5;
        private final AtomicInteger counter;
        private final Function<TableReference, DbDdlTable> factory;

        OnceFailingOracleDdlFactory(Function<TableReference, DbDdlTable> factory) {
            this.factory = factory;
            this.counter = new AtomicInteger();
        }

        @Override
        public DbDdlTable apply(TableReference tableReference) {
            if (counter.incrementAndGet() == NUMBER_OF_TABLES_TO_CREATE_BEFORE_THROWING_ONCE) {
                throw new RuntimeException("induced failure");
            }
            return factory.apply(tableReference);
        }
    }
}
