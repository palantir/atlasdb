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

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.NamespaceCleaner;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.ImmutableOracleNamespaceCleanerParameters;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceCleaner;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceCleaner.OracleNamespaceCleanerParameters;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCache;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.SqliteOracleAdapter.TableDetails;
import com.palantir.nexus.db.sql.SqlConnection;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class OracleNamespaceCleanerTests {
    private static final String TABLE_PREFIX = "a_";
    private static final String OVERFLOW_TABLE_PREFIX = "ao_";
    private static final String ANOTHER_TABLE_PREFIX = "test_";
    private static final String ANOTHER_OVERFLOW_TABLE_PREFIX = "over_";
    private static final String TABLE_NAME_1 = "abc";
    private static final String TABLE_NAME_2 = "hello";
    private static final String TABLE_NAME_3 = "world";
    private static final String TEST_USER = "testuser";
    private static final String TEST_USER_2 = "testuser2";
    private ExecutorService executorService;
    private SqliteOracleAdapter sqliteOracleAdapter;

    @Before
    public void before() throws IOException {
        executorService = MoreExecutors.newDirectExecutorService();
        sqliteOracleAdapter = new SqliteOracleAdapter();
        sqliteOracleAdapter.initializeMetadataAndMappingTables();
    }

    @After
    public void after() {
        executorService.shutdown();
        sqliteOracleAdapter.close();
    }

    @Test
    public void dropAllTablesOnlyDropsTablesWithConfigPrefixesForCurrentUser() {
        NamespaceCleaner namespaceCleaner = createDefaultNamespaceCleaner();
        Set<TableDetails> tablesToDelete =
                Sets.union(createDefaultTable(TABLE_NAME_1), createDefaultTable(TABLE_NAME_2));

        Set<TableDetails> tablesThatShouldNotBeDeleted = Set.of(
                createTable(ANOTHER_TABLE_PREFIX, TABLE_NAME_1, TEST_USER),
                createTable(ANOTHER_OVERFLOW_TABLE_PREFIX, TABLE_NAME_2, TEST_USER),
                createTable(TABLE_PREFIX, TABLE_NAME_3, TEST_USER_2),
                // In most SQL implementations, _ is the single character wildcard (so a_ matches ab too).
                // We obviously do not want to drop a table named abc when the prefix is a_, so the following explicitly
                // tests that case.
                createTable("", TABLE_NAME_1, TEST_USER));

        assertThatTableDetailsMatchPersistedData(Sets.union(tablesToDelete, tablesThatShouldNotBeDeleted));
        namespaceCleaner.dropAllTables();
        assertThatTableDetailsMatchPersistedData(tablesThatShouldNotBeDeleted);
    }

    @Test
    public void areAllTablesDroppedReturnsTrueIfNoTablesWithConfigPrefixAndUser() {
        NamespaceCleaner namespaceCleaner = createDefaultNamespaceCleaner();

        createTable(ANOTHER_TABLE_PREFIX, TABLE_NAME_1, TEST_USER);
        createTable(ANOTHER_OVERFLOW_TABLE_PREFIX, TABLE_NAME_2, TEST_USER);
        createTable(TABLE_PREFIX, TABLE_NAME_3, TEST_USER_2);

        createTable("", TABLE_NAME_1, TEST_USER);

        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isTrue();
    }

    @Test
    public void areAllTablesDroppedReturnsFalseIfTablePrefixExistsForConfigUser() {
        NamespaceCleaner namespaceCleaner = createDefaultNamespaceCleaner();

        createTable(TABLE_PREFIX, TABLE_NAME_1, TEST_USER);
        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isFalse();
    }

    @Test
    public void areAllTablesDroppedReturnsFalseIfTableOverflowPrefixExistsForConfigUser() {
        NamespaceCleaner namespaceCleaner = createDefaultNamespaceCleaner();

        createTable(OVERFLOW_TABLE_PREFIX, TABLE_NAME_1, TEST_USER);
        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isFalse();
    }

    @Test
    public void areAllTablesDroppedReturnsFalseIfTablePrefixAndOverflowTablePrefixExistsForConfigUser() {
        NamespaceCleaner namespaceCleaner = createDefaultNamespaceCleaner();

        createDefaultTable(TABLE_NAME_1);
        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isFalse();
    }

    @Test
    public void dropAllTablesMakesProgressInSpiteOfFailures() {
        NamespaceCleaner namespaceCleaner =
                new OracleNamespaceCleaner(createNamespaceCleanerParameters(new UnstableConnectionSupplier(25))
                        .build());

        for (int i = 0; i < 10; i++) {
            createDefaultTable(TABLE_NAME_1 + i);
        }

        assertThat(listAllTables()).hasSize(20);
        assertThatThrownBy(namespaceCleaner::dropAllTables);
        assertThat(listAllTables()).hasSizeLessThan(20);
    }

    @Test
    public void dropAllTablesIsRetryable() {
        NamespaceCleaner namespaceCleaner =
                new OracleNamespaceCleaner(createNamespaceCleanerParameters(new UnstableConnectionSupplier(25))
                        .build());

        for (int i = 0; i < 10; i++) {
            createDefaultTable(TABLE_NAME_1 + i);
        }

        assertThat(listAllTables()).hasSize(20);
        assertThatThrownBy(namespaceCleaner::dropAllTables);
        assertThat(listAllTables()).hasSizeBetween(1, 19);

        sqliteOracleAdapter.refreshTableList();
        namespaceCleaner.dropAllTables();
        assertThat(listAllTables()).isEmpty();
    }

    @Test
    public void areAllTablesDeletedDoesNotExecuteArbitrarySqlOnOwner() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .userId("1'; CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --")
                .build());
        Set<TableDetails> tables = createDefaultTable(TABLE_NAME_1);
        assertThatTableDetailsMatchPersistedData(tables);
        namespaceCleaner.areAllTablesSuccessfullyDropped();
        assertThatTableDetailsMatchPersistedData(tables);
    }

    @Test
    public void areAllTablesDeletedDoesNotExecuteArbitrarySqlOnTablePrefix() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .tablePrefix("1'); CREATE TABLE mwahahaha (evil" + " VARCHAR(128) PRIMARY KEY); --_")
                .build());
        Set<TableDetails> tables = createDefaultTable(TABLE_NAME_1);
        assertThatTableDetailsMatchPersistedData(tables);
        namespaceCleaner.areAllTablesSuccessfullyDropped();
        assertThatTableDetailsMatchPersistedData(tables);
    }

    @Test
    public void areAllTablesDeletedDoesNotExecuteArbitrarySqlOnOverflowTablePrefix() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .overflowTablePrefix("1'); CREATE TABLE mwahahaha (evil" + " VARCHAR(128) PRIMARY KEY); --_")
                .build());
        Set<TableDetails> tables = createDefaultTable(TABLE_NAME_1);
        assertThatTableDetailsMatchPersistedData(tables);
        namespaceCleaner.areAllTablesSuccessfullyDropped();
        assertThatTableDetailsMatchPersistedData(tables);
    }

    @Test
    public void dropTablesDoesNotExecuteArbitrarySqlOnOwner() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .userId("1'; CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --")
                .build());
        Set<TableDetails> tables = createDefaultTable(TABLE_NAME_1);
        assertThatTableDetailsMatchPersistedData(tables);
        namespaceCleaner.dropAllTables();
        assertThatTableDetailsMatchPersistedData(tables);
    }

    @Test
    public void dropTablesDoesNotExecuteArbitrarySqlOnTablePrefix() {
        NamespaceCleaner namespaceCleaner =
                new OracleNamespaceCleaner(createNamespaceCleanerParameters(getDefaultDdlConfig()
                                .overflowTablePrefix(ANOTHER_OVERFLOW_TABLE_PREFIX)
                                .build())
                        .tablePrefix("1'); CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --_")
                        .build());
        Set<TableDetails> tables = createDefaultTable(TABLE_NAME_1);
        assertThatTableDetailsMatchPersistedData(tables);
        namespaceCleaner.dropAllTables();
        assertThatTableDetailsMatchPersistedData(tables);
    }

    @Test
    public void dropTablesDoesNotExecuteArbitrarySqlOnOverflowTablePrefix() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createNamespaceCleanerParameters(
                        getDefaultDdlConfig().tablePrefix(ANOTHER_TABLE_PREFIX).build())
                .overflowTablePrefix("1'); CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --_")
                .build());
        Set<TableDetails> tables = createDefaultTable(TABLE_NAME_1);
        assertThatTableDetailsMatchPersistedData(tables);
        namespaceCleaner.dropAllTables();
        assertThatTableDetailsMatchPersistedData(tables);
    }

    @Test
    public void closeClosesConnectionSupplier() throws IOException {
        ConnectionSupplier mockConnectionSupplier = mock(ConnectionSupplier.class);
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(
                createNamespaceCleanerParameters(mockConnectionSupplier).build());
        namespaceCleaner.close();
    }

    private Set<TableDetails> createDefaultTable(String tableName) {
        return Set.of(
                createTable(TABLE_PREFIX, tableName, TEST_USER),
                createTable(OVERFLOW_TABLE_PREFIX, tableName, TEST_USER));
    }

    private TableDetails createTable(String prefix, String tableName, String owner) {
        return sqliteOracleAdapter.createTable(prefix, tableName, owner);
    }

    private void assertThatTableDetailsMatchPersistedData(Collection<TableDetails> tableDetails) {
        assertThat(listAllTables())
                .containsExactlyInAnyOrderElementsOf(TableDetails.mapToPhysicalTableNames(tableDetails));
        assertThat(listAllTablesWithMapping())
                .containsExactlyInAnyOrderElementsOf(TableDetails.mapToReversibleTableNames(tableDetails));
        assertThat(listAllTablesWithMetadata())
                .containsExactlyInAnyOrderElementsOf(TableDetails.mapToTableReferences(tableDetails));
    }

    private Set<String> listAllTables() {
        return sqliteOracleAdapter.listAllTables();
    }

    private Set<TableReference> listAllTablesWithMetadata() {
        return sqliteOracleAdapter.listAllTablesWithMetadata();
    }

    private Set<String> listAllTablesWithMapping() {
        return sqliteOracleAdapter.listAllTablesWithMapping();
    }

    private NamespaceCleaner createDefaultNamespaceCleaner() {
        return new OracleNamespaceCleaner(
                createDefaultNamespaceCleanerParameters().build());
    }

    private ImmutableOracleDdlConfig.Builder getDefaultDdlConfig() {
        return ImmutableOracleDdlConfig.builder()
                .tablePrefix(TABLE_PREFIX)
                .overflowTablePrefix(OVERFLOW_TABLE_PREFIX)
                .overflowMigrationState(OverflowMigrationState.UNSTARTED)
                .useTableMapping(true)
                .metadataTable(SqliteOracleAdapter.METADATA_TABLE);
    }

    private ImmutableOracleNamespaceCleanerParameters.Builder createDefaultNamespaceCleanerParameters() {
        return createNamespaceCleanerParameters(
                getDefaultDdlConfig().build(),
                new ConnectionSupplier(sqliteOracleAdapter.createSqlConnectionSupplier()));
    }

    private ImmutableOracleNamespaceCleanerParameters.Builder createNamespaceCleanerParameters(
            ConnectionSupplier connectionSupplier) {
        return createNamespaceCleanerParameters(getDefaultDdlConfig().build(), connectionSupplier);
    }

    private ImmutableOracleNamespaceCleanerParameters.Builder createNamespaceCleanerParameters(
            OracleDdlConfig ddlConfig) {
        return createNamespaceCleanerParameters(
                ddlConfig, new ConnectionSupplier(sqliteOracleAdapter.createSqlConnectionSupplier()));
    }

    private ImmutableOracleNamespaceCleanerParameters.Builder createNamespaceCleanerParameters(
            OracleDdlConfig ddlConfig, ConnectionSupplier connectionSupplier) {
        OracleTableNameGetter tableNameGetter = new OracleTableNameGetter(ddlConfig);
        Function<TableReference, OracleDdlTable> ddlTableFactory = (tableReference) -> OracleDdlTable.create(
                tableReference,
                connectionSupplier,
                ddlConfig,
                tableNameGetter,
                new TableValueStyleCache(),
                executorService);
        return OracleNamespaceCleanerParameters.builder()
                .tablePrefix(ddlConfig.tablePrefix())
                .overflowTablePrefix(ddlConfig.overflowTablePrefix())
                .userId(TEST_USER)
                .oracleDdlTableFactory(ddlTableFactory)
                .connectionSupplier(connectionSupplier)
                .tableNameGetter(tableNameGetter);
    }

    private class UnstableConnectionSupplier extends ConnectionSupplier {
        private final AtomicInteger counter;
        private final int numberOfConnectionsBeforeThrowingOnce;

        UnstableConnectionSupplier(int numberOfConnectionsBeforeThrowingOnce) {
            super(sqliteOracleAdapter.createSqlConnectionSupplier());
            this.numberOfConnectionsBeforeThrowingOnce = numberOfConnectionsBeforeThrowingOnce;
            this.counter = new AtomicInteger();
        }

        @Override
        public SqlConnection get() {
            if (numberOfConnectionsBeforeThrowingOnce == counter.incrementAndGet()) {
                throw new RuntimeException("induced failure");
            }
            return super.get();
        }
    }
}
