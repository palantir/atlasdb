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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableTableReferenceWrapper;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleErrorConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.CaseSensitivity;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyle;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCache;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.nexus.db.sql.SqlConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public final class OracleDdlTableTest {
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("ns.test");
    private static final OracleDdlConfig TABLE_MAPPING_DEFAULT_CONFIG = ImmutableOracleDdlConfig.builder()
            .overflowMigrationState(OverflowMigrationState.FINISHED)
            .useTableMapping(true)
            .addAlterTablesOrMetadataToMatch(ImmutableTableReferenceWrapper.of(TEST_TABLE))
            .build();
    private static final OracleDdlConfig NON_TABLE_MAPPING_DEFAULT_CONFIG = ImmutableOracleDdlConfig.builder()
            .overflowMigrationState(OverflowMigrationState.FINISHED)
            .useTableMapping(false)
            .build();

    private static final String PREFIXED_TABLE_NAME =
            TABLE_MAPPING_DEFAULT_CONFIG.tablePrefix() + DbKvs.internalTableName(TEST_TABLE);

    private static final String PREFIXED_OVERFLOW_TABLE_NAME =
            TABLE_MAPPING_DEFAULT_CONFIG.overflowTablePrefix() + DbKvs.internalTableName(TEST_TABLE);

    private static final String INTERNAL_TABLE_NAME = "iaminternal";
    private static final String INTERNAL_OVERFLOW_TABLE_NAME = "iaminternaloverflow";

    private static final String MISSING_OVERFLOW_EXCEPTION_MESSAGE =
            "Unsupported table change from raw to overflow, likely due to a schema change.";

    @Mock
    private ConnectionSupplier connectionSupplier;

    @Mock
    private OracleTableNameGetter tableNameGetter;

    @Mock
    private SqlConnection sqlConnection;

    @Mock
    private TableValueStyleCache tableValueStyleCache;

    private OracleDdlTable tableMappingDdlTable;
    private OracleDdlTable nonTableMappingDdlTable;
    private ExecutorService executorService;

    @BeforeEach
    public void before() {
        executorService = PTExecutors.newSingleThreadExecutor();
        tableMappingDdlTable = createOracleDdlTable(TABLE_MAPPING_DEFAULT_CONFIG);
        nonTableMappingDdlTable = createOracleDdlTable(NON_TABLE_MAPPING_DEFAULT_CONFIG);

        when(connectionSupplier.get()).thenReturn(sqlConnection);
    }

    @AfterEach
    public void after() {
        executorService.shutdown();
    }

    @Test
    public void dropTablesDropsAllPhysicalTablesWithPurge() throws TableMappingNotFoundException {
        createTableAndOverflow();

        tableMappingDdlTable.drop();

        verifyTableDeleted(INTERNAL_TABLE_NAME);
        verifyTableDeleted(INTERNAL_OVERFLOW_TABLE_NAME);
    }

    @Test
    public void dropTablesDropsOnlyNonOverflowTableWithoutThrowing() throws TableMappingNotFoundException {
        createTable();
        when(tableNameGetter.getInternalShortOverflowTableName(connectionSupplier, TEST_TABLE))
                .thenThrow(TableMappingNotFoundException.class);

        tableMappingDdlTable.drop();

        verifyTableDeleted(INTERNAL_TABLE_NAME);
    }

    @Test
    public void dropTablesDropsOnlyOverflowTableWithoutThrowing() throws TableMappingNotFoundException {
        createOverflowTable();
        when(tableNameGetter.getInternalShortTableName(connectionSupplier, TEST_TABLE))
                .thenThrow(TableMappingNotFoundException.class);

        tableMappingDdlTable.drop();

        verifyTableDeleted(INTERNAL_OVERFLOW_TABLE_NAME);
    }

    @Test
    public void dropTablesDeletesTableMappingIfTableMappingConfigured() throws TableMappingNotFoundException {
        createTableAndOverflow();

        tableMappingDdlTable.drop();

        verify(sqlConnection)
                .executeUnregisteredQuery(
                        "DELETE FROM " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE + " WHERE table_name = ?",
                        PREFIXED_TABLE_NAME);
        verify(sqlConnection)
                .executeUnregisteredQuery(
                        "DELETE FROM " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE + " WHERE table_name = ?",
                        PREFIXED_OVERFLOW_TABLE_NAME);
    }

    @Test
    public void dropTablesDoesNotDeleteTableMappingIfTableMappingNotConfigured() throws TableMappingNotFoundException {
        createTableAndOverflow();

        nonTableMappingDdlTable.drop();

        verify(sqlConnection, never())
                .executeUnregisteredQuery(contains(AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE), any());
    }

    @Test
    public void dropTablesDeletesTableMetadataCaseSensitively() throws TableMappingNotFoundException {
        createTableAndOverflow();

        tableMappingDdlTable.drop();

        verify(sqlConnection)
                .executeUnregisteredQuery(
                        "DELETE FROM " + TABLE_MAPPING_DEFAULT_CONFIG.metadataTable() + " WHERE table_name = ?",
                        TEST_TABLE.getQualifiedName());
    }

    @Test
    public void dropTablesDeletesTableMetadataCaseInsensitivelyIfCaseInsensitiveSet()
            throws TableMappingNotFoundException {
        when(sqlConnection.selectCount(any(), any(), any())).thenReturn(1L);
        createTableAndOverflow();

        tableMappingDdlTable.drop(CaseSensitivity.CASE_INSENSITIVE);

        verify(sqlConnection)
                .executeUnregisteredQuery(
                        "DELETE FROM " + TABLE_MAPPING_DEFAULT_CONFIG.metadataTable() + " WHERE LOWER(table_name) ="
                                + " LOWER(?)",
                        TEST_TABLE.getQualifiedName());
    }

    @Test
    public void dropTablesThrowsIfMultipleMatchingTableReferencesExistAndCaseInsensitiveDrop()
            throws TableMappingNotFoundException {
        long numberOfMatchingTableReferences = 2;
        when(sqlConnection.selectCount(any(), any(), any())).thenReturn(numberOfMatchingTableReferences);
        createTableAndOverflow();

        assertThatLoggableExceptionThrownBy(() -> tableMappingDdlTable.drop(CaseSensitivity.CASE_INSENSITIVE))
                .hasLogMessage("There are multiple tables that have the same case insensitive table reference."
                        + " Throwing to avoid accidentally deleting the wrong table reference."
                        + " Please contact support to delete the metadata, which will involve deleting the row from"
                        + " the DB manually.")
                .hasExactlyArgs(
                        SafeArg.of("numberOfMatchingTableReferences", numberOfMatchingTableReferences),
                        UnsafeArg.of("tableReference", TEST_TABLE));
    }

    @Test
    public void dropTablesDoesNotThrowIfMultipleMatchingTableReferencesExistAndCaseSensitiveDrop()
            throws TableMappingNotFoundException {
        // This mock is never called on the case sensitive code path, but this makes the test easier to read.
        when(sqlConnection.selectCount(any(), any(), any())).thenReturn((long) 2);
        createTableAndOverflow();

        assertThatCode(() -> tableMappingDdlTable.drop()).doesNotThrowAnyException();
    }

    @Test
    public void createDoesNothingWhenAlterSpecifiedButOverflowColumnExists() throws TableMappingNotFoundException {
        createTableAndOverflow();
        setTableToHaveOverflowColumn(true);
        setTableValueStyleCacheOverflowConfigForTable(true);
        assertThatCode(() -> tableMappingDdlTable.create(createMetadata(true))).doesNotThrowAnyException();
        verifyTableNotAltered();
    }

    @Test
    public void createAltersTableIfConfiguredAndOverflowTableExists() throws TableMappingNotFoundException {
        testAlterTableForMigrationState(OverflowMigrationState.FINISHED);
        verifyTableAltered();
    }

    @Test
    public void createDoesNothingWhenAlterSpecifiedButMigrationUnstarted() throws TableMappingNotFoundException {
        testAlterTableForMigrationState(OverflowMigrationState.UNSTARTED);
        verifyTableNotAltered();
    }

    @Test
    public void createDoesNothingWhenAlterSpecifiedButMigrationInProgress() throws TableMappingNotFoundException {
        testAlterTableForMigrationState(OverflowMigrationState.IN_PROGRESS);
        verifyTableNotAltered();
    }

    @Test
    public void createDoesNothingWhenAlterSpecifiedButMigrationFinishing() throws TableMappingNotFoundException {
        testAlterTableForMigrationState(OverflowMigrationState.FINISHING);
        verifyTableNotAltered();
    }

    private void testAlterTableForMigrationState(OverflowMigrationState overflowMigrationState)
            throws TableMappingNotFoundException {
        createTableAndOverflow();
        setTableToHaveOverflowColumn(false);
        setTableValueStyleCacheOverflowConfigForTable(true);
        OracleDdlTable ddlTable = createOracleDdlTable(ImmutableOracleDdlConfig.builder()
                .from(TABLE_MAPPING_DEFAULT_CONFIG)
                .overflowMigrationState(overflowMigrationState)
                .build());
        ddlTable.create(createMetadata(true));
    }

    @Test
    public void createDoesNothingWhenAlterSpecifiedButOverflowTableDoesNotExist() throws TableMappingNotFoundException {
        createTable();
        setOverflowTableToExist(false);
        setTableToHaveOverflowColumn(true);
        setTableValueStyleCacheOverflowConfigForTable(true);
        tableMappingDdlTable.create(createMetadata(true));
        verifyTableNotAltered();
    }

    @Test
    public void createDoesNothingWhenAlterSpecifiedButOverflowColumnIsNotNeeded() throws TableMappingNotFoundException {
        createTableAndOverflow();
        setTableToHaveOverflowColumn(false);
        setTableValueStyleCacheOverflowConfigForTable(false);
        assertThatCode(() -> tableMappingDdlTable.create(createMetadata(false))).doesNotThrowAnyException();
        verifyTableNotAltered();
    }

    @Test
    public void createDoesNotAlterTableIfOverflowColumnNeededButTableNotListedInConfig()
            throws TableMappingNotFoundException {
        createTableAndOverflow();
        setTableToHaveOverflowColumn(false);
        setTableValueStyleCacheOverflowConfigForTable(false);
        OracleDdlTable ddlTable = createOracleDdlTable(ImmutableOracleDdlConfig.builder()
                .from(TABLE_MAPPING_DEFAULT_CONFIG)
                .alterTablesOrMetadataToMatch(Set.of())
                .build());
        assertThatThrownBy(() -> ddlTable.create(createMetadata(true)))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining(MISSING_OVERFLOW_EXCEPTION_MESSAGE);
        verifyTableNotAltered();
    }

    @Test
    public void createThrowsWhenTableMappingMissingAndTableShouldBeAltered() throws TableMappingNotFoundException {
        createTableAndOverflow();
        setTableToHaveOverflowColumn(false);
        setTableValueStyleCacheOverflowConfigForTable(true);
        when(tableNameGetter.getInternalShortTableName(connectionSupplier, TEST_TABLE))
                .thenThrow(new TableMappingNotFoundException("foo"));
        assertThatThrownBy(() -> tableMappingDdlTable.create(createMetadata(true)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining(
                        "Unable to test whether table must be altered to have overflow column due to a table mapping"
                                + " error.");
    }

    @Test
    public void createDoesNotThrowTableMappingExceptionWhenTableIsNotListedToBeAlteredInConfig()
            throws TableMappingNotFoundException {
        createTableAndOverflow();
        setTableToHaveOverflowColumn(false);
        setTableValueStyleCacheOverflowConfigForTable(true);
        when(tableNameGetter.getInternalShortTableName(connectionSupplier, TEST_TABLE))
                .thenThrow(new TableMappingNotFoundException("foo"));
        OracleDdlTable ddlTable = createOracleDdlTable(ImmutableOracleDdlConfig.builder()
                .from(TABLE_MAPPING_DEFAULT_CONFIG)
                .alterTablesOrMetadataToMatch(Set.of())
                .build());
        assertThatCode(() -> ddlTable.create(createMetadata(true))).doesNotThrowAnyException();
    }

    @Test
    public void createDoesNotThrowWhenOverflowColumnAlreadyExistsAndAlterIsRan() throws TableMappingNotFoundException {
        createTableAndOverflow();
        setTableToHaveOverflowColumn(false);
        setTableValueStyleCacheOverflowConfigForTable(true);
        doThrow(PalantirSqlException.create(OracleErrorConstants.ORACLE_COLUMN_ALREADY_EXISTS))
                .when(sqlConnection)
                .executeUnregisteredQuery("ALTER TABLE " + INTERNAL_TABLE_NAME + " ADD (overflow NUMBER(38))");
        tableMappingDdlTable.create(createMetadata(true));
        verifyTableAltered();
    }

    @Test
    public void conservativeSweptTablesHaveIndexCompression2() {
        for (int numColumns = 0; numColumns <= 2; numColumns++) {
            TableMetadata.Builder metadataBuilder = TableMetadata.builder().sweepStrategy(SweepStrategy.CONSERVATIVE);
            for (int colName = 0; colName < numColumns; colName++) {
                metadataBuilder.singleNamedColumn("c" + colName, "col" + colName, ValueType.STRING);
            }
            TableMetadata metadata = metadataBuilder.build();
            assertThat(OracleDdlTable.getOptimalIndexCompression(metadata)).isEqualTo(2);
        }
    }

    @Test
    public void thoroughSweptTablesWithDynamicColsHaveIndexCompression1() {
        TableMetadata.Builder metadataBuilder = TableMetadata.builder().sweepStrategy(SweepStrategy.THOROUGH);
        TableMetadata metadata = metadataBuilder
                .dynamicColumns(List.of(NameComponentDescription.of("c", ValueType.STRING)), ValueType.STRING)
                .build();
        assertThat(OracleDdlTable.getOptimalIndexCompression(metadata)).isEqualTo(1);
    }

    @Test
    public void thoroughSweptTablesWithMultipleNamedColsHaveIndexCompression1() {
        for (int numColumns = 2; numColumns <= 3; numColumns++) {
            TableMetadata.Builder metadataBuilder = TableMetadata.builder().sweepStrategy(SweepStrategy.THOROUGH);
            List<NamedColumnDescription> columns = new ArrayList<>();
            for (int colDesc = 1; colDesc <= numColumns; colDesc++) {
                columns.add(new NamedColumnDescription(
                        "c" + colDesc, "col" + colDesc, ColumnValueDescription.forType(ValueType.STRING)));
            }
            TableMetadata metadata = metadataBuilder
                    .columns(new ColumnMetadataDescription(columns))
                    .build();
            assertThat(OracleDdlTable.getOptimalIndexCompression(metadata)).isEqualTo(1);
        }
    }

    @Test
    public void thoroughSweptTablesWithOneNamedColHaveIndexCompressionDisabled() {
        TableMetadata.Builder metadataBuilder = TableMetadata.builder().sweepStrategy(SweepStrategy.THOROUGH);
        TableMetadata metadata = metadataBuilder
                .columns(new ColumnMetadataDescription(List.of(
                        new NamedColumnDescription("c", "col", ColumnValueDescription.forType(ValueType.STRING)))))
                .build();
        assertThat(OracleDdlTable.getOptimalIndexCompression(metadata)).isEqualTo(0);
    }

    private void createTable() throws TableMappingNotFoundException {
        // Not all tests will call OracleDdlTable#createTable, which makes sense as this test "creates" it for them!
        when(tableNameGetter.generateShortTableName(connectionSupplier, TEST_TABLE))
                .thenReturn(INTERNAL_TABLE_NAME);
        when(tableNameGetter.getPrefixedTableName(TEST_TABLE)).thenReturn(PREFIXED_TABLE_NAME);
        when(tableNameGetter.getInternalShortTableName(connectionSupplier, TEST_TABLE))
                .thenReturn(INTERNAL_TABLE_NAME);
        when(sqlConnection.selectExistsUnregisteredQuery(
                        eq("SELECT 1 FROM "
                                + TABLE_MAPPING_DEFAULT_CONFIG.metadataTable().getQualifiedName()
                                + " WHERE table_name = ?"),
                        eq(TEST_TABLE.getQualifiedName())))
                .thenReturn(true);
    }

    private void createOverflowTable() throws TableMappingNotFoundException {
        when(tableNameGetter.getPrefixedOverflowTableName(TEST_TABLE)).thenReturn(PREFIXED_OVERFLOW_TABLE_NAME);
        setOverflowTableToExist(true);
    }

    private void createTableAndOverflow() throws TableMappingNotFoundException {
        createTable();
        createOverflowTable();
    }

    private void verifyTableDeleted(String tableName) {
        verify(sqlConnection).executeUnregisteredQuery("DROP TABLE " + tableName + " PURGE");
    }

    private byte[] createMetadata(boolean hasOverflow) {
        return TableMetadata.builder()
                .singleNamedColumn("foo", "foobar", hasOverflow ? ValueType.STRING : ValueType.FIXED_LONG)
                .build()
                .persistToBytes();
    }

    private void setTableToHaveOverflowColumn(boolean hasColumn) {
        when(sqlConnection.selectExistsUnregisteredQuery(
                        eq("SELECT 1 FROM user_tab_cols WHERE TABLE_NAME = ? AND COLUMN_NAME = 'OVERFLOW'"),
                        eq(INTERNAL_TABLE_NAME.toUpperCase(Locale.ROOT))))
                .thenReturn(hasColumn);
    }

    private void verifyTableAltered() {
        verifyNumberOfTimesTableAltered(1);
    }

    private void verifyTableNotAltered() {
        verifyNumberOfTimesTableAltered(0);
    }

    private void verifyNumberOfTimesTableAltered(int numberOfTimes) {
        verify(sqlConnection, times(numberOfTimes))
                .executeUnregisteredQuery("ALTER TABLE " + INTERNAL_TABLE_NAME + " ADD (overflow NUMBER(38))");
    }

    private OracleDdlTable createOracleDdlTable(OracleDdlConfig config) {
        return OracleDdlTable.create(
                TEST_TABLE, connectionSupplier, config, tableNameGetter, tableValueStyleCache, executorService);
    }

    private void setTableValueStyleCacheOverflowConfigForTable(boolean hasOverflow) {
        TableValueStyle style = hasOverflow ? TableValueStyle.OVERFLOW : TableValueStyle.RAW;
        when(tableValueStyleCache.getTableType(eq(connectionSupplier), eq(TEST_TABLE), any()))
                .thenReturn(style);
    }

    private void setOverflowTableToExist(boolean exists) throws TableMappingNotFoundException {
        when(tableNameGetter.getInternalShortOverflowTableName(connectionSupplier, TEST_TABLE))
                .thenReturn(INTERNAL_OVERFLOW_TABLE_NAME);
        when(sqlConnection.selectExistsUnregisteredQuery(
                        eq("SELECT 1 FROM user_tables WHERE TABLE_NAME = ?"),
                        eq(INTERNAL_OVERFLOW_TABLE_NAME.toUpperCase(Locale.ROOT))))
                .thenReturn(exists);
    }
}
