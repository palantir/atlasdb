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
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbDdlTable;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCache;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.nexus.db.sql.SqlConnection;
import java.util.concurrent.ExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class OracleDdlTableTest {
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("ns.test");
    private static final OracleDdlConfig TABLE_MAPPING_DEFAULT_CONFIG = ImmutableOracleDdlConfig.builder()
            .overflowMigrationState(OverflowMigrationState.UNSTARTED)
            .useTableMapping(true)
            .build();

    private static final OracleDdlConfig NON_TABLE_MAPPING_DEFAULT_CONFIG = ImmutableOracleDdlConfig.builder()
            .overflowMigrationState(OverflowMigrationState.UNSTARTED)
            .useTableMapping(false)
            .build();

    private static final String PREFIXED_TABLE_NAME =
            TABLE_MAPPING_DEFAULT_CONFIG.tablePrefix() + DbKvs.internalTableName(TEST_TABLE);

    private static final String PREFIXED_OVERFLOW_TABLE_NAME =
            TABLE_MAPPING_DEFAULT_CONFIG.overflowTablePrefix() + DbKvs.internalTableName(TEST_TABLE);

    private static final String INTERNAL_TABLE_NAME = "iaminternal";
    private static final String INTERNAL_OVERFLOW_TABLE_NAME = "iaminternaloverflow";

    @Mock
    private ConnectionSupplier connectionSupplier;

    @Mock
    private OracleTableNameGetter tableNameGetter;

    @Mock
    private SqlConnection sqlConnection;

    private TableValueStyleCache tableValueStyleCache;
    private DbDdlTable tableMappingDdlTable;
    private DbDdlTable nonTableMappingDdlTable;
    private ExecutorService executorService;

    @Before
    public void before() {
        tableValueStyleCache = new TableValueStyleCache();
        executorService = PTExecutors.newSingleThreadExecutor();
        tableMappingDdlTable = OracleDdlTable.create(
                TEST_TABLE,
                connectionSupplier,
                TABLE_MAPPING_DEFAULT_CONFIG,
                tableNameGetter,
                tableValueStyleCache,
                executorService);

        nonTableMappingDdlTable = OracleDdlTable.create(
                TEST_TABLE,
                connectionSupplier,
                NON_TABLE_MAPPING_DEFAULT_CONFIG,
                tableNameGetter,
                tableValueStyleCache,
                executorService);

        when(connectionSupplier.get()).thenReturn(sqlConnection);
    }

    @After
    public void after() {
        executorService.shutdown();
    }

    @Test
    public void dropTablesDropsAllPhysicalTablesWithPurge() throws TableMappingNotFoundException {
        createTable();
        createOverflowTable();
        tableMappingDdlTable.drop();

        verifyTableDeleted(INTERNAL_TABLE_NAME);
        verifyTableDeleted(INTERNAL_OVERFLOW_TABLE_NAME);
    }

    @Test
    public void dropTablesDropsOnlyNonOverflowTableWithoutThrowing() throws TableMappingNotFoundException {
        createTable();
        tableMappingDdlTable.drop();
        verifyTableDeleted(INTERNAL_TABLE_NAME);
    }

    @Test
    public void dropTablesDropsOnlyOverflowTableWithoutThrowing() throws TableMappingNotFoundException {
        createOverflowTable();
        tableMappingDdlTable.drop();
        verifyTableDeleted(INTERNAL_OVERFLOW_TABLE_NAME);
    }

    @Test
    public void dropTablesDeletesTableMappingIfTableMappingConfigured() throws TableMappingNotFoundException {
        createTable();
        createOverflowTable();
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
        createTable();
        createOverflowTable();
        nonTableMappingDdlTable.drop();
        verify(sqlConnection, never())
                .executeUnregisteredQuery(contains(AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE), any());
    }

    @Test
    public void dropTablesDeletesTableMetadata() throws TableMappingNotFoundException {
        createTable();
        createOverflowTable();
        tableMappingDdlTable.drop();
        verify(sqlConnection)
                .executeUnregisteredQuery(
                        "DELETE FROM " + TABLE_MAPPING_DEFAULT_CONFIG.metadataTable() + " WHERE table_name = ?",
                        TEST_TABLE.getQualifiedName());
    }

    private void createTable() throws TableMappingNotFoundException {
        when(tableNameGetter.getPrefixedTableName(TEST_TABLE)).thenReturn(PREFIXED_TABLE_NAME);
        when(tableNameGetter.getInternalShortTableName(connectionSupplier, TEST_TABLE))
                .thenReturn(INTERNAL_TABLE_NAME);
    }

    private void createOverflowTable() throws TableMappingNotFoundException {
        when(tableNameGetter.getPrefixedOverflowTableName(TEST_TABLE)).thenReturn(PREFIXED_OVERFLOW_TABLE_NAME);
        when(tableNameGetter.getInternalShortOverflowTableName(connectionSupplier, TEST_TABLE))
                .thenReturn(INTERNAL_OVERFLOW_TABLE_NAME);
    }

    private void verifyTableDeleted(String tableName) {
        verify(sqlConnection).executeUnregisteredQuery("DROP TABLE " + tableName + " PURGE");
    }
}
