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

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetterImpl;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyle;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public final class OracleAlterTableIntegrationTest {
    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(DbKvsOracleTestSuite::createKvs);

    private static final Namespace NAMESPACE = Namespace.create("test_namespace");
    private static final TableReference TABLE_REFERENCE = TableReference.create(NAMESPACE, "foo");
    private static final DbKeyValueServiceConfig CONFIG_WITH_ALTER = ImmutableDbKeyValueServiceConfig.builder()
            .from(DbKvsOracleTestSuite.getKvsConfig())
            .ddl(ImmutableOracleDdlConfig.builder()
                    .overflowMigrationState(OverflowMigrationState.FINISHED)
                    .addAlterTablesOrMetadataToMatch(TABLE_REFERENCE)
                    .build())
            .build();

    private static final String COLUMN_NAME = "variable";

    private static final TableMetadata EXPECTED_TABLE_METADATA = TableMetadata.builder()
            .singleRowComponent("name", ValueType.STRING)
            .singleNamedColumn("var", COLUMN_NAME, ValueType.STRING)
            .build();

    private static final TableMetadata OLD_TABLE_METADATA = TableMetadata.builder()
            .singleRowComponent("name", ValueType.STRING)
            .singleNamedColumn("var", COLUMN_NAME, ValueType.FIXED_LONG)
            .build();

    private static final Cell DEFAULT_CELL_1 = createCell("foo", "bar");
    private static final Cell DEFAULT_CELL_2 = createCell("something", "bar");

    private static final byte[] DEFAULT_VALUE_1 = PtBytes.toBytes("amazing");
    private static final byte[] DEFAULT_VALUE_2 = PtBytes.toBytes("coolbeans");
    private static final long TIMESTAMP_1 = 1L;
    private static final long TIMESTAMP_2 = 2L;

    private static final Map<Cell, byte[]> ROW_1 = Map.of(DEFAULT_CELL_1, DEFAULT_VALUE_1);
    private static final Map<Cell, byte[]> ROW_2 = Map.of(DEFAULT_CELL_2, DEFAULT_VALUE_2);

    private KeyValueService defaultKvs;
    private ConnectionSupplier connectionSupplier;
    private OracleTableNameGetter oracleTableNameGetter;

    @Before
    public void before() {
        defaultKvs = TRM.getDefaultKvs();
        connectionSupplier = DbKvsOracleTestSuite.getConnectionSupplier(defaultKvs);
        oracleTableNameGetter = OracleTableNameGetterImpl.createDefault(
                (OracleDdlConfig) DbKvsOracleTestSuite.getKvsConfig().ddl());
    }

    @After
    public void after() {
        defaultKvs.dropTables(defaultKvs.getAllTableNames());
        defaultKvs.dropTable(DbKvsOracleTestSuite.getKvsConfig().ddl().metadataTable());
        connectionSupplier.close();
    }

    @Test
    public void whenConfiguredAlterMetadataToMatchTableSchemaAndOldDataIsStillReadable() {
        defaultKvs.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        writeData(defaultKvs, ROW_1, TIMESTAMP_1);
        assertThatDataCanBeRead(defaultKvs, DEFAULT_CELL_1, TIMESTAMP_1, DEFAULT_VALUE_1);

        modifyMetadataToNotHaveOverflow();
        // metadata is cached, thus we must create a new kvs to forcefully refresh the cache
        KeyValueService badKvs = createKvs(DbKvsOracleTestSuite.getKvsConfig());
        assertThatThrownBy(() -> writeData(badKvs, ROW_2, TIMESTAMP_2));

        KeyValueService workingKvs = createKvs(CONFIG_WITH_ALTER);
        workingKvs.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        assertThatDataCanBeRead(defaultKvs, DEFAULT_CELL_1, TIMESTAMP_1, DEFAULT_VALUE_1);
        assertThatMetadataHasOverflow();
        assertThatCode(() -> writeData(defaultKvs, ROW_2, TIMESTAMP_2)).doesNotThrowAnyException();
        assertThatDataCanBeRead(defaultKvs, DEFAULT_CELL_2, TIMESTAMP_2, DEFAULT_VALUE_2);
    }

    @Test
    public void whenConfiguredAlterTableToMatchMetadataAndOldDataIsStillReadable() {
        defaultKvs.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        writeData(defaultKvs, ROW_1, TIMESTAMP_1);
        assertThatDataCanBeRead(defaultKvs, DEFAULT_CELL_1, TIMESTAMP_1, DEFAULT_VALUE_1);

        dropOverflowColumn();
        defaultKvs.putMetadataForTable(TABLE_REFERENCE, OLD_TABLE_METADATA.persistToBytes());
        assertThatThrownBy(() -> fetchData(defaultKvs, DEFAULT_CELL_1, TIMESTAMP_1));

        KeyValueService workingKvs = createKvs(CONFIG_WITH_ALTER);
        workingKvs.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        assertThatDataCanBeRead(defaultKvs, DEFAULT_CELL_1, TIMESTAMP_1, DEFAULT_VALUE_1);
        assertThatOverflowColumnExists();
        assertThatCode(() -> writeData(defaultKvs, ROW_2, TIMESTAMP_2)).doesNotThrowAnyException();
        assertThatDataCanBeRead(defaultKvs, DEFAULT_CELL_2, TIMESTAMP_2, DEFAULT_VALUE_2);
    }

    @Test
    public void whenConfiguredAlterTableDoesNothingWhenMatching() {
        KeyValueService kvsWithAlter = createKvs(CONFIG_WITH_ALTER);
        kvsWithAlter.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        writeData(defaultKvs, ROW_1, TIMESTAMP_1);
        assertThatDataCanBeRead(defaultKvs, DEFAULT_CELL_1, TIMESTAMP_1, DEFAULT_VALUE_1);

        kvsWithAlter.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        writeData(defaultKvs, ROW_2, TIMESTAMP_2);
        assertThatDataCanBeRead(defaultKvs, DEFAULT_CELL_2, TIMESTAMP_2, DEFAULT_VALUE_2);
    }

    private KeyValueService createKvs(DbKeyValueServiceConfig config) {
        KeyValueService kvs = ConnectionManagerAwareDbKvs.create(config);
        TRM.registerKvs(kvs);
        return kvs;
    }

    private void assertThatOverflowColumnExists() {
        assertThatCode(() -> {
                    String tableName =
                            oracleTableNameGetter.getInternalShortTableName(connectionSupplier, TABLE_REFERENCE);
                    boolean columnExists = connectionSupplier
                            .get()
                            .selectExistsUnregisteredQuery(
                                    "SELECT 1 FROM user_tab_cols WHERE TABLE_NAME = ? AND COLUMN_NAME = 'OVERFLOW'",
                                    tableName.toUpperCase());
                    assertThat(columnExists).isTrue();
                })
                .doesNotThrowAnyException();
    }

    private void dropOverflowColumn() {
        try {
            String tableName = oracleTableNameGetter.getInternalShortTableName(connectionSupplier, TABLE_REFERENCE);
            connectionSupplier.get().executeUnregisteredQuery("ALTER TABLE " + tableName + " DROP COLUMN overflow");
        } catch (TableMappingNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void modifyMetadataToNotHaveOverflow() {
        LoggerFactory.getLogger(OracleAlterTableIntegrationTest.class).info("booboo");
        AgnosticResultSet results = connectionSupplier
                .get()
                .selectResultSetUnregisteredQuery(
                        "SELECT table_size FROM "
                                + CONFIG_WITH_ALTER.ddl().metadataTable().getQualifiedName() + " WHERE table_name = ?",
                        TABLE_REFERENCE.getQualifiedName());
        System.out.println(results);
        connectionSupplier
                .get()
                .updateUnregisteredQuery(
                        "UPDATE " + CONFIG_WITH_ALTER.ddl().metadataTable().getQualifiedName()
                                + " SET table_size = ? WHERE table_name = ?",
                        TableValueStyle.RAW.getId(),
                        TABLE_REFERENCE.getQualifiedName());
        LoggerFactory.getLogger(OracleAlterTableIntegrationTest.class).info("boohoo");
        results = connectionSupplier
                .get()
                .selectResultSetUnregisteredQuery(
                        "SELECT table_size FROM "
                                + CONFIG_WITH_ALTER.ddl().metadataTable().getQualifiedName() + " WHERE table_name = ?",
                        TABLE_REFERENCE.getQualifiedName());
        System.out.println(results);
        Uninterruptibles.sleepUninterruptibly(15, TimeUnit.MINUTES);
    }

    private void assertThatMetadataHasOverflow() {
        AgnosticResultSet results = connectionSupplier
                .get()
                .selectResultSetUnregisteredQuery(
                        "SELECT table_size FROM "
                                + CONFIG_WITH_ALTER.ddl().metadataTable().getQualifiedName() + " WHERE table_name = ?",
                        TABLE_REFERENCE.getQualifiedName());
        int type = Iterables.getOnlyElement(results.rows()).getInteger("table_size");
        assertThat(type).isEqualTo(TableValueStyle.OVERFLOW.getId());
    }

    private static void writeData(KeyValueService kvs, Map<Cell, byte[]> value, long timestamp) {
        kvs.put(TABLE_REFERENCE, value, timestamp);
    }

    private static Map<Cell, Value> fetchData(KeyValueService kvs, Cell cell, long timestamp) {
        return kvs.get(TABLE_REFERENCE, Map.of(cell, timestamp + 1));
    }

    private static void assertThatDataCanBeRead(KeyValueService kvs, Cell cell, long timestamp, byte[] content) {
        Map<Cell, Value> values = fetchData(kvs, cell, timestamp);
        assertThat(values).hasSize(1);
        assertThat(values).containsKey(cell);
        Value value = values.get(cell);
        assertThat(value.getTimestamp()).isEqualTo(timestamp);
        assertThat(value.getContents()).isEqualTo(content);
    }

    private static Cell createCell(String row, String columnValue) {
        return Cell.create(PtBytes.toBytes(row), PtBytes.toBytes(columnValue));
    }
}
