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
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.exception.TableMappingNotFoundException;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class OracleAlterTableTest {

    private static final Namespace NAMESPACE = Namespace.create("test_namespace");
    private static final TableReference TABLE_REFERENCE = TableReference.create(NAMESPACE, "foo");

    private static final DbKeyValueServiceConfig CONFIG = ImmutableDbKeyValueServiceConfig.builder()
            .from(DbKvsOracleTestSuite.getKvsConfig())
            .ddl(ImmutableOracleDdlConfig.builder()
                    .overflowMigrationState(OverflowMigrationState.FINISHED)
                    .build())
            .build();

    private static final DbKeyValueServiceConfig CONFIG_WITH_ALTER = ImmutableDbKeyValueServiceConfig.builder()
            .from(CONFIG)
            .ddl(ImmutableOracleDdlConfig.builder()
                    .overflowMigrationState(OverflowMigrationState.FINISHED)
                    .addAlterTablesOrMetadataToMatch(TABLE_REFERENCE)
                    .build())
            .build();

    @ClassRule
    public static final TestResourceManager TRM =
            new TestResourceManager(() -> ConnectionManagerAwareDbKvs.create(CONFIG));

    private static final String COLUMN_NAME = "variable";

    private static final TableMetadata EXPECTED_TABLE_METADATA = TableMetadata.builder()
            .singleRowComponent("name", ValueType.STRING)
            .singleNamedColumn("var", COLUMN_NAME, ValueType.STRING)
            .build();

    private static final TableMetadata OLD_TABLE_METADATA = TableMetadata.builder()
            .singleRowComponent("name", ValueType.STRING)
            .singleNamedColumn("var", COLUMN_NAME, ValueType.FIXED_LONG)
            .build();

    private static final Cell DEFAULT_CELL = createCell("foo", "bar");
    private static final Cell DEFAULT_CELL_2 = createCell("something", "bar");

    private static final byte[] DEFAULT_VALUE = PtBytes.toBytes("amazing");
    private static final long TIMESTAMP = 1L;

    private ConnectionManagerAwareDbKvs defaultKvs;
    private ConnectionSupplier connectionSupplier;
    private OracleTableNameGetter oracleTableNameGetter;

    @Before
    public void setup() {
        defaultKvs = (ConnectionManagerAwareDbKvs) TRM.getDefaultKvs();
        connectionSupplier = DbKvsOracleTestSuite.getConnectionSupplier(defaultKvs);
        oracleTableNameGetter = OracleTableNameGetterImpl.createDefault((OracleDdlConfig) CONFIG.ddl());
    }

    @After
    public void tearDown() {
        defaultKvs.dropTables(defaultKvs.getAllTableNames());
        connectionSupplier.close();
    }

    @Test
    public void whenConfiguredAlterTableToMatchMetadataAndOldDataIsStillReadable() {
        defaultKvs.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        writeData(defaultKvs);
        assertThatDataCanBeRead(defaultKvs);
        dropOverflowColumn(defaultKvs);
        defaultKvs.putMetadataForTable(TABLE_REFERENCE, OLD_TABLE_METADATA.persistToBytes());
        assertThatThrownBy(() -> fetchData(defaultKvs));
        ConnectionManagerAwareDbKvs workingKvs = ConnectionManagerAwareDbKvs.create(CONFIG_WITH_ALTER);
        workingKvs.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        assertThatDataCanBeRead(workingKvs);
        assertThatOverflowColumnExists(workingKvs);
        assertThatDataCanBeWritten(workingKvs);
    }

    @Test
    public void whenConfiguredAlterTableDoesNothingWhenMatching() {
        ConnectionManagerAwareDbKvs kvsWithAlter = ConnectionManagerAwareDbKvs.create(CONFIG_WITH_ALTER);
        kvsWithAlter.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        writeData(kvsWithAlter);
        assertThatDataCanBeRead(defaultKvs);
    }

    private void assertThatDataCanBeWritten(ConnectionManagerAwareDbKvs kvs) {
        assertThatCode(() -> kvs.put(TABLE_REFERENCE, Map.of(DEFAULT_CELL_2, DEFAULT_VALUE), TIMESTAMP))
                .doesNotThrowAnyException();
    }

    private void assertThatOverflowColumnExists(ConnectionManagerAwareDbKvs kvs) {
        assertThatCode(() -> {
                    String tableName =
                            oracleTableNameGetter.getInternalShortTableName(connectionSupplier, TABLE_REFERENCE);
                    boolean columnExists = kvs.getSqlConnectionSupplier()
                            .get()
                            .selectExistsUnregisteredQuery(
                                    "SELECT 1 FROM user_tab_cols WHERE TABLE_NAME = ? AND COLUMN_NAME = 'OVERFLOW'",
                                    tableName.toUpperCase());
                    assertThat(columnExists).isTrue();
                })
                .doesNotThrowAnyException();
    }

    private void dropOverflowColumn(ConnectionManagerAwareDbKvs kvs) {
        try {
            String tableName = oracleTableNameGetter.getInternalShortTableName(connectionSupplier, TABLE_REFERENCE);
            kvs.getSqlConnectionSupplier()
                    .get()
                    .executeUnregisteredQuery("ALTER TABLE " + tableName + " DROP COLUMN overflow");
        } catch (TableMappingNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeData(KeyValueService kvs) {
        kvs.put(TABLE_REFERENCE, Map.of(DEFAULT_CELL, DEFAULT_VALUE), TIMESTAMP);
    }

    private static Map<Cell, Value> fetchData(KeyValueService kvs) {
        return kvs.get(TABLE_REFERENCE, Map.of(DEFAULT_CELL, TIMESTAMP + 1));
    }

    private static void assertThatDataCanBeRead(KeyValueService kvs) {
        Map<Cell, Value> values = fetchData(kvs);
        assertThat(values).hasSize(1);
        assertThat(values).containsKey(DEFAULT_CELL);
        Value value = values.get(DEFAULT_CELL);
        assertThat(value.getTimestamp()).isEqualTo(TIMESTAMP);
        assertThat(value.getContents()).isEqualTo(DEFAULT_VALUE);
    }

    private static Cell createCell(String row, String columnValue) {
        return Cell.create(PtBytes.toBytes(row), PtBytes.toBytes(columnValue));
    }
}
