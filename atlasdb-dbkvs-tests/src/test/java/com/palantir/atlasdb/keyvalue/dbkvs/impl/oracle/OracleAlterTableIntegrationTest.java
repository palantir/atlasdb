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
import com.palantir.atlasdb.keyvalue.dbkvs.AlterTableMetadataReference;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableTableReferenceWrapper;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetterImpl;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManagerV2;
import com.palantir.atlasdb.logging.KvsProfilingLogger.CallableCheckedException;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.exception.TableMappingNotFoundException;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(DbKvsOracleExtension.class)
public final class OracleAlterTableIntegrationTest {
    @RegisterExtension
    public static final TestResourceManagerV2 TRM = new TestResourceManagerV2(DbKvsOracleExtension::createKvs);

    private static final Namespace NAMESPACE = Namespace.create("test_namespace");
    private static final TableReference TABLE_REFERENCE = TableReference.create(NAMESPACE, "foo");
    private static final DbKeyValueServiceConfig CONFIG_WITH_ALTER = ImmutableDbKeyValueServiceConfig.builder()
            .from(DbKvsOracleExtension.getKvsConfig())
            .ddl(ImmutableOracleDdlConfig.builder()
                    .overflowMigrationState(OverflowMigrationState.FINISHED)
                    .addAlterTablesOrMetadataToMatch(ImmutableTableReferenceWrapper.of(TABLE_REFERENCE))
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

    @BeforeEach
    public void before() {
        defaultKvs = TRM.getDefaultKvs();
        connectionSupplier = DbKvsOracleExtension.getConnectionSupplier(defaultKvs);
        oracleTableNameGetter = OracleTableNameGetterImpl.createDefault(
                (OracleDdlConfig) DbKvsOracleExtension.getKvsConfig().ddl());
    }

    @AfterEach
    public void after() {
        defaultKvs.dropTables(defaultKvs.getAllTableNames());
        defaultKvs.dropTable(DbKvsOracleExtension.getKvsConfig().ddl().metadataTable());
        connectionSupplier.close();
    }

    @Test
    public void whenConfiguredWithTableReferenceAlterTableToMatchMetadataAndOldDataIsStillReadable() {
        whenConfiguredAlterTableToMatchMetadataAndOldDataIsStillReadable(() -> createKvs(CONFIG_WITH_ALTER));
    }

    @Test
    public void whenConfiguredWithPhysicalTableNameAlterTableToMatchMetadataAndOldDataIsStillReadable()
            throws TableMappingNotFoundException {
        CallableCheckedException<KeyValueService, TableMappingNotFoundException> workingKvsSupplier = () -> {
            String physicalTableName =
                    oracleTableNameGetter.getInternalShortTableName(connectionSupplier, TABLE_REFERENCE);
            return createKvs(getConfigWithAlterTableFromPhysicalTableName(physicalTableName));
        };
        whenConfiguredAlterTableToMatchMetadataAndOldDataIsStillReadable(workingKvsSupplier);
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

    private <E extends Exception> void whenConfiguredAlterTableToMatchMetadataAndOldDataIsStillReadable(
            CallableCheckedException<KeyValueService, E> workingKvsSupplier) throws E {
        defaultKvs.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        writeData(defaultKvs, ROW_1, TIMESTAMP_1);
        assertThatDataCanBeRead(defaultKvs, DEFAULT_CELL_1, TIMESTAMP_1, DEFAULT_VALUE_1);

        dropOverflowColumn();
        defaultKvs.putMetadataForTable(TABLE_REFERENCE, OLD_TABLE_METADATA.persistToBytes());
        assertThatThrownBy(() -> fetchData(defaultKvs, DEFAULT_CELL_1, TIMESTAMP_1));

        KeyValueService workingKvs = workingKvsSupplier.call();
        workingKvs.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        assertThatDataCanBeRead(defaultKvs, DEFAULT_CELL_1, TIMESTAMP_1, DEFAULT_VALUE_1);
        assertThatOverflowColumnExists();
        assertThatCode(() -> writeData(defaultKvs, ROW_2, TIMESTAMP_2)).doesNotThrowAnyException();
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
                                    tableName.toUpperCase(Locale.ROOT));
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

    private static DbKeyValueServiceConfig getConfigWithAlterTableFromPhysicalTableName(String physicalTableName) {
        return ImmutableDbKeyValueServiceConfig.builder()
                .from(CONFIG_WITH_ALTER)
                .ddl(ImmutableOracleDdlConfig.builder()
                        .from(CONFIG_WITH_ALTER.ddl())
                        .addAlterTablesOrMetadataToMatch(AlterTableMetadataReference.of(physicalTableName))
                        .build())
                .build();
    }
}
