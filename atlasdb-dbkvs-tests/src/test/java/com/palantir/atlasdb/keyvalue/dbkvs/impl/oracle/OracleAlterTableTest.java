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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableOracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
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
                    .alterTablesOrMetadataToMatch(ImmutableSet.of(TABLE_REFERENCE))
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

    private static final byte[] DEFAULT_VALUE = PtBytes.toBytes("amazing");
    private static final long TIMESTAMP = 1L;

    private KeyValueService kvs;
    private ConnectionSupplier connectionSupplier;

    @Before
    public void setup() {
        kvs = TRM.getDefaultKvs();
        connectionSupplier = DbKvsOracleTestSuite.getConnectionSupplier(kvs);
    }

    @After
    public void tearDown() {
        kvs.dropTables(kvs.getAllTableNames());
        connectionSupplier.close();
    }

    @Test
    public void canReadWriteData() {
        kvs.createTable(TABLE_REFERENCE, EXPECTED_TABLE_METADATA.persistToBytes());
        writeData();
        canReadData();
    }

    public void writeData() {
        kvs.put(TABLE_REFERENCE, Map.of(DEFAULT_CELL, DEFAULT_VALUE), TIMESTAMP);
    }

    public void canReadData() {
        Map<Cell, Value> values = kvs.get(TABLE_REFERENCE, Map.of(DEFAULT_CELL, TIMESTAMP));
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
