/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.table.description;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings({"checkstyle:Indentation", "checkstyle:RightCurly", "checkstyle:WhitespaceAround"})
public class SchemasTest {
    private static final String TABLE_NAME = "testTable";
    private static final TableReference TABLE_REF = TableReference.createWithEmptyNamespace("testTable");
    private static final Namespace NAMESPACE = Namespace.create("testNamespace");
    private KeyValueService kvs;

    @Before
    public void before() {
        kvs = mock(KeyValueService.class);
    }

    @Test
    public void testGetFullTableReferenceString() {
        assertThat(Schemas.getTableReferenceString(TABLE_NAME, NAMESPACE))
                .isEqualTo("TableReference.createFromFullyQualifiedName(\"" + NAMESPACE.getName() + "." + TABLE_NAME
                        + "\")");
    }

    @Test
    public void testGetFullTableReferenceStringLegacy() {
        assertThat(Schemas.getTableReferenceString(TABLE_NAME, Namespace.create("met")))
                .isEqualTo("TableReference.createWithEmptyNamespace(\"" + TABLE_NAME + "\")");
    }

    @Test
    public void testGetFullTableReferenceStringEmptyNamespace() {
        assertThat(Schemas.getTableReferenceString(TABLE_NAME, Namespace.EMPTY_NAMESPACE))
                .isEqualTo("TableReference.createWithEmptyNamespace(\"" + TABLE_NAME + "\")");
    }

    @Test
    public void testCreateTable() {
        ArgumentCaptor<Map<TableReference, byte[]>> argumentCaptor = ArgumentCaptor.forClass(Map.class);
        doNothing().when(kvs).createTables(argumentCaptor.capture());

        Schemas.createTable(kvs, TABLE_REF, getSimpleTableDefinition(TABLE_REF));
        verify(kvs).createTables(eq(argumentCaptor.getValue()));
        assertThat(argumentCaptor.getValue())
                .extractingByKey(TABLE_REF)
                .asInstanceOf(InstanceOfAssertFactories.BYTE_ARRAY)
                .isEqualTo(getSimpleTableDefinitionAsBytes(TABLE_REF));
    }

    @Test
    public void testCreateTables() {
        ArgumentCaptor<Map<TableReference, byte[]>> argumentCaptor = ArgumentCaptor.forClass(Map.class);
        doNothing().when(kvs).createTables(argumentCaptor.capture());

        TableReference tableName1 = TableReference.createWithEmptyNamespace(TABLE_NAME + "1");
        TableReference tableName2 = TableReference.createWithEmptyNamespace(TABLE_NAME + "2");

        Map<TableReference, TableDefinition> tables = ImmutableMap.of(
                tableName1, getSimpleTableDefinition(tableName1),
                tableName2, getSimpleTableDefinition(tableName2));

        Schemas.createTables(kvs, tables);
        verify(kvs).createTables(eq(argumentCaptor.getValue()));
        assertThat(argumentCaptor.getValue())
                .extractingByKey(tableName1)
                .asInstanceOf(InstanceOfAssertFactories.BYTE_ARRAY)
                .isEqualTo(getSimpleTableDefinitionAsBytes(tableName1));
        assertThat(argumentCaptor.getValue())
                .extractingByKey(tableName2)
                .asInstanceOf(InstanceOfAssertFactories.BYTE_ARRAY)
                .isEqualTo(getSimpleTableDefinitionAsBytes(tableName2));
    }

    @Test
    public void testDeleteTablesForSweepSchema() {
        Set<TableReference> allTableNames =
                ImmutableSet.of(TableReference.createFromFullyQualifiedName("sweep.priority"));
        when(kvs.getAllTableNames()).thenReturn(allTableNames);

        Schemas.deleteTablesAndIndexes(SweepSchema.INSTANCE.getLatestSchema(), kvs);
        verify(kvs).getAllTableNames();
        verify(kvs).dropTables(allTableNames);
        verify(kvs).getAllTableNames();
    }

    private static TableDefinition getSimpleTableDefinition(TableReference tableRef) {
        return new TableDefinition() {
            {
                javaTableName(tableRef.getTableName());
                rowName();
                rowComponent("rowName", ValueType.STRING);
                columns();
                column("col1", "1", ValueType.VAR_LONG);
                column("col2", "2", ValueType.VAR_LONG);
                conflictHandler(ConflictHandler.IGNORE_ALL);
            }
        };
    }

    private static byte[] getSimpleTableDefinitionAsBytes(TableReference tableRef) {
        return getSimpleTableDefinition(tableRef).toTableMetadata().persistToBytes();
    }
}
