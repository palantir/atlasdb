/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.table.description;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;

public class TableDefinitionTest {
    private static final TableReference TABLE_REF = TableReference.create(Namespace.DEFAULT_NAMESPACE, "foo");
    private static final String ROW_NAME = "foo";
    private static final String COLUMN_NAME = "bar";
    private static final String COLUMN_SHORTNAME = "b";

    private static final TableDefinition BASE_DEFINITION = new TableDefinition() {{
        javaTableName(TABLE_REF.getTablename());
        rowName();
        rowComponent(ROW_NAME, ValueType.STRING);
        columns();
        column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
    }};

    @Test
    public void tableNameNotLoggableByDefault() {
        assertThat(BASE_DEFINITION.toTableMetadata().isNameLoggable()).isFalse();
    }

    @Test
    public void canSpecifyTableNameAsLoggable() {
        TableDefinition definition = new TableDefinition() {{
            tableNameIsSafeLoggable();
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING);
            noColumns();
        }};
        assertThat(definition.toTableMetadata().isNameLoggable()).isTrue();
    }

    @Test
    public void cannotSpecifyTableNameAsLoggableWhenSpecifyingRowName() {
        assertThatThrownBy(() -> new TableDefinition() {{
            javaTableName(TABLE_REF.getTablename());
            rowName();
            tableNameIsSafeLoggable();
            rowComponent(ROW_NAME, ValueType.STRING);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }}).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void cannotSpecifyTableNameAsLoggableWhenSpecifyingColumns() {
        assertThatThrownBy(() -> new TableDefinition() {{
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING);
            columns();
            tableNameIsSafeLoggable();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }}).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void componentsAreNotSafeByDefault() {
        assertRowComponentSafety(BASE_DEFINITION, false);
        assertNamedColumnSafety(BASE_DEFINITION, false);
    }

    @Test
    public void canSpecifyComponentsAsSafeByDefault() {
        TableDefinition definition = new TableDefinition() {{
            namedComponentsSafeByDefault();
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }};

        assertRowComponentSafety(definition, true);
        assertNamedColumnSafety(definition, true);
    }

    @Test
    public void cannotSpecifyComponentsAsSafeByDefaultWhenSpecifyingRowName() {
        assertThatThrownBy(() -> new TableDefinition() {{
            javaTableName(TABLE_REF.getTablename());
            rowName();
            namedComponentsSafeByDefault();
            rowComponent(ROW_NAME, ValueType.STRING);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }}).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void cannotSpecifyComponentsAsSafeByDefaultWhenSpecifyingColumns() {
        assertThatThrownBy(() -> new TableDefinition() {{
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING);
            columns();
            namedComponentsSafeByDefault();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }}).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void canDeclareSpecificComponentsAsSafe() {
        TableDefinition definition = new TableDefinition() {{
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING, TableMetadataPersistence.ValueByteOrder.ASCENDING, true);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }};

        assertRowComponentSafety(definition, true);
        assertNamedColumnSafety(definition, false);
    }

    @Test
    public void canDeclareComponentsAsSafeByDefaultAndSpecificComponentsAsUnsafe() {
        TableDefinition definition = new TableDefinition() {{
            namedComponentsSafeByDefault();
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING, TableMetadataPersistence.ValueByteOrder.ASCENDING, false);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }};

        assertRowComponentSafety(definition, false);
        assertNamedColumnSafety(definition, true);
    }

    /**
     * Asserts that the only row component for the TableDefinition object passed in has loggability matching
     * expectedSafety. Throws if the actual safety doesn't match the expected safety, or if it is not the case that
     * there is exactly one row component.
     */
    private static void assertRowComponentSafety(TableDefinition tableDefinition, boolean expectedSafety) {
        TableMetadata metadata = tableDefinition.toTableMetadata();
        NameComponentDescription nameComponent = Iterables.getOnlyElement(metadata.getRowMetadata().getRowParts());
        assertThat(nameComponent.isNameLoggable()).isEqualTo(expectedSafety);
    }

    /**
     * Asserts that the only named column for the TableDefinition object passed in has loggability matching
     * expectedSafety. Throws if the actual safety doesn't match the expected safety, or if it is not the case that
     * there is exactly one named column.
     */
    private static void assertNamedColumnSafety(TableDefinition tableDefinition, boolean expectedSafety) {
        TableMetadata metadata = tableDefinition.toTableMetadata();
        NamedColumnDescription namedColumn = Iterables.getOnlyElement(metadata.getColumns().getNamedColumns());
        assertThat(namedColumn.isNameLoggable()).isEqualTo(expectedSafety);
    }
}
