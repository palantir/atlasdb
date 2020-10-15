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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import org.junit.Test;

@SuppressWarnings("checkstyle:all") // too many warnings to fix
public class TableDefinitionTest {
    private static final TableReference TABLE_REF = TableReference.create(Namespace.DEFAULT_NAMESPACE,
            TableDefinitionTest.class.getSimpleName());
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
        assertThat(BASE_DEFINITION.toTableMetadata().getNameLogSafety()).isEqualTo(LogSafety.UNSAFE);
    }

    @Test
    public void canSpecifyTableNameAsLoggable() {
        TableDefinition definition = new TableDefinition() {{
            tableNameLogSafety(LogSafety.SAFE);
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING);
            noColumns();
        }};
        assertThat(definition.toTableMetadata().getNameLogSafety()).isEqualTo(LogSafety.SAFE);
    }

    @Test
    public void cannotSpecifyTableNameAsLoggableWhenSpecifyingRowName() {
        assertThatThrownBy(() -> new TableDefinition() {{
            javaTableName(TABLE_REF.getTablename());
            rowName();
            tableNameLogSafety(LogSafety.SAFE);
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
            tableNameLogSafety(LogSafety.SAFE);
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }}).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void componentsAreNotSafeByDefault() {
        assertRowComponentSafety(BASE_DEFINITION, LogSafety.UNSAFE);
        assertNamedColumnSafety(BASE_DEFINITION, LogSafety.UNSAFE);
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

        assertRowComponentSafety(definition, LogSafety.SAFE);
        assertNamedColumnSafety(definition, LogSafety.SAFE);
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
            rowComponent(ROW_NAME, ValueType.STRING, LogSafety.SAFE);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }};

        assertRowComponentSafety(definition, LogSafety.SAFE);
        assertNamedColumnSafety(definition, LogSafety.UNSAFE);
    }

    @Test
    public void canDeclareComponentsAsSafeByDefaultAndSpecificComponentsAsUnsafe() {
        TableDefinition definition = new TableDefinition() {{
            namedComponentsSafeByDefault();
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING, LogSafety.UNSAFE);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }};

        assertRowComponentSafety(definition, LogSafety.UNSAFE);
        assertNamedColumnSafety(definition, LogSafety.SAFE);
    }

    @Test
    public void canSpecifyEverythingAsSafe() {
        TableDefinition definition = new TableDefinition() {{
            allSafeForLoggingByDefault();
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING, LogSafety.UNSAFE);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }};

        assertThat(definition.toTableMetadata().getNameLogSafety()).isEqualTo(LogSafety.SAFE);
        assertRowComponentSafety(definition, LogSafety.UNSAFE);
        assertNamedColumnSafety(definition, LogSafety.SAFE);
    }

    @Test
    public void cannotSpecifyEverythingAsSafeWhenSpecifyingRowName() {
        assertThatThrownBy(() -> new TableDefinition() {{
            javaTableName(TABLE_REF.getTablename());
            rowName();
            allSafeForLoggingByDefault();
            rowComponent(ROW_NAME, ValueType.STRING);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }}).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void cannotSpecifyEverythingAsSafeWhenSpecifyingColumnName() {
        assertThatThrownBy(() -> new TableDefinition() {{
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING);
            columns();
            allSafeForLoggingByDefault();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }}).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void canDeclareEverythingAsSafeByDefaultAndSpecificComponentsAsUnsafe() {
        TableDefinition definition = new TableDefinition() {{
            allSafeForLoggingByDefault();
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING, LogSafety.UNSAFE);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }};

        assertThat(definition.toTableMetadata().getNameLogSafety()).isEqualTo(LogSafety.SAFE);
        assertRowComponentSafety(definition, LogSafety.UNSAFE);
        assertNamedColumnSafety(definition, LogSafety.SAFE);
    }

    @Test
    public void cannotSpecifyTableNameLogSafetyAsBothSafeAndUnsafe() {
        assertThatThrownBy(() -> new TableDefinition() {{
            javaTableName(TABLE_REF.getTablename());
            tableNameLogSafety(LogSafety.SAFE);
            tableNameLogSafety(LogSafety.UNSAFE);
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }}).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void cannotSpecifyTableNameIsUnsafeWithEverythingSafe() {
        assertThatThrownBy(() -> new TableDefinition() {{
            javaTableName(TABLE_REF.getTablename());
            tableNameLogSafety(LogSafety.UNSAFE);
            allSafeForLoggingByDefault();
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }}).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void canSpecifyTableNameAsUnsafeWithAllComponentsAsSafe() {
        TableDefinition definition = new TableDefinition() {{
            tableNameLogSafety(LogSafety.UNSAFE);
            namedComponentsSafeByDefault();
            javaTableName(TABLE_REF.getTablename());
            rowName();
            rowComponent(ROW_NAME, ValueType.STRING);
            columns();
            column(COLUMN_NAME, COLUMN_SHORTNAME, ValueType.VAR_LONG);
        }};

        assertThat(definition.toTableMetadata().getNameLogSafety()).isEqualTo(LogSafety.UNSAFE);
        assertRowComponentSafety(definition, LogSafety.SAFE);
        assertNamedColumnSafety(definition, LogSafety.SAFE);
    }

    /**
     * Asserts that the only row component for the TableDefinition object passed in has loggability matching
     * expectedSafety. Throws if the actual safety doesn't match the expected safety, or if it is not the case that
     * there is exactly one row component.
     */
    private static void assertRowComponentSafety(TableDefinition tableDefinition, LogSafety expectedSafety) {
        TableMetadata metadata = tableDefinition.toTableMetadata();
        NameComponentDescription nameComponent = Iterables.getOnlyElement(metadata.getRowMetadata().getRowParts());
        assertThat(nameComponent.getLogSafety()).isEqualTo(expectedSafety);
    }

    /**
     * Asserts that the only named column for the TableDefinition object passed in has loggability matching
     * expectedSafety. Throws if the actual safety doesn't match the expected safety, or if it is not the case that
     * there is exactly one named column.
     */
    private static void assertNamedColumnSafety(TableDefinition tableDefinition, LogSafety expectedSafety) {
        TableMetadata metadata = tableDefinition.toTableMetadata();
        NamedColumnDescription namedColumn = Iterables.getOnlyElement(metadata.getColumns().getNamedColumns());
        assertThat(namedColumn.getLogSafety()).isEqualTo(expectedSafety);
    }
}
