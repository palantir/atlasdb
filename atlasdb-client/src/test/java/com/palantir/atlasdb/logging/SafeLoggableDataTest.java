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
package com.palantir.atlasdb.logging;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import org.junit.Test;

public class SafeLoggableDataTest {
    private static final TableReference TABLE_REFERENCE_1 = TableReference.createFromFullyQualifiedName("atlas.db");
    private static final TableReference TABLE_REFERENCE_2 = TableReference.createFromFullyQualifiedName("safe.log");
    private static final TableReference TABLE_REFERENCE_3 = TableReference.createFromFullyQualifiedName("pala.ntir");

    private static final String ROW_COMPONENT_1 = "rowNameOne";
    private static final String ROW_COMPONENT_2 = "rowNameTwo";

    private static final String COLUMN_NAME_1 = "columnNameOne";
    private static final String COLUMN_NAME_2 = "columnNameTwo";

    @Test
    public void tableReferencesExplicitlyMarkedSafeAreSafe() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder()
                .addPermittedTableReferences(TABLE_REFERENCE_1)
                .build();

        assertThat(safeLoggableData.isTableReferenceSafe(TABLE_REFERENCE_1)).isTrue();
    }

    @Test
    public void tableReferencesNotExplicitlyMarkedSafeAreUnsafe() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder()
                .addPermittedTableReferences(TABLE_REFERENCE_1)
                .build();

        assertThat(safeLoggableData.isTableReferenceSafe(TABLE_REFERENCE_2)).isFalse();
    }

    @Test
    public void rowComponentsExplicitlyMarkedSafeAreSafe() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder()
                .putPermittedRowComponents(TABLE_REFERENCE_1, ImmutableSet.of(ROW_COMPONENT_1))
                .build();

        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_1, ROW_COMPONENT_1))
                .isTrue();
    }

    @Test
    public void rowComponentsNotExplicitlyMarkedSafeAreUnsafe() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder()
                .putPermittedRowComponents(TABLE_REFERENCE_1, ImmutableSet.of(ROW_COMPONENT_1))
                .build();

        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_1, ROW_COMPONENT_2))
                .isFalse();
    }

    @Test
    public void rowComponentsMarkedSafeOnlyForOtherTablesAreNotSafe() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder()
                .putPermittedRowComponents(TABLE_REFERENCE_1, ImmutableSet.of(ROW_COMPONENT_1))
                .putPermittedRowComponents(TABLE_REFERENCE_2, ImmutableSet.of(ROW_COMPONENT_2))
                .build();

        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_1, ROW_COMPONENT_2))
                .isFalse();
        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_2, ROW_COMPONENT_1))
                .isFalse();
    }

    @Test
    public void rowComponentsInUnknownTablesAreNotSafe() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder().build();
        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_3, ROW_COMPONENT_1))
                .isFalse();
    }

    @Test
    public void columnNamesExplicitlyMarkedSafeAreSafe() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder()
                .putPermittedColumnNames(TABLE_REFERENCE_1, ImmutableSet.of(COLUMN_NAME_1))
                .build();

        assertThat(safeLoggableData.isColumnNameSafe(TABLE_REFERENCE_1, COLUMN_NAME_1))
                .isTrue();
    }

    @Test
    public void columnNamesNotExplicitlyMarkedSafeAreUnsafe() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder()
                .putPermittedColumnNames(TABLE_REFERENCE_1, ImmutableSet.of(COLUMN_NAME_1))
                .build();

        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_1, COLUMN_NAME_2))
                .isFalse();
    }

    @Test
    public void columnNamesMarkedSafeOnlyForOtherTablesAreNotSafe() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder()
                .putPermittedColumnNames(TABLE_REFERENCE_1, ImmutableSet.of(COLUMN_NAME_1))
                .putPermittedColumnNames(TABLE_REFERENCE_2, ImmutableSet.of(COLUMN_NAME_2))
                .build();

        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_1, COLUMN_NAME_2))
                .isFalse();
        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_2, COLUMN_NAME_1))
                .isFalse();
    }

    @Test
    public void columnNamesInUnknownTablesAreNotSafe() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder().build();
        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_3, ROW_COMPONENT_1))
                .isFalse();
    }

    @Test
    public void columnNamesMarkedAsSafeRowComponentNamesAreNotSafe() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder()
                .putPermittedRowComponents(TABLE_REFERENCE_1, ImmutableSet.of(ROW_COMPONENT_1))
                .build();

        assertThat(safeLoggableData.isColumnNameSafe(TABLE_REFERENCE_1, ROW_COMPONENT_1))
                .isFalse();
    }

    @Test
    public void rowComponentsDoNotInheritSafetyFromTableReferences() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder()
                .addPermittedTableReferences(TABLE_REFERENCE_1)
                .build();

        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_1, ROW_COMPONENT_1))
                .isFalse();
    }

    @Test
    public void columnNamesDoNotInheritSafetyFromTableReferences() {
        SafeLoggableData safeLoggableData = ImmutableSafeLoggableData.builder()
                .addPermittedTableReferences(TABLE_REFERENCE_1)
                .build();

        assertThat(safeLoggableData.isColumnNameSafe(TABLE_REFERENCE_1, COLUMN_NAME_1))
                .isFalse();
    }
}
