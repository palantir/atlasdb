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

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.UniformRowNamePartitioner;
import com.palantir.atlasdb.table.description.ValueType;
import org.junit.Test;

public class SafeLoggableDataUtilsTest {
    private static final TableReference TABLE_REFERENCE_1 = TableReference.createFromFullyQualifiedName("atlas.db");
    private static final TableReference TABLE_REFERENCE_2 = TableReference.createFromFullyQualifiedName("atlas.db2");
    private static final TableReference TABLE_REFERENCE_3 = TableReference.createFromFullyQualifiedName("atlas.db3");

    private static final String ROW_COMPONENT_NAME = "foo";
    private static final String COLUMN_LONG_NAME = "barrrr";
    private static final NameMetadataDescription NAME_METADATA_DESCRIPTION = NameMetadataDescription.create(
            ImmutableList.of(
                    new NameComponentDescription.Builder()
                            .componentName(ROW_COMPONENT_NAME)
                            .type(ValueType.VAR_LONG)
                            .byteOrder(TableMetadataPersistence.ValueByteOrder.ASCENDING)
                            .uniformRowNamePartitioner(new UniformRowNamePartitioner(ValueType.VAR_LONG))
                            .logSafety(LogSafety.SAFE)
                            .build()),
            0);
    private static final ColumnMetadataDescription COLUMN_METADATA_DESCRIPTION =
            new ColumnMetadataDescription(ImmutableList.of(new NamedColumnDescription("bar", COLUMN_LONG_NAME,
                    ColumnValueDescription.forType(ValueType.VAR_LONG), LogSafety.SAFE)));

    private static final TableMetadata TABLE_METADATA_1 = TableMetadata.builder()
            .singleRowComponent(ROW_COMPONENT_NAME, ValueType.VAR_LONG)
            .singleNamedColumn("bar", COLUMN_LONG_NAME, ValueType.VAR_LONG)
            .build();

    private static final TableMetadata TABLE_METADATA_2 = TableMetadata.builder()
            .rowMetadata(NAME_METADATA_DESCRIPTION)
            .columns(COLUMN_METADATA_DESCRIPTION)
            .nameLogSafety(LogSafety.SAFE)
            .build();
    private static final TableMetadata TABLE_METADATA_DYNAMIC_COLUMNS = TableMetadata.builder()
            .rowMetadata(NAME_METADATA_DESCRIPTION)
            .nameLogSafety(LogSafety.SAFE)
            .build();

    @Test
    public void nothingLoggableByDefault() {
        ImmutableSafeLoggableData.Builder builder = ImmutableSafeLoggableData.builder();
        SafeLoggableDataUtils.addLoggableNamesToBuilder(builder, TABLE_REFERENCE_1, TABLE_METADATA_1);
        SafeLoggableData safeLoggableData = builder.build();

        assertThat(safeLoggableData.isTableReferenceSafe(TABLE_REFERENCE_1)).isFalse();
        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_1, ROW_COMPONENT_NAME)).isFalse();
        assertThat(safeLoggableData.isColumnNameSafe(TABLE_REFERENCE_1, "barrrr")).isFalse();
    }

    @Test
    public void canSpecifyItemsAsLoggable() {
        ImmutableSafeLoggableData.Builder builder = ImmutableSafeLoggableData.builder();
        SafeLoggableDataUtils.addLoggableNamesToBuilder(builder, TABLE_REFERENCE_1, TABLE_METADATA_2);
        SafeLoggableData safeLoggableData = builder.build();

        assertThat(safeLoggableData.isTableReferenceSafe(TABLE_REFERENCE_1)).isTrue();
        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_1, ROW_COMPONENT_NAME)).isTrue();
        assertThat(safeLoggableData.isColumnNameSafe(TABLE_REFERENCE_1, COLUMN_LONG_NAME)).isTrue();
    }

    @Test
    public void canSpecifyTableReferenceAndRowComponentsAsSafeEvenWhenUsingDynamicColumns() {
        ImmutableSafeLoggableData.Builder builder = ImmutableSafeLoggableData.builder();
        SafeLoggableDataUtils.addLoggableNamesToBuilder(builder, TABLE_REFERENCE_1, TABLE_METADATA_DYNAMIC_COLUMNS);
        SafeLoggableData safeLoggableData = builder.build();

        assertThat(safeLoggableData.isTableReferenceSafe(TABLE_REFERENCE_1)).isTrue();
        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_1, ROW_COMPONENT_NAME)).isTrue();
    }

    @Test
    public void canCoalesceDataFromMultipleTables() {
        ImmutableSafeLoggableData.Builder builder = ImmutableSafeLoggableData.builder();
        SafeLoggableDataUtils.addLoggableNamesToBuilder(builder, TABLE_REFERENCE_1, TABLE_METADATA_1);
        SafeLoggableDataUtils.addLoggableNamesToBuilder(builder, TABLE_REFERENCE_2, TABLE_METADATA_2);
        SafeLoggableDataUtils.addLoggableNamesToBuilder(builder, TABLE_REFERENCE_3, TABLE_METADATA_DYNAMIC_COLUMNS);
        SafeLoggableData safeLoggableData = builder.build();

        assertThat(safeLoggableData.isTableReferenceSafe(TABLE_REFERENCE_1)).isFalse();
        assertThat(safeLoggableData.isTableReferenceSafe(TABLE_REFERENCE_2)).isTrue();
        assertThat(safeLoggableData.isTableReferenceSafe(TABLE_REFERENCE_3)).isTrue();

        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_1, ROW_COMPONENT_NAME)).isFalse();
        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_2, ROW_COMPONENT_NAME)).isTrue();
        assertThat(safeLoggableData.isRowComponentNameSafe(TABLE_REFERENCE_3, ROW_COMPONENT_NAME)).isTrue();

        assertThat(safeLoggableData.isColumnNameSafe(TABLE_REFERENCE_1, COLUMN_LONG_NAME)).isFalse();
        assertThat(safeLoggableData.isColumnNameSafe(TABLE_REFERENCE_2, COLUMN_LONG_NAME)).isTrue();
        assertThat(safeLoggableData.isColumnNameSafe(TABLE_REFERENCE_3, COLUMN_LONG_NAME)).isFalse();
    }
}
