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

package com.palantir.atlasdb.logging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ImmutableNameComponentDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.UniformRowNamePartitioner;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class SafeLoggableDataUtilsTest {
    private static final TableReference TABLE_REFERENCE_1 = TableReference.createFromFullyQualifiedName("atlas.db");
    private static final TableReference TABLE_REFERENCE_2 = TableReference.createFromFullyQualifiedName("atlas.db2");
    private static final TableReference TABLE_REFERENCE_3 = TableReference.createFromFullyQualifiedName("atlas.db3");

    private static final String ROW_COMPONENT_NAME = "foo";
    private static final String COLUMN_LONG_NAME = "barrrr";
    private static final NameMetadataDescription NAME_METADATA_DESCRIPTION = NameMetadataDescription.create(
            ImmutableList.of(
                    ImmutableNameComponentDescription.builder()
                            .componentName(ROW_COMPONENT_NAME)
                            .type(ValueType.VAR_LONG)
                            .byteOrder(TableMetadataPersistence.ValueByteOrder.ASCENDING)
                            .uniformPartitioner(new UniformRowNamePartitioner(ValueType.VAR_LONG))
                            .logSafety(LogSafety.SAFE)
                            .build()),
            0);
    private static final ColumnMetadataDescription COLUMN_METADATA_DESCRIPTION =
            new ColumnMetadataDescription(ImmutableList.of(new NamedColumnDescription("bar", COLUMN_LONG_NAME,
                    ColumnValueDescription.forType(ValueType.VAR_LONG), LogSafety.SAFE)));

    private static final TableMetadata TABLE_METADATA_1 = new TableMetadata(
            NameMetadataDescription.create(
                    ImmutableList.of(ImmutableNameComponentDescription.builder()
                            .componentName(ROW_COMPONENT_NAME).type(ValueType.VAR_LONG).build()),
                    0),
            new ColumnMetadataDescription(
                    ImmutableList.of(new NamedColumnDescription("bar", "barrrr",
                            ColumnValueDescription.forType(ValueType.VAR_LONG)))),
            ConflictHandler.RETRY_ON_WRITE_WRITE);
    private static final TableMetadata TABLE_METADATA_2 = createTableMetadataWithLoggingAllowed(
            NAME_METADATA_DESCRIPTION, COLUMN_METADATA_DESCRIPTION);
    private static final TableMetadata TABLE_METADATA_DYNAMIC_COLUMNS = createTableMetadataWithLoggingAllowed(
            NAME_METADATA_DESCRIPTION,
            new ColumnMetadataDescription()
    );

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

    private static TableMetadata createTableMetadataWithLoggingAllowed(
            NameMetadataDescription nameMetadataDescription,
            ColumnMetadataDescription columnMetadataDescription) {
        return new TableMetadata(
                nameMetadataDescription,
                columnMetadataDescription,
                ConflictHandler.RETRY_ON_WRITE_WRITE,
                TableMetadataPersistence.CachePriority.WARM,
                TableMetadataPersistence.PartitionStrategy.ORDERED,
                false,
                0,
                false,
                TableMetadataPersistence.SweepStrategy.CONSERVATIVE,
                TableMetadataPersistence.ExpirationStrategy.NEVER,
                false,
                LogSafety.SAFE);
    }
}
