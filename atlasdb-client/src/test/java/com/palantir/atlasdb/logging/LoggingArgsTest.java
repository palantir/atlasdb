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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class LoggingArgsTest {
    private static final String ARG_NAME = "argName";
    private static final TableReference SAFE_TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("foo.safe");
    private static final TableReference UNSAFE_TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("foo.bar");
    private static final List<TableReference> LIST_OF_SAFE_AND_UNSAFE_TABLE_REFERENCES = Lists.newArrayList(
            SAFE_TABLE_REFERENCE,
            UNSAFE_TABLE_REFERENCE
    );

    private static final String SAFE_ROW_NAME = "saferow";
    private static final String UNSAFE_ROW_NAME = "row";
    private static final String SAFE_COLUMN_NAME = "safecolumn";
    private static final String UNSAFE_COLUMN_NAME = "column";

    private static final byte[] SAFE_ROW_NAME_BYTES = PtBytes.toBytes(SAFE_ROW_NAME);
    private static final byte[] UNSAFE_ROW_NAME_BYTES = PtBytes.toBytes(UNSAFE_ROW_NAME);
    private static final byte[] SAFE_COLUMN_NAME_BYTES = PtBytes.toBytes(SAFE_COLUMN_NAME);
    private static final byte[] UNSAFE_COLUMN_NAME_BYTES = PtBytes.toBytes(UNSAFE_COLUMN_NAME);

    private static final RangeRequest SAFE_RANGE_REQUEST = RangeRequest.builder()
            .retainColumns(ImmutableList.of(SAFE_ROW_NAME_BYTES)).build();
    private static final RangeRequest UNSAFE_RANGE_REQUEST = RangeRequest.builder()
            .retainColumns(ImmutableList.of(UNSAFE_ROW_NAME_BYTES)).build();
    private static final RangeRequest MIXED_RANGE_REQUEST = RangeRequest.builder()
            .retainColumns(ImmutableList.of(SAFE_ROW_NAME_BYTES, UNSAFE_ROW_NAME_BYTES)).build();

    private static final ColumnRangeSelection SAFE_COLUMN_RANGE = new ColumnRangeSelection(
            SAFE_COLUMN_NAME_BYTES, SAFE_COLUMN_NAME_BYTES);
    private static final ColumnRangeSelection UNSAFE_COLUMN_RANGE = new ColumnRangeSelection(
            UNSAFE_COLUMN_NAME_BYTES, UNSAFE_COLUMN_NAME_BYTES);
    private static final ColumnRangeSelection MIXED_COLUMN_RANGE = new ColumnRangeSelection(
            SAFE_COLUMN_NAME_BYTES, UNSAFE_COLUMN_NAME_BYTES);
    private static final BatchColumnRangeSelection SAFE_BATCH_COLUMN_RANGE = BatchColumnRangeSelection.create(
            SAFE_COLUMN_RANGE, 1);
    private static final BatchColumnRangeSelection UNSAFE_BATCH_COLUMN_RANGE = BatchColumnRangeSelection.create(
            UNSAFE_COLUMN_RANGE, 1);
    private static final BatchColumnRangeSelection MIXED_BATCH_COLUMN_RANGE = BatchColumnRangeSelection.create(
            MIXED_COLUMN_RANGE, 1);

    private static final KeyValueServiceLogArbitrator arbitrator = Mockito.mock(KeyValueServiceLogArbitrator.class);

    @BeforeClass
    public static void setUpMocks() {
        when(arbitrator.isTableReferenceSafe(any())).thenAnswer(invocation -> {
            TableReference tableReference = (TableReference) invocation.getArguments()[0];
            return tableReference.getQualifiedName().contains("safe");
        });

        // Technically this may be inconsistent with the above, but this will do for our testing purposes
        when(arbitrator.isInternalTableReferenceSafe(any())).thenAnswer(invocation -> {
            String internalTableReference = (String) invocation.getArguments()[0];
            return internalTableReference.contains("safe");
        });

        when(arbitrator.isRowComponentNameSafe(any(), any(String.class))).thenAnswer(invocation -> {
            String rowName = (String) invocation.getArguments()[1];
            return rowName.contains("safe");
        });

        when(arbitrator.isColumnNameSafe(any(), any(String.class))).thenAnswer(invocation -> {
            String columnName = (String) invocation.getArguments()[1];
            return columnName.contains("safe");
        });

        LoggingArgs.setLogArbitrator(arbitrator);
    }

    @AfterClass
    public static void tearDownClass() {
        LoggingArgs.setLogArbitrator(KeyValueServiceLogArbitrator.ALL_UNSAFE);
    }

    @Test
    public void returnsSafeInternalTableNameCorrectly() {
        Arg<String> internalTableNameArg = LoggingArgs.internalTableName(SAFE_TABLE_REFERENCE);
        assertThat(internalTableNameArg.getName()).isEqualTo("tableRef");
        assertThat(internalTableNameArg.getValue()).isEqualTo(
                AbstractKeyValueService.internalTableName(SAFE_TABLE_REFERENCE));
        assertThat(internalTableNameArg).isInstanceOf(SafeArg.class);
    }

    @Test
    public void returnsUnsafeInternalTableNameCorrectly() {
        Arg<String> internalTableNameArg = LoggingArgs.internalTableName(UNSAFE_TABLE_REFERENCE);
        assertThat(internalTableNameArg.getName()).isEqualTo("unsafeTableRef");
        assertThat(internalTableNameArg.getValue()).isEqualTo(
                AbstractKeyValueService.internalTableName(UNSAFE_TABLE_REFERENCE));
        assertThat(internalTableNameArg).isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void propagatesNameAndTableReferenceIfSafe() {
        Arg<String> tableReferenceArg = LoggingArgs.tableRef(ARG_NAME, SAFE_TABLE_REFERENCE);
        assertThat(tableReferenceArg.getName()).isEqualTo(ARG_NAME);
        assertThat(tableReferenceArg.getValue()).isEqualTo(SAFE_TABLE_REFERENCE.toString());
    }

    @Test
    public void propagatesNameAndRowComponentNameIfSafe() {
        Arg<String> rowNameArg = LoggingArgs.rowComponent(ARG_NAME, SAFE_TABLE_REFERENCE, SAFE_ROW_NAME);
        assertThat(rowNameArg.getName()).isEqualTo(ARG_NAME);
        assertThat(rowNameArg.getValue()).isEqualTo(SAFE_ROW_NAME);
    }

    @Test
    public void propagatesNameAndColumnNameIfSafe() {
        Arg<String> columnNameArg = LoggingArgs.columnName(ARG_NAME, SAFE_TABLE_REFERENCE, SAFE_COLUMN_NAME);
        assertThat(columnNameArg.getName()).isEqualTo(ARG_NAME);
        assertThat(columnNameArg.getValue()).isEqualTo(SAFE_COLUMN_NAME);
    }

    @Test
    public void canReturnBothSafeAndUnsafeTableReferences() {
        assertThat(LoggingArgs.tableRef(ARG_NAME, SAFE_TABLE_REFERENCE)).isInstanceOf(SafeArg.class);
        assertThat(LoggingArgs.tableRef(ARG_NAME, UNSAFE_TABLE_REFERENCE)).isInstanceOf(UnsafeArg.class);
    }

    @Test
    @SuppressWarnings("CheckReturnValue") // We test that returnedArgs will contain both a safe and unsafe references.
    public void canReturnListOfSafeTableReferences() {
        LoggingArgs.SafeAndUnsafeTableReferences returnedArgs =
                LoggingArgs.tableRefs(LIST_OF_SAFE_AND_UNSAFE_TABLE_REFERENCES);

        assertThat(returnedArgs.safeTableRefs().getValue().contains(SAFE_TABLE_REFERENCE));
        assertThat(returnedArgs.unsafeTableRefs().getValue().contains(UNSAFE_TABLE_REFERENCE));
    }

    @Test
    public void canReturnBothSafeAndUnsafeRowComponentNames() {
        assertThat(LoggingArgs.rowComponent(ARG_NAME, SAFE_TABLE_REFERENCE, SAFE_ROW_NAME))
                .isInstanceOf(SafeArg.class);
        assertThat(LoggingArgs.rowComponent(ARG_NAME, UNSAFE_TABLE_REFERENCE, UNSAFE_ROW_NAME))
                .isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void canReturnBothSafeAndUnsafeColumnNames() {
        assertThat(LoggingArgs.columnName(ARG_NAME, SAFE_TABLE_REFERENCE, SAFE_COLUMN_NAME))
                .isInstanceOf(SafeArg.class);
        assertThat(LoggingArgs.columnName(ARG_NAME, UNSAFE_TABLE_REFERENCE, UNSAFE_COLUMN_NAME))
                .isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void canReturnSafeRowComponentEvenIfTableReferenceIsUnsafe() {
        assertThat(LoggingArgs.rowComponent(ARG_NAME, UNSAFE_TABLE_REFERENCE, SAFE_ROW_NAME))
                .isInstanceOf(SafeArg.class);
    }

    @Test
    public void canReturnUnsafeRowComponentEvenIfTableReferenceIsSafe() {
        assertThat(LoggingArgs.rowComponent(ARG_NAME, SAFE_TABLE_REFERENCE, UNSAFE_ROW_NAME))
                .isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void canReturnSafeColumnNameEvenIfTableReferenceIsUnsafe() {
        assertThat(LoggingArgs.columnName(ARG_NAME, UNSAFE_TABLE_REFERENCE, SAFE_COLUMN_NAME))
                .isInstanceOf(SafeArg.class);
    }

    @Test
    public void canReturnUnsafeColumnNameEvenIfTableReferenceIsSafe() {
        assertThat(LoggingArgs.columnName(ARG_NAME, SAFE_TABLE_REFERENCE, UNSAFE_COLUMN_NAME))
                .isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void returnsSafeRangeWhenAllSafe() {
        assertThat(LoggingArgs.range(SAFE_TABLE_REFERENCE, SAFE_RANGE_REQUEST))
                .isInstanceOf(SafeArg.class);
    }

    @Test
    public void returnsUnsafeRangeWhenAllColumnsUnsafe() {
        assertThat(LoggingArgs.range(SAFE_TABLE_REFERENCE, UNSAFE_RANGE_REQUEST))
                .isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void returnsUnsafeRangeEvenWhenContainsSafeColumns() {
        assertThat(LoggingArgs.range(SAFE_TABLE_REFERENCE, MIXED_RANGE_REQUEST))
                .isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void returnsSafeColumnRangeWhenStartEndBothSafe() {
        assertThat(LoggingArgs.columnRangeSelection(SAFE_TABLE_REFERENCE, SAFE_COLUMN_RANGE))
                .isInstanceOf(SafeArg.class);
    }

    @Test
    public void returnsUnsafeColumnRangeWhenBothColumnsUnsafe() {
        assertThat(LoggingArgs.columnRangeSelection(SAFE_TABLE_REFERENCE, UNSAFE_COLUMN_RANGE))
                .isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void returnsUnsafeColumnRangeEvenWhenContainsSafeColumns() {
        assertThat(LoggingArgs.columnRangeSelection(SAFE_TABLE_REFERENCE, MIXED_COLUMN_RANGE))
                .isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void returnsSafeBatchBatchColumnRangeWhenStartEndBothSafe() {
        assertThat(LoggingArgs.batchColumnRangeSelection(SAFE_TABLE_REFERENCE, SAFE_BATCH_COLUMN_RANGE))
                .isInstanceOf(SafeArg.class);
    }

    @Test
    public void returnsUnsafeBatchCOlumnRangeWhenBothColumnsUnsafe() {
        assertThat(LoggingArgs.batchColumnRangeSelection(SAFE_TABLE_REFERENCE, UNSAFE_BATCH_COLUMN_RANGE))
                .isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void returnsUnsafeBatchBatchColumnRangeEvenWhenContainsSafeColumns() {
        assertThat(LoggingArgs.batchColumnRangeSelection(SAFE_TABLE_REFERENCE, MIXED_BATCH_COLUMN_RANGE))
                .isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void returnsSafeTableWhenTableIsSafe() {
        assertThat(LoggingArgs.safeTableOrPlaceholder(SAFE_TABLE_REFERENCE)).isEqualTo(SAFE_TABLE_REFERENCE);
    }

    @Test
    public void returnsPlaceholderWhenTableIsUnsafe() {
        assertThat(LoggingArgs.safeTableOrPlaceholder(UNSAFE_TABLE_REFERENCE))
                .isEqualTo(LoggingArgs.PLACEHOLDER_TABLE_REFERENCE);
    }

    @Test
    public void returnsTablesAndPlaceholderWhenTablesAreSafeAndUnsafe() {
        List<TableReference> tables = ImmutableList.of(SAFE_TABLE_REFERENCE, UNSAFE_TABLE_REFERENCE);
        List<TableReference> returnedList = Lists.newArrayList(LoggingArgs.safeTablesOrPlaceholder(tables));
        List<TableReference> expectedList = Lists.newArrayList(SAFE_TABLE_REFERENCE,
                LoggingArgs.PLACEHOLDER_TABLE_REFERENCE);

        assertThat(returnedList).containsOnly(expectedList.toArray(new TableReference[expectedList.size()]));
    }

    @Test
    public void hydrateDoesNotThrowOnInvalidMetadata() {
        LoggingArgs.hydrate(ImmutableMap.of(SAFE_TABLE_REFERENCE, AtlasDbConstants.EMPTY_TABLE_METADATA));
        LoggingArgs.setLogArbitrator(arbitrator);
    }
}
