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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class LoggingArgsTest {
    private static final String ARG_NAME = "argName";
    private static final TableReference SAFE_TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("foo.safe");
    private static final TableReference UNSAFE_TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("foo.bar");

    private static final String SAFE_ROW_NAME = "saferow";
    private static final String UNSAFE_ROW_NAME = "row";
    private static final String SAFE_COLUMN_NAME = "safecolumn";
    private static final String UNSAFE_COLUMN_NAME = "column";

    private static final KeyValueServiceLogArbitrator arbitrator = Mockito.mock(KeyValueServiceLogArbitrator.class);

    @BeforeClass
    public static void setUpMocks() {
        when(arbitrator.isTableReferenceSafe(any())).thenAnswer(invocation -> {
            TableReference tableReference = (TableReference) invocation.getArguments()[0];
            return tableReference.getQualifiedName().contains("safe");
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
    public void propagatesNameAndTableReferenceIfSafe() {
        Arg<TableReference> tableReferenceArg = LoggingArgs.tableRef(ARG_NAME, SAFE_TABLE_REFERENCE);
        assertThat(tableReferenceArg.getName()).isEqualTo(ARG_NAME);
        assertThat(tableReferenceArg.getValue()).isEqualTo(SAFE_TABLE_REFERENCE);
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
}
