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

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.UnsafeArg;

public class KeyValueServiceLoggingArgSupplierTest {
    private static final KeyValueServiceLoggingArgSupplier ALL_UNSAFE = KeyValueServiceLoggingArgSupplier.ALL_UNSAFE;

    private static final String ARG_NAME = "argName";
    private static final TableReference TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("foo.bar");

    private static final String ROW_NAME = "row";
    private static final String COLUMN_NAME = "column";

    @Test
    public void allUnsafeSupplierReturnsUnsafeTableReferences() {
        Arg<TableReference> tableReferenceArg = ALL_UNSAFE.tableRef(ARG_NAME, TABLE_REFERENCE);
        assertThat(tableReferenceArg).isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void allUnsafeSupplierReturnsUnsafeRowComponents() {
        Arg<String> rowNameArg = ALL_UNSAFE.rowComponent(ARG_NAME, TABLE_REFERENCE, ROW_NAME);
        assertThat(rowNameArg).isInstanceOf(UnsafeArg.class);
    }

    @Test
    public void allUnsafeSupplierReturnsUnsafeColumnNames() {
        Arg<String> columnNameArg = ALL_UNSAFE.columnName(ARG_NAME, TABLE_REFERENCE, COLUMN_NAME);
        assertThat(columnNameArg).isInstanceOf(UnsafeArg.class);
    }


    @Test
    public void propagatesNameAndTableReferenceIfUnsafe() {
        Arg<TableReference> tableReferenceArg = ALL_UNSAFE.tableRef(ARG_NAME, TABLE_REFERENCE);
        assertThat(tableReferenceArg.getName()).isEqualTo(ARG_NAME);
        assertThat(tableReferenceArg.getValue()).isEqualTo(TABLE_REFERENCE);
    }

    @Test
    public void propagatesNameAndRowComponentNameIfUnsafe() {
        Arg<String> rowNameArg = ALL_UNSAFE.rowComponent(ARG_NAME, TABLE_REFERENCE, ROW_NAME);
        assertThat(rowNameArg.getName()).isEqualTo(ARG_NAME);
        assertThat(rowNameArg.getValue()).isEqualTo(ROW_NAME);
    }

    @Test
    public void propagatesNameAndColumnNameIfUnsafe() {
        Arg<String> columnNameArg = ALL_UNSAFE.columnName(ARG_NAME, TABLE_REFERENCE, COLUMN_NAME);
        assertThat(columnNameArg.getName()).isEqualTo(ARG_NAME);
        assertThat(columnNameArg.getValue()).isEqualTo(COLUMN_NAME);
    }
}
