/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

public class KeyValueServiceLogArbitratorTest {
    private static final KeyValueServiceLogArbitrator ALL_UNSAFE = KeyValueServiceLogArbitrator.ALL_UNSAFE;

    private static final TableReference TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("foo.bar");

    private static final String ROW_NAME = "row";
    private static final String COLUMN_NAME = "column";

    @Test
    public void allUnsafeArbitratorMarksTableNamesAsUnsafe() {
        assertThat(ALL_UNSAFE.isTableReferenceSafe(TABLE_REFERENCE)).isFalse();
    }

    @Test
    public void allUnsafeArbitratorMarksRowNamesAsUnsafe() {
        assertThat(ALL_UNSAFE.isRowComponentNameSafe(TABLE_REFERENCE, ROW_NAME)).isFalse();
    }

    @Test
    public void allUnsafeArbitratorMarksColumnNamesAsUnsafe() {
        assertThat(ALL_UNSAFE.isColumnNameSafe(TABLE_REFERENCE, COLUMN_NAME)).isFalse();
    }
}
