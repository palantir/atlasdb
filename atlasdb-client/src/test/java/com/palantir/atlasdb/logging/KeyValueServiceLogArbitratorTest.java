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

import com.palantir.atlasdb.keyvalue.api.TableReference;
import org.junit.Test;

public class KeyValueServiceLogArbitratorTest {
    private static final KeyValueServiceLogArbitrator ALL_UNSAFE = KeyValueServiceLogArbitrator.ALL_UNSAFE;
    private static final KeyValueServiceLogArbitrator ALL_SAFE = KeyValueServiceLogArbitrator.ALL_SAFE;

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

    @Test
    public void allSafeArbitratorMarksTableNamesAsSafe() {
        assertThat(ALL_SAFE.isTableReferenceSafe(TABLE_REFERENCE)).isTrue();
    }

    @Test
    public void allSafeArbitratorMarksRowNamesAsSafe() {
        assertThat(ALL_SAFE.isRowComponentNameSafe(TABLE_REFERENCE, ROW_NAME)).isTrue();
    }

    @Test
    public void allSafeArbitratorMarksColumnNamesAsSafe() {
        assertThat(ALL_SAFE.isColumnNameSafe(TABLE_REFERENCE, COLUMN_NAME)).isTrue();
    }
}
