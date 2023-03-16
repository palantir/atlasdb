/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.store;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.palantir.logsafe.SafeArg;
import java.util.Locale;
import org.junit.Test;

public final class IndexTableTest {

    private static final String TABLE_1 = "foo";
    private static final String TABLE_2 = "bar";

    @Test
    public void doesNotThrowWhenNamesAreDifferent() {
        assertThatCode(() -> IndexTable.of("foo", "bar")).doesNotThrowAnyException();
    }

    @Test
    public void indexTableNameIsFirstArgumentAndPrimaryTableIsSecondArgument() {
        IndexTable indexTable = IndexTable.of(TABLE_1, TABLE_2);
        assertThat(indexTable.indexTableName()).isEqualTo(TABLE_1);
        assertThat(indexTable.primaryTableName()).isEqualTo(TABLE_2);
    }

    @Test
    public void throwsWhenIndexNameIsTheSameAsPrimary() {
        assertThatLoggableExceptionThrownBy(() -> IndexTable.of(TABLE_1, TABLE_1))
                .hasMessageContaining("Index table name and primary table name are identical.")
                .hasExactlyArgs(SafeArg.of("name", TABLE_1), SafeArg.of("primaryTable", TABLE_1));
    }

    @Test
    public void throwsWhenIndexNameIsTheSameAsPrimaryIgnoresCase() {
        assertThatLoggableExceptionThrownBy(() -> IndexTable.of(TABLE_1, TABLE_1.toUpperCase(Locale.ROOT)))
                .hasMessageContaining("Index table name and primary table name are identical.")
                .hasExactlyArgs(
                        SafeArg.of("name", TABLE_1), SafeArg.of("primaryTable", TABLE_1.toUpperCase(Locale.ROOT)));
    }
}
