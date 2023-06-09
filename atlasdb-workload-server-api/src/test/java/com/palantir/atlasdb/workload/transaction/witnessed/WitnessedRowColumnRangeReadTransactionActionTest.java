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

package com.palantir.atlasdb.workload.transaction.witnessed;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.collect.ImmutableList;
<<<<<<< HEAD
=======
import com.google.common.collect.ImmutableSet;
>>>>>>> origin/develop
import com.palantir.atlasdb.workload.store.ColumnValue;
import com.palantir.atlasdb.workload.transaction.ColumnRangeSelection;
import com.palantir.atlasdb.workload.transaction.RowColumnRangeReadTransactionAction;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
<<<<<<< HEAD
=======
import java.util.List;
>>>>>>> origin/develop
import org.junit.Test;

public class WitnessedRowColumnRangeReadTransactionActionTest {
    @Test
    public void canCreateWitness() {
        assertThatCode(() -> WitnessedRowColumnRangeReadTransactionAction.builder()
                        .addColumnsAndValues(ColumnValue.of(1, 5), ColumnValue.of(5, 1), ColumnValue.of(3, 3))
                        .originalQuery(RowColumnRangeReadTransactionAction.builder()
                                .table("foo")
                                .row(1)
                                .columnRangeSelection(
                                        ColumnRangeSelection.builder().build())
                                .build())
                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    public void cannotCreateWitnessWithDuplicateColumns() {
<<<<<<< HEAD
        assertThatLoggableExceptionThrownBy(() -> WitnessedRowColumnRangeReadTransactionAction.builder()
                        .addColumnsAndValues(ColumnValue.of(1, 5), ColumnValue.of(5, 1), ColumnValue.of(1, 3))
=======
        List<ColumnValue> columnsAndValues = ImmutableList.of(
                ColumnValue.of(1, 5),
                ColumnValue.of(5, 1),
                ColumnValue.of(1, 3),
                ColumnValue.of(5, 8),
                ColumnValue.of(9, 99));

        assertThatLoggableExceptionThrownBy(() -> WitnessedRowColumnRangeReadTransactionAction.builder()
                        .addAllColumnsAndValues(columnsAndValues)
>>>>>>> origin/develop
                        .originalQuery(RowColumnRangeReadTransactionAction.builder()
                                .table("foo")
                                .row(1)
                                .columnRangeSelection(
                                        ColumnRangeSelection.builder().build())
                                .build())
                        .build())
                .isInstanceOf(SafeIllegalStateException.class)
<<<<<<< HEAD
                .hasMessageContaining("Duplicate column in columnsAndValues")
                .hasExactlyArgs(
                        SafeArg.of("duplicatedColumn", 1),
                        SafeArg.of(
                                "columnsAndValues",
                                ImmutableList.of(ColumnValue.of(1, 5), ColumnValue.of(5, 1), ColumnValue.of(1, 3))));
=======
                .hasMessageContaining("Duplicate columns in columnsAndValues")
                .hasExactlyArgs(
                        SafeArg.of("duplicateColumns", ImmutableSet.of(1, 5)),
                        SafeArg.of("columnsAndValues", columnsAndValues));
>>>>>>> origin/develop
    }
}
