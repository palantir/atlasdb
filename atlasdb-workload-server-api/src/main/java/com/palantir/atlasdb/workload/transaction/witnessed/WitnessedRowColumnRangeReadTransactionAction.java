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

import com.palantir.atlasdb.workload.store.ColumnAndValue;
import com.palantir.atlasdb.workload.transaction.RowColumnRangeReadTransactionAction;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
public interface WitnessedRowColumnRangeReadTransactionAction extends WitnessedTransactionAction {
    RowColumnRangeReadTransactionAction originalQuery();

    // This is a List rather than a Set or Map because the ordering in which the columns and values are returned is
    // important (it must match the ordering specified by the contract of row column range reads).
    List<ColumnAndValue> columnsAndValues();

    @Override
    default <T> T accept(WitnessedTransactionActionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Value.Check
    default void check() {
        Set<Integer> knownColumns = new HashSet<>();
        Set<Integer> duplicateColumns = columnsAndValues().stream()
                .map(ColumnAndValue::column)
                .filter(column -> !knownColumns.add(column))
                .collect(Collectors.toSet());
        Preconditions.checkState(
                duplicateColumns.isEmpty(),
                "Duplicate columns in columnsAndValues",
                SafeArg.of("duplicateColumns", duplicateColumns),
                SafeArg.of("columnsAndValues", columnsAndValues()));
    }

    static ImmutableWitnessedRowColumnRangeReadTransactionAction.Builder builder() {
        return ImmutableWitnessedRowColumnRangeReadTransactionAction.builder();
    }
}
