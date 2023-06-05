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

package com.palantir.atlasdb.workload.invariant;

import com.google.common.collect.Ordering;
import com.palantir.atlasdb.workload.store.Columns;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InMemoryValidationStore;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.store.ValidationStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import io.vavr.Tuple;
import io.vavr.collection.List;
import io.vavr.collection.SortedMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public enum DurableWritesInvariant implements Invariant<Map<TableAndWorkloadCell, MismatchedValue>> {
    INSTANCE;

    @Override
    public void accept(
            WorkflowHistory workflowHistory, Consumer<Map<TableAndWorkloadCell, MismatchedValue>> invariantListener) {
        ValidationStore expectedState = InMemoryValidationStore.create(workflowHistory.history());
        ReadableTransactionStore storeToValidate = workflowHistory.transactionStore();
        Map<TableAndWorkloadCell, MismatchedValue> cellsThatDoNotMatch = expectedState
                .values()
                .flatMap((tableName, table) -> {
                    SortedMap<Integer, Columns> tableContents = table.snapshot();

                    // There is an issue with vavr, where flatMap on a sorted map is assumed to return a sorted map,
                    // but this should not be the case if the key type on the returned tuples is not comparable. We
                    // thus need to specify this ordering.
                    return tableContents.flatMap(Ordering.arbitrary(), (row, columns) -> {
                        SortedMap<Integer, Optional<Integer>> columnContents = columns.snapshot();
                        return columnContents.flatMap(Ordering.arbitrary(), (column, value) -> {
                            WorkloadCell cell = ImmutableWorkloadCell.of(row, column);

                            Optional<Integer> actualValue = storeToValidate.get(tableName, cell);

                            if (!actualValue.equals(value)) {
                                return List.of(Tuple.of(
                                        TableAndWorkloadCell.of(tableName, cell),
                                        MismatchedValue.of(actualValue, value)));
                            }
                            return List.of();
                        });
                    });
                })
                .toJavaMap();
        invariantListener.accept(cellsThatDoNotMatch);
    }
}
