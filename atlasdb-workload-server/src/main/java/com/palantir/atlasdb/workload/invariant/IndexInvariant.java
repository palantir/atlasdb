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

import com.palantir.atlasdb.workload.store.ImmutableValueReference;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.store.ValueReference;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * This invariant checks that an index (which can be a table, row or defined in some other way) is consistent with
 * cells in a base table.
 */
public final class IndexInvariant implements Invariant<List<CrossCellInconsistency>> {
    private final Function<ReadableTransactionStore, Map<TableAndWorkloadCell, Integer>> baseReader;
    private final Function<ReadableTransactionStore, Map<TableAndWorkloadCell, Integer>> indexReader;
    private final UnaryOperator<ValueReference> expectedIndexGivenCell;
    private final UnaryOperator<ValueReference> expectedCellGivenIndex;

    public IndexInvariant(Function<ReadableTransactionStore, Map<TableAndWorkloadCell, Integer>> baseReader, Function<ReadableTransactionStore, Map<TableAndWorkloadCell, Integer>> indexReader, UnaryOperator<ValueReference> expectedIndexGivenCell, UnaryOperator<ValueReference> expectedCellGivenIndex) {
        this.baseReader = baseReader;
        this.indexReader = indexReader;
        this.expectedIndexGivenCell = expectedIndexGivenCell;
        this.expectedCellGivenIndex = expectedCellGivenIndex;
    }


    public static IndexInvariant createWithinSameTable(Function<ReadableTransactionStore, Map<TableAndWorkloadCell, Integer>> baseReader, Function<ReadableTransactionStore, Map<TableAndWorkloadCell, Integer>> indexReader, UnaryOperator<WorkloadCell> expectedIndexGivenCell, UnaryOperator<WorkloadCell> expectedCellGivenIndex) {
        return new IndexInvariant(
                baseReader,
                indexReader,
                baseValue -> ImmutableValueReference.builder()
                        .from(baseValue)
                        .tableAndWorkloadCell(TableAndWorkloadCell.of(baseValue.tableAndWorkloadCell().tableName(),
                                expectedIndexGivenCell.apply(baseValue.tableAndWorkloadCell().cell())))
                        .build(),
                indexValue -> ImmutableValueReference.builder()
                        .from(indexValue)
                        .tableAndWorkloadCell(TableAndWorkloadCell.of(indexValue.tableAndWorkloadCell().tableName(),
                                expectedCellGivenIndex.apply(indexValue.tableAndWorkloadCell().cell())))
                        .build()
        );
    }

    @Override
    public void accept(WorkflowHistory workflowHistory, Consumer<List<CrossCellInconsistency>> violationsConsumer) {
        // TODO (jkong): Add support for finding inconsistency in the middle of a workflow.
        violationsConsumer.accept(findInconsistencyInEndState(workflowHistory.transactionStore()));
    }

    private List<CrossCellInconsistency> findInconsistencyInEndState(ReadableTransactionStore readableTransactionStore) {
        List<CrossCellInconsistency> violations = new ArrayList<>();
        Map<TableAndWorkloadCell, Integer> index = indexReader.apply(readableTransactionStore);
        Map<TableAndWorkloadCell, Integer> baseCells = baseReader.apply(readableTransactionStore);

        violations.addAll(findHangingReferences(baseCells, expectedIndexGivenCell, index));
        violations.addAll(findHangingReferences(index, expectedCellGivenIndex, baseCells));
        return violations;
    }

    private List<CrossCellInconsistency> findHangingReferences(Map<TableAndWorkloadCell, Integer> references, UnaryOperator<ValueReference> referenceToExpectedCell, Map<TableAndWorkloadCell, Integer> expectedCellReads) {
        List<CrossCellInconsistency> violations = new ArrayList<>();
        for (Map.Entry<TableAndWorkloadCell, Integer> indexValue : references.entrySet()) {
            ValueReference expectedBase = referenceToExpectedCell.apply(ImmutableValueReference.builder()
                    .tableAndWorkloadCell(indexValue.getKey())
                    .value(indexValue.getValue())
                    .build());
            Optional<Integer> actualValue = Optional.ofNullable(expectedCellReads.get(expectedBase.tableAndWorkloadCell()));
            if (!actualValue.equals(Optional.of(expectedBase.value()))) {
                violations.add(CrossCellInconsistency.builder()
                        .putInconsistentValues(indexValue.getKey(), Optional.of(indexValue.getValue()))
                        .putInconsistentValues(expectedBase.tableAndWorkloadCell(), actualValue)
                        .build());
            }
        }
        return violations;
    }
}
