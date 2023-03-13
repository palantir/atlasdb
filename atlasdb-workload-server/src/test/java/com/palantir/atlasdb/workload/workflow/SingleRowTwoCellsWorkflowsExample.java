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

package com.palantir.atlasdb.workload.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.common.concurrent.PTExecutors;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.Test;

public class SingleRowTwoCellsWorkflowsExample {
    private static final TableReference TEST_TABLE =
            TableReference.create(Namespace.create("test"), "singleRowTwoCells");
    private static final int ITERATION_COUNT = 1_000;

    @Test
    public void workflowPassesWithSerializableConflictChecking() {
        runWorkflowWithConflictHandler(ConflictHandler.SERIALIZABLE, ITERATION_COUNT);
    }

    @Test
    public void workflowDoesNotPassWhenConflictsAreNotHandled() {
        assertThatThrownBy(() -> runWorkflowWithConflictHandler(ConflictHandler.IGNORE_ALL, 100))
                .isInstanceOf(AssertionError.class);
    }

    private void runWorkflowWithConflictHandler(ConflictHandler conflictHandler, int iterations) {
        SingleRowTwoCellsWorkflowConfiguration configuration = ImmutableSingleRowTwoCellsWorkflowConfiguration.builder()
                .iterationCount(iterations)
                .executionExecutor(MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(100)))
                .tableConfiguration(ImmutableTableConfiguration.builder()
                        .tableName(TEST_TABLE.getTableName())
                        .conflictHandler(conflictHandler)
                        .build())
                .build();

        TransactionStore transactionStore = AtlasDbTransactionStore.create(
                TransactionManagers.createInMemory(ImmutableSet.of()),
                ImmutableMap.of(
                        TEST_TABLE,
                        AtlasDbUtils.tableMetadata(
                                configuration.tableConfiguration().conflictHandler())));
        Workflow workflow = SingleRowTwoCellsWorkflows.createSingleRowTwoCell(transactionStore, configuration);
        assertWorkflowHistoryConsistent(workflow.run());
    }

    // This is basically a very simple invariant checker
    private void assertWorkflowHistoryConsistent(WorkflowHistory workflowHistory) {
        List<WitnessedTransaction> transactionsByCommitTime = workflowHistory.history();
        CellStateCheckingVisitor cellStateCheckingVisitor = new CellStateCheckingVisitor();

        transactionsByCommitTime.forEach(witnessedTransaction -> {
            LocalWriteWitnessVisitor localWriteWitnessVisitor = new LocalWriteWitnessVisitor();
            witnessedTransaction.actions().forEach(action -> {
                action.accept(localWriteWitnessVisitor);
                action.accept(cellStateCheckingVisitor);
            });
        });

        // Start from 1 intentional as we want to look at pairs of transactions
        for (int index = 1; index < transactionsByCommitTime.size(); index++) {
            WitnessedTransaction predecessor = transactionsByCommitTime.get(index - 1);
            WitnessedTransaction successor = transactionsByCommitTime.get(index);

            assertThat(predecessor.commitTimestamp())
                    .hasValueSatisfying(predecessorCommitTimestamp ->
                            assertThat(predecessorCommitTimestamp).isLessThan(successor.startTimestamp()))
                    .as("given all transactions touch the same cells, overlapping transactions should conflict and so "
                            + "they should not be allowed");
        }

        validateFinalTableState(workflowHistory);
    }

    private static void validateFinalTableState(WorkflowHistory workflowHistory) {
        Optional<Integer> firstCellState = workflowHistory
                .transactionStore()
                .get(TEST_TABLE.getTableName(), SingleRowTwoCellsWorkflows.FIRST_CELL);
        Optional<Integer> secondCellState = workflowHistory
                .transactionStore()
                .get(TEST_TABLE.getTableName(), SingleRowTwoCellsWorkflows.SECOND_CELL);
        assertThat(firstCellState.isPresent() ^ secondCellState.isPresent())
                .as("exactly one of the cells should be present")
                .isTrue();
        int cellValue = Stream.of(firstCellState, secondCellState)
                .flatMap(Optional::stream)
                .findAny()
                .orElseThrow();
        assertThat(cellValue)
                .as("cell value must correspond with the ID of a writer")
                .isBetween(0, ITERATION_COUNT);
    }

    private static final class LocalWriteWitnessVisitor implements WitnessedTransactionActionVisitor<Void> {
        private final Map<WorkloadCell, Optional<Integer>> localWriteMap = new HashMap<>();

        @Override
        public Void visit(WitnessedReadTransactionAction readTransactionAction) {
            if (localWriteMap.containsKey(readTransactionAction.cell())) {
                assertThat(readTransactionAction.value()).isEqualTo(localWriteMap.get(readTransactionAction.cell()));
            }
            return null;
        }

        @Override
        public Void visit(WitnessedWriteTransactionAction writeTransactionAction) {
            localWriteMap.put(writeTransactionAction.cell(), Optional.of(writeTransactionAction.value()));
            return null;
        }

        @Override
        public Void visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
            localWriteMap.put(deleteTransactionAction.cell(), Optional.empty());
            return null;
        }
    }

    private static final class CellStateCheckingVisitor implements WitnessedTransactionActionVisitor<Void> {
        private final Map<WorkloadCell, Integer> tableState = new HashMap<>();

        @Override
        public Void visit(WitnessedReadTransactionAction readTransactionAction) {
            Optional<Integer> expected = Optional.ofNullable(tableState.get(readTransactionAction.cell()));
            assertThat(readTransactionAction.value())
                    .as("read a cell that does not match the expected table state")
                    .isEqualTo(expected);
            return null;
        }

        @Override
        public Void visit(WitnessedWriteTransactionAction writeTransactionAction) {
            tableState.put(writeTransactionAction.cell(), writeTransactionAction.value());
            return null;
        }

        @Override
        public Void visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
            tableState.remove(deleteTransactionAction.cell());
            return null;
        }
    }
}
