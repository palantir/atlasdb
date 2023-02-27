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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.Test;

public class SingleRowTwoCellsWorkflowsTest {
    private static final TableReference TEST_TABLE =
            TableReference.create(Namespace.create("test"), "singleRowTwoCells");
    private static final TransactionStore TRANSACTION_STORE = AtlasDbTransactionStore.create(
            TransactionManagers.createInMemory(ImmutableSet.of()),
            ImmutableMap.of(TEST_TABLE, AtlasDbUtils.tableMetadata(ConflictHandler.SERIALIZABLE)));
    private static final int ITERATION_COUNT = 1_000;

    @Test
    public void smokeTest() {
        Workflow workflow = SingleRowTwoCellsWorkflows.createSingleRowTwoCell(
                TRANSACTION_STORE,
                ImmutableSingleCellWorkflowConfiguration.builder()
                        .tableName(TEST_TABLE.getTableName())
                        .genericWorkflowConfiguration(ImmutableWorkflowConfiguration.builder()
                                .iterationCount(ITERATION_COUNT)
                                .executionExecutor(
                                        MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(100)))
                                .build())
                        .build());
        assertWorkflowHistoryConsistent(workflow.run());
    }

    // This is basically a very simple invariant checker
    private void assertWorkflowHistoryConsistent(WorkflowHistory workflowHistory) {
        workflowHistory.history().forEach(SingleRowTwoCellsWorkflowsTest::validateLocalReadsMatchLocalWrites);

        List<WitnessedTransaction> transactionsByCommitTime = workflowHistory.history();

        // Start from 1 intentional as we want to look at pairs of transactions
        for (int index = 1; index < transactionsByCommitTime.size(); index++) {
            WitnessedTransaction predecessor = transactionsByCommitTime.get(index - 1);
            WitnessedTransaction successor = transactionsByCommitTime.get(index);

            assertThat(predecessor.commitTimestamp().orElseThrow())
                    .as("given all transactions touch the same cells, overlapping transactions should conflict and so "
                            + "they should not be allowed")
                    .isLessThan(successor.startTimestamp());

            validatePredecessorWritesSeenBySuccessor(predecessor, successor);
        }

        validateFinalTableState(workflowHistory);
    }

    private static void validateLocalReadsMatchLocalWrites(WitnessedTransaction transaction) {
        // The indexing is nasty, but I imagine verifiers are always going to be coupled to the implementation of the
        // task.
        WitnessedReadTransactionAction firstCellRead =
                transaction.actions().get(4).accept(ReadWitnessCaster.INSTANCE);
        WitnessedReadTransactionAction secondCellRead =
                transaction.actions().get(5).accept(ReadWitnessCaster.INSTANCE);
        transaction.actions().get(2).accept(new ReadValidationVisitor(firstCellRead));
        transaction.actions().get(3).accept(new ReadValidationVisitor(secondCellRead));
    }

    private static void validatePredecessorWritesSeenBySuccessor(
            WitnessedTransaction predecessor, WitnessedTransaction successor) {
        // The indexing is nasty, but I imagine verifiers are always going to be coupled to the implementation of the
        // task.
        WitnessedReadTransactionAction firstCellRead =
                successor.actions().get(0).accept(ReadWitnessCaster.INSTANCE);
        WitnessedReadTransactionAction secondCellRead =
                successor.actions().get(1).accept(ReadWitnessCaster.INSTANCE);
        predecessor.actions().get(2).accept(new ReadValidationVisitor(firstCellRead));
        predecessor.actions().get(3).accept(new ReadValidationVisitor(secondCellRead));
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

    private static final class ReadValidationVisitor implements WitnessedTransactionActionVisitor<Void> {
        private final WitnessedReadTransactionAction readWitness;

        private ReadValidationVisitor(WitnessedReadTransactionAction readWitness) {
            this.readWitness = readWitness;
        }

        @Override
        public Void visit(WitnessedReadTransactionAction readTransactionAction) {
            throw new SafeIllegalStateException("Meaningless to validate read against another read");
        }

        @Override
        public Void visit(WitnessedWriteTransactionAction writeTransactionAction) {
            Preconditions.checkState(
                    writeTransactionAction.cell().equals(readWitness.cell()),
                    "Read validations should only happen in relation to mutations of the same cell",
                    SafeArg.of("readWitness", readWitness),
                    SafeArg.of("writeTransactionAction", writeTransactionAction));
            assertThat(readWitness.value())
                    .as("reads should match the preceding write")
                    .contains(writeTransactionAction.value());
            return null;
        }

        @Override
        public Void visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
            Preconditions.checkState(
                    deleteTransactionAction.cell().equals(readWitness.cell()),
                    "Read validations should only happen in relation to mutations of the same cell",
                    SafeArg.of("readWitness", readWitness),
                    SafeArg.of("deleteTransactionAction", deleteTransactionAction));
            assertThat(readWitness.value())
                    .as("deletions should mean we do not read a value")
                    .isEmpty();
            return null;
        }
    }

    private enum ReadWitnessCaster implements WitnessedTransactionActionVisitor<WitnessedReadTransactionAction> {
        INSTANCE;

        @Override
        public WitnessedReadTransactionAction visit(WitnessedReadTransactionAction readTransactionAction) {
            return readTransactionAction;
        }

        @Override
        public WitnessedReadTransactionAction visit(WitnessedWriteTransactionAction writeTransactionAction) {
            throw new SafeIllegalStateException("Expecting a read transaction action, but found a write");
        }

        @Override
        public WitnessedReadTransactionAction visit(WitnessedDeleteTransactionAction deleteTransactionAction) {
            throw new SafeIllegalStateException("Expecting a read transaction action, but found a delete");
        }
    }
}
