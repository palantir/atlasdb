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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.workload.invariant.CrossCellInconsistency;
import com.palantir.atlasdb.workload.invariant.Invariant;
import com.palantir.atlasdb.workload.store.AtlasDbTransactionStore;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.IsolationLevel;
import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableFullyWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedSingleCellReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.Test;

public class TransientRowsWorkflowsTest {
    private static final int ITERATION_COUNT = 5;
    private static final String TABLE_NAME = "transient.rows";
    private static final TransientRowsWorkflowConfiguration CONFIGURATION =
            ImmutableTransientRowsWorkflowConfiguration.builder()
                    .tableConfiguration(ImmutableTableConfiguration.builder()
                            .tableName(TABLE_NAME)
                            .isolationLevel(IsolationLevel.SERIALIZABLE)
                            .build())
                    .iterationCount(ITERATION_COUNT)
                    .build();

    private final InteractiveTransactionStore transactionStore = AtlasDbTransactionStore.create(
            TransactionManagers.createInMemory(ImmutableSet.of()),
            ImmutableMap.of(
                    TableReference.createWithEmptyNamespace(TABLE_NAME),
                    AtlasDbUtils.tableMetadata(IsolationLevel.SERIALIZABLE)));
    private final Workflow transientRowsWorkflow = TransientRowsWorkflows.create(
            transactionStore, CONFIGURATION, MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(1)));
    private final Invariant<List<CrossCellInconsistency>> invariant =
            TransientRowsWorkflows.getSummaryLogInvariantReporter(CONFIGURATION).invariant();

    @Test
    public void transactionStoreIsReadOnly() {
        WorkflowHistory history = transientRowsWorkflow.run();
        assertThat(history.transactionStore()).isInstanceOf(ReadOnlyTransactionStore.class);
    }

    @Test
    public void eachTransactionIsWitnessed() {
        WorkflowHistory history = transientRowsWorkflow.run();
        List<WitnessedTransaction> witnessedTransactions = history.history();
        assertThat(witnessedTransactions).hasSize(ITERATION_COUNT);
    }

    @Test
    public void firstTransactionWritesTwoCells() {
        WorkflowHistory history = transientRowsWorkflow.run();
        List<WitnessedTransaction> witnessedTransactions = history.history();
        assertThat(witnessedTransactions.get(0).actions())
                .hasSize(2)
                .containsExactlyInAnyOrder(
                        WitnessedWriteTransactionAction.of(
                                TABLE_NAME,
                                ImmutableWorkloadCell.of(0, TransientRowsWorkflows.COLUMN),
                                TransientRowsWorkflows.VALUE),
                        WitnessedWriteTransactionAction.of(
                                TABLE_NAME,
                                ImmutableWorkloadCell.of(TransientRowsWorkflows.SUMMARY_ROW, 0),
                                TransientRowsWorkflows.VALUE));
    }

    @Test
    public void subsequentTransactionsWriteThenReadThenDelete() {
        WorkflowHistory history = transientRowsWorkflow.run();
        List<WitnessedTransaction> witnessedTransactions = history.history();
        for (int index = 1; index < ITERATION_COUNT; index++) {
            assertThat(witnessedTransactions.get(index).actions()).hasSize(7).satisfies(actions -> {
                assertThat(actions.subList(0, 2)).allMatch(WitnessedWriteTransactionAction.class::isInstance);
                assertThat(actions.subList(2, 5)).allMatch(WitnessedSingleCellReadTransactionAction.class::isInstance);
                assertThat(actions.subList(5, 7)).allMatch(WitnessedDeleteTransactionAction.class::isInstance);
            });
        }
    }

    @Test
    public void invariantDoesNotReportViolationsUnderNormalOperation() {
        OnceSettableAtomicReference<List<CrossCellInconsistency>> reference = new OnceSettableAtomicReference<>();
        invariant.accept(transientRowsWorkflow.run(), reference::set);
        assertThat(reference.get()).isEmpty();
    }

    @Test
    public void invariantReportsViolationsFromFinalStateWhenCellsAreMissing() {
        OnceSettableAtomicReference<List<CrossCellInconsistency>> reference = new OnceSettableAtomicReference<>();
        WorkflowHistory history = transientRowsWorkflow.run();
        transactionStore.readWrite(txn -> txn.delete(TABLE_NAME, ImmutableWorkloadCell.of(ITERATION_COUNT - 1, 1)));
        invariant.accept(history, reference::set);
        assertThat(reference.get())
                .containsExactly(CrossCellInconsistency.builder()
                        .putInconsistentValues(
                                TableAndWorkloadCell.of(
                                        TABLE_NAME,
                                        ImmutableWorkloadCell.of(ITERATION_COUNT - 1, TransientRowsWorkflows.COLUMN)),
                                Optional.empty())
                        .putInconsistentValues(
                                TableAndWorkloadCell.of(
                                        TABLE_NAME,
                                        ImmutableWorkloadCell.of(
                                                TransientRowsWorkflows.SUMMARY_ROW, ITERATION_COUNT - 1)),
                                Optional.of(0))
                        .build());
    }

    @Test
    public void invariantReportsViolationsFromFinalStateWhenExtraCellsArePresent() {
        OnceSettableAtomicReference<List<CrossCellInconsistency>> reference = new OnceSettableAtomicReference<>();
        WorkflowHistory history = transientRowsWorkflow.run();
        transactionStore.readWrite(txn -> txn.write(
                TABLE_NAME,
                ImmutableWorkloadCell.of(ITERATION_COUNT - 2, TransientRowsWorkflows.COLUMN),
                TransientRowsWorkflows.VALUE));
        invariant.accept(history, reference::set);
        assertThat(reference.get())
                .containsExactly(CrossCellInconsistency.builder()
                        .putInconsistentValues(
                                TableAndWorkloadCell.of(
                                        TABLE_NAME,
                                        ImmutableWorkloadCell.of(ITERATION_COUNT - 2, TransientRowsWorkflows.COLUMN)),
                                Optional.of(0))
                        .putInconsistentValues(
                                TableAndWorkloadCell.of(
                                        TABLE_NAME,
                                        ImmutableWorkloadCell.of(
                                                TransientRowsWorkflows.SUMMARY_ROW, ITERATION_COUNT - 2)),
                                Optional.empty())
                        .build());
    }

    @Test
    public void invariantReportsViolationsFromTransactionHistory() {
        OnceSettableAtomicReference<List<CrossCellInconsistency>> reference = new OnceSettableAtomicReference<>();
        WorkflowHistory history = transientRowsWorkflow.run();
        WorkflowHistory falseHistory = ImmutableWorkflowHistory.builder()
                .transactionStore(history.transactionStore())
                .history(history.history().stream()
                        .map(witnessedTransaction -> ImmutableFullyWitnessedTransaction.builder()
                                .actions(witnessedTransaction.actions().stream()
                                        .map(TransientRowsWorkflowsTest::rewriteReadHistoryAsAlwaysInconsistent)
                                        .collect(Collectors.toList()))
                                .startTimestamp(witnessedTransaction.startTimestamp())
                                .commitTimestamp(witnessedTransaction.commitTimestamp())
                                .build())
                        .collect(Collectors.toList()))
                .build();
        invariant.accept(falseHistory, reference::set);

        assertThat(reference.get())
                .hasSize(ITERATION_COUNT - 1)
                .allSatisfy(TransientRowsWorkflowsTest::inconsistencyInvolvesPairOfPrimaryAndSummaryCells);
    }

    @Test
    public void invariantThrowsOnTransactionsFailingToReadTheSummaryRow() {
        WitnessedSingleCellReadTransactionAction readWitness = WitnessedSingleCellReadTransactionAction.of(
                TABLE_NAME, ImmutableWorkloadCell.of(5, TransientRowsWorkflows.COLUMN), Optional.empty());
        WorkflowHistory history = getWorkflowHistory(ImmutableList.of(readWitness));
        assertThatLoggableExceptionThrownBy(() -> invariant.accept(history, inconsistencies -> {}))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Expected to find a read of the summary row")
                .hasExactlyArgs(SafeArg.of("actions", ImmutableList.of(readWitness)));
    }

    @Test
    public void invariantThrowsOnTransactionsReadingTheSummaryButFailingToReadTheCorrespondingPrimaryRow() {
        List<WitnessedTransactionAction> actions = ImmutableList.of(WitnessedSingleCellReadTransactionAction.of(
                TABLE_NAME, ImmutableWorkloadCell.of(TransientRowsWorkflows.SUMMARY_ROW, 3), Optional.empty()));
        WorkflowHistory history = getWorkflowHistory(actions);
        assertThatLoggableExceptionThrownBy(() -> invariant.accept(history, inconsistencies -> {}))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Expected to find a read of a corresponding normal row")
                .hasExactlyArgs(SafeArg.of("actions", actions));
    }

    @Test
    public void invariantRecordsNoViolationIfCorrespondingCellsBothEmpty() {
        OnceSettableAtomicReference<List<CrossCellInconsistency>> reference = new OnceSettableAtomicReference<>();
        List<WitnessedTransactionAction> actions =
                getReadWitnessesForSingleTransaction(2, Optional.empty(), Optional.empty());
        WorkflowHistory history = getWorkflowHistory(actions);
        invariant.accept(history, reference::set);
        assertThat(reference.get()).isEmpty();
    }

    @Test
    public void invariantRecordsNoViolationIfCorrespondingCellsBothPresent() {
        OnceSettableAtomicReference<List<CrossCellInconsistency>> reference = new OnceSettableAtomicReference<>();
        List<WitnessedTransactionAction> actions = getReadWitnessesForSingleTransaction(
                2, Optional.of(TransientRowsWorkflows.VALUE), Optional.of(TransientRowsWorkflows.VALUE));
        WorkflowHistory history = getWorkflowHistory(actions);
        invariant.accept(history, reference::set);
        assertThat(reference.get()).isEmpty();
    }

    @Test
    public void invariantRecordsViolationsIfSummaryIsPresentAndPrimaryIsAbsent() {
        OnceSettableAtomicReference<List<CrossCellInconsistency>> reference = new OnceSettableAtomicReference<>();
        List<WitnessedTransactionAction> actions =
                getReadWitnessesForSingleTransaction(2, Optional.of(TransientRowsWorkflows.VALUE), Optional.empty());
        WorkflowHistory history = getWorkflowHistory(actions);
        invariant.accept(history, reference::set);
        assertThat(reference.get())
                .hasSize(1)
                .allSatisfy(TransientRowsWorkflowsTest::inconsistencyInvolvesPairOfPrimaryAndSummaryCells);
    }

    @Test
    public void invariantRecordsViolationsIfSummaryIsAbsentAndPrimaryIsPresent() {
        OnceSettableAtomicReference<List<CrossCellInconsistency>> reference = new OnceSettableAtomicReference<>();
        List<WitnessedTransactionAction> actions =
                getReadWitnessesForSingleTransaction(2, Optional.empty(), Optional.of(TransientRowsWorkflows.VALUE));
        WorkflowHistory history = getWorkflowHistory(actions);
        invariant.accept(history, reference::set);
        assertThat(reference.get())
                .hasSize(1)
                .allSatisfy(TransientRowsWorkflowsTest::inconsistencyInvolvesPairOfPrimaryAndSummaryCells);
    }

    private ImmutableWorkflowHistory getWorkflowHistory(List<WitnessedTransactionAction> actions) {
        return ImmutableWorkflowHistory.builder()
                .transactionStore(transactionStore)
                .history(ImmutableList.of(ImmutableFullyWitnessedTransaction.builder()
                        .addAllActions(actions)
                        .startTimestamp(0L)
                        .commitTimestamp(1L)
                        .build()))
                .build();
    }

    private static List<WitnessedTransactionAction> getReadWitnessesForSingleTransaction(
            int columnIndex, Optional<Integer> summaryValue, Optional<Integer> primaryValue) {
        return ImmutableList.of(
                WitnessedSingleCellReadTransactionAction.of(
                        TABLE_NAME,
                        ImmutableWorkloadCell.of(TransientRowsWorkflows.SUMMARY_ROW, columnIndex),
                        summaryValue),
                WitnessedSingleCellReadTransactionAction.of(
                        TABLE_NAME,
                        ImmutableWorkloadCell.of(columnIndex, TransientRowsWorkflows.COLUMN),
                        primaryValue));
    }

    private static void inconsistencyInvolvesPairOfPrimaryAndSummaryCells(CrossCellInconsistency inconsistency) {
        Map<TableAndWorkloadCell, Optional<Integer>> readValues = inconsistency.inconsistentValues();
        TableAndWorkloadCell summaryCell = readValues.keySet().stream()
                .filter(tableAndWorkloadCell -> tableAndWorkloadCell.cell().key() == TransientRowsWorkflows.SUMMARY_ROW)
                .findAny()
                .orElseThrow(() -> new SafeIllegalStateException(
                        "Expected to find a read of the summary row", SafeArg.of("readValues", readValues)));
        TableAndWorkloadCell primaryCell = getPrimaryCellForSummaryCell(summaryCell);

        assertThat(readValues.keySet()).containsExactlyInAnyOrder(summaryCell, primaryCell);
        assertThat(readValues.values())
                .containsExactlyInAnyOrder(Optional.empty(), Optional.of(TransientRowsWorkflows.VALUE));
    }

    // This workflow uses a single table, where the summary row's columns correspond to rows with primary values.
    private static TableAndWorkloadCell getPrimaryCellForSummaryCell(TableAndWorkloadCell summaryCell) {
        return TableAndWorkloadCell.of(
                summaryCell.tableName(),
                ImmutableWorkloadCell.of(summaryCell.cell().column(), TransientRowsWorkflows.COLUMN));
    }

    private static WitnessedTransactionAction rewriteReadHistoryAsAlwaysInconsistent(
            WitnessedTransactionAction action) {
        if (action instanceof WitnessedSingleCellReadTransactionAction) {
            WitnessedSingleCellReadTransactionAction readAction = (WitnessedSingleCellReadTransactionAction) action;
            if (readAction.cell().key() == TransientRowsWorkflows.SUMMARY_ROW) {
                return WitnessedSingleCellReadTransactionAction.of(
                        readAction.table(), readAction.cell(), Optional.empty());
            } else {
                return WitnessedSingleCellReadTransactionAction.of(
                        readAction.table(), readAction.cell(), Optional.of(TransientRowsWorkflows.VALUE));
            }
        } else {
            return action;
        }
    }

    private static final class OnceSettableAtomicReference<T> {
        private final AtomicReference<T> delegate;

        private OnceSettableAtomicReference() {
            this.delegate = new AtomicReference<>();
        }

        private T get() {
            return Preconditions.checkNotNull(delegate.get(), "Underlying atomic reference has not been set!");
        }

        private void set(T value) {
            Preconditions.checkNotNull(value, "Expecting values set to a once-settable atomic reference to be nonnull");
            if (!delegate.compareAndSet(null, value)) {
                throw new SafeIllegalStateException("Value already set");
            }
        }
    }
}
