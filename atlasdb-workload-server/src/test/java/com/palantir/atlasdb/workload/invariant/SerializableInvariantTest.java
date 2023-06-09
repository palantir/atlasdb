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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.workload.store.ColumnAndValue;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.store.RowResult;
import com.palantir.atlasdb.workload.transaction.ColumnRangeSelection;
import com.palantir.atlasdb.workload.transaction.RangeSlice;
import com.palantir.atlasdb.workload.transaction.WitnessedTransactionsBuilder;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedRowColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedRowRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedSingleCellTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedSingleCellReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.workflow.ImmutableWorkflowHistory;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class SerializableInvariantTest {

    @Mock
    private ReadableTransactionStore readableTransactionStore;

    @Test
    public void handlesLocalWrites() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .read(5, 10)
                .write(5, 10, 15)
                .read(5, 10, 15)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        assertThat(invalidTransactions).isEmpty();
    }

    @Test
    public void noInvalidTransactionsWhenSerializable() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .endTransaction()
                .startTransaction()
                .read(5, 10, 15)
                .write(23, 1, 20)
                .write(7, 15, 14)
                .endTransaction()
                .startTransaction()
                .read(5, 10, 15)
                .read(23, 1, 20)
                .read(7, 15, 14)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        assertThat(invalidTransactions).isEmpty();
    }

    @Test
    public void catchesWriteSkew() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(6, 10, 15)
                .endTransaction()
                .startTransaction()
                .read(5, 10, 15)
                .read(6, 10, 15)
                .write(5, 10, 0)
                .endTransaction()
                .startTransaction()
                .read(5, 10, 15)
                .read(6, 10, 15)
                .write(6, 10, 0)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                getSingularFinalInvalidAction(invalidTransactions, transactions);

        assertThat(invalidWitnessedTransactionAction)
                .isInstanceOfSatisfying(
                        InvalidWitnessedSingleCellTransactionAction.class,
                        invalidSingleCellWitnessedTransactionAction -> {
                            assertThat(invalidSingleCellWitnessedTransactionAction.action())
                                    .isInstanceOfSatisfying(WitnessedSingleCellReadTransactionAction.class, action -> {
                                        assertThat(action.cell()).isEqualTo(ImmutableWorkloadCell.of(5, 10));
                                        assertThat(action.value()).contains(15);
                                    });
                            assertThat(invalidSingleCellWitnessedTransactionAction.mismatchedValue())
                                    .isEqualTo(MismatchedValue.of(Optional.of(15), Optional.of(0)));
                        });
    }

    @Test
    public void handlesDeletes() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(6, 10, 15)
                .endTransaction()
                .startTransaction()
                .read(5, 10, 15)
                .delete(5, 10)
                .read(5, 10)
                .endTransaction()
                .startTransaction()
                .read(5, 10)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        assertThat(invalidTransactions).isEmpty();
    }

    @Test
    public void readsEmptyRowRangeIfNoCellsInRange() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(5, 15, 21)
                .endTransaction()
                .startTransaction()
                .rowColumnRangeRead(
                        5,
                        ColumnRangeSelection.builder()
                                .startColumnInclusive(8888888)
                                .build(),
                        ImmutableList.of())
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        assertThat(invalidTransactions).isEmpty();
    }

    @Test
    public void readsEmptyRowRangeIfNoCellsInRow() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(5, 15, 21)
                .endTransaction()
                .startTransaction()
                .rowColumnRangeRead(10, ColumnRangeSelection.builder().build(), ImmutableList.of())
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        assertThat(invalidTransactions).isEmpty();
    }

    @Test
    public void readsRowRangeToExhaustion() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(5, 15, 21)
                .endTransaction()
                .startTransaction()
                .rowColumnRangeRead(
                        5,
                        ColumnRangeSelection.builder().build(),
                        ImmutableList.of(ColumnAndValue.of(10, 15), ColumnAndValue.of(15, 21)))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        assertThat(invalidTransactions).isEmpty();
    }

    @Test
    public void incorporatesLocalWritesIntoRowColumnRangeQueries() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(5, 15, 21)
                .write(5, 20, 34)
                .endTransaction()
                .startTransaction()
                .rowColumnRangeRead(
                        5,
                        ColumnRangeSelection.builder().build(),
                        ImmutableList.of(
                                ColumnAndValue.of(10, 15), ColumnAndValue.of(15, 21), ColumnAndValue.of(20, 34)))
                .write(5, 10, 99)
                .write(5, 13, 24)
                .delete(5, 15)
                .write(5, 18, 36)
                .rowColumnRangeRead(
                        5,
                        ColumnRangeSelection.builder().build(),
                        ImmutableList.of(
                                ColumnAndValue.of(10, 99),
                                ColumnAndValue.of(13, 24),
                                ColumnAndValue.of(18, 36),
                                ColumnAndValue.of(20, 34)))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        assertThat(invalidTransactions).isEmpty();
    }

    @Test
    public void identifiesUnexpectedMissingCellInRowColumnRangeRead() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(5, 15, 21)
                .write(5, 20, 34)
                .endTransaction()
                .startTransaction()
                .rowColumnRangeRead(
                        5,
                        ColumnRangeSelection.builder().build(),
                        ImmutableList.of(ColumnAndValue.of(10, 15), ColumnAndValue.of(20, 34)))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                getSingularFinalInvalidAction(invalidTransactions, transactions);
        assertThat(invalidWitnessedTransactionAction)
                .isInstanceOfSatisfying(InvalidWitnessedRowColumnRangeReadTransactionAction.class, action -> assertThat(
                                action.expectedColumnsAndValues())
                        .containsExactly(
                                ColumnAndValue.of(10, 15), ColumnAndValue.of(15, 21), ColumnAndValue.of(20, 34)));
    }

    @Test
    public void identifiesUnexpectedAdditionalCellInRowColumnRangeRead() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(5, 20, 34)
                .endTransaction()
                .startTransaction()
                .rowColumnRangeRead(
                        5,
                        ColumnRangeSelection.builder().build(),
                        ImmutableList.of(
                                ColumnAndValue.of(10, 15), ColumnAndValue.of(15, 24), ColumnAndValue.of(20, 34)))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                getSingularFinalInvalidAction(invalidTransactions, transactions);
        assertThat(invalidWitnessedTransactionAction)
                .isInstanceOfSatisfying(InvalidWitnessedRowColumnRangeReadTransactionAction.class, action -> assertThat(
                                action.expectedColumnsAndValues())
                        .containsExactly(ColumnAndValue.of(10, 15), ColumnAndValue.of(20, 34)));
    }

    @Test
    public void identifiesIncorrectCellValuesInRowColumnRangeRead() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(5, 20, 34)
                .endTransaction()
                .startTransaction()
                .rowColumnRangeRead(
                        5,
                        ColumnRangeSelection.builder().build(),
                        ImmutableList.of(ColumnAndValue.of(10, 8888888), ColumnAndValue.of(20, 34)))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                getSingularFinalInvalidAction(invalidTransactions, transactions);
        assertThat(invalidWitnessedTransactionAction)
                .isInstanceOfSatisfying(InvalidWitnessedRowColumnRangeReadTransactionAction.class, action -> assertThat(
                                action.expectedColumnsAndValues())
                        .containsExactly(ColumnAndValue.of(10, 15), ColumnAndValue.of(20, 34)));
    }

    @Test
    public void identifiesIncorrectOrderingInRowColumnRangeRead() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 20, 34)
                .write(5, 10, 15)
                .endTransaction()
                .startTransaction()
                .rowColumnRangeRead(
                        5,
                        ColumnRangeSelection.builder().build(),
                        ImmutableList.of(ColumnAndValue.of(20, 34), ColumnAndValue.of(10, 15)))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                getSingularFinalInvalidAction(invalidTransactions, transactions);
        assertThat(invalidWitnessedTransactionAction)
                .isInstanceOfSatisfying(InvalidWitnessedRowColumnRangeReadTransactionAction.class, action -> assertThat(
                                action.expectedColumnsAndValues())
                        .containsExactly(ColumnAndValue.of(10, 15), ColumnAndValue.of(20, 34)));
    }

    @Test
    public void readsEmptyRangeIfNoCellsPresent() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(10, 10, 21)
                .endTransaction()
                .startTransaction()
                .rowRangeRead(
                        RangeSlice.builder()
                                .startInclusive(15)
                                .endExclusive(100)
                                .build(),
                        ImmutableSortedSet.of(10),
                        ImmutableList.of())
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        assertThat(invalidTransactions).isEmpty();
    }

    @Test
    public void readsRangeOfPresentCells() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(10, 10, 21)
                .endTransaction()
                .startTransaction()
                .rowRangeRead(
                        RangeSlice.all(),
                        ImmutableSortedSet.of(10),
                        ImmutableList.of(
                                RowResult.builder()
                                        .row(5)
                                        .addColumns(ColumnAndValue.of(10, 15))
                                        .build(),
                                RowResult.builder()
                                        .row(10)
                                        .addColumns(ColumnAndValue.of(10, 21))
                                        .build()))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        assertThat(invalidTransactions).isEmpty();
    }

    @Test
    public void incorporatesLocalWritesIntoRowRangeScans() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(10, 10, 15)
                .write(15, 10, 21)
                .write(20, 10, 34)
                .endTransaction()
                .startTransaction()
                .write(13, 10, 24)
                .delete(15, 10)
                .write(20, 10, 38)
                .rowRangeRead(
                        RangeSlice.all(),
                        ImmutableSortedSet.of(10),
                        ImmutableList.of(
                                RowResult.builder()
                                        .row(10)
                                        .addColumns(ColumnAndValue.of(10, 15))
                                        .build(),
                                RowResult.builder()
                                        .row(13)
                                        .addColumns(ColumnAndValue.of(10, 24))
                                        .build(),
                                RowResult.builder()
                                        .row(20)
                                        .addColumns(ColumnAndValue.of(10, 38))
                                        .build()))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        assertThat(invalidTransactions).isEmpty();
    }

    @Test
    public void identifiesUnexpectedMissingCellInRowRangeRead() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(10, 10, 21)
                .endTransaction()
                .startTransaction()
                .rowRangeRead(
                        RangeSlice.all(),
                        ImmutableSortedSet.of(10),
                        ImmutableList.of(RowResult.builder()
                                .row(10)
                                .addColumns(ColumnAndValue.of(10, 21))
                                .build()))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                getSingularFinalInvalidAction(invalidTransactions, transactions);
        assertThat(invalidWitnessedTransactionAction)
                .isInstanceOfSatisfying(InvalidWitnessedRowRangeReadTransactionAction.class, action -> assertThat(
                                action.expectedResults())
                        .containsExactly(
                                RowResult.builder()
                                        .row(5)
                                        .addColumns(ColumnAndValue.of(10, 15))
                                        .build(),
                                RowResult.builder()
                                        .row(10)
                                        .addColumns(ColumnAndValue.of(10, 21))
                                        .build()));
    }

    @Test
    public void identifiesUnexpectedAdditionalCellInRowRangeRead() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .endTransaction()
                .startTransaction()
                .rowRangeRead(
                        RangeSlice.all(),
                        ImmutableSortedSet.of(10),
                        ImmutableList.of(
                                RowResult.builder()
                                        .row(5)
                                        .addColumns(ColumnAndValue.of(10, 21))
                                        .build(),
                                RowResult.builder()
                                        .row(10)
                                        .addColumns(ColumnAndValue.of(10, 21))
                                        .build()))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                getSingularFinalInvalidAction(invalidTransactions, transactions);
        assertThat(invalidWitnessedTransactionAction)
                .isInstanceOfSatisfying(InvalidWitnessedRowRangeReadTransactionAction.class, action -> assertThat(
                                action.expectedResults())
                        .containsExactly(RowResult.builder()
                                .row(5)
                                .addColumns(ColumnAndValue.of(10, 15))
                                .build()));
    }

    @Test
    public void identifiesIncorrectCellValueInRowRangeRead() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 20)
                .endTransaction()
                .startTransaction()
                .rowRangeRead(
                        RangeSlice.all(),
                        ImmutableSortedSet.of(10),
                        ImmutableList.of(RowResult.builder()
                                .row(5)
                                .addColumns(ColumnAndValue.of(10, 21))
                                .build()))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                getSingularFinalInvalidAction(invalidTransactions, transactions);
        assertThat(invalidWitnessedTransactionAction)
                .isInstanceOfSatisfying(InvalidWitnessedRowRangeReadTransactionAction.class, action -> assertThat(
                                action.expectedResults())
                        .containsExactly(RowResult.builder()
                                .row(5)
                                .addColumns(ColumnAndValue.of(10, 20))
                                .build()));
    }

    @Test
    public void identifiesIncorrectOrderingInRowRangeRead() {
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 20)
                .write(15, 10, 28)
                .endTransaction()
                .startTransaction()
                .rowRangeRead(
                        RangeSlice.all(),
                        ImmutableSortedSet.of(10),
                        ImmutableList.of(
                                RowResult.builder()
                                        .row(15)
                                        .addColumns(ColumnAndValue.of(10, 28))
                                        .build(),
                                RowResult.builder()
                                        .row(5)
                                        .addColumns(ColumnAndValue.of(10, 20))
                                        .build()))
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);
        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                getSingularFinalInvalidAction(invalidTransactions, transactions);
        assertThat(invalidWitnessedTransactionAction)
                .isInstanceOfSatisfying(InvalidWitnessedRowRangeReadTransactionAction.class, action -> assertThat(
                                action.expectedResults())
                        .containsExactly(
                                RowResult.builder()
                                        .row(5)
                                        .addColumns(ColumnAndValue.of(10, 20))
                                        .build(),
                                RowResult.builder()
                                        .row(15)
                                        .addColumns(ColumnAndValue.of(10, 28))
                                        .build()));
    }

    private static InvalidWitnessedTransactionAction getSingularFinalInvalidAction(
            List<InvalidWitnessedTransaction> invalidTransactions, List<WitnessedTransaction> transactions) {
        InvalidWitnessedTransaction invalidWitnessedTransaction = Iterables.getOnlyElement(invalidTransactions);

        assertThat(invalidWitnessedTransaction.transaction()).isEqualTo(Iterables.getLast(transactions));
        return Iterables.getOnlyElement(invalidWitnessedTransaction.invalidActions());
    }
}
