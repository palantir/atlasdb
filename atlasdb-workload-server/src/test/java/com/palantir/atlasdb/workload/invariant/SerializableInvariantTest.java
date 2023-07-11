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
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.workload.store.ColumnAndValue;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.transaction.ColumnRangeSelection;
import com.palantir.atlasdb.workload.transaction.WitnessedTransactionsBuilder;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedRowColumnRangeReadTransactionAction;
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

    private static InvalidWitnessedTransactionAction getSingularFinalInvalidAction(
            List<InvalidWitnessedTransaction> invalidTransactions, List<WitnessedTransaction> transactions) {
        InvalidWitnessedTransaction invalidWitnessedTransaction = Iterables.getOnlyElement(invalidTransactions);

        assertThat(invalidWitnessedTransaction.transaction()).isEqualTo(Iterables.getLast(transactions));
        return Iterables.getOnlyElement(invalidWitnessedTransaction.invalidActions());
    }
}
