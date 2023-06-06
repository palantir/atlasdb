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

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.workload.store.CellReferenceAndValue;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.transaction.WitnessedTransactionsBuilder;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedSingleCellTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.range.ImmutableWitnessedRowsColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.range.InvalidWitnessedRowsColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.range.WitnessedRowsColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.workflow.ImmutableWorkflowHistory;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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
                assertAndGetFinalTransactionInvalidity(invalidTransactions, transactions);

        assertThat(invalidWitnessedTransactionAction)
                .isInstanceOfSatisfying(
                        InvalidWitnessedSingleCellTransactionAction.class,
                        invalidSingleCellWitnessedTransactionAction -> {
                            assertThat(invalidSingleCellWitnessedTransactionAction.action())
                                    .isInstanceOfSatisfying(WitnessedReadTransactionAction.class, action -> {
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
    public void readsRowRangeToExhaustion() {
        UUID iteratorId = UUID.randomUUID();

        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(5, 15, 21)
                .endTransaction()
                .startTransaction()
                .createRowColumnRangeIterator(iteratorId, 5, 5, 20)
                .rowColumnRangeRead(iteratorId, 5, 10, 15)
                .rowColumnRangeRead(iteratorId, 5, 15, 21)
                .rowColumnRangeExhaustion(iteratorId, 5)
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
    public void onlyReadsRelevantValuesInRowColumnRangeScans() {
        UUID iteratorId = UUID.randomUUID();

        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 5, 5)
                .write(5, 10, 15)
                .write(5, 15, 21)
                .endTransaction()
                .startTransaction()
                .createRowColumnRangeIterator(iteratorId, 5, 8, 13)
                .rowColumnRangeRead(iteratorId, 5, 10, 15)
                .rowColumnRangeExhaustion(iteratorId, 5)
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
    public void identifiesMissingCellFromRowColumnRangeRead() {
        UUID iteratorId = UUID.randomUUID();
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .endTransaction()
                .startTransaction()
                .createRowColumnRangeIterator(iteratorId, 5, 0, 99)
                .rowColumnRangeExhaustion(iteratorId, 5)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);

        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                assertAndGetFinalTransactionInvalidity(invalidTransactions, transactions);
        assertThat(invalidWitnessedTransactionAction)
                .isEqualTo(InvalidWitnessedRowsColumnRangeReadTransactionAction.builder()
                        .rowColumnRangeRead(createWitnessedRowColumnRangeExhaustion(iteratorId, 5))
                        .expectedRead(createCellReferenceAndValue(5, 10, 15))
                        .build());
    }

    @Test
    public void identifiesIncorrectCellFromRowColumnRangeRead() {
        UUID iteratorId = UUID.randomUUID();
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .endTransaction()
                .startTransaction()
                .createRowColumnRangeIterator(iteratorId, 5, 0, 99)
                .rowColumnRangeRead(iteratorId, 5, 8, 15)
                .rowColumnRangeExhaustion(iteratorId, 5)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);

        InvalidWitnessedTransaction invalidWitnessedTransaction = Iterables.getOnlyElement(invalidTransactions);
        assertThat(invalidWitnessedTransaction.transaction()).isEqualTo(Iterables.getLast(transactions));

        // Note: This assertion may be slightly too sensitive, depending on how we continue verification after
        // discovering an error.
        assertThat(invalidWitnessedTransaction.invalidActions())
                .containsExactly(
                        InvalidWitnessedRowsColumnRangeReadTransactionAction.builder()
                                .rowColumnRangeRead(createWitnessedRowColumnRangeRead(iteratorId, 5, 8, 15))
                                .expectedRead(createCellReferenceAndValue(5, 10, 15))
                                .build(),
                        InvalidWitnessedRowsColumnRangeReadTransactionAction.builder()
                                .rowColumnRangeRead(createWitnessedRowColumnRangeExhaustion(iteratorId, 5))
                                .expectedRead(createCellReferenceAndValue(5, 10, 15))
                                .build());
    }

    @Test
    public void identifiesIncorrectValueFromRowColumnRangeRead() {
        UUID iteratorId = UUID.randomUUID();
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .endTransaction()
                .startTransaction()
                .createRowColumnRangeIterator(iteratorId, 5, 0, 99)
                .rowColumnRangeRead(iteratorId, 5, 10, 77777)
                .rowColumnRangeExhaustion(iteratorId, 5)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);

        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                assertAndGetFinalTransactionInvalidity(invalidTransactions, transactions);
        assertThat(invalidWitnessedTransactionAction)
                .isEqualTo(InvalidWitnessedRowsColumnRangeReadTransactionAction.builder()
                        .rowColumnRangeRead(createWitnessedRowColumnRangeRead(iteratorId, 5, 10, 77777))
                        .expectedRead(createCellReferenceAndValue(5, 10, 15))
                        .build());
    }

    @Test
    public void identifiesAdditionalCellFromRowColumnRangeRead() {
        UUID iteratorId = UUID.randomUUID();
        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(5, 20, 25)
                .endTransaction()
                .startTransaction()
                .createRowColumnRangeIterator(iteratorId, 5, 15, 99)
                .rowColumnRangeRead(iteratorId, 5, 10, 15)
                .rowColumnRangeRead(iteratorId, 5, 20, 25)
                .rowColumnRangeExhaustion(iteratorId, 5)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);

        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                assertAndGetFinalTransactionInvalidity(invalidTransactions, transactions);
        assertThat(invalidWitnessedTransactionAction)
                .isEqualTo(InvalidWitnessedRowsColumnRangeReadTransactionAction.builder()
                        .rowColumnRangeRead(createWitnessedRowColumnRangeRead(iteratorId, 5, 10, 15))
                        .expectedRead(createCellReferenceAndValue(5, 20, 25))
                        .build());
    }

    @Test
    public void permitsUndefinedRowColumnRangeScanBehaviourFollowingLocalConcurrentModification() {
        UUID readingInsertedValue = UUID.randomUUID();
        UUID notReadingInsertedValue = UUID.randomUUID();
        UUID readingCorruptedData = UUID.randomUUID();

        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .endTransaction()
                .startTransaction()
                .createRowColumnRangeIterator(readingInsertedValue, 5, 0, 99)
                .createRowColumnRangeIterator(notReadingInsertedValue, 5, 0, 99)
                .createRowColumnRangeIterator(readingCorruptedData, 5, 0, 99)
                .write(5, 5, 85)
                .rowColumnRangeRead(readingInsertedValue, 5, 5, 85)
                .rowColumnRangeRead(readingInsertedValue, 5, 10, 15)
                .rowColumnRangeExhaustion(readingInsertedValue, 5)
                .rowColumnRangeRead(notReadingInsertedValue, 5, 10, 15)
                .rowColumnRangeExhaustion(notReadingInsertedValue, 5)
                .rowColumnRangeRead(readingCorruptedData, 5, 7171717, 3141592)
                .rowColumnRangeExhaustion(readingCorruptedData, 5)
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
    public void includesPrecedingLocalWritesInRowColumnRangeScans() {
        UUID createdAfterWriteReadingValue = UUID.randomUUID();
        UUID createdAfterWriteNotReadingValue = UUID.randomUUID();

        List<InvalidWitnessedTransaction> invalidTransactions = new ArrayList<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .endTransaction()
                .startTransaction()
                .write(5, 5, 85)
                .createRowColumnRangeIterator(createdAfterWriteReadingValue, 5, 0, 99)
                .createRowColumnRangeIterator(createdAfterWriteNotReadingValue, 5, 0, 99)
                .rowColumnRangeRead(createdAfterWriteReadingValue, 5, 5, 85)
                .rowColumnRangeRead(createdAfterWriteReadingValue, 5, 10, 15)
                .rowColumnRangeExhaustion(createdAfterWriteReadingValue, 5)
                .rowColumnRangeRead(createdAfterWriteNotReadingValue, 5, 10, 15)
                .rowColumnRangeExhaustion(createdAfterWriteNotReadingValue, 5)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::addAll);

        InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                assertAndGetFinalTransactionInvalidity(invalidTransactions, transactions);

        assertThat(invalidWitnessedTransactionAction)
                .isEqualTo(InvalidWitnessedRowsColumnRangeReadTransactionAction.builder()
                        .rowColumnRangeRead(
                                createWitnessedRowColumnRangeRead(createdAfterWriteNotReadingValue, 5, 10, 15))
                        .expectedRead(createCellReferenceAndValue(5, 5, 85))
                        .build());
    }

    private static InvalidWitnessedTransactionAction assertAndGetFinalTransactionInvalidity(
            List<InvalidWitnessedTransaction> invalidTransactions, List<WitnessedTransaction> transactions) {
        InvalidWitnessedTransaction invalidWitnessedTransaction = Iterables.getOnlyElement(invalidTransactions);

        assertThat(invalidWitnessedTransaction.transaction()).isEqualTo(Iterables.getLast(transactions));
        return Iterables.getOnlyElement(invalidWitnessedTransaction.invalidActions());
    }

    private static ImmutableWitnessedRowsColumnRangeReadTransactionAction createWitnessedRowColumnRangeExhaustion(
            UUID iteratorId, int specificRow) {
        return WitnessedRowsColumnRangeReadTransactionAction.builder()
                .iteratorIdentifier(iteratorId)
                .specificRow(specificRow)
                .table("table")
                .build();
    }

    private static ImmutableWitnessedRowsColumnRangeReadTransactionAction createWitnessedRowColumnRangeRead(
            UUID iteratorId, int specificRow, int column, int value) {
        return WitnessedRowsColumnRangeReadTransactionAction.builder()
                .iteratorIdentifier(iteratorId)
                .specificRow(specificRow)
                .table("table")
                .cell(ImmutableWorkloadCell.of(specificRow, column))
                .value(value)
                .build();
    }

    private static CellReferenceAndValue createCellReferenceAndValue(int specificRow, int column, int value) {
        return CellReferenceAndValue.builder()
                .tableAndWorkloadCell(TableAndWorkloadCell.of("table", ImmutableWorkloadCell.of(specificRow, column)))
                .value(value)
                .build();
    }
}
