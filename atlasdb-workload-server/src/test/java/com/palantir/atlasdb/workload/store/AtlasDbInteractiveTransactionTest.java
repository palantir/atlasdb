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

package com.palantir.atlasdb.workload.store;

import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.NAMES_TO_REFERENCES_TABLE_1;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLES_TO_ATLAS_METADATA;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.TABLE_1;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.VALUE_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_ONE;
import static com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers.WORKLOAD_CELL_TWO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.workload.transaction.ImmutableRowsColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowsColumnRangeExhaustionTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowsColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;

public final class AtlasDbInteractiveTransactionTest {

    private TransactionManager manager;

    @Before
    public void before() {
        manager = TransactionManagers.createInMemory(Set.of());
        manager.getKeyValueService().createTables(TABLES_TO_ATLAS_METADATA);
    }

    @Test
    public void witnessRecordsAllSingleCellActions() {
        assertThat(readWrite(transaction -> {
                    transaction.write(TABLE_1, WORKLOAD_CELL_TWO, VALUE_ONE);
                    transaction.read(TABLE_1, WORKLOAD_CELL_ONE);
                    transaction.delete(TABLE_1, WORKLOAD_CELL_ONE);
                }))
                .containsExactly(
                        WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, VALUE_ONE),
                        WitnessedReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, Optional.empty()),
                        WitnessedDeleteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE));
    }

    @Test
    public void witnessRecordsRowColumnRangeReads() {
        Integer keyOne = WORKLOAD_CELL_ONE.key();
        Integer keyTwo = WORKLOAD_CELL_TWO.key();
        assertThat(readWrite(transaction -> {
                    transaction.write(TABLE_1, WORKLOAD_CELL_TWO, VALUE_ONE);
                    Map<Integer, RowColumnRangeIterator> iteratorMap = transaction.getRowsColumnRange(
                            TABLE_1,
                            ImmutableList.of(keyOne, keyTwo),
                            ImmutableWorkloadColumnRangeSelection.builder().build());

                    RowColumnRangeIterator expectedEmpty = iteratorMap.get(keyOne);
                    assertThat(expectedEmpty.hasNext()).isFalse();

                    RowColumnRangeIterator expectedPresent = iteratorMap.get(keyTwo);
                    while (expectedPresent.hasNext()) {
                        expectedPresent.next();
                    }
                }))
                .containsExactly(
                        WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, VALUE_ONE),
                        WitnessedRowsColumnRangeExhaustionTransactionAction.builder()
                                .originalAction(ImmutableRowsColumnRangeReadTransactionAction.builder()
                                        .table(TABLE_1)
                                        .rows(ImmutableList.of(keyOne, keyTwo))
                                        .batchColumnRangeSelection(ImmutableWorkloadColumnRangeSelection.builder()
                                                .build())
                                        .build())
                                .specificRow(keyOne)
                                .build(),
                        WitnessedRowsColumnRangeReadTransactionAction.builder()
                                .originalAction(ImmutableRowsColumnRangeReadTransactionAction.builder()
                                        .table(TABLE_1)
                                        .rows(ImmutableList.of(keyOne, keyTwo))
                                        .batchColumnRangeSelection(ImmutableWorkloadColumnRangeSelection.builder()
                                                .build())
                                        .build())
                                .cell(WORKLOAD_CELL_TWO)
                                .specificRow(keyTwo)
                                .value(VALUE_ONE)
                                .build(),
                        WitnessedRowsColumnRangeExhaustionTransactionAction.builder()
                                .originalAction(ImmutableRowsColumnRangeReadTransactionAction.builder()
                                        .table(TABLE_1)
                                        .rows(ImmutableList.of(keyOne, keyTwo))
                                        .batchColumnRangeSelection(ImmutableWorkloadColumnRangeSelection.builder()
                                                .build())
                                        .build())
                                .specificRow(keyTwo)
                                .build());
    }

    @Test
    public void readHandlesEmptyAndPresentValue() {
        assertThat(readWrite(transaction -> {
                    transaction.read(TABLE_1, WORKLOAD_CELL_ONE);
                    transaction.write(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE);
                    transaction.read(TABLE_1, WORKLOAD_CELL_ONE);
                }))
                .containsExactly(
                        WitnessedReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, Optional.empty()),
                        WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE),
                        WitnessedReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, Optional.of(VALUE_ONE)));
    }

    @Test
    public void readThrowsWhenTableDoesNotExist() {
        assertThatThrownWhenUnknownTableReferenced(transaction -> transaction.read(TABLE_1, WORKLOAD_CELL_ONE));
    }

    @Test
    public void writeThrowsWhenTableDoesNotExist() {
        assertThatThrownWhenUnknownTableReferenced(
                transaction -> transaction.write(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE));
    }

    @Test
    public void deleteThrowsWhenTableDoesNotExist() {
        assertThatThrownWhenUnknownTableReferenced(transaction -> transaction.delete(TABLE_1, WORKLOAD_CELL_ONE));
    }

    @Test
    public void getRowsColumnRangeThrowsWhenTableDoesNotExist() {
        assertThatThrownWhenUnknownTableReferenced(transaction -> transaction.getRowsColumnRange(
                TABLE_1,
                List.of(WORKLOAD_CELL_ONE.key()),
                ImmutableWorkloadColumnRangeSelection.builder().build()));
    }

    @Test
    public void readThrowsWhenInteractiveTransactionAlreadyWitnessed() {
        assertThatThrownWhenInteractiveTransactionAlreadyWitnessed(
                transaction -> transaction.read(TABLE_1, WORKLOAD_CELL_ONE));
    }

    @Test
    public void writeThrowsWhenInteractiveTransactionAlreadyWitnessed() {
        assertThatThrownWhenInteractiveTransactionAlreadyWitnessed(
                transaction -> transaction.write(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE));
    }

    @Test
    public void deleteThrowsWhenInteractiveTransactionAlreadyWitnessed() {
        assertThatThrownWhenInteractiveTransactionAlreadyWitnessed(
                transaction -> transaction.delete(TABLE_1, WORKLOAD_CELL_ONE));
    }

    @Test
    public void getRowsColumnRangeThrowsWhenInteractiveTransactionAlreadyWitnessed() {
        assertThatThrownWhenInteractiveTransactionAlreadyWitnessed(transaction -> transaction.getRowsColumnRange(
                TABLE_1,
                List.of(WORKLOAD_CELL_ONE.key()),
                ImmutableWorkloadColumnRangeSelection.builder().build()));
    }

    private List<WitnessedTransactionAction> readWrite(Consumer<AtlasDbInteractiveTransaction> transactionConsumer) {
        return manager.runTaskWithRetry(atlasTransaction -> {
            AtlasDbInteractiveTransaction interactiveTransaction =
                    new AtlasDbInteractiveTransaction(atlasTransaction, NAMES_TO_REFERENCES_TABLE_1);
            transactionConsumer.accept(interactiveTransaction);
            return interactiveTransaction.witness();
        });
    }

    private void assertThatThrownWhenUnknownTableReferenced(
            Consumer<AtlasDbInteractiveTransaction> transactionConsumer) {
        assertThatThrownBy(() -> manager.runTaskWithRetry(txn -> {
                    transactionConsumer.accept(new AtlasDbInteractiveTransaction(txn, Map.of()));
                    return null;
                }))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Transaction action has unknown table.");
    }

    private void assertThatThrownWhenInteractiveTransactionAlreadyWitnessed(
            Consumer<AtlasDbInteractiveTransaction> transactionConsumer) {
        assertThatThrownBy(() -> manager.runTaskWithRetry(txn -> {
                    AtlasDbInteractiveTransaction transaction =
                            new AtlasDbInteractiveTransaction(txn, NAMES_TO_REFERENCES_TABLE_1);
                    transaction.witness();
                    transactionConsumer.accept(transaction);
                    return null;
                }))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Transaction has already been witnessed and can no longer perform any actions.");
    }
}
