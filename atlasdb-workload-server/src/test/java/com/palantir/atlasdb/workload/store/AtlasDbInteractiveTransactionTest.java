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

import com.google.common.collect.ImmutableSortedSet;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.workload.transaction.ColumnRangeSelection;
import com.palantir.atlasdb.workload.transaction.RangeSlice;
import com.palantir.atlasdb.workload.transaction.RowColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.RowRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedSingleCellReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
                        WitnessedSingleCellReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, Optional.empty()),
                        WitnessedDeleteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE));
    }

    @Test
    public void readHandlesEmptyAndPresentValue() {
        assertThat(readWrite(transaction -> {
                    transaction.read(TABLE_1, WORKLOAD_CELL_ONE);
                    transaction.write(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE);
                    transaction.read(TABLE_1, WORKLOAD_CELL_ONE);
                }))
                .containsExactly(
                        WitnessedSingleCellReadTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, Optional.empty()),
                        WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_ONE, VALUE_ONE),
                        WitnessedSingleCellReadTransactionAction.of(
                                TABLE_1, WORKLOAD_CELL_ONE, Optional.of(VALUE_ONE)));
    }

    @Test
    public void witnessRecordsColumnRangeActions() {
        assertThat(readWrite(transaction -> {
                    transaction.write(TABLE_1, WORKLOAD_CELL_TWO, VALUE_ONE);
                    transaction.getRowColumnRange(
                            TABLE_1,
                            WORKLOAD_CELL_TWO.key(),
                            ColumnRangeSelection.builder().build());
                }))
                .containsExactly(
                        WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, VALUE_ONE),
                        WitnessedRowColumnRangeReadTransactionAction.builder()
                                .originalQuery(RowColumnRangeReadTransactionAction.builder()
                                        .table(TABLE_1)
                                        .row(WORKLOAD_CELL_TWO.key())
                                        .columnRangeSelection(
                                                ColumnRangeSelection.builder().build())
                                        .build())
                                .addColumnsAndValues(ColumnValue.of(WORKLOAD_CELL_TWO.column(), VALUE_ONE))
                                .build());
    }

    @Test
    public void emptyColumnRangeReadsAreRecorded() {
        assertThat(readWrite(transaction -> {
                    transaction.getRowColumnRange(
                            TABLE_1,
                            WORKLOAD_CELL_TWO.key(),
                            ColumnRangeSelection.builder().build());
                }))
                .containsExactly(WitnessedRowColumnRangeReadTransactionAction.builder()
                        .originalQuery(RowColumnRangeReadTransactionAction.builder()
                                .table(TABLE_1)
                                .row(WORKLOAD_CELL_TWO.key())
                                .columnRangeSelection(
                                        ColumnRangeSelection.builder().build())
                                .build())
                        .build());
    }

    @Test
    public void allRelevantCellsAreRecordedForFullColumnScan() {
        int iterationCount = 1000;
        assertThat(readWrite(transaction -> {
                    IntStream.range(0, iterationCount)
                            .forEach(column ->
                                    transaction.write(TABLE_1, ImmutableWorkloadCell.of(1, column), VALUE_ONE));
                    transaction.getRowColumnRange(
                            TABLE_1, 1, ColumnRangeSelection.builder().build());
                }))
                .hasSize(iterationCount + 1)
                .element(iterationCount)
                .satisfies(rowColumnRangeReadAction -> assertThat(rowColumnRangeReadAction)
                        .isInstanceOfSatisfying(
                                WitnessedRowColumnRangeReadTransactionAction.class,
                                witness -> assertThat(witness.columnsAndValues())
                                        .isEqualTo(IntStream.range(0, iterationCount)
                                                .mapToObj(column -> ColumnValue.of(column, VALUE_ONE))
                                                .collect(Collectors.toList()))));
    }

    @Test
    public void allRelevantCellsAreRecordedForSpecificRangeSubquery() {
        int iterationCount = 1000;
        int startInclusive = 313;
        int endExclusive = 855;
        assertThat(readWrite(transaction -> {
                    IntStream.range(0, iterationCount)
                            .forEach(column ->
                                    transaction.write(TABLE_1, ImmutableWorkloadCell.of(1, column), VALUE_ONE));
                    transaction.getRowColumnRange(
                            TABLE_1,
                            1,
                            ColumnRangeSelection.builder()
                                    .startColumnInclusive(startInclusive)
                                    .endColumnExclusive(endExclusive)
                                    .build());
                }))
                .hasSize(iterationCount + 1)
                .element(iterationCount)
                .satisfies(rowColumnRangeReadAction -> assertThat(rowColumnRangeReadAction)
                        .isInstanceOfSatisfying(
                                WitnessedRowColumnRangeReadTransactionAction.class,
                                witness -> assertThat(witness.columnsAndValues())
                                        .isEqualTo(IntStream.range(startInclusive, endExclusive)
                                                .mapToObj(column -> ColumnValue.of(column, VALUE_ONE))
                                                .collect(Collectors.toList()))));
    }

    @Test
    public void witnessRecordsRowRangeScans() {
        assertThat(readWrite(transaction -> {
                    transaction.write(TABLE_1, WORKLOAD_CELL_TWO, VALUE_ONE);
                    transaction.getRange(
                            TABLE_1, RangeSlice.builder().build(), ImmutableSortedSet.of(WORKLOAD_CELL_TWO.column()));
                }))
                .containsExactly(
                        WitnessedWriteTransactionAction.of(TABLE_1, WORKLOAD_CELL_TWO, VALUE_ONE),
                        WitnessedRowRangeReadTransactionAction.builder()
                                .originalQuery(RowRangeReadTransactionAction.builder()
                                        .table(TABLE_1)
                                        .rowsToRead(RangeSlice.builder().build())
                                        .columns(ImmutableSortedSet.of(WORKLOAD_CELL_TWO.column()))
                                        .build())
                                .addResults(RowResult.builder()
                                        .row(WORKLOAD_CELL_TWO.key())
                                        .addColumns(ColumnValue.of(WORKLOAD_CELL_TWO.column(), VALUE_ONE))
                                        .build())
                                .build());
    }

    @Test
    public void emptyRowRangeScansAreRecorded() {
        assertThat(readWrite(transaction -> {
                    transaction.getRange(
                            TABLE_1, RangeSlice.builder().build(), ImmutableSortedSet.of(WORKLOAD_CELL_TWO.column()));
                }))
                .containsExactly(WitnessedRowRangeReadTransactionAction.builder()
                        .originalQuery(RowRangeReadTransactionAction.builder()
                                .table(TABLE_1)
                                .rowsToRead(RangeSlice.builder().build())
                                .columns(ImmutableSortedSet.of(WORKLOAD_CELL_TWO.column()))
                                .build())
                        .build());
    }

    @Test
    public void rowRangeScansReturnPreciselyValuesInRange() {
        int numWrites = 10;
        int column = 5;
        List<WitnessedTransactionAction> witnessedActions = readWrite(transaction -> {
            IntStream.range(0, numWrites)
                    .forEach(index -> transaction.write(
                            TABLE_1,
                            ImmutableWorkloadCell.builder()
                                    .key(index)
                                    .column(column)
                                    .build(),
                            index));
            transaction.getRange(
                    TABLE_1,
                    RangeSlice.builder().startInclusive(3).endExclusive(8).build(),
                    ImmutableSortedSet.of(column));
        });

        assertThat(witnessedActions)
                .hasSize(numWrites + 1)
                .endsWith(WitnessedRowRangeReadTransactionAction.builder()
                        .originalQuery(RowRangeReadTransactionAction.builder()
                                .table(TABLE_1)
                                .rowsToRead(RangeSlice.builder()
                                        .startInclusive(3)
                                        .endExclusive(8)
                                        .build())
                                .columns(ImmutableSortedSet.of(column))
                                .build())
                        .addAllResults(IntStream.range(3, 8)
                                .mapToObj(index -> RowResult.builder()
                                        .row(index)
                                        .addColumns(ColumnValue.of(column, index))
                                        .build())
                                .collect(Collectors.toList()))
                        .build());
    }

    @Test
    public void rowRangeScansOnlyReturnNecessaryColumns() {
        int numRows = 3;
        List<WitnessedTransactionAction> witnessedActions = readWrite(transaction -> {
            IntStream.range(0, numRows).forEach(index -> {
                transaction.write(
                        TABLE_1,
                        ImmutableWorkloadCell.builder().key(index).column(1).build(),
                        index);
                transaction.write(
                        TABLE_1,
                        ImmutableWorkloadCell.builder().key(index).column(2).build(),
                        index + 1);
            });
            transaction.getRange(TABLE_1, RangeSlice.builder().build(), ImmutableSortedSet.of(2));
        });

        assertThat(witnessedActions)
                .hasSize(2 * numRows + 1)
                .endsWith(WitnessedRowRangeReadTransactionAction.builder()
                        .originalQuery(RowRangeReadTransactionAction.builder()
                                .table(TABLE_1)
                                .rowsToRead(RangeSlice.builder().build())
                                .columns(ImmutableSortedSet.of(2))
                                .build())
                        .addAllResults(IntStream.range(0, numRows)
                                .mapToObj(index -> RowResult.builder()
                                        .row(index)
                                        .addColumns(ColumnValue.of(2, index + 1))
                                        .build())
                                .collect(Collectors.toList()))
                        .build());
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
        assertThatThrownWhenUnknownTableReferenced(transaction -> transaction.getRowColumnRange(
                TABLE_1, WORKLOAD_CELL_ONE.key(), ColumnRangeSelection.builder().build()));
    }

    @Test
    public void getRangeThrowsWhenTableDoesNotExist() {
        assertThatThrownWhenUnknownTableReferenced(transaction -> transaction.getRange(
                TABLE_1, RangeSlice.builder().build(), ImmutableSortedSet.of(WORKLOAD_CELL_ONE.column())));
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
    public void getRowColumnRangeThrowsWhenInteractiveTransactionAlreadyWitnessed() {
        assertThatThrownWhenInteractiveTransactionAlreadyWitnessed(transaction -> transaction.getRowColumnRange(
                TABLE_1, WORKLOAD_CELL_ONE.key(), ColumnRangeSelection.builder().build()));
    }

    @Test
    public void getRangeThrowsWhenInteractiveTransactionAlreadyWitnessed() {
        assertThatThrownWhenInteractiveTransactionAlreadyWitnessed(transaction -> transaction.getRange(
                TABLE_1, RangeSlice.builder().build(), ImmutableSortedSet.of(WORKLOAD_CELL_ONE.column())));
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
