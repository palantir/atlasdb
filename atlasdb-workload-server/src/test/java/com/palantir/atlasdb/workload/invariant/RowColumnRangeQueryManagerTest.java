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

import com.palantir.atlasdb.keyvalue.api.cache.StructureHolder;
import com.palantir.atlasdb.workload.invariant.RowColumnRangeQueryManager.RowColumnRangeQueryState;
import com.palantir.atlasdb.workload.invariant.RowColumnRangeQueryManager.SingleTableLiveQueries;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.WorkloadColumnRangeSelection;
import com.palantir.atlasdb.workload.transaction.RowsColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.WorkloadTestHelpers;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedWriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.range.WitnessedRowsColumnRangeIteratorCreationTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.range.WitnessedRowsColumnRangeReadTransactionAction;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;

public class RowColumnRangeQueryManagerTest {
    private static final int ROW_1 = 5;
    private static final int ROW_2 = 8;
    private static final int START_COLUMN_1 = 10;
    private static final int START_COLUMN_2 = 30;
    private static final RowsColumnRangeReadTransactionAction READ_TRANSACTION_ACTION_1 =
            RowsColumnRangeReadTransactionAction.builder()
                    .addRows(ROW_1)
                    .table(WorkloadTestHelpers.TABLE_1)
                    .batchColumnRangeSelection(WorkloadColumnRangeSelection.builder()
                            .startColumnInclusive(START_COLUMN_1)
                            .endColumnExclusive(50)
                            .build())
                    .build();
    private static final RowsColumnRangeReadTransactionAction READ_TRANSACTION_ACTION_2 =
            RowsColumnRangeReadTransactionAction.builder()
                    .addRows(ROW_1, ROW_2)
                    .table(WorkloadTestHelpers.TABLE_1)
                    .batchColumnRangeSelection(WorkloadColumnRangeSelection.builder()
                            .startColumnInclusive(START_COLUMN_2)
                            .endColumnExclusive(70)
                            .build())
                    .build();

    private static final UUID UUID_1 = UUID.randomUUID();
    private static final UUID UUID_2 = UUID.randomUUID();
    private static final UUID UUID_3 = UUID.randomUUID();

    private static final WitnessedRowsColumnRangeIteratorCreationTransactionAction
            WITNESSED_CREATION_TRANSACTION_ACTION_1 =
                    WitnessedRowsColumnRangeIteratorCreationTransactionAction.builder()
                            .specificRow(ROW_1)
                            .iteratorIdentifier(UUID_1)
                            .originalAction(READ_TRANSACTION_ACTION_1)
                            .build();
    private static final WitnessedRowsColumnRangeIteratorCreationTransactionAction
            WITNESSED_CREATION_TRANSACTION_ACTION_2 =
                    WitnessedRowsColumnRangeIteratorCreationTransactionAction.builder()
                            .specificRow(ROW_1)
                            .iteratorIdentifier(UUID_2)
                            .originalAction(READ_TRANSACTION_ACTION_2)
                            .build();
    private static final WitnessedRowsColumnRangeIteratorCreationTransactionAction
            WITNESSED_CREATION_TRANSACTION_ACTION_3 =
                    WitnessedRowsColumnRangeIteratorCreationTransactionAction.builder()
                            .specificRow(ROW_2)
                            .iteratorIdentifier(UUID_3)
                            .originalAction(READ_TRANSACTION_ACTION_2)
                            .build();

    private static final RowColumnRangeQueryState ROW_COLUMN_RANGE_QUERY_STATE_1 =
            ImmutableRowColumnRangeQueryState.builder()
                    .iteratorId(UUID_1)
                    .workloadColumnRangeSelection(READ_TRANSACTION_ACTION_1.batchColumnRangeSelection())
                    .lastReadCell(Optional.empty())
                    .build();
    private static final RowColumnRangeQueryState ROW_COLUMN_RANGE_QUERY_STATE_2 =
            ImmutableRowColumnRangeQueryState.builder()
                    .iteratorId(UUID_2)
                    .workloadColumnRangeSelection(READ_TRANSACTION_ACTION_2.batchColumnRangeSelection())
                    .lastReadCell(Optional.empty())
                    .build();
    private static final RowColumnRangeQueryState ROW_COLUMN_RANGE_QUERY_STATE_3 =
            ImmutableRowColumnRangeQueryState.builder()
                    .iteratorId(UUID_3)
                    .workloadColumnRangeSelection(READ_TRANSACTION_ACTION_2.batchColumnRangeSelection())
                    .lastReadCell(Optional.empty())
                    .build();

    private final StructureHolder<Map<String, SingleTableLiveQueries>> queriesByTable =
            StructureHolder.create(HashMap::empty);
    private final RowColumnRangeQueryManager queryManager = new RowColumnRangeQueryManager(queriesByTable);

    @Test
    public void tracksSingleQueryCreation() {
        queryManager.trackQueryCreation(WITNESSED_CREATION_TRANSACTION_ACTION_1);
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_1, ROW_1, START_COLUMN_1)))
                .contains(ROW_COLUMN_RANGE_QUERY_STATE_1);
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_1, ROW_1 + 1, START_COLUMN_1)))
                .as("rows and IDs must be matched")
                .isEmpty();
    }

    @Test
    public void tracksMultipleQueryCreationAndAssociatesCorrectMatchingState() {
        queryManager.trackQueryCreation(WITNESSED_CREATION_TRANSACTION_ACTION_1);
        queryManager.trackQueryCreation(WITNESSED_CREATION_TRANSACTION_ACTION_2);
        queryManager.trackQueryCreation(WITNESSED_CREATION_TRANSACTION_ACTION_3);
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_1, ROW_1, START_COLUMN_1)))
                .contains(ROW_COLUMN_RANGE_QUERY_STATE_1);
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_2, ROW_1, START_COLUMN_2)))
                .contains(ROW_COLUMN_RANGE_QUERY_STATE_2);
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_3, ROW_2, START_COLUMN_2)))
                .contains(ROW_COLUMN_RANGE_QUERY_STATE_3);
    }

    @Test
    public void invalidatesQueriesAffectedByWrites() {
        queryManager.trackQueryCreation(WITNESSED_CREATION_TRANSACTION_ACTION_1);
        queryManager.invalidateOverlappingQueries(WitnessedWriteTransactionAction.of(
                WorkloadTestHelpers.TABLE_1,
                ImmutableWorkloadCell.of(ROW_1, START_COLUMN_1),
                WorkloadTestHelpers.VALUE_ONE));
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_1, ROW_1, START_COLUMN_1)))
                .as("a write was performed to a row covered by the row column range scan, so it should be invalidated")
                .isEmpty();
    }

    @Test
    public void doesNotInvalidateQueryWhenWriteWasMadeToADifferentTable() {
        queryManager.trackQueryCreation(WITNESSED_CREATION_TRANSACTION_ACTION_1);
        queryManager.invalidateOverlappingQueries(WitnessedWriteTransactionAction.of(
                WorkloadTestHelpers.TABLE_2,
                ImmutableWorkloadCell.of(ROW_1, START_COLUMN_1),
                WorkloadTestHelpers.VALUE_ONE));
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_1, ROW_1, START_COLUMN_1)))
                .as("a write was performed to a row in table 2, so an iterator on table 1 is still valid")
                .contains(ROW_COLUMN_RANGE_QUERY_STATE_1);
    }

    @Test
    public void doesNotInvalidateQueryWhenWriteWasMadeToADifferentRow() {
        queryManager.trackQueryCreation(WITNESSED_CREATION_TRANSACTION_ACTION_1);
        queryManager.invalidateOverlappingQueries(WitnessedWriteTransactionAction.of(
                WorkloadTestHelpers.TABLE_1,
                ImmutableWorkloadCell.of(ROW_1 + 1, START_COLUMN_1),
                WorkloadTestHelpers.VALUE_ONE));
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_1, ROW_1, START_COLUMN_1)))
                .as("a write was performed to a different row, so the iterator is still valid")
                .contains(ROW_COLUMN_RANGE_QUERY_STATE_1);
    }

    @Test
    public void doesNotInvalidateQueryWhenWriteWasMadeToAColumnNotInRange() {
        queryManager.trackQueryCreation(WITNESSED_CREATION_TRANSACTION_ACTION_1);
        queryManager.invalidateOverlappingQueries(WitnessedWriteTransactionAction.of(
                WorkloadTestHelpers.TABLE_1, ImmutableWorkloadCell.of(ROW_1, START_COLUMN_1 - 1), 25));
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_1, ROW_1, START_COLUMN_1)))
                .as("a write was performed before the start of the query range, so the iterator is still valid")
                .contains(ROW_COLUMN_RANGE_QUERY_STATE_1);

        queryManager.invalidateOverlappingQueries(WitnessedWriteTransactionAction.of(
                WorkloadTestHelpers.TABLE_1,
                ImmutableWorkloadCell.of(
                        5,
                        READ_TRANSACTION_ACTION_1
                                .batchColumnRangeSelection()
                                .endColumnExclusive()
                                .orElseThrow(() -> new SafeRuntimeException(
                                        "Expected test query to have a start column, but it did not"))),
                25));
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_1, ROW_1, START_COLUMN_1)))
                .as("a write was performed at the exclusive end of the query range, so the iterator is still valid")
                .contains(ROW_COLUMN_RANGE_QUERY_STATE_1);
    }

    @Test
    public void invalidatesAllOverlappingQueries() {
        assertThat(READ_TRANSACTION_ACTION_2
                        .batchColumnRangeSelection()
                        .startColumnInclusive()
                        .orElseThrow(() ->
                                new SafeRuntimeException("Expected query to have a start column, but it did not")))
                .as("This test requires that the second query's range overlaps the first query's range")
                .isLessThan(READ_TRANSACTION_ACTION_1
                        .batchColumnRangeSelection()
                        .endColumnExclusive()
                        .orElseThrow(() ->
                                new SafeRuntimeException("Expected query to have an end column, but it did not")));

        queryManager.trackQueryCreation(WITNESSED_CREATION_TRANSACTION_ACTION_1);
        queryManager.trackQueryCreation(WITNESSED_CREATION_TRANSACTION_ACTION_2);
        queryManager.trackQueryCreation(WITNESSED_CREATION_TRANSACTION_ACTION_3);

        queryManager.invalidateOverlappingQueries(WitnessedWriteTransactionAction.of(
                WorkloadTestHelpers.TABLE_1, ImmutableWorkloadCell.of(ROW_1, START_COLUMN_2), 25));

        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_1, ROW_1, START_COLUMN_1)))
                .isEmpty();
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_2, ROW_1, START_COLUMN_2)))
                .isEmpty();
        assertThat(queryManager.getLiveQueryState(
                        getWitnessedRowsColumnRangeReadTransactionAction(UUID_3, ROW_2, START_COLUMN_2)))
                .as("the write does not relate to row 2, so this iterator is still valid")
                .contains(ROW_COLUMN_RANGE_QUERY_STATE_3);
    }

    private static WitnessedRowsColumnRangeReadTransactionAction getWitnessedRowsColumnRangeReadTransactionAction(
            UUID iteratorIdentifier, int row, int column) {
        return WitnessedRowsColumnRangeReadTransactionAction.builder()
                .iteratorIdentifier(iteratorIdentifier)
                .specificRow(row)
                .table(WorkloadTestHelpers.TABLE_1)
                .cell(ImmutableWorkloadCell.of(row, column))
                .value(20031023)
                .build();
    }
}
