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

package com.palantir.atlasdb.workload.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSortedSet;
import com.palantir.atlasdb.keyvalue.api.cache.StructureHolder;
import com.palantir.atlasdb.workload.store.ColumnAndValue;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.RowResult;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class SimpleRangeQueryReaderTest {
    private final StructureHolder<Map<TableAndWorkloadCell, Optional<Integer>>> valueMap =
            StructureHolder.create(HashMap::empty);
    private final SimpleRangeQueryReader reader = new SimpleRangeQueryReader(valueMap::getSnapshot);

    @Test
    public void columnRangeScansDoNotReadCellsFromDifferentTable() {
        valueMap.with(map -> map.put(
                TableAndWorkloadCell.of(WorkloadTestHelpers.TABLE_1, WorkloadTestHelpers.WORKLOAD_CELL_ONE),
                Optional.of(WorkloadTestHelpers.VALUE_ONE)));
        assertThat(reader.readColumnRange(RowColumnRangeReadTransactionAction.builder()
                        .table(WorkloadTestHelpers.TABLE_2)
                        .row(WorkloadTestHelpers.WORKLOAD_CELL_ONE.key())
                        .columnRangeSelection(ColumnRangeSelection.builder().build())
                        .build()))
                .isEmpty();
    }

    @Test
    public void columnRangeScansDoNotReadCellsFromDifferentRow() {
        valueMap.with(map -> map.put(
                TableAndWorkloadCell.of(WorkloadTestHelpers.TABLE_1, WorkloadTestHelpers.WORKLOAD_CELL_ONE),
                Optional.of(WorkloadTestHelpers.VALUE_ONE)));
        assertThat(reader.readColumnRange(RowColumnRangeReadTransactionAction.builder()
                        .table(WorkloadTestHelpers.TABLE_1)
                        .row(WorkloadTestHelpers.WORKLOAD_CELL_ONE.key() + 1)
                        .columnRangeSelection(ColumnRangeSelection.builder().build())
                        .build()))
                .isEmpty();
    }

    @Test
    public void columnRangeScansReadCorrectCellsFromCorrectTableInOrder() {
        for (int column = 0; column < 10; column++) {
            int finalColumn = column;
            valueMap.with(map -> map.put(
                    TableAndWorkloadCell.of(WorkloadTestHelpers.TABLE_1, ImmutableWorkloadCell.of(1, finalColumn)),
                    Optional.of(WorkloadTestHelpers.VALUE_ONE)));
        }
        assertThat(reader.readColumnRange(RowColumnRangeReadTransactionAction.builder()
                        .table(WorkloadTestHelpers.TABLE_1)
                        .row(1)
                        .columnRangeSelection(ColumnRangeSelection.builder()
                                .startColumnInclusive(2)
                                .endColumnExclusive(4)
                                .build())
                        .build()))
                .containsExactly(
                        ColumnAndValue.of(2, WorkloadTestHelpers.VALUE_ONE),
                        ColumnAndValue.of(3, WorkloadTestHelpers.VALUE_ONE));
    }

    @Test
    public void columnRangeScansDoNotReadCachedTombstones() {
        valueMap.with(map -> map.put(
                TableAndWorkloadCell.of(WorkloadTestHelpers.TABLE_1, WorkloadTestHelpers.WORKLOAD_CELL_ONE),
                Optional.empty()));
        assertThat(reader.readColumnRange(RowColumnRangeReadTransactionAction.builder()
                        .table(WorkloadTestHelpers.TABLE_1)
                        .row(WorkloadTestHelpers.WORKLOAD_CELL_ONE.key())
                        .columnRangeSelection(ColumnRangeSelection.builder().build())
                        .build()))
                .isEmpty();
    }

    @Test
    public void rowRangeScansDoNotReadCellsFromDifferentTable() {
        valueMap.with(map -> map.put(
                TableAndWorkloadCell.of(WorkloadTestHelpers.TABLE_1, WorkloadTestHelpers.WORKLOAD_CELL_ONE),
                Optional.of(WorkloadTestHelpers.VALUE_ONE)));
        assertThat(reader.readRowRange(RowRangeReadTransactionAction.builder()
                        .table(WorkloadTestHelpers.TABLE_2)
                        .rowsToRead(RangeSlice.all())
                        .columns(ImmutableSortedSet.of(WorkloadTestHelpers.WORKLOAD_CELL_ONE.column()))
                        .build()))
                .isEmpty();
    }

    @Test
    public void rowRangeScansDoNotReadCachedTombstones() {
        valueMap.with(map -> map.put(
                TableAndWorkloadCell.of(WorkloadTestHelpers.TABLE_1, WorkloadTestHelpers.WORKLOAD_CELL_ONE),
                Optional.empty()));
        assertThat(reader.readRowRange(RowRangeReadTransactionAction.builder()
                        .table(WorkloadTestHelpers.TABLE_1)
                        .rowsToRead(RangeSlice.all())
                        .columns(ImmutableSortedSet.of(WorkloadTestHelpers.WORKLOAD_CELL_ONE.column()))
                        .build()))
                .isEmpty();
    }

    @Test
    public void rowRangeScansReadCorrectColumns() {
        int expectedRows = 10;
        for (int row = 0; row < expectedRows; row++) {
            int finalRow = row;
            valueMap.with(map -> map.put(
                    TableAndWorkloadCell.of(WorkloadTestHelpers.TABLE_1, ImmutableWorkloadCell.of(finalRow, 1)),
                    Optional.of(finalRow)));
            valueMap.with(map -> map.put(
                    TableAndWorkloadCell.of(WorkloadTestHelpers.TABLE_1, ImmutableWorkloadCell.of(finalRow, 2)),
                    Optional.of(finalRow * 2)));
        }

        assertThat(reader.readRowRange(RowRangeReadTransactionAction.builder()
                        .table(WorkloadTestHelpers.TABLE_1)
                        .rowsToRead(RangeSlice.all())
                        .columns(ImmutableSortedSet.of(1))
                        .build()))
                .isEqualTo(IntStream.range(0, expectedRows)
                        .mapToObj(row -> RowResult.builder()
                                .row(row)
                                .addColumns(ColumnAndValue.of(1, row))
                                .build())
                        .collect(Collectors.toList()));
    }

    @Test
    public void rowRangeScansReadCorrectRows() {
        int expectedRows = 10;
        for (int row = 0; row < expectedRows; row++) {
            int finalRow = row;
            valueMap.with(map -> map.put(
                    TableAndWorkloadCell.of(WorkloadTestHelpers.TABLE_1, ImmutableWorkloadCell.of(finalRow, 1)),
                    Optional.of(finalRow)));
        }

        assertThat(reader.readRowRange(RowRangeReadTransactionAction.builder()
                        .table(WorkloadTestHelpers.TABLE_1)
                        .rowsToRead(RangeSlice.builder()
                                .startInclusive(3)
                                .endExclusive(8)
                                .build())
                        .columns(ImmutableSortedSet.of(1))
                        .build()))
                .isEqualTo(IntStream.range(3, 8)
                        .mapToObj(row -> RowResult.builder()
                                .row(row)
                                .addColumns(ColumnAndValue.of(1, row))
                                .build())
                        .collect(Collectors.toList()));
    }
}
