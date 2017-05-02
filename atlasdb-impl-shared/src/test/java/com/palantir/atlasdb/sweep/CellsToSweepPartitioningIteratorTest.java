/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.sweep.SweepableCellFilter.BatchOfCellsToSweep;
import com.palantir.atlasdb.sweep.SweepableCellFilter.CellToSweep;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

public class CellsToSweepPartitioningIteratorTest {

    @Test
    public void exactPartition() {
        List<BatchOfCellsToSweep> batches = partition(
                ImmutableList.of(batch(0, 2, 3, 2, 1), batch(2, 2, 3, 4, 3), batch(4, 2, 3, 6, 5)),
                12,
                RangeRequests.getFirstRowName(),
                1000);
        assertThat(batches).containsExactly(batch(0, 4, 3, 4, 3), batch(4, 2, 3, 6, 5));
    }

    @Test
    public void inexactPartition() {
        List<BatchOfCellsToSweep> batches = partition(
                ImmutableList.of(batch(0, 2, 3, 2, 1), batch(2, 2, 3, 4, 3), batch(4, 2, 3, 6, 5)),
                8,
                RangeRequests.getFirstRowName(),
                1000);
        assertThat(batches).containsExactly(batch(0, 4, 3, 4, 3), batch(4, 2, 3, 6, 5));
    }

    @Test
    public void examinedCellLimit() {
        List<BatchOfCellsToSweep> batches = partition(
                ImmutableList.of(batch(0, 20, 3, 20, 19), batch(20, 20, 3, 40, 39), batch(40, 20, 3, 60, 59)),
                10000,
                RangeRequests.getFirstRowName(),
                30);
        assertThat(batches).containsExactly(batch(0, 40, 3, 40, 39));
    }

    @Test
    public void ignoreExaminedCellLimitUntilFinishedStartRow() {
        List<BatchOfCellsToSweep> batches = partition(
                ImmutableList.of(
                    batch(ImmutableList.of(cellToSweep(0, 0, 4), cellToSweep(0, 1, 4)), 10, cell(0, 9)),
                    batch(ImmutableList.of(cellToSweep(0, 10, 4), cellToSweep(0, 11, 4)), 20, cell(0, 19)),
                    batch(ImmutableList.of(cellToSweep(0, 20, 4), cellToSweep(1, 0, 4)), 30, cell(1, 7)),
                    batch(ImmutableList.of(cellToSweep(1, 10, 4), cellToSweep(1, 11, 4)), 40, cell(1, 20))),
                8,
                row(0),
                5);
        assertThat(batches).containsExactly(
                batch(ImmutableList.of(cellToSweep(0, 0, 4), cellToSweep(0, 1, 4)), 10, cell(0, 9)),
                batch(ImmutableList.of(cellToSweep(0, 10, 4), cellToSweep(0, 11, 4)), 20, cell(0, 19)),
                batch(ImmutableList.of(cellToSweep(0, 20, 4), cellToSweep(1, 0, 4)), 30, cell(1, 7)));
    }

    private static List<BatchOfCellsToSweep> partition(List<BatchOfCellsToSweep> input,
                                                       int tsBatchSize,
                                                       byte[] startRow,
                                                       int maxCellsToExamine) {
        return ImmutableList.copyOf(new CellsToSweepPartitioningIterator(
                input.iterator(),
                tsBatchSize,
                new CellsToSweepPartitioningIterator.ExaminedCellLimit(startRow, maxCellsToExamine)));
    }

    private static BatchOfCellsToSweep batch(int firstCell,
                                             int numCells,
                                             int numTsPerCell,
                                             long numCellsExaminedSoFar,
                                             int lastCellExamined) {
        List<CellToSweep> cells = Lists.newArrayList();
        for (int i = 0; i < numCells; ++i) {
            cells.add(cellToSweep(firstCell + i, 0, numTsPerCell));
        }
        return ImmutableBatchOfCellsToSweep.builder()
                .cells(cells)
                .numCellTsPairsExaminedSoFar(numCellsExaminedSoFar)
                .lastCellExamined(cell(lastCellExamined, 0))
                .build();
    }

    private static BatchOfCellsToSweep batch(List<CellToSweep> cells, int numCellTsPairsExamined, Cell lastExamined) {
        return ImmutableBatchOfCellsToSweep.builder()
                    .cells(cells)
                    .numCellTsPairsExaminedSoFar(numCellTsPairsExamined)
                    .lastCellExamined(lastExamined)
                    .build();
    }

    private static CellToSweep cellToSweep(int row, int col, int numTs) {
        TLongList tss = new TLongArrayList();
        for (int t = 0; t < numTs; ++t) {
            tss.add(1000 + t);
        }
        return ImmutableCellToSweep.builder()
                .cell(cell(row, col))
                .sortedTimestamps(tss)
                .needSentinel(false)
                .build();
    }

    private static Cell cell(int row, int col) {
        return Cell.create(row(row), row(col));
    }

    private static byte[] row(int increment) {
        return Ints.toByteArray(1000 + increment);
    }

}
