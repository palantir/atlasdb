/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class CellsToSweepPartitioningIteratorTest {
    @Test
    public void canNotCreateIteratorWithZeroBatchSize() {
        assertThatThrownBy(() -> new CellsToSweepPartitioningIterator(
                        ImmutableList.of(batchWithThreeTssPerCell(1, 1, 10)).iterator(),
                        0,
                        new CellsToSweepPartitioningIterator.ExaminedCellLimit(PtBytes.toBytes("row"), 1L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void exactPartition() {
        List<BatchOfCellsToSweep> batches = partition(
                // Three input batches with 6 (cell, timestamp) pairs in each
                ImmutableList.of(
                        batchWithThreeTssPerCell(0, 2, 6),
                        batchWithThreeTssPerCell(2, 2, 6),
                        batchWithThreeTssPerCell(4, 2, 6)),
                // Request 12 (cell, ts) pairs per output batch: this should amount to
                // exactly two input batches per one output batch
                12,
                RangeRequests.getFirstRowName(),
                // An arbitrarily large examined cell limit to make sure we go through the entire input
                1000);
        // Expect two output batches: the first one should be the first two input batches combined
        assertThat(batches).containsExactly(batchWithThreeTssPerCell(0, 4, 12), batchWithThreeTssPerCell(4, 2, 6));
    }

    @Test
    public void inexactPartition() {
        List<BatchOfCellsToSweep> batches = partition(
                // Three input batches with 6 (cell, timestamp) pairs in each
                ImmutableList.of(
                        batchWithThreeTssPerCell(0, 2, 6),
                        batchWithThreeTssPerCell(2, 2, 6),
                        batchWithThreeTssPerCell(4, 2, 6)),
                // Request 8 (cell, ts) pairs per output batch. The first input batch is not sufficient
                // to fill that, but the first two batches are.
                8,
                RangeRequests.getFirstRowName(),
                1000);
        assertThat(batches).containsExactly(batchWithThreeTssPerCell(0, 4, 12), batchWithThreeTssPerCell(4, 2, 6));
    }

    @Test
    public void examinedCellLimit() {
        List<BatchOfCellsToSweep> batches = partition(
                ImmutableList.of(
                        batchWithThreeTssPerCell(0, 20, 30),
                        batchWithThreeTssPerCell(20, 20, 30),
                        batchWithThreeTssPerCell(40, 20, 30)),
                // A large timestamp batch size. Without the examined cell limit, we would
                // combine all three input batches in one.
                100000,
                RangeRequests.getFirstRowName(),
                50);
        // The first input batch examines 30 cells and is not sufficient to satisfy the limit (50).
        // However, the first two batches combined together examine 60 cells, which covers the limit.
        // Hence we expect one output batch which is the concatenation of the first two input batches.
        assertThat(batches).containsExactly(batchWithThreeTssPerCell(0, 40, 60));
    }

    @Test
    public void ignoreExaminedCellLimitUntilFinishedStartRow() {
        // Four input batches of two cells each, three timestamps per cell
        BatchOfCellsToSweep batch1 =
                batch(ImmutableList.of(cellWithThreeTimestamps(0, 0), cellWithThreeTimestamps(0, 1)), 10, cell(0, 9));
        BatchOfCellsToSweep batch2 = batch(
                ImmutableList.of(cellWithThreeTimestamps(0, 10), cellWithThreeTimestamps(0, 11)), 20, cell(0, 19));
        BatchOfCellsToSweep batch3 =
                batch(ImmutableList.of(cellWithThreeTimestamps(0, 20), cellWithThreeTimestamps(1, 0)), 30, cell(1, 7));
        BatchOfCellsToSweep batch4 = batch(
                ImmutableList.of(cellWithThreeTimestamps(1, 10), cellWithThreeTimestamps(1, 11)), 40, cell(1, 20));
        List<BatchOfCellsToSweep> batches = partition(
                ImmutableList.of(batch1, batch2, batch3, batch4),
                // Request 6 (cell, ts) pairs per batch: this means exactly one input batch per one output batch
                6,
                row(0),
                // Request just one (cell, ts) pair to examine.
                1);
        // Despite the limit being reached after the first input batch, we need to keep returning batches until
        // we examine at least one full row.
        assertThat(batches).containsExactly(batch1, batch2, batch3);
    }

    private static List<BatchOfCellsToSweep> partition(
            List<BatchOfCellsToSweep> input, int tsBatchSize, byte[] startRow, int maxCellsToExamine) {
        return ImmutableList.copyOf(new CellsToSweepPartitioningIterator(
                input.iterator(),
                tsBatchSize,
                new CellsToSweepPartitioningIterator.ExaminedCellLimit(startRow, maxCellsToExamine)));
    }

    private static BatchOfCellsToSweep batchWithThreeTssPerCell(
            int firstCell, int numCells, int numCellsExaminedInBatch) {
        List<CellToSweep> cells = new ArrayList<>();
        for (int i = 0; i < numCells; ++i) {
            cells.add(cellWithThreeTimestamps(firstCell + i, 0));
        }
        return ImmutableBatchOfCellsToSweep.builder()
                .cells(cells)
                .numCellTsPairsExamined(numCellsExaminedInBatch)
                .lastCellExamined(cell(firstCell + numCells, 0))
                .build();
    }

    private static BatchOfCellsToSweep batch(List<CellToSweep> cells, int numCellTsPairsExamined, Cell lastExamined) {
        return ImmutableBatchOfCellsToSweep.builder()
                .cells(cells)
                .numCellTsPairsExamined(numCellTsPairsExamined)
                .lastCellExamined(lastExamined)
                .build();
    }

    private static CellToSweep cellWithThreeTimestamps(int row, int col) {
        List<Long> tss = new ArrayList<>();
        for (long t = 0; t < 3; ++t) {
            tss.add(1000 + t);
        }
        return ImmutableCellToSweep.builder()
                .cell(cell(row, col))
                .sortedTimestamps(tss)
                .needsSentinel(false)
                .build();
    }

    private static Cell cell(int row, int col) {
        return Cell.create(row(row), row(col));
    }

    private static byte[] row(int increment) {
        return Ints.toByteArray(1000 + increment);
    }
}
