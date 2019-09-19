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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.logsafe.Preconditions;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// The batches can end up very small or even empty after we filter out unsweepable cells,
// so we want to re-partition them before deleting.
public class CellsToSweepPartitioningIterator extends AbstractIterator<BatchOfCellsToSweep> {
    private final Iterator<BatchOfCellsToSweep> cellsToSweep;
    private final int deleteBatchSize;
    private final ExaminedCellLimit limit;
    private boolean limitReached = false;

    public CellsToSweepPartitioningIterator(Iterator<BatchOfCellsToSweep> cellsToSweep, int deleteBatchSize,
            ExaminedCellLimit limit) {
        Preconditions.checkArgument(deleteBatchSize > 0, "Iterator batch size must be positive");
        this.cellsToSweep = cellsToSweep;
        this.deleteBatchSize = deleteBatchSize;
        this.limit = limit;
    }

    public static class ExaminedCellLimit {
        private final byte[] startRow;
        private final long maxCellTsPairsToExamine;

        public ExaminedCellLimit(byte[] startRow, long maxCellTsPairsToExamine) {
            this.startRow = startRow;
            this.maxCellTsPairsToExamine = maxCellTsPairsToExamine;
        }

        public boolean examinedEnoughCells(long numCellTsPairsExamined, Cell lastCellExamined) {
            // We want to limit the number of cells we can examine in a single run,
            // but we need to make sure we finish at least one full row.
            return numCellTsPairsExamined >= maxCellTsPairsToExamine
                    && !Arrays.equals(startRow, lastCellExamined.getRowName());
        }
    }

    @Override
    protected BatchOfCellsToSweep computeNext() {
        if (limitReached) {
            return endOfData();
        } else {
            List<CellToSweep> batch = Lists.newArrayList();
            int cellTsPairsToDelete = 0;
            long numCellTsPairsExamined = 0;
            Cell lastCellExamined = null;
            while (cellTsPairsToDelete < deleteBatchSize && cellsToSweep.hasNext()) {
                BatchOfCellsToSweep sourceBatch = cellsToSweep.next();
                batch.addAll(sourceBatch.cells());
                for (CellToSweep cell : sourceBatch.cells()) {
                    cellTsPairsToDelete += cell.sortedTimestamps().size();
                }
                numCellTsPairsExamined += sourceBatch.numCellTsPairsExamined();
                lastCellExamined = sourceBatch.lastCellExamined();
                if (limit.examinedEnoughCells(numCellTsPairsExamined, lastCellExamined)) {
                    limitReached = true;
                    break;
                }
            }
            return lastCellExamined == null
                    ? endOfData()
                    : ImmutableBatchOfCellsToSweep.builder()
                            .cells(batch)
                            .numCellTsPairsExamined(numCellTsPairsExamined)
                            .lastCellExamined(lastCellExamined)
                            .build();
        }
    }
}
