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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class SweepCellScanner {

    private final CqlExecutor cqlExecutor;
    private final TableReference tableRef;
    private final CandidateCellForSweepingRequest request;

    int batchHint = request.batchSizeHint().orElse(8096);

    public SweepCellScanner(CqlExecutor cqlExecutor, TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        this.cqlExecutor = cqlExecutor;
        this.tableRef = tableRef;
        this.request = request;
    }

    public List<CandidateCellForSweeping> getSweepableCells() {
        byte[] startRow = request.startRowInclusive();
        int batchHint = request.batchSizeHint().orElse(8096);

        List<CellWithTimestamp> cells = cqlExecutor.getCellTimestamps(tableRef, startRow, batchHint);
        if (cells.isEmpty()) {
            return Collections.emptyList();
        }

        CellWithTimestamp lastCell = Iterables.getLast(cells);
        byte[] lastRow = lastCell.cell().getRowName();
        byte[] lastCol = lastCell.cell().getColumnName();
        long lastTimestamp = lastCell.timestamp();

        while (true) {
            List<CellWithTimestamp> more = cqlExecutor.getCellTimestampsWithinRow(tableRef, lastRow, lastCol,
                    lastTimestamp, batchHint);
            cells.addAll(more);

            if (more.isEmpty()) {
                break;
            }
        }

        List<CandidateCellForSweeping> candidates = Lists.newArrayList();
        Cell currentCell = null;
        List<Long> timestampsForCell = null;

        for (CellWithTimestamp cell : cells) {
            if (currentCell != null && !currentCell.equals(cell.cell())) {
                candidates.add(ImmutableCandidateCellForSweeping.builder()
                        .cell(currentCell)
                        .sortedTimestamps(toArray(timestampsForCell))
                        .numCellsTsPairsExamined(0L)
                        .isLatestValueEmpty(false)
                        .build());
            }

            if (!Objects.equals(cell.cell(), currentCell)) {
                currentCell = cell.cell();
                timestampsForCell = Lists.newArrayList();
            }

            timestampsForCell.add(cell.timestamp());
        }

        candidates.add(ImmutableCandidateCellForSweeping.builder()
                .cell(currentCell)
                .sortedTimestamps(toArray(timestampsForCell))
                .numCellsTsPairsExamined(0L)
                .isLatestValueEmpty(false)
                .build());

        return candidates;
    }

    private long[] toArray(List<Long> values) {
        long[] result = new long[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = values.get(i);
        }
        return result;
    }
}
