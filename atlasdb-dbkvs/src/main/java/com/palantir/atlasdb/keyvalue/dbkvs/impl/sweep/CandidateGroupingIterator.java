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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

public final class CandidateGroupingIterator implements Iterator<List<CandidateCellForSweeping>> {
    private final Iterator<List<CellTsPairInfo>> cellTsIterator;

    private byte[] currentRowName = PtBytes.EMPTY_BYTE_ARRAY;
    private byte[] currentColName = PtBytes.EMPTY_BYTE_ARRAY;
    private final TLongList currentCellTimestamps = new TLongArrayList();
    private boolean currentIsLatestValueEmpty = false;
    private long cellTsPairsExamined = 0L;

    private CandidateGroupingIterator(Iterator<List<CellTsPairInfo>> cellTsIterator) {
        this.cellTsIterator = cellTsIterator;
    }

    /** The 'cellTsIterator' is expected to return (cell, ts) pairs in strict lexicographically increasing order.
     **/
    public static Iterator<List<CandidateCellForSweeping>> create(Iterator<List<CellTsPairInfo>> cellTsIterator) {
        return new CandidateGroupingIterator(cellTsIterator);
    }

    @Override
    public boolean hasNext() {
        return cellTsIterator.hasNext();
    }

    @Override
    public List<CandidateCellForSweeping> next() {
        Preconditions.checkState(hasNext());
        List<CellTsPairInfo> cellTsBatch = cellTsIterator.next();
        List<CandidateCellForSweeping> candidates = new ArrayList<>();
        for (CellTsPairInfo cellTs : cellTsBatch) {
            checkCurrentCellAndUpdateIfNecessary(cellTs).ifPresent(candidates::add);
            if (currentCellTimestamps.size() > 0) {
                // We expect the timestamps in ascending order. This check costs us a few CPU cycles
                // but it's worth it for paranoia reasons - mistaking a cell with data for an empty one
                // can cause data corruption.
                Preconditions.checkArgument(cellTs.ts > currentCellTimestamps.get(currentCellTimestamps.size() - 1),
                        "Timestamps for each cell must be fed in strictly increasing order");
            }
            updateStateAfterSingleCellTsPairProcessed(cellTs);
        }
        if (!cellTsIterator.hasNext()) {
            getCurrentCandidate().ifPresent(candidates::add);
        }
        return candidates;
    }

    private void updateStateAfterSingleCellTsPairProcessed(CellTsPairInfo cellTs) {
        currentIsLatestValueEmpty = cellTs.hasEmptyValue;
        currentCellTimestamps.add(cellTs.ts);
        cellTsPairsExamined += 1;
    }

    private Optional<CandidateCellForSweeping> checkCurrentCellAndUpdateIfNecessary(CellTsPairInfo cellTs) {
        if (isCurrentCell(cellTs)) {
            return Optional.empty();
        } else {
            Optional<CandidateCellForSweeping> candidate = getCurrentCandidate();
            updateStateForNewCell(cellTs);
            return candidate;
        }
    }

    private boolean isCurrentCell(CellTsPairInfo cellTs) {
        return Arrays.equals(currentRowName, cellTs.rowName) && Arrays.equals(currentColName, cellTs.colName);
    }

    private Optional<CandidateCellForSweeping> getCurrentCandidate() {
        if (currentCellTimestamps.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(ImmutableCandidateCellForSweeping.builder()
                    .cell(Cell.create(currentRowName, currentColName))
                    .sortedTimestamps(currentCellTimestamps.toArray())
                    .isLatestValueEmpty(currentIsLatestValueEmpty)
                    .numCellsTsPairsExamined(cellTsPairsExamined)
                    .build());
        }
    }

    private void updateStateForNewCell(CellTsPairInfo cell) {
        currentCellTimestamps.clear();
        currentRowName = cell.rowName;
        currentColName = cell.colName;
        currentIsLatestValueEmpty = false;
    }

}
