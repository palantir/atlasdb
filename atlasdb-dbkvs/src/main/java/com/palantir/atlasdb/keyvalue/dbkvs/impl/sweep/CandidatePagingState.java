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
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

public final class CandidatePagingState {
    private byte[] currentRowName;
    private byte[] currentColName = PtBytes.EMPTY_BYTE_ARRAY;
    private Long maxSeenTimestampForCurrentCell = null;
    private final TLongList currentCellTimestamps = new TLongArrayList();
    private boolean currentIsLatestValueEmpty = false;
    private long cellTsPairsExamined = 0L;
    private long cellTsPairsExaminedInCurrentRow = 0L;
    private boolean reachedEnd = false;

    private CandidatePagingState(byte[] currentRowName) {
        this.currentRowName = currentRowName;
    }

    public static CandidatePagingState create(byte[] startRowInclusive) {
        return new CandidatePagingState(startRowInclusive);
    }

    public static class StartingPosition {
        public final byte[] rowName;
        public final byte[] colName;
        @Nullable public final Long timestamp;

        public StartingPosition(byte[] rowName, byte[] colName, Long timestamp) {
            this.rowName = rowName;
            this.colName = colName;
            this.timestamp = timestamp;
        }
    }

    public static class CellTsPairInfo {
        public final byte[] rowName;
        public final byte[] colName;
        public final long ts;
        public final boolean isEmptyValue;

        public CellTsPairInfo(byte[] rowName, byte[] colName, long ts, boolean isEmptyValue) {
            this.rowName = rowName;
            this.colName = colName;
            this.ts = ts;
            this.isEmptyValue = isEmptyValue;
        }
    }

    /** The caller is expected to feed (cell, ts) pairs in strict lexicographically increasing order,
     *  even across several batches.
     **/
    public List<CandidateCellForSweeping> processBatch(List<CellTsPairInfo> cellTsPairs, boolean reachedEndOfResults) {
        List<CandidateCellForSweeping> candidates = new ArrayList<>();
        for (CellTsPairInfo cellTs : cellTsPairs) {
            checkCurrentCellAndUpdateIfNecessary(cellTs).ifPresent(candidates::add);
            if (maxSeenTimestampForCurrentCell != null) {
                // We expect the timestamps in ascending order. This check costs us a few CPU cycles
                // but it's worth it for paranoia reasons - mistaking a cell with data for an empty one
                // can cause data corruption.
                Preconditions.checkArgument(cellTs.ts > maxSeenTimestampForCurrentCell,
                        "Timestamps for each cell must be fed in strictly increasing order");
            }
            maxSeenTimestampForCurrentCell = cellTs.ts;
            currentIsLatestValueEmpty = cellTs.isEmptyValue;
            currentCellTimestamps.add(cellTs.ts);
            cellTsPairsExamined += 1;
            cellTsPairsExaminedInCurrentRow += 1;
        }
        if (reachedEndOfResults) {
            getCurrentCandidate().ifPresent(candidates::add);
            reachedEnd = true;
        }
        return candidates;
    }

    public Optional<StartingPosition> getNextStartingPosition() {
        if (reachedEnd) {
            return Optional.empty();
        } else {
            return Optional.of(new StartingPosition(currentRowName, currentColName, getNextStartTimestamp()));
        }
    }

    public long getCellTsPairsExaminedInCurrentRow() {
        return cellTsPairsExaminedInCurrentRow;
    }

    public void restartFromNextRow() {
        @Nullable byte[] nextRow = RangeRequests.getNextStartRowUnlessTerminal(false, currentRowName);
        if (nextRow == null) {
            reachedEnd = true;
        } else {
            currentRowName = nextRow;
            currentColName = PtBytes.EMPTY_BYTE_ARRAY;
            maxSeenTimestampForCurrentCell = null;
            currentCellTimestamps.clear();
            currentIsLatestValueEmpty = false;
            cellTsPairsExaminedInCurrentRow = 0L;
            reachedEnd = false;
        }
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

    @Nullable
    private Long getNextStartTimestamp() {
        if (maxSeenTimestampForCurrentCell == null) {
            return null;
        } else {
            // This can never happen because we request 'WHERE ts < ?', where '?' is some 'long' value.
            // So if the timestamp is strictly less than another 'long', it can not be Long.MAX_VALUE.
            // But we check anyway for general paranoia reasons.
            Preconditions.checkState(maxSeenTimestampForCurrentCell != Long.MAX_VALUE,
                    "Timestamps must be strictly less than Long.MAX_VALUE");
            return maxSeenTimestampForCurrentCell + 1;
        }
    }

    private Optional<CandidateCellForSweeping> checkCurrentCellAndUpdateIfNecessary(CellTsPairInfo cellTs) {
        boolean sameRow = Arrays.equals(currentRowName, cellTs.rowName);
        boolean sameCol = Arrays.equals(currentColName, cellTs.colName);
        if (!sameRow) {
            cellTsPairsExaminedInCurrentRow = 0L;
        }
        if (!sameRow || !sameCol) {
            Optional<CandidateCellForSweeping> candidate = getCurrentCandidate();
            currentCellTimestamps.clear();
            maxSeenTimestampForCurrentCell = null;
            currentRowName = cellTs.rowName;
            currentColName = cellTs.colName;
            currentIsLatestValueEmpty = false;
            return candidate;
        } else {
            return Optional.empty();
        }
    }

}
