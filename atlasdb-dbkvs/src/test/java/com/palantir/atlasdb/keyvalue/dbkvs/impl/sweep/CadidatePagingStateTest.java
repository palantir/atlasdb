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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState.BatchResult;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState.StartingPosition;

public class CadidatePagingStateTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void initialStartingPositionFromBeginningOfTable() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        Optional<StartingPosition> pos = state.getNextStartingPosition();
        assertTrue(pos.isPresent());
        assertArrayEquals(PtBytes.EMPTY_BYTE_ARRAY, pos.get().rowName);
        assertArrayEquals(PtBytes.EMPTY_BYTE_ARRAY, pos.get().colName);
        assertNull(pos.get().timestamp);
    }

    @Test
    public void initialStartingPositionFromSpecificRow() {
        CandidatePagingState state = CandidatePagingState.create(bytes("foo"));
        Optional<StartingPosition> pos = state.getNextStartingPosition();
        assertTrue(pos.isPresent());
        assertArrayEquals(bytes("foo"), pos.get().rowName);
        assertArrayEquals(PtBytes.EMPTY_BYTE_ARRAY, pos.get().colName);
        assertNull(pos.get().timestamp);
    }

    @Test
    public void startingPositionForSecondBatch() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        state.processBatch(ImmutableList.of(entry("a", "b", 10L), entry("a", "b", 20L)), 2);
        Optional<StartingPosition> pos = state.getNextStartingPosition();
        assertTrue(pos.isPresent());
        assertArrayEquals(bytes("a"), pos.get().rowName);
        assertArrayEquals(bytes("b"), pos.get().colName);
        assertEquals((Long)21L, pos.get().timestamp);
    }

    @Test
    public void noNextStartingPositionIfReachedEnd() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        // We requested up to 3 (cell, ts) pairs but got 2 back - this indicates that we reached the end
        state.processBatch(ImmutableList.of(entry("a", "b", 10L), entry("a", "b", 20L)), 3);
        Optional<StartingPosition> pos = state.getNextStartingPosition();
        assertFalse(pos.isPresent());
    }

    @Test
    public void reachedEndAfterScanningSingleCell() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);

        // We requested up to 3 (cell, ts) pairs but got 2 back - this indicates that we reached the end,
        // so we expect a candidate.
        BatchResult result = state.processBatch(ImmutableList.of(entry("a", "b", 10L), entry("a", "b", 20L)), 3);
        assertThat(result.candidates, contains(candidate("a", "b", false, 2L, 10L, 20L)));
        assertTrue(result.reachedEnd);
    }

    @Test
    public void singleCellSpanningTwoBatches() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);

        // We requested up to 2 (cell, ts) pair and got 2 back, all belonging to the same cell.
        // This means that we can't form a candidate yet - there could be more timestamps for that cell
        // in the next batch
        BatchResult firstBatch = state.processBatch(ImmutableList.of(entry("a", "b", 10L), entry("a", "b", 20L)), 2);
        assertThat(firstBatch.candidates, empty());
        assertFalse(firstBatch.reachedEnd);

        BatchResult secondBatch = state.processBatch(ImmutableList.of(entry("a", "b", 30L)), 2);
        assertThat(secondBatch.candidates, contains(candidate("a", "b", false, 3L, 10L, 20L, 30L)));
        assertTrue(secondBatch.reachedEnd);
    }

    @Test
    public void throwIfMaxCellTsExpectedIsZero() {
        thrown.expectMessage("maxCellTsPairsExpected must be strictly positive");
        CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY).processBatch(ImmutableList.of(), 0);
    }

    @Test
    public void throwIfGotRepeatingTimestamps() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        state.processBatch(ImmutableList.of(entry("a", "b", 20)), 1);
        thrown.expectMessage("Timestamps for each cell must be fed in strictly increasing order");
        state.processBatch(ImmutableList.of(entry("a", "b", 20)), 1);
    }

    @Test
    public void throwIfTimestampIsMaxLong() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        state.processBatch(ImmutableList.of(entry("a", "b", Long.MAX_VALUE)), 1);
        thrown.expectMessage("Timestamps must be strictly less than Long.MAX_VALUE");
        state.getNextStartingPosition();
    }

    private static CandidatePagingState.KvsEntryInfo entry(String row, String col, long ts) {
        return entry(row, col, ts, false);
    }

    private static CandidatePagingState.KvsEntryInfo entry(String row, String col, long ts, boolean emptyVal) {
        return new CandidatePagingState.KvsEntryInfo(bytes(row), bytes(col), ts, emptyVal);
    }

    private static CandidateCellForSweeping candidate(String row,
                                                      String col,
                                                      boolean emptyLatestVal,
                                                      long numCellTsPairsExamined,
                                                      long... sortedTimestamps) {
        return ImmutableCandidateCellForSweeping.builder()
                .cell(Cell.create(bytes(row), bytes(col)))
                .isLatestValueEmpty(emptyLatestVal)
                .isLatestValueEmpty(emptyLatestVal)
                .numCellsTsPairsExamined(numCellTsPairsExamined)
                .sortedTimestamps(sortedTimestamps)
                .build();
    }

    private static byte[] bytes(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

}
