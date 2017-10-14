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
import java.util.List;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState.CellTsPairInfo;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.sweep.CandidatePagingState.StartingPosition;

public class CandidatePagingStateTest {

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
        state.processBatch(ImmutableList.of(cellTs("a", "b", 10L), cellTs("a", "b", 20L)), false);
        Optional<StartingPosition> pos = state.getNextStartingPosition();
        assertTrue(pos.isPresent());
        assertArrayEquals(bytes("a"), pos.get().rowName);
        assertArrayEquals(bytes("b"), pos.get().colName);
        assertEquals((Long) 21L, pos.get().timestamp);
    }

    @Test
    public void noNextStartingPositionAfterReachedEnd() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        state.processBatch(ImmutableList.of(cellTs("a", "b", 10L)), true);
        Optional<StartingPosition> pos = state.getNextStartingPosition();
        assertFalse(pos.isPresent());
    }

    @Test
    public void reachedEndAfterScanningSingleCell() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);

        // We got 2 (cell, ts) pairs with the same cell and reached the end, so we expect a candidate.
        List<CandidateCellForSweeping> candidates = state.processBatch(
                ImmutableList.of(cellTs("a", "b", 10L), cellTs("a", "b", 20L)), true);
        assertThat(candidates, contains(candidate("a", "b", false, 2L, 10L, 20L)));
    }

    @Test
    public void singleCellSpanningTwoBatches() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);

        // We got 2 (cell, ts) pairs, both belonging to the same cell, and have not reached the end yet.
        // This means that we can't form a candidate yet - there could be more timestamps for that cell
        // in the next batch
        List<CandidateCellForSweeping> firstBatch = state.processBatch(
                ImmutableList.of(cellTs("a", "b", 10L), cellTs("a", "b", 20L)), false);
        assertThat(firstBatch, empty());

        List<CandidateCellForSweeping> secondBatch = state.processBatch(ImmutableList.of(cellTs("a", "b", 30L)), true);
        assertThat(secondBatch, contains(candidate("a", "b", false, 3L, 10L, 20L, 30L)));
    }

    @Test
    public void severalCellsInOneBatch() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        List<CandidateCellForSweeping> batch = state.processBatch(
                ImmutableList.of(cellTs("a", "x", 10L), cellTs("a", "y", 10L), cellTs("b", "y", 10L)), true);
        assertThat(batch, contains(
                candidate("a", "x", false, 1L, 10L),
                candidate("a", "y", false, 2L, 10L),
                candidate("b", "y", false, 3L, 10L)));
    }

    @Test
    public void testLatestValueEmpty() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        List<CandidateCellForSweeping> batch = state.processBatch(
                ImmutableList.of(cellTs("a", "x", 10L, false), cellTs("a", "x", 20L, true)), true);
        // Since the latest timestamp has the "isEmptyValue" flag set to true, we expect the candidate to
        // indicate that the latest value is empty
        assertThat(batch, contains(candidate("a", "x", true, 2L, 10L, 20L)));
    }

    @Test
    public void testNonLatestValueEmpty() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        List<CandidateCellForSweeping> batch = state.processBatch(
                ImmutableList.of(cellTs("a", "x", 10L, true), cellTs("a", "x", 20L, false)), true);
        // Since the latest timestamp has the "isEmptyValue" flag set to false, we expect the candidate to
        // indicate that the latest value is not empty
        assertThat(batch, contains(candidate("a", "x", false, 2L, 10L, 20L)));
    }

    @Test
    public void restartFromNextRow() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        state.processBatch(ImmutableList.of(cellTs("a", "x", 10L)), true);
        state.restartFromNextRow();
        Optional<StartingPosition> pos = state.getNextStartingPosition();
        assertTrue(pos.isPresent());
        // [ 'a', 0 ] is the lexicographically next key after [ 'a' ]
        assertArrayEquals(new byte[] { 'a', 0 }, pos.get().rowName);
        assertArrayEquals(PtBytes.EMPTY_BYTE_ARRAY, pos.get().colName);
        assertNull(pos.get().timestamp);
    }

    @Test
    public void restartFromNextRowWhenThereIsNoNextRow() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        byte[] rowName = RangeRequests.getLastRowName();
        CellTsPairInfo cellTs = new CellTsPairInfo(rowName, bytes("a"), 10L, false);
        state.processBatch(ImmutableList.of(cellTs), false);
        state.restartFromNextRow();
        // There is no next row if the current row is lexicographically last
        assertFalse(state.getNextStartingPosition().isPresent());
    }

    @Test
    public void cellTsPairsExaminedInCurrentRow() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        state.processBatch(ImmutableList.of(
                cellTs("a", "x", 10L), cellTs("b", "y", 10L), cellTs("b", "y", 20L), cellTs("b", "z", 30L)),
                false);
        // Three (cell, ts) pairs in row "b" so far: (b, y, 10), (b, y, 20), (b, z, 30)
        assertEquals(3, state.getCellTsPairsExaminedInCurrentRow());

        state.processBatch(ImmutableList.of(cellTs("b", "z", 40L)), false);
        // Added (b, z, 40), so should be 4 total now
        assertEquals(4, state.getCellTsPairsExaminedInCurrentRow());

        state.processBatch(ImmutableList.of(cellTs("c", "z", 40L)), false);
        // Started a new row "c", and only examined one entry
        assertEquals(1, state.getCellTsPairsExaminedInCurrentRow());

        state.restartFromNextRow();
        // Nothing examined yet if starting from a fresh row
        assertEquals(0, state.getCellTsPairsExaminedInCurrentRow());
    }

    @Test
    public void throwIfGotRepeatingTimestamps() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        state.processBatch(ImmutableList.of(cellTs("a", "b", 20L)), false);
        thrown.expectMessage("Timestamps for each cell must be fed in strictly increasing order");
        state.processBatch(ImmutableList.of(cellTs("a", "b", 20L)), false);
    }

    @Test
    public void throwIfTimestampIsMaxLong() {
        CandidatePagingState state = CandidatePagingState.create(PtBytes.EMPTY_BYTE_ARRAY);
        state.processBatch(ImmutableList.of(cellTs("a", "b", Long.MAX_VALUE)), false);
        thrown.expectMessage("Timestamps must be strictly less than Long.MAX_VALUE");
        state.getNextStartingPosition();
    }

    private static CellTsPairInfo cellTs(String row, String col, long ts) {
        return cellTs(row, col, ts, false);
    }

    private static CellTsPairInfo cellTs(String row, String col, long ts, boolean emptyVal) {
        return new CellTsPairInfo(bytes(row), bytes(col), ts, emptyVal);
    }

    private static CandidateCellForSweeping candidate(String row,
                                                      String col,
                                                      boolean emptyLatestVal,
                                                      long numCellTsPairsExamined,
                                                      long... sortedTimestamps) {
        return ImmutableCandidateCellForSweeping.builder()
                .cell(Cell.create(bytes(row), bytes(col)))
                .isLatestValueEmpty(emptyLatestVal)
                .numCellsTsPairsExamined(numCellTsPairsExamined)
                .sortedTimestamps(sortedTimestamps)
                .build();
    }

    private static byte[] bytes(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

}
