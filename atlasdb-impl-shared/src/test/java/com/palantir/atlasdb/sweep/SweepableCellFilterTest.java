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
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Test;

public class SweepableCellFilterTest {
    private static final long LOW_START_TS = 6L;
    private static final long LOW_COMMIT_TS = 7L;
    private static final long HIGH_START_TS = 100L;
    private static final long HIGH_COMMIT_TS = 102L;

    private static final Cell SINGLE_CELL =
            Cell.create("cellRow".getBytes(StandardCharsets.UTF_8), "cellCol".getBytes(StandardCharsets.UTF_8));
    private static final Cell ANOTHER_CELL =
            Cell.create("cellRow2".getBytes(StandardCharsets.UTF_8), "cellCol2".getBytes(StandardCharsets.UTF_8));

    private final TransactionService mockTransactionService = mock(TransactionService.class);
    private final CommitTsCache commitTsCache = CommitTsCache.create(mockTransactionService);

    @Test
    public void conservative_getTimestampsToSweep_twoEntriesBelowSweepTimestamp_returnsLowerOne() {
        long sweepTimestampHigherThanCommitTimestamp = HIGH_COMMIT_TS + 1;
        List<CandidateCellForSweeping> candidates = twoCommittedTimestampsForSingleCell();
        SweepableCellFilter filter =
                new SweepableCellFilter(commitTsCache, Sweeper.CONSERVATIVE, sweepTimestampHigherThanCommitTimestamp);
        List<CellToSweep> cells = filter.getCellsToSweep(candidates).cells();
        assertThat(cells).hasSize(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(LOW_START_TS);
    }

    @Test
    public void conservative_getTimestampsToSweep_oneEntryBelowTimestamp_oneAbove_returnsNone() {
        long sweepTimestampLowerThanCommitTimestamp = HIGH_COMMIT_TS - 1;
        List<CandidateCellForSweeping> candidates = twoCommittedTimestampsForSingleCell();
        SweepableCellFilter filter =
                new SweepableCellFilter(commitTsCache, Sweeper.CONSERVATIVE, sweepTimestampLowerThanCommitTimestamp);
        List<CellToSweep> cells = filter.getCellsToSweep(candidates).cells();
        assertThat(cells).isEmpty();
    }

    @Test
    public void conservativeGetTimestampToSweepAddsSentinels() {
        long sweepTimestampHigherThanCommitTimestamp = HIGH_COMMIT_TS + 1;
        List<CandidateCellForSweeping> candidates = twoCommittedTimestampsForSingleCell();
        SweepableCellFilter filter =
                new SweepableCellFilter(commitTsCache, Sweeper.CONSERVATIVE, sweepTimestampHigherThanCommitTimestamp);
        List<CellToSweep> cells = filter.getCellsToSweep(candidates).cells();
        assertThat(Iterables.getOnlyElement(cells).needsSentinel()).isTrue();
    }

    @Test
    public void thoroughGetTimestampToSweepDoesNotAddSentinels() {
        long sweepTimestampHigherThanCommitTimestamp = HIGH_COMMIT_TS + 1;
        List<CandidateCellForSweeping> candidates = twoCommittedTimestampsForSingleCell();
        SweepableCellFilter filter =
                new SweepableCellFilter(commitTsCache, Sweeper.THOROUGH, sweepTimestampHigherThanCommitTimestamp);
        List<CellToSweep> cells = filter.getCellsToSweep(candidates).cells();
        assertThat(Iterables.getOnlyElement(cells).needsSentinel()).isFalse();
    }

    @Test
    public void getTimestampsToSweep_onlyTransactionUncommitted_returnsIt() {
        List<CandidateCellForSweeping> candidate = ImmutableList.of(ImmutableCandidateCellForSweeping.builder()
                .cell(SINGLE_CELL)
                .sortedTimestamps(ImmutableList.of(LOW_START_TS))
                .isLatestValueEmpty(false)
                .build());
        when(mockTransactionService.get(anyCollection()))
                .thenReturn(ImmutableMap.of(LOW_START_TS, TransactionConstants.FAILED_COMMIT_TS));
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, Sweeper.CONSERVATIVE, HIGH_START_TS);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells).hasSize(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(LOW_START_TS);
    }

    @Test
    public void thorough_getTimestampsToSweep_oneTransaction_emptyValue_returnsIt() {
        List<CandidateCellForSweeping> candidate = ImmutableList.of(ImmutableCandidateCellForSweeping.builder()
                .cell(SINGLE_CELL)
                .sortedTimestamps(ImmutableList.of(LOW_START_TS))
                .isLatestValueEmpty(true)
                .build());
        when(mockTransactionService.get(anyCollection())).thenReturn(ImmutableMap.of(LOW_START_TS, LOW_COMMIT_TS));
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, Sweeper.THOROUGH, HIGH_START_TS);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells).hasSize(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(LOW_START_TS);
    }

    @Test
    public void thorough_getTimestampsToSweep_oneSentinel_returnsIt() {
        List<CandidateCellForSweeping> candidate = ImmutableList.of(ImmutableCandidateCellForSweeping.builder()
                .cell(SINGLE_CELL)
                .sortedTimestamps(ImmutableList.of(Value.INVALID_VALUE_TIMESTAMP))
                .isLatestValueEmpty(true)
                .build());
        when(mockTransactionService.get(anyCollection())).thenReturn(ImmutableMap.of());
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, Sweeper.THOROUGH, HIGH_START_TS);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells).hasSize(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(Value.INVALID_VALUE_TIMESTAMP);
    }

    @Test
    public void testNoCandidates() {
        List<CandidateCellForSweeping> candidate = ImmutableList.of(ImmutableCandidateCellForSweeping.builder()
                .cell(SINGLE_CELL)
                .sortedTimestamps(ImmutableList.of())
                .isLatestValueEmpty(true)
                .build());
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, Sweeper.CONSERVATIVE, 100L);
        BatchOfCellsToSweep result = filter.getCellsToSweep(candidate);
        assertThat(result.lastCellExamined()).isEqualTo(SINGLE_CELL);
        assertThat(result.cells()).isEmpty();
        assertThat(result.numCellTsPairsExamined()).isEqualTo(0L);
    }

    @Test
    public void testTwoCandidates() {
        long sweepTimestampHigherThanCommitTimestamp = HIGH_COMMIT_TS + 1;
        CandidateCellForSweeping snd = ImmutableCandidateCellForSweeping.builder()
                .cell(ANOTHER_CELL)
                .sortedTimestamps(ImmutableList.of(HIGH_START_TS))
                .isLatestValueEmpty(true)
                .build();
        List<CandidateCellForSweeping> candidates =
                ImmutableList.of(twoCommittedTimestampsForSingleCell().get(0), snd);
        SweepableCellFilter filter =
                new SweepableCellFilter(commitTsCache, Sweeper.THOROUGH, sweepTimestampHigherThanCommitTimestamp);
        BatchOfCellsToSweep result = filter.getCellsToSweep(candidates);
        assertThat(result.lastCellExamined()).isEqualTo(ANOTHER_CELL);
        assertThat(result.cells()).hasSize(2);
        assertThat(result.numCellTsPairsExamined()).isEqualTo(3L);
        assertThat(result.cells().get(0).sortedTimestamps()).containsExactly(LOW_START_TS);
        assertThat(result.cells().get(1).sortedTimestamps()).containsExactly(HIGH_START_TS);
    }

    private List<CandidateCellForSweeping> twoCommittedTimestampsForSingleCell() {
        List<CandidateCellForSweeping> ret = ImmutableList.of(ImmutableCandidateCellForSweeping.builder()
                .cell(SINGLE_CELL)
                .sortedTimestamps(ImmutableList.of(LOW_START_TS, HIGH_START_TS))
                .isLatestValueEmpty(false)
                .build());
        when(mockTransactionService.get(anyCollection()))
                .thenReturn(ImmutableMap.of(
                        LOW_START_TS, LOW_COMMIT_TS,
                        HIGH_START_TS, HIGH_COMMIT_TS));
        return ret;
    }
}
