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
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;

public class SweepableCellFilterTest {
    private static final long LOW_START_TS = 6L;
    private static final long LOW_COMMIT_TS = 7L;
    private static final long HIGH_START_TS = 100L;
    private static final long HIGH_COMMIT_TS = 102L;

    private static final long LAST_TS = 45L;
    private static final Cell SINGLE_CELL = Cell.create(
            "cellRow".getBytes(StandardCharsets.UTF_8),
            "cellCol".getBytes(StandardCharsets.UTF_8));

    private final TransactionService mockTransactionService = mock(TransactionService.class);
    private final CommitTsCache commitTsCache = CommitTsCache.create(mockTransactionService);

    private static final Map<Long, Long> DEFAULT_COMMITS = ImmutableMap.of(
            10L, 12L,
            20L, 22L,
            13L, 50L,
            43L, 70L,
            16L, TransactionConstants.FAILED_COMMIT_TS);

    @Test
    public void conservative_getTimestampsToSweep_twoEntriesBelowSweepTimestamp_returnsLowerOne() {
        long sweepTimestampHigherThanCommitTimestamp = HIGH_COMMIT_TS + 1;
        List<CandidateCellForSweeping> candidates = twoCommittedTimestampsForSingleCell();
        SweepableCellFilter filter = new SweepableCellFilter(
                commitTsCache, Sweeper.CONSERVATIVE, sweepTimestampHigherThanCommitTimestamp);
        List<CellToSweep> cells = filter.getCellsToSweep(candidates).cells();
        assertThat(cells.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(LOW_START_TS);
    }

    @Test
    public void conservative_getTimestampsToSweep_oneEntryBelowTimestamp_oneAbove_returnsNone() {
        long sweepTimestampLowerThanCommitTimestamp = HIGH_COMMIT_TS - 1;
        List<CandidateCellForSweeping> candidates = twoCommittedTimestampsForSingleCell();
        SweepableCellFilter filter = new SweepableCellFilter(
                commitTsCache, Sweeper.CONSERVATIVE, sweepTimestampLowerThanCommitTimestamp);
        List<CellToSweep> cells = filter.getCellsToSweep(candidates).cells();
        assertThat(cells).isEmpty();
    }

    @Test
    public void conservativeGetTimestampToSweepAddsSentinels() {
        long sweepTimestampHigherThanCommitTimestamp = HIGH_COMMIT_TS + 1;
        List<CandidateCellForSweeping> candidates = twoCommittedTimestampsForSingleCell();
        SweepableCellFilter filter = new SweepableCellFilter(
                commitTsCache, Sweeper.CONSERVATIVE, sweepTimestampHigherThanCommitTimestamp);
        List<CellToSweep> cells = filter.getCellsToSweep(candidates).cells();
        assertThat(Iterables.getOnlyElement(cells).needsSentinel()).isTrue();
    }

    @Test
    public void thoroughGetTimestampToSweepDoesNotAddSentinels() {
        long sweepTimestampHigherThanCommitTimestamp = HIGH_COMMIT_TS + 1;
        List<CandidateCellForSweeping> candidates = twoCommittedTimestampsForSingleCell();
        SweepableCellFilter filter = new SweepableCellFilter(
                commitTsCache, Sweeper.THOROUGH, sweepTimestampHigherThanCommitTimestamp);
        List<CellToSweep> cells = filter.getCellsToSweep(candidates).cells();
        assertThat(Iterables.getOnlyElement(cells).needsSentinel()).isFalse();
    }

    @Test
    public void getTimestampsToSweep_onlyTransactionUncommitted_returnsIt() {
        List<CandidateCellForSweeping> candidate = ImmutableList.of(
                ImmutableCandidateCellForSweeping.builder()
                    .cell(SINGLE_CELL)
                    .sortedTimestamps(ImmutableList.of(LOW_START_TS))
                    .isLatestValueEmpty(false)
                    .build());
        when(mockTransactionService.get(anyCollection()))
                .thenReturn(ImmutableMap.of(LOW_START_TS, TransactionConstants.FAILED_COMMIT_TS));
        SweepableCellFilter filter = new SweepableCellFilter(
                commitTsCache, Sweeper.CONSERVATIVE, HIGH_START_TS);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(LOW_START_TS);
    }

    @Test
    public void thorough_getTimestampsToSweep_oneTransaction_emptyValue_returnsIt() {
        List<CandidateCellForSweeping> candidate = ImmutableList.of(
                ImmutableCandidateCellForSweeping.builder()
                    .cell(SINGLE_CELL)
                    .sortedTimestamps(ImmutableList.of(LOW_START_TS))
                    .isLatestValueEmpty(true)
                    .build());
        when(mockTransactionService.get(anyCollection()))
                .thenReturn(ImmutableMap.of(LOW_START_TS, LOW_COMMIT_TS));
        SweepableCellFilter filter = new SweepableCellFilter(
                commitTsCache, Sweeper.THOROUGH, HIGH_START_TS);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(LOW_START_TS);
    }

    @Test
    public void lastTombstoneLastCommittedBeforeSweepTsThorough() {
        List<CandidateCellForSweeping> candidate = setup(true, 46L);
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, Sweeper.THOROUGH, 50L);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(10L, 20L, 45L, 16L);
    }

    @Test
    public void lastNotTombstoneLastCommittedBeforeSweepTsThorough() {
        List<CandidateCellForSweeping> candidate = setup(false, 46L);
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, Sweeper.THOROUGH, 50L);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(10L, 20L, 16L);
    }

    @Test
    public void lastTombstoneLastCommittedBeforeSweepTsConservative() {
        List<CandidateCellForSweeping> candidate = setup(true, 46L);
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, Sweeper.CONSERVATIVE, 50L);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(10L, 20L, 16L);
    }

    @Test
    public void lastTombstoneLastCommittedAfterSweepTsThorough() {
        List<CandidateCellForSweeping> candidate = setup(true, 56L);
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, Sweeper.THOROUGH, 50L);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(10L, 16L);
    }

    @Test
    public void lastTombstoneLastCommittedAfterSweepTsConservative() {
        List<CandidateCellForSweeping> candidate = setup(true, 56L);
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, Sweeper.CONSERVATIVE, 50L);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(10L, 16L);
    }

    @Test
    public void lastTombstoneLastAbortedThorough() {
        List<CandidateCellForSweeping> candidate = setup(true, TransactionConstants.FAILED_COMMIT_TS);
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, Sweeper.THOROUGH, 50L);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(10L, 16L, 45L);
    }

    @Test
    public void lastTombstoneLastAbortedConservative() {
        List<CandidateCellForSweeping> candidate = setup(true, TransactionConstants.FAILED_COMMIT_TS);
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, Sweeper.CONSERVATIVE, 50L);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells.size()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(cells).sortedTimestamps()).containsExactly(10L, 16L, 45L);
    }

    private List<CandidateCellForSweeping> twoCommittedTimestampsForSingleCell() {
        List<CandidateCellForSweeping> ret = ImmutableList.of(
                ImmutableCandidateCellForSweeping.builder()
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

    private List<CandidateCellForSweeping> setup(boolean lastIsTombstone, long lastCommitTs) {
        Map<Long, Long> startTsToCommitTs = new HashMap<>(DEFAULT_COMMITS);
        startTsToCommitTs.put(LAST_TS, lastCommitTs);
        when(mockTransactionService.get(anyCollection())).thenReturn(startTsToCommitTs);
        return ImmutableList.of(
                ImmutableCandidateCellForSweeping.builder()
                        .cell(SINGLE_CELL)
                        .sortedTimestamps(ImmutableList.sortedCopyOf(startTsToCommitTs.keySet()))
                        .isLatestValueEmpty(lastIsTombstone)
                        .build());
    }
}
