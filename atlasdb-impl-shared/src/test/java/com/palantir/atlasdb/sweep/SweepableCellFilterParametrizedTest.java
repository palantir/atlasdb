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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SweepableCellFilterParametrizedTest {
    private static final Set<Boolean> BOOLEANS = ImmutableSet.of(true, false);

    private static final Cell SINGLE_CELL = Cell.create(
            "cellRow".getBytes(StandardCharsets.UTF_8),
            "cellCol".getBytes(StandardCharsets.UTF_8));
    private static final List<Long> COMMITTED_BEFORE = ImmutableList.of(10L, 20L, 30L, 40L);
    private static final List<Long> COMMITTED_AFTER = ImmutableList.of(13L, 23L, 33L);
    private static final List<Long> ABORTED_TS = ImmutableList.of(16L, 26L, 36L);
    private static final long SWEEP_TS = 50L;
    private static final long LAST_TS = SWEEP_TS - 5;

    private final TransactionService mockTransactionService = mock(TransactionService.class);
    private final CommitTsCache commitTsCache = CommitTsCache.create(mockTransactionService);
    private List<CandidateCellForSweeping> candidate;

    @Parameterized.Parameter
    public boolean lastIsTombstone;

    @Parameterized.Parameter(value = 1)
    public Committed status;

    @Parameterized.Parameter(value = 2)
    public Sweeper sweeper;

    @Parameterized.Parameters(name = "{0}, {1}, {2}")
    public static Collection<Object[]> parameters() {
        return Sets.<Object>cartesianProduct(BOOLEANS,
                ImmutableSet.copyOf(Committed.values()),
                ImmutableSet.copyOf(Sweeper.values())).stream()
                .map(List::toArray)
                .collect(Collectors.toList());
    }

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        Map<Long, Long> startTsToCommitTs = new HashMap<>();
        COMMITTED_BEFORE.forEach(startTs -> startTsToCommitTs.put(startTs, startTs));
        COMMITTED_AFTER.forEach(startTs -> startTsToCommitTs.put(startTs, startTs + SWEEP_TS));
        ABORTED_TS.forEach(startTs -> startTsToCommitTs.put(startTs, TransactionConstants.FAILED_COMMIT_TS));
        startTsToCommitTs.put(LAST_TS, status.commitTs);
        when(mockTransactionService.get(anyCollection())).thenReturn(startTsToCommitTs);
        candidate = ImmutableList.of(ImmutableCandidateCellForSweeping.builder()
                .cell(SINGLE_CELL)
                .sortedTimestamps(ImmutableList.sortedCopyOf(startTsToCommitTs.keySet()))
                .isLatestValueEmpty(lastIsTombstone)
                .build());
    }

    /**
     * The following tests are testing all combinations of whether the last write with start timestamp before the sweep
     * timestamp was a tombstone; whether that last write was committed before, after sweep ts, or was aborted; and
     * sweep strategy.
     *
     * Writes that were aborted should always appear.
     * Writes that were committed after should never appear.
     * Writes that were committed before should appear, in sorted order. However, the greatest such write should appear
     * (if and) only if it is a tombstone (we only have this information for the greatest overall write), and sweep is
     * thorough.
     */
    @Test
    public void sweepableCellFilterTest() {
        List<Long> timestamps = getCellsToSweepFor().sortedTimestamps();

        assertThat(timestamps).doesNotContainAnyElementsOf(COMMITTED_AFTER);

        List<Long> expectedAll = new ArrayList<>(ABORTED_TS);
        if (status == Committed.ABORTED) {
            expectedAll.add(LAST_TS);
        }

        List<Long> expectedCommitted;
        if (status == Committed.BEFORE) {
            expectedCommitted = new ArrayList<>(COMMITTED_BEFORE);
            if (lastIsTombstone && sweeper == Sweeper.THOROUGH) {
                expectedCommitted.add(LAST_TS);
            }
        } else {
            expectedCommitted = new ArrayList<>(COMMITTED_BEFORE.subList(0, COMMITTED_BEFORE.size() - 1));
        }

        expectedAll.addAll(expectedCommitted);

        assertThat(timestamps).hasSameElementsAs(expectedAll);
        assertThat(timestamps.stream().filter(expectedCommitted::contains)).isSorted();
    }

    private CellToSweep getCellsToSweepFor() {
        SweepableCellFilter filter = new SweepableCellFilter(commitTsCache, sweeper, SWEEP_TS);
        List<CellToSweep> cells = filter.getCellsToSweep(candidate).cells();
        assertThat(cells.size()).isEqualTo(1);
        return Iterables.getOnlyElement(cells);
    }

    private enum Committed {
        BEFORE(SWEEP_TS - 1), AFTER(SWEEP_TS + 1), ABORTED(TransactionConstants.FAILED_COMMIT_TS);

        private long commitTs;

        Committed(long commitTs) {
            this.commitTs = commitTs;
        }
    }
}
