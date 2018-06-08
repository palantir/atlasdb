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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public class SweepableCellFilter {
    private final CommitTsCache commitTsCache;
    private final Sweeper sweeper;
    private final long sweepTs;

    public SweepableCellFilter(CommitTsCache commitTsCache, Sweeper sweeper, long sweepTs) {
        this.commitTsCache = commitTsCache;
        this.sweeper = sweeper;
        this.sweepTs = sweepTs;
    }

    // For a given list of candidates, decide which ones we should actually sweep.
    // Here we need to load the commit timestamps, and it's important to do that in bulk
    // to reduce the number of round trips to the database.
    public BatchOfCellsToSweep getCellsToSweep(List<CandidateCellForSweeping> candidates) {
        Preconditions.checkArgument(!candidates.isEmpty(),
                "Got an empty collection of candidates. This is a programming error.");
        Map<Long, Long> startToCommitTs = commitTsCache.loadBatch(getAllTimestamps(candidates));
        ImmutableBatchOfCellsToSweep.Builder builder = ImmutableBatchOfCellsToSweep.builder();
        long numCellTsPairsExamined = 0;
        Cell lastCellExamined = null;
        for (CandidateCellForSweeping candidate : candidates) {
            if (candidate.sortedTimestamps().size() > 0) {
                getCellToSweep(candidate, startToCommitTs).ifPresent(builder::addCells);
            }
            numCellTsPairsExamined += candidate.sortedTimestamps().size();
            lastCellExamined = candidate.cell();
        }
        return builder.numCellTsPairsExamined(numCellTsPairsExamined).lastCellExamined(lastCellExamined).build();
    }

    private Optional<CellToSweep> getCellToSweep(CandidateCellForSweeping candidate, Map<Long, Long> startToCommitTs) {
        Preconditions.checkArgument(candidate.sortedTimestamps().size() > 0);
        List<Long> timestampsToSweep = new ArrayList<>();
        List<Long> uncommittedTimestamps = new ArrayList<>();
        long maxStartTs = TransactionConstants.FAILED_COMMIT_TS;
        boolean maxStartTsIsCommitted = false;
        for (long startTs : candidate.sortedTimestamps()) {
            long commitTs = startToCommitTs.get(startTs);

            if (startTs > maxStartTs && commitTs < sweepTs) {
                maxStartTs = startTs;
                maxStartTsIsCommitted = commitTs != TransactionConstants.FAILED_COMMIT_TS;
            }
            // Note: there could be an open transaction whose start timestamp is equal to
            // sweepTimestamp; thus we want to sweep all cells such that:
            // (1) their commit timestamp is less than sweepTimestamp
            // (2) their start timestamp is NOT the greatest possible start timestamp
            //     passing condition (1)
            if (commitTs > 0 && commitTs < sweepTs) {
                timestampsToSweep.add(startTs);
            } else if (commitTs == TransactionConstants.FAILED_COMMIT_TS) {
                uncommittedTimestamps.add(startTs);
            }
        }
        boolean needsSentinel = sweeper.shouldAddSentinels() && timestampsToSweep.size() > 1;
        boolean shouldSweepLastCommitted = sweeper.shouldSweepLastCommitted()
                && candidate.isLatestValueEmpty()
                && maxStartTsIsCommitted;
        if (!timestampsToSweep.isEmpty() && !shouldSweepLastCommitted) {
            timestampsToSweep.remove(timestampsToSweep.size() - 1);
        }
        timestampsToSweep.addAll(uncommittedTimestamps);
        if (timestampsToSweep.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(ImmutableCellToSweep.builder()
                    .cell(candidate.cell())
                    .sortedTimestamps(timestampsToSweep)
                    .needsSentinel(needsSentinel)
                    .build()
            );
        }
    }

    private static Set<Long> getAllTimestamps(Collection<CandidateCellForSweeping> candidates) {
        Set<Long> ret = new HashSet<>();
        for (CandidateCellForSweeping candidate : candidates) {
            ret.addAll(candidate.sortedTimestamps());
        }
        return ret;
    }
}
