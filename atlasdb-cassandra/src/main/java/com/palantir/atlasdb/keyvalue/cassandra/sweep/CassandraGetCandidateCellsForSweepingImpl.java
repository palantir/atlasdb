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

package com.palantir.atlasdb.keyvalue.cassandra.sweep;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CassandraRawCellValue;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CellPager;
import com.palantir.util.Pair;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

/*
 * Simply use the CellPager to iterate over raw cells in a table and group the returned entries by the cell key.
 */
public class CassandraGetCandidateCellsForSweepingImpl {

    private static final long[] EMPTY_LONG_ARRAY = new long[0];

    private final CellPager cellPager;

    public CassandraGetCandidateCellsForSweepingImpl(CellPager cellPager) {
        this.cellPager = cellPager;
    }

    public Iterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef,
            CandidateCellForSweepingRequest request,
            ConsistencyLevel consistencyLevel) {
        Iterator<List<CassandraRawCellValue>> rawIter = cellPager.createCellIterator(
                tableRef,
                request.startRowInclusive(),
                request.batchSizeHint().orElse(AtlasDbConstants.DEFAULT_SWEEP_CANDIDATE_BATCH_HINT),
                consistencyLevel);
        return new CellGroupingIterator(rawIter, request);
    }

    private static class CellGroupingIterator extends AbstractIterator<List<CandidateCellForSweeping>> {
        private final CandidateCellForSweepingRequest request;

        private final Iterator<List<CassandraRawCellValue>> rawIter;
        private Cell currentCell = null;
        private final TLongList currentTimestamps = new TLongArrayList();
        private boolean currentLatestValEmpty;
        private long numCellTsPairsExamined = 0;
        private boolean reachedEnd = false;

        CellGroupingIterator(Iterator<List<CassandraRawCellValue>> rawIter, CandidateCellForSweepingRequest request) {
            this.rawIter = rawIter;
            this.request = request;
        }

        @Override
        protected List<CandidateCellForSweeping> computeNext() {
            if (reachedEnd) {
                return endOfData();
            } else {
                List<CandidateCellForSweeping> candidates = Lists.newArrayList();
                while (candidates.isEmpty() && rawIter.hasNext()) {
                    List<CassandraRawCellValue> cols = rawIter.next();
                    for (CassandraRawCellValue col : cols) {
                        processColumn(col).ifPresent(candidates::add);
                    }
                }
                if (candidates.isEmpty()) {
                    reachedEnd = true;
                    if (!currentTimestamps.isEmpty()) {
                        // Since we reached the end of data, we know there can't be more timestamps for this cell,
                        // so create a candidate from them.
                        return ImmutableList.of(createCandidate());
                    } else {
                        return endOfData();
                    }
                } else {
                    return candidates;
                }
            }
        }

        private Optional<CandidateCellForSweeping> processColumn(CassandraRawCellValue col) {
            Optional<CandidateCellForSweeping> maybeCandidate = Optional.empty();
            Pair<byte[], Long> colNameAndTs = CassandraKeyValueServices.decomposeName(col.getColumn());
            Cell cell = Cell.create(col.getRowKey(), colNameAndTs.getLhSide());
            if (!cell.equals(currentCell)) {
                if (!currentTimestamps.isEmpty()) {
                    maybeCandidate = Optional.of(createCandidate());
                }
                currentCell = cell;
            }
            long ts = colNameAndTs.getRhSide();
            if (ts < request.sweepTimestamp() && Longs.indexOf(request.timestampsToIgnore(), ts) < 0) {
                if (currentTimestamps.isEmpty()) {
                    // Timestamps are in the decreasing order, so we pick the first timestamp below sweepTimestamp
                    // to check the value for emptiness
                    currentLatestValEmpty = request.shouldCheckIfLatestValueIsEmpty()
                                        && col.getColumn().getValue().length == 0;
                }
                currentTimestamps.add(ts);
                numCellTsPairsExamined += 1;
            }
            return maybeCandidate;
        }

        private CandidateCellForSweeping createCandidate() {
            boolean isCandidate = isCandidate();
            long[] sortedTimestamps = isCandidate ? sortTimestamps() : EMPTY_LONG_ARRAY;
            currentTimestamps.clear();
            return ImmutableCandidateCellForSweeping.builder()
                    .cell(currentCell)
                    .sortedTimestamps(sortedTimestamps)
                    .isLatestValueEmpty(currentLatestValEmpty)
                    .numCellsTsPairsExamined(numCellTsPairsExamined)
                    .build();
        }

        private long[] sortTimestamps() {
            currentTimestamps.reverse();
            return currentTimestamps.toArray();
        }

        private boolean isCandidate() {
            return currentTimestamps.size() > 1
                    || currentLatestValEmpty
                    || (currentTimestamps.size() == 1 && timestampIsPotentiallySweepable(currentTimestamps.get(0)));
        }

        private boolean timestampIsPotentiallySweepable(long ts) {
            return ts == Value.INVALID_VALUE_TIMESTAMP || ts >= request.minUncommittedStartTimestamp();
        }
    }

}
