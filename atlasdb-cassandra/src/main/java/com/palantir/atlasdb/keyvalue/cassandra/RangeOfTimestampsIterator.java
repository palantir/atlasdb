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

import java.util.Arrays;
import java.util.Set;
import java.util.SortedMap;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.ClosableIterator;

public class RangeOfTimestampsIterator implements ClosableIterator<RowResult<Set<Long>>> {
    private final ClosableIterator<CandidateCellForSweeping> delegate;
    private final PeekingIterator<CandidateCellForSweeping> candidates;
    private final RangeRequest rangeRequest;

    public RangeOfTimestampsIterator(KeyValueService keyValueService, TableReference tableRef,
            RangeRequest rangeRequest, long timestamp) {
        CandidateCellForSweepingRequest request = ImmutableCandidateCellForSweepingRequest.builder()
                .startRowInclusive(rangeRequest.getStartInclusive())
                .maxTimestampExclusive(timestamp)
                .addTimestampsToIgnore()
                .build();
        this.rangeRequest = rangeRequest;
        this.delegate = keyValueService.getCandidateCellsForSweeping(
                tableRef,
                request).flatMap(list -> list);
        this.candidates = Iterators.peekingIterator(delegate);
    }

    @Override
    public boolean hasNext() {
        return candidates.hasNext() && rangeRequest.inRange(peekNextRowName());
    }

    @Override
    public RowResult<Set<Long>> next() {
        byte[] currentRow = peekNextRowName();

        SortedMap<byte[], Set<Long>> timestampsByCell = collectTimestampsForRow(currentRow);

        return RowResult.create(currentRow, timestampsByCell);
    }

    private byte[] peekNextRowName() {
        return candidates.peek().cell().getRowName();
    }

    private SortedMap<byte[], Set<Long>> collectTimestampsForRow(byte[] currentRow) {
        SortedMap<byte[], Set<Long>> timestampsByCell = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());

        while (candidates.hasNext() && Arrays.equals(currentRow, peekNextRowName())) {
            CandidateCellForSweeping candidate = candidates.next();
            timestampsByCell.put(candidate.cell().getColumnName(), ImmutableSet.copyOf(candidate.sortedTimestamps()));
        }

        return timestampsByCell;
    }

    @Override
    public void close() {
        delegate.close();
    }
}
