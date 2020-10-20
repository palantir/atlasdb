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
package com.palantir.atlasdb.keyvalue.cassandra.sweep;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.immutables.value.Value;

@Value.Immutable
public interface CandidateRowForSweeping {

    byte[] rowName();

    List<CandidateCellForSweeping> cells();

    default RowResult<Set<Long>> toRowResult() {
        SortedMap<byte[], Set<Long>> timestampsByCell = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        for (CandidateCellForSweeping cell : cells()) {
            timestampsByCell.put(cell.cell().getColumnName(), ImmutableSet.copyOf(cell.sortedTimestamps()));
        }

        return RowResult.create(rowName(), timestampsByCell);
    }

    static CandidateRowForSweeping of(byte[] rowName, List<CandidateCellForSweeping> orderedCells) {
        return ImmutableCandidateRowForSweeping.builder()
                .cells(orderedCells)
                .rowName(rowName)
                .build();
    }
}
