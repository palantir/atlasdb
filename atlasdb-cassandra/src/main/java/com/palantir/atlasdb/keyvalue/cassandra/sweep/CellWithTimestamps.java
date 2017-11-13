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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;

@Value.Immutable
public interface CellWithTimestamps {

    Cell cell();

    List<Long> sortedTimestamps();

    static CellWithTimestamps of(Cell cell, List<Long> unsortedTimestamps) {
        List<Long> sortedTimestamps = Lists.newArrayList(unsortedTimestamps);
        Collections.sort(sortedTimestamps);
        return ImmutableCellWithTimestamps.builder()
                .cell(cell)
                .sortedTimestamps(sortedTimestamps)
                .build();
    }

    default CandidateCellForSweeping toSweepCandidate(
            long maxTimestampExclusive,
            Set<Long> timestampsToIgnore,
            boolean latestValueEmpty) {
        List<Long> filteredTimestamps = sortedTimestamps().stream()
                .filter(ts -> ts < maxTimestampExclusive && !timestampsToIgnore.contains(ts))
                .collect(Collectors.toList());
        return ImmutableCandidateCellForSweeping.builder()
                .cell(cell())
                .sortedTimestamps(filteredTimestamps)
                .isLatestValueEmpty(latestValueEmpty)
                .build();
    }

}
