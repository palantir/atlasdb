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

package com.palantir.atlasdb.keyvalue.cassandra.paging;

import java.util.List;

import org.immutables.value.Value;

import com.google.common.collect.Collections2;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;

@Value.Immutable
public interface CellWithTimestamps {

    Cell cell();

    List<Long> sortedTimestamps();

    static CellWithTimestamps of(Cell cell, List<Long> sortedTimestamps) {
        return ImmutableCellWithTimestamps.builder()
                .cell(cell)
                .sortedTimestamps(sortedTimestamps)
                .build();
    }

    default CandidateCellForSweeping toSweepCandidate(long maxTimestampExclusive, boolean latestValueEmpty) {
        return ImmutableCandidateCellForSweeping.builder()
                .cell(cell())
                .sortedTimestamps(Collections2.filter(sortedTimestamps(), ts -> ts < maxTimestampExclusive))
                .isLatestValueEmpty(latestValueEmpty)
                .build();
    }

}
