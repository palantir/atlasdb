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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CellWithTimestamp;
import com.palantir.common.base.ClosableIterator;

public class CandidateCellsForSweepingIterator implements ClosableIterator<List<CandidateCellForSweeping>> {

    private final CellTimestampsIterator cellTimestamps;
    private final KeyValueService keyValueService;
    private final CandidateCellForSweepingRequest request;
    private final TableReference table;

    public CandidateCellsForSweepingIterator(
            CellTimestampsIterator cellTimestamps,
            KeyValueService keyValueService,
            CandidateCellForSweepingRequest request,
            TableReference table) {
        this.cellTimestamps = cellTimestamps;
        this.keyValueService = keyValueService;
        this.request = request;
        this.table = table;
    }

    @Override
    public boolean hasNext() {
        return cellTimestamps.hasNext();
    }

    @Override
    public List<CandidateCellForSweeping> next() {
        List<CellWithTimestamps> cellWithTimestampsBatch = cellTimestamps.next();

        Set<Cell> cellsWithEmptyValues = request.shouldCheckIfLatestValueIsEmpty()
                ? getCellsWithLatestValueEmpty(cellWithTimestampsBatch)
                : Collections.emptySet();

        return Lists.transform(
                cellWithTimestampsBatch,
                cellTimestamps -> cellTimestamps.toSweepCandidate(
                        request.maxTimestampExclusiveHint(),
                        cellsWithEmptyValues.contains(cellTimestamps.cell()));
    }

    private Set<Cell> getCellsWithLatestValueEmpty(List<CellWithTimestamps> batch) {
        long timestamp = request.maxTimestampExclusiveHint();
        Map<Cell, Long> timestampsByCell = batch.stream()
                .map(CellWithTimestamps::cell)
                .collect(Collectors.toMap(
                        cell -> cell,
                        ignored -> timestamp));

        return Maps.filterValues(
                keyValueService.get(table, timestampsByCell),
                value -> value.getContents().length == 0)
                .keySet();
    }
}
