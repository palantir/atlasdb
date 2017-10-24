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

package com.palantir.atlasdb.cassandra;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CellWithTimestamps;

public class GetEmptyLatestValues {

    private final List<CellWithTimestamps> cellTimestamps;
    private final KeyValueService keyValueService;
    private final TableReference table;
    private final long maxTimestampExclusive;
    private final int batchSize;

    public GetEmptyLatestValues(
            List<CellWithTimestamps> cellTimestamps,
            KeyValueService keyValueService,
            TableReference table,
            long maxTimestampExclusive,
            int batchSize) {
        this.cellTimestamps = cellTimestamps;
        this.keyValueService = keyValueService;
        this.table = table;
        this.maxTimestampExclusive = maxTimestampExclusive;
        this.batchSize = batchSize;
    }

    /**
     * Returns the subset of {@link Cell}s whose latest values prior to {@code maxTimestampExclusive} are empty.
     */
    public Set<Cell> execute() {
        Set<Cell> result = Sets.newHashSet();
        for (List<CellWithTimestamps> batch : Iterables.partition(cellTimestamps, batchSize)) {
            result.addAll(getSingleBatch(batch));
        }
        return result;
    }

    private Set<Cell> getSingleBatch(
            List<CellWithTimestamps> batch) {
        Map<Cell, Long> timestampsByCell = batch.stream()
                .collect(Collectors.toMap(
                        CellWithTimestamps::cell,
                        ignored -> maxTimestampExclusive));

        Map<Cell, Value> valuesByCell = keyValueService.get(table, timestampsByCell);
        return Maps.filterValues(valuesByCell, Value::isEmpty).keySet();
    }

}
