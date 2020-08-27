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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GetEmptyLatestValues {

    private final List<CellWithTimestamps> cellTimestamps;
    private final ValuesLoader valuesLoader;
    private final TableReference table;
    private final long maxTimestampExclusive;
    private final int batchSize;

    public GetEmptyLatestValues(
            List<CellWithTimestamps> cellTimestamps,
            ValuesLoader valuesLoader,
            TableReference table,
            long maxTimestampExclusive,
            int batchSize) {
        this.cellTimestamps = cellTimestamps;
        this.valuesLoader = valuesLoader;
        this.table = table;
        this.maxTimestampExclusive = maxTimestampExclusive;
        this.batchSize = batchSize;
    }

    /**
     * Returns the subset of {@link Cell}s whose latest values prior to {@code maxTimestampExclusive} are empty.
     */
    public Set<Cell> execute() {
        Set<Cell> result = Sets.newHashSet();
        for (List<CellWithTimestamps> batch : Lists.partition(cellTimestamps, batchSize)) {
            result.addAll(getSingleBatch(batch));
        }
        return result;
    }

    private Set<Cell> getSingleBatch(List<CellWithTimestamps> batch) {
        Set<Cell> cells = batch.stream()
                .map(CellWithTimestamps::cell)
                .collect(Collectors.toSet());

        Map<Cell, Value> valuesByCell = valuesLoader.getValues(table, cells, maxTimestampExclusive);
        return Maps.filterValues(valuesByCell, Value::isEmpty).keySet();
    }

}
