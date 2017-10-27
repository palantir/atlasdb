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

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CqlExecutor;

public class GetCellTimestamps {

    private final CqlExecutor cqlExecutor;
    private final TableReference tableRef;
    private final byte[] startRowInclusive;
    private final int batchHint;

    private final Collection<CellWithTimestamp> timestamps = Lists.newArrayList();

    public GetCellTimestamps(
            CqlExecutor cqlExecutor,
            TableReference tableRef,
            byte[] startRowInclusive,
            int batchHint) {
        this.cqlExecutor = cqlExecutor;
        this.tableRef = tableRef;
        this.startRowInclusive = startRowInclusive;
        this.batchHint = batchHint;
    }

    /**
     * Fetches a batch of timestamps, grouped by cell. The returned {@link CellWithTimestamp}s are ordered by cell.
     */
    public List<CellWithTimestamps> execute() {
        fetchBatchOfTimestamps();

        return groupTimestampsByCell();
    }

    /**
     * We always finish the last whole row when fetching timestamps. Sweep actually only requires that we return a
     * whole cell at a time, but due to the limited types of queries cassandra supports, it's easiest to finish a whole
     * row.
     */
    private void fetchBatchOfTimestamps() {
        fetchAllTimestampsBeginningAtStartRow();
        fetchRemainingTimestampsForLastRow();
    }

    private void fetchAllTimestampsBeginningAtStartRow() {
        List<CellWithTimestamp> batch = cqlExecutor.getTimestamps(tableRef, startRowInclusive, batchHint);

        timestamps.addAll(batch);
    }

    private void fetchRemainingTimestampsForLastRow() {
        boolean moreToFetch = !timestamps.isEmpty();
        while (moreToFetch) {
            moreToFetch = fetchBatchOfRemainingTimestampsForLastRow();
        }
    }

    private boolean fetchBatchOfRemainingTimestampsForLastRow() {
        CellWithTimestamp lastCell = Iterables.getLast(timestamps);

        List<CellWithTimestamp> batch = cqlExecutor.getTimestampsWithinRow(
                tableRef,
                lastCell.cell().getRowName(),
                lastCell.cell().getColumnName(),
                lastCell.timestamp(),
                batchHint);

        return timestamps.addAll(batch);
    }

    private List<CellWithTimestamps> groupTimestampsByCell() {
        Map<Cell, List<Long>> timestampsByCell = timestamps.stream().collect(
                Collectors.groupingBy(CellWithTimestamp::cell,
                        Collectors.mapping(CellWithTimestamp::timestamp, Collectors.toList())));

        return timestampsByCell.entrySet().stream()
                .map(entry -> CellWithTimestamps.of(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparing(CellWithTimestamps::cell))
                .collect(Collectors.toList());
    }
}
