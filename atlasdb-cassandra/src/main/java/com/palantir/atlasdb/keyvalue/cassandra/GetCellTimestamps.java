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

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CellWithTimestamps;

public class GetCellTimestamps {

    private final CqlExecutor cqlExecutor;
    private final TableReference tableRef;
    private final byte[] startRowInclusive;
    private final int batchHint;

    private final Collection<CellWithTimestamp> cells = Lists.newArrayList();

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

    private void fetchRemainingTimestampsForLastRow() {
        while (fetchBatchOfRemainingCellTsPairsInLastRow()) { }
    }

    private boolean fetchAllTimestampsBeginningAtStartRow() {
        List<CellWithTimestamp> batch = cqlExecutor.getCellTimestamps(tableRef, startRowInclusive, batchHint);

        return cells.addAll(batch);
    }

    private boolean fetchBatchOfRemainingCellTsPairsInLastRow() {
        if (cells.isEmpty()) {
            return false;
        }

        CellWithTimestamp lastCell = Iterables.getLast(cells);

        List<CellWithTimestamp> batch = cqlExecutor.getCellTimestampsWithinRow(
                tableRef,
                lastCell.cell().getRowName(),
                lastCell.cell().getColumnName(),
                lastCell.timestamp(),
                batchHint);

        return cells.addAll(batch);
    }

    private List<CellWithTimestamps> groupTimestampsByCell() {
        Map<Cell, List<Long>> timestampsByCell = cells.stream().collect(
                groupingBy(CellWithTimestamp::cell,
                        mapping(CellWithTimestamp::timestamp, toList())));

        return timestampsByCell.entrySet().stream()
                .map(entry -> CellWithTimestamps.of(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparing(CellWithTimestamps::cell))
                .collect(toList());
    }
}
