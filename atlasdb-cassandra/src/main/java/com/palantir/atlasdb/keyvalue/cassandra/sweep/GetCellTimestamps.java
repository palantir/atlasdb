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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CqlExecutor;
import com.palantir.atlasdb.keyvalue.cassandra.paging.RowGetter;
import com.palantir.common.concurrent.PTExecutors;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class GetCellTimestamps {

    private final RowGetter rowGetter;
    private final CqlExecutor cqlExecutor;
    private final TableReference tableRef;
    private final byte[] startRowInclusive;
    private final int batchHint;
    private CassandraKeyValueServiceConfig config;

    private final Collection<CellWithTimestamp> timestamps = Lists.newArrayList();

    public GetCellTimestamps(
            CqlExecutor cqlExecutor,
            RowGetter rowGetter,
            TableReference tableRef,
            byte[] startRowInclusive,
            int batchHint,
            CassandraKeyValueServiceConfig config) {
        this.cqlExecutor = cqlExecutor;
        this.rowGetter = rowGetter;
        this.tableRef = tableRef;
        this.startRowInclusive = startRowInclusive;
        this.batchHint = batchHint;
        this.config = config;
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
        fetchBatchOfTimestampsBeginningAtStartRow();

        fetchRemainingTimestampsForLastRow();
    }

    /**
     * The complexity in this method is due to a desire to avoid scanning over large numbers of tombstoned rows.
     * <p>
     * Details: An unbounded CQL query will scan over an unbounded number of tombstoned rows in an attempt to find
     * either the end of the table, or the desired number of results. This can lead to timeouts if there are large
     * segments of tombstoned rows (which can happen with THOROUGH sweep). Thrift, on the other hand, will actually
     * return the tombstoned rows (they will just be empty KeySlices), which allows us to page over them. So, we first
     * execute a thrift range scan to determine an upper bound on the CQL range scan, and page over the data until we
     * find some live cells.
     * <p>
     * This is a hack (and a perf hit) until we have a better solution to avoid scanning massive numbers of
     * tombstones (ideally this will involve catching some exception and reducing the range appropriately).
     */
    private void fetchBatchOfTimestampsBeginningAtStartRow() {
        byte[] rangeStart = startRowInclusive;

        Integer executorThreads = config.sweepReadThreads();
        ExecutorService executor = PTExecutors.newFixedThreadPool(executorThreads);

        while (timestamps.isEmpty()) {
            List<byte[]> rows = rowGetter.getRowKeysInRange(rangeStart, PtBytes.EMPTY_BYTE_ARRAY, batchHint);
            if (rows.isEmpty()) {
                return;
            }

            // Note that both ends of this range are *inclusive*
            List<CellWithTimestamp> batch = cqlExecutor.getTimestamps(tableRef, rows, batchHint, executor,
                    executorThreads);
            timestamps.addAll(batch);
            rangeStart = RangeRequests.nextLexicographicName(Iterables.getLast(rows));
        }

        executor.shutdown();
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
