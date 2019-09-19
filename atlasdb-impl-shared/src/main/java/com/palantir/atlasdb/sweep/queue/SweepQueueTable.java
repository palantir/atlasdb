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
package com.palantir.atlasdb.sweep.queue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public abstract class SweepQueueTable {
    final KeyValueService kvs;
    private final TableReference tableRef;
    private final WriteInfoPartitioner partitioner;
    final Optional<TargetedSweepMetrics> maybeMetrics;

    public SweepQueueTable(KeyValueService kvs, TableReference tableRef, WriteInfoPartitioner partitioner,
            @Nullable TargetedSweepMetrics metrics) {
        this.kvs = kvs;
        this.tableRef = tableRef;
        this.partitioner = partitioner;
        this.maybeMetrics = Optional.ofNullable(metrics);
    }

    public void enqueue(List<WriteInfo> allWrites) {
        Map<Cell, byte[]> referencesToDedicatedCells = new HashMap<>();
        Map<Cell, byte[]> cellsToWrite = new HashMap<>();
        Map<PartitionInfo, List<WriteInfo>> partitionedWrites = partitioner.filterAndPartition(allWrites);

        SweepQueueUtils.validateNumberOfCellsWritten(partitionedWrites.values());

        partitionedWrites.forEach((partitionInfo, writes) -> {
            referencesToDedicatedCells.putAll(populateReferences(partitionInfo, writes));
            cellsToWrite.putAll(populateCells(partitionInfo, writes));
        });

        partitionedWrites.keySet().stream()
                .map(PartitionInfo::timestamp)
                .mapToLong(x -> x)
                .max()
                .ifPresent(timestamp -> {
                    write(referencesToDedicatedCells, timestamp);
                    write(cellsToWrite, timestamp);
                    updateWriteMetrics(partitionedWrites);
                });

    }

    private void updateWriteMetrics(Map<PartitionInfo, List<WriteInfo>> partitionedWrites) {
        maybeMetrics.ifPresent(metrics ->
                partitionedWrites.forEach((info, writes) ->
                        metrics.updateEnqueuedWrites(ShardAndStrategy.fromInfo(info), writes.size())));
    }

    /**
     * Returns a map representing all the map entries that act as references to entries returned by
     * {@link #populateCells(PartitionInfo, List)}. Necessary to allow batched writes.
     */
    abstract Map<Cell, byte[]> populateReferences(PartitionInfo partitionInfo, List<WriteInfo> writes);

    /**
     * Converts a list of write infos into the required format to be persisted into the kvs. This method assumes all
     * the writes correspond to the same partition and have the same start timestamp (they are all part of the same
     * transaction), as given by info.
     *
     * @param info information about the partition the writes fall into, and the start timestamp of the transaction the
     * writes correspond to.
     * @param writes list detailing the information for each of the writes
     * @return map of cell to byte array persisting the write infomations into the kvs
     */
    abstract Map<Cell, byte[]> populateCells(PartitionInfo info, List<WriteInfo> writes);

    private void write(Map<Cell, byte[]> cells, long timestamp) {
        if (!cells.isEmpty()) {
            kvs.multiPut(ImmutableMap.of(tableRef, cells), timestamp);
        }
    }

    RowColumnRangeIterator getRowsColumnRange(Iterable<byte[]> rows, ColumnRangeSelection columnRange, int batchSize) {
        return new LocalRowColumnRangeIterator(Streams.stream(rows)
                .flatMap(row -> getBatchForRow(row, columnRange, batchSize))
                .flatMap(Streams::stream)
                .iterator());
    }

    private Stream<RowColumnRangeIterator> getBatchForRow(byte[] row, ColumnRangeSelection columnRange, int batchSize) {
        return kvs.getRowsColumnRange(
                tableRef, Collections.singleton(row),
                BatchColumnRangeSelection.create(columnRange, batchSize),
                SweepQueueUtils.READ_TS).values().stream();
    }

    void deleteRows(Iterable<byte[]> rows) {
        kvs.deleteRows(tableRef, rows);
    }
}
