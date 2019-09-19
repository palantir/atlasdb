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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepableTimestampsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.util.PersistableBoolean;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SweepableTimestamps extends SweepQueueTable {
    private static final byte[] DUMMY = new byte[0];

    public SweepableTimestamps(KeyValueService kvs, WriteInfoPartitioner partitioner) {
        super(kvs, TargetedSweepTableFactory.of().getSweepableTimestampsTable(null).getTableRef(), partitioner, null);
    }

    @Override
    Map<Cell, byte[]> populateReferences(PartitionInfo partitionInfo, List<WriteInfo> writes) {
        return ImmutableMap.of();
    }

    @Override
    Map<Cell, byte[]> populateCells(PartitionInfo info, List<WriteInfo> writes) {
        SweepableTimestampsTable.SweepableTimestampsRow row = computeRow(info);
        SweepableTimestampsTable.SweepableTimestampsColumn col = computeColumn(info);

        SweepableTimestampsTable.SweepableTimestampsColumnValue colVal =
                SweepableTimestampsTable.SweepableTimestampsColumnValue.of(col, DUMMY);

        return ImmutableMap.of(SweepQueueUtils.toCell(row, colVal), colVal.persistValue());
    }

    private SweepableTimestampsTable.SweepableTimestampsRow computeRow(PartitionInfo partitionInfo) {
        return SweepableTimestampsTable.SweepableTimestampsRow.of(
                partitionInfo.shard(),
                SweepQueueUtils.tsPartitionCoarse(partitionInfo.timestamp()),
                partitionInfo.isConservative().persistToBytes());
    }

    private SweepableTimestampsTable.SweepableTimestampsColumn computeColumn(PartitionInfo info) {
        return SweepableTimestampsTable.SweepableTimestampsColumn.of(SweepQueueUtils.tsPartitionFine(info.timestamp()));
    }

    /**
     * Returns fine partition that should have unprocessed entries in the Sweepable Cells table.
     *
     * @param shardStrategy desired shard and strategy
     * @param lastSweptTs exclusive minimum timestamp to check for
     * @param sweepTs exclusive maximum timestamp to check for
     * @return Optional containing the fine partition, or Optional.empty() if there are no more candidates before
     * sweepTs
     */
    Optional<Long> nextSweepableTimestampPartition(ShardAndStrategy shardStrategy, long lastSweptTs, long sweepTs) {
        long minFineInclusive = SweepQueueUtils.tsPartitionFine(lastSweptTs + 1);
        long maxFineInclusive = SweepQueueUtils.tsPartitionFine(sweepTs - 1);
        return nextSweepablePartition(shardStrategy, minFineInclusive, maxFineInclusive);
    }

    private Optional<Long> nextSweepablePartition(ShardAndStrategy shardAndStrategy, long minFineInclusive,
            long maxFineInclusive) {
        ColumnRangeSelection range = getColRangeSelection(minFineInclusive, maxFineInclusive + 1);

        long current = SweepQueueUtils.partitionFineToCoarse(minFineInclusive);
        long maxCoarseInclusive = SweepQueueUtils.partitionFineToCoarse(maxFineInclusive);

        while (current <= maxCoarseInclusive) {
            Optional<Long> candidateFine = getCandidatesInCoarsePartition(shardAndStrategy, current, range);
            if (candidateFine.isPresent()) {
                return candidateFine;
            }
            current++;
        }
        return Optional.empty();
    }

    private Optional<Long> getCandidatesInCoarsePartition(ShardAndStrategy shardStrategy, long partitionCoarse,
            ColumnRangeSelection colRange) {
        byte[] rowBytes = computeRowBytes(shardStrategy, partitionCoarse);

        RowColumnRangeIterator colIterator = getRowsColumnRange(ImmutableList.of(rowBytes), colRange, 1);
        if (!colIterator.hasNext()) {
            return Optional.empty();
        }
        Map.Entry<Cell, Value> firstColumnEntry = colIterator.next();

        return Optional.of(getFinePartitionFromEntry(firstColumnEntry));
    }

    private ColumnRangeSelection getColRangeSelection(long minFineInclusive, long maxFineExclusive) {
        byte[] start = SweepableTimestampsTable.SweepableTimestampsColumn.of(minFineInclusive).persistToBytes();
        byte[] end = SweepableTimestampsTable.SweepableTimestampsColumn.of(maxFineExclusive).persistToBytes();
        return new ColumnRangeSelection(start, end);
    }

    private byte[] computeRowBytes(ShardAndStrategy shardStrategy, long coarsePartition) {
        SweepableTimestampsTable.SweepableTimestampsRow row = SweepableTimestampsTable.SweepableTimestampsRow.of(
                shardStrategy.shard(), coarsePartition,
                PersistableBoolean.of(shardStrategy.isConservative()).persistToBytes());
        return row.persistToBytes();
    }

    private long getFinePartitionFromEntry(Map.Entry<Cell, Value> entry) {
        byte[] colName = entry.getKey().getColumnName();
        return SweepableTimestampsTable.SweepableTimestampsColumn.BYTES_HYDRATOR.hydrateFromBytes(colName)
                .getTimestampModulus();
    }

    /**
     * Deletes complete rows of the Sweepable Timestamps table.
     * @param shardStrategy desired shard and strategy
     * @param partitionsCoarse coarse partitions for which the row should be deleted
     */
    void deleteCoarsePartitions(ShardAndStrategy shardStrategy, Set<Long> partitionsCoarse) {
        Set<byte[]> rowsBytes = partitionsCoarse.stream()
                .map(partition -> computeRowBytes(shardStrategy, partition))
                .collect(Collectors.toSet());

        deleteRows(rowsBytes);
    }
}
