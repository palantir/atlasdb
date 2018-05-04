/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepableTimestampsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.util.PersistableBoolean;

public class SweepableTimestamps extends KvsSweepQueueWriter {
    private static final byte[] DUMMY = new byte[0];

    public SweepableTimestamps(KeyValueService kvs, WriteInfoPartitioner partitioner) {
        super(kvs, TargetedSweepTableFactory.of().getSweepableTimestampsTable(null).getTableRef(), partitioner);
    }

    @Override
    void populateCells(PartitionInfo info, List<WriteInfo> writes, Map<Cell, byte[]> cellsToWrite) {
        SweepableTimestampsTable.SweepableTimestampsRow row = computeRow(info);
        SweepableTimestampsTable.SweepableTimestampsColumn col = computeColumn(info);

        SweepableTimestampsTable.SweepableTimestampsColumnValue colVal =
                SweepableTimestampsTable.SweepableTimestampsColumnValue.of(col, DUMMY);

        cellsToWrite.put(SweepQueueUtils.toCell(row, colVal), colVal.persistValue());
    }

    Optional<Long> nextSweepableTimestampPartition(ShardAndStrategy shardStrategy, long lastSweptFine, long sweepTs) {
        long minFineExclusive = lastSweptFine;
        long maxFineExclusive = SweepQueueUtils.tsPartitionFine(sweepTs);
        return nextSweepablePartition(shardStrategy, minFineExclusive + 1, maxFineExclusive);
    }

    void deleteRow(ShardAndStrategy shardStrategy, long partitionFine) {
        byte[] row = computeRow(shardStrategy, SweepQueueUtils.partitionFineToCoarse(partitionFine)).persistToBytes();

        RangeRequest request = RangeRequest.builder()
                .startRowInclusive(row)
                .endRowExclusive(RangeRequests.nextLexicographicName(row))
                .retainColumns(ColumnSelection.all())
                .build();

        deleteRange(request);
    }

    private Optional<Long> nextSweepablePartition(ShardAndStrategy shardAndStrategy, long minFine, long maxFine) {
        Optional<ColumnRangeSelection> range = getColRangeSelection(minFine, maxFine);

        if (!range.isPresent()) {
            return Optional.empty();
        }

        long current = SweepQueueUtils.partitionFineToCoarse(minFine);
        long maxCoarse = SweepQueueUtils.partitionFineToCoarse(maxFine);

        while (current <= maxCoarse) {
            Optional<Long> candidateFine = getCandidatesInCoarsePartition(shardAndStrategy, current, range.get());
            if (candidateFine.isPresent()) {
                return candidateFine;
            }
            current++;
        }
        return Optional.empty();
    }

    private Optional<ColumnRangeSelection> getColRangeSelection(long minFine, long maxFine) {
        if (minFine >= maxFine) {
            return Optional.empty();
        }
        byte[] start = SweepableTimestampsTable.SweepableTimestampsColumn.of(minFine).persistToBytes();
        byte[] end = SweepableTimestampsTable.SweepableTimestampsColumn.of(maxFine).persistToBytes();
        return Optional.of(new ColumnRangeSelection(start, end));
    }

    private Optional<Long> getCandidatesInCoarsePartition(ShardAndStrategy shardStrategy, long partitionCoarse,
            ColumnRangeSelection colRange) {
        byte[] row = computeRow(shardStrategy, partitionCoarse).persistToBytes();

        RowColumnRangeIterator col = getRowsColumnRange(ImmutableList.of(row), colRange, 1);
        if (!col.hasNext()) {
            return Optional.empty();
        }
        Map.Entry<Cell, Value> firstColumnEntry = col.next();

        return Optional.of(getFinePartitionFromEntry(firstColumnEntry));
    }

    private long getFinePartitionFromEntry(Map.Entry<Cell, Value> entry) {
        byte[] colName = entry.getKey().getColumnName();
        return SweepableTimestampsTable.SweepableTimestampsColumn.BYTES_HYDRATOR.hydrateFromBytes(colName)
                .getTimestampModulus();
    }

    private SweepableTimestampsTable.SweepableTimestampsRow computeRow(PartitionInfo partitionInfo) {
        return SweepableTimestampsTable.SweepableTimestampsRow.of(
                partitionInfo.shard(),
                SweepQueueUtils.tsPartitionCoarse(partitionInfo.timestamp()),
                partitionInfo.isConservative().persistToBytes());
    }

    private SweepableTimestampsTable.SweepableTimestampsRow computeRow(ShardAndStrategy shardStrategy, long coarse) {
        return SweepableTimestampsTable.SweepableTimestampsRow.of(
                shardStrategy.shard(),
                coarse,
                PersistableBoolean.of(shardStrategy.isConservative()).persistToBytes());
    }

    private SweepableTimestampsTable.SweepableTimestampsColumn computeColumn(PartitionInfo info) {
        return SweepableTimestampsTable.SweepableTimestampsColumn.of(SweepQueueUtils.tsPartitionFine(info.timestamp()));
    }
}
