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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepableTimestampsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.util.PersistableBoolean;

public class SweepableTimestamps extends KvsSweepQueueWriter {
    private static final byte[] DUMMY = new byte[0];

    private final WriteInfoPartitioner partitioner;
    private final KvsSweepQueueProgress progress;


    SweepableTimestamps(KeyValueService kvs, WriteInfoPartitioner part, KvsSweepQueueProgress progress) {
        super(kvs, TargetedSweepTableFactory.of().getSweepableTimestampsTable(null).getTableRef());
        this.progress = progress;
        this.partitioner = part;
    }

    @Override
    protected Map<Cell, byte[]> batchWrites(List<WriteInfo> writes) {
        Map<Cell, byte[]> result = new HashMap<>();
        Map<PartitionInfo, List<WriteInfo>> partitionedWrites = partitioner.filterAndPartition(writes);
        partitionedWrites.forEach((partitionInfo, writeInfos) -> putWrite(partitionInfo, result));
        return result;
    }

    private void putWrite(PartitionInfo partitionInfo, Map<Cell, byte[]> result) {
        SweepableTimestampsTable.SweepableTimestampsRow row = toRow(partitionInfo);

        SweepableTimestampsTable.SweepableTimestampsColumn col = SweepableTimestampsTable.SweepableTimestampsColumn.of(
                SweepQueueUtils.tsPartitionFine(partitionInfo.timestamp()));

        SweepableTimestampsTable.SweepableTimestampsColumnValue colVal =
                SweepableTimestampsTable.SweepableTimestampsColumnValue.of(col, DUMMY);

        result.put(SweepQueueUtils.toCell(row, colVal), colVal.persistValue());
    }

    void deleteRow(PartitionInfo partitionInfo) {
        byte[] row = toRow(partitionInfo).persistToBytes();

        RangeRequest request = RangeRequest.builder()
                .startRowInclusive(row)
                .endRowExclusive(RangeRequests.nextLexicographicName(row))
                .retainColumns(ColumnSelection.all())
                .build();

        deleteRange(request);
    }

    private SweepableTimestampsTable.SweepableTimestampsRow toRow(PartitionInfo partitionInfo) {
        return SweepableTimestampsTable.SweepableTimestampsRow.of(
                partitionInfo.shard(),
                SweepQueueUtils.tsPartitionCoarse(partitionInfo.timestamp()),
                partitionInfo.isConservative().persistToBytes());
    }

    private static final TableReference TABLE_REF = TargetedSweepTableFactory.of()
            .getSweepableTimestampsTable(null).getTableRef();

    public Optional<Long> nextSweepableTimestampPartition(ShardAndStrategy shardAndStrategy, long sweepTimestamp) {
        // todo(gmaretic): if we find no candidates and the sweep timestamp is far enough in the future, andvance progress
        long minFineExclusive = progress.getLastSweptTimestampPartition(shardAndStrategy);
        long maxFineExclusive = SweepQueueUtils.tsPartitionFine(sweepTimestamp);
        return nextSweepablePartition(shardAndStrategy, minFineExclusive + 1, maxFineExclusive);
    }

    private Optional<Long> nextSweepablePartition(ShardAndStrategy shardAndStrategy, long minFine, long maxFine) {
        Optional<BatchColumnRangeSelection> range = getColRangeSelection(minFine, maxFine);

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

    private Optional<BatchColumnRangeSelection> getColRangeSelection(long minFine, long maxFine) {
        if (minFine >= maxFine) {
            return Optional.empty();
        }
        byte[] start = SweepableTimestampsTable.SweepableTimestampsColumn.of(minFine).persistToBytes();
        byte[] end = SweepableTimestampsTable.SweepableTimestampsColumn.of(maxFine).persistToBytes();
        return Optional.of(BatchColumnRangeSelection.create(start, end, 1));
    }

    private Optional<Long> getCandidatesInCoarsePartition(ShardAndStrategy shardAndStrategy, long partitionCoarse,
            BatchColumnRangeSelection colRange) {
        byte[] row = SweepableTimestampsTable.SweepableTimestampsRow.of(shardAndStrategy.shard(),
                partitionCoarse,
                PersistableBoolean.of(shardAndStrategy.isConservative()).persistToBytes())
                .persistToBytes();

        Map<byte[], RowColumnRangeIterator> response = kvs.getRowsColumnRange(
                TABLE_REF, ImmutableList.of(row), colRange, SweepQueueUtils.CAS_TS);

        RowColumnRangeIterator col = Iterables.getOnlyElement(response.values());
        if (!col.hasNext()) {
            return Optional.empty();
        }
        Map.Entry<Cell, Value> firstColumnEntry = col.next();

        return Optional.of(getPartitionFromEntry(firstColumnEntry));
    }

    private long getPartitionFromEntry(Map.Entry<Cell, Value> entry) {
        byte[] colName = entry.getKey().getColumnName();
        return SweepableTimestampsTable.SweepableTimestampsColumn.BYTES_HYDRATOR.hydrateFromBytes(colName)
                .getTimestampModulus();
    }
}
