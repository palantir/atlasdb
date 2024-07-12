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
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepableTimestampsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SweepableTimestamps extends SweepQueueTable {
    private static final byte[] DUMMY = new byte[0];

    public SweepableTimestamps(KeyValueService kvs, WriteInfoPartitioner partitioner) {
        super(
                kvs,
                TargetedSweepTableFactory.of().getSweepableTimestampsTable(null).getTableRef(),
                partitioner,
                null);
    }

    @Override
    Map<Cell, byte[]> populateReferences(PartitionInfo partitionInfo, List<WriteInfo> writes) {
        return ImmutableMap.of();
    }

    @Override
    Map<Cell, byte[]> populateCells(PartitionInfo info, List<WriteInfo> writes) {
        SweepableTimestampsTable.SweepableTimestampsRow row = computeRow(info);
        SweepableTimestampsTable.SweepableTimestampsColumn col = computeColumn(info.timestamp());

        SweepableTimestampsTable.SweepableTimestampsColumnValue colVal =
                SweepableTimestampsTable.SweepableTimestampsColumnValue.of(col, DUMMY);

        return ImmutableMap.of(SweepQueueUtils.toCell(row, colVal), colVal.persistValue());
    }

    private SweepableTimestampsTable.SweepableTimestampsRow computeRow(PartitionInfo partitionInfo) {
        return SweepableTimestampsTable.SweepableTimestampsRow.of(
                partitionInfo.shardAndStrategy().shard(),
                SweepQueueUtils.tsPartitionCoarse(partitionInfo.timestamp()),
                partitionInfo.shardAndStrategy().strategy().persistToBytes());
    }

    private SweepableTimestampsTable.SweepableTimestampsColumn computeColumn(long startTimestamp) {
        return SweepableTimestampsTable.SweepableTimestampsColumn.of(SweepQueueUtils.tsPartitionFine(startTimestamp));
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
    Optional<Long> nextTimestampPartition(ShardAndStrategy shardStrategy, long lastSweptTs, long sweepTs) {
        List<Long> partitions = nextTimestampPartitions(shardStrategy, lastSweptTs, sweepTs, 1);
        if (partitions.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(partitions.get(0));
        }
    }
    // TODO: This is Jeremy's code - clean it up.
    // Public because testing out visibility things
    public List<Long> nextTimestampPartitions(
            ShardAndStrategy shardStrategy, long lastSweptTs, long sweepTs, int limit) {
        long minFineInclusive = SweepQueueUtils.tsPartitionFine(lastSweptTs + 1);
        long maxFineInclusive = SweepQueueUtils.tsPartitionFine(sweepTs - 1);
        return nextSweepablePartitionsLazy(shardStrategy, minFineInclusive, maxFineInclusive, 1)
                .limit(limit)
                .collect(Collectors.toList());
    }

    //    // Alternative possibility: maybe more complex, but nicer properties - only try to get more things if needed
    //    public Stream<SweepQueueBucket> getSweepableBucketStream(Map<ShardAndStrategy, Long> lastSweptTsPerShard,
    //                                                             long sweepTs,
    //                                                             int limitPerShard) {
    //        // TODO (jkong): Duplication is bad
    //        Map<ShardAndStrategy, Stream<SweepQueueBucket>> shardAndStrategyToBuckets =
    // KeyedStream.stream(lastSweptTsPerShard)
    //                .map((shardAndStrategy, lastSweptTs) -> nextSweepablePartitionsLazy(shardAndStrategy,
    //                        SweepQueueUtils.tsPartitionFine(lastSweptTs + 1),
    //                        SweepQueueUtils.tsPartitionFine(sweepTs - 1),
    //                        DEFAULT_BATCH_SIZE)
    //                        .map(finePartition -> (SweepQueueBucket) SweepQueueBucket.builder()
    //                                .shardAndStrategy(shardAndStrategy)
    //                                .partition(finePartition)
    //                                .build())
    //                        .limit(limitPerShard))
    //                .collectToMap();
    //        // Need some mechanism for merging streams
    //    }

    private Stream<Long> nextSweepablePartitionsLazy(
            ShardAndStrategy shardAndStrategy, long minFineInclusive, long maxFineInclusive, int batchSize) {
        ColumnRangeSelection range = getColRangeSelection(minFineInclusive, maxFineInclusive + 1);

        long current = SweepQueueUtils.partitionFineToCoarse(minFineInclusive);
        long maxCoarseInclusive = SweepQueueUtils.partitionFineToCoarse(maxFineInclusive);

        return LongStream.rangeClosed(current, maxCoarseInclusive)
                .boxed()
                .flatMap(coarsePartition ->
                        getCandidatesInCoarsePartitionLazy(shardAndStrategy, coarsePartition, range, batchSize));
    }

    private Stream<Long> getCandidatesInCoarsePartitionLazy(
            ShardAndStrategy shardStrategy, long partitionCoarse, ColumnRangeSelection colRange, int batchSize) {
        byte[] rowBytes = computeRowBytes(shardStrategy, partitionCoarse);

        RowColumnRangeIterator colIterator = getRowsColumnRange(ImmutableList.of(rowBytes), colRange, batchSize);
        Iterator<Long> partitionIterator = Iterators.transform(colIterator, this::getFinePartitionFromEntry);
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        partitionIterator, Spliterator.NONNULL | Spliterator.DISTINCT | Spliterator.ORDERED),
                false);
    }

    private ColumnRangeSelection getColRangeSelection(long minFineInclusive, long maxFineExclusive) {
        byte[] start = SweepableTimestampsTable.SweepableTimestampsColumn.of(minFineInclusive)
                .persistToBytes();
        byte[] end = SweepableTimestampsTable.SweepableTimestampsColumn.of(maxFineExclusive)
                .persistToBytes();
        return new ColumnRangeSelection(start, end);
    }

    private byte[] computeRowBytes(ShardAndStrategy shardStrategy, long coarsePartition) {
        SweepableTimestampsTable.SweepableTimestampsRow row = SweepableTimestampsTable.SweepableTimestampsRow.of(
                shardStrategy.shard(), coarsePartition, shardStrategy.strategy().persistToBytes());
        return row.persistToBytes();
    }

    private long getFinePartitionFromEntry(Map.Entry<Cell, Value> entry) {
        byte[] colName = entry.getKey().getColumnName();
        return SweepableTimestampsTable.SweepableTimestampsColumn.BYTES_HYDRATOR
                .hydrateFromBytes(colName)
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
