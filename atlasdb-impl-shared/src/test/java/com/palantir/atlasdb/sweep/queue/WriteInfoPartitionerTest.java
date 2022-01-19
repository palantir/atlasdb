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

import static com.palantir.atlasdb.sweep.queue.AbstractSweepQueueTest.TABLE_CONS;
import static com.palantir.atlasdb.sweep.queue.AbstractSweepQueueTest.metadataBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withinPercentage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class WriteInfoPartitionerTest {
    private static final TableReference NOTHING = getTableRef("nothing");
    private static final TableReference CONSERVATIVE = getTableRef("conservative");
    private static final TableReference CONSERVATIVE2 = getTableRef("conservative2");
    private static final TableReference THOROUGH = getTableRef("thorough");
    private static final ImmutableMap<TableReference, byte[]> METADATA_MAP = ImmutableMap.of(
            NOTHING, metadataBytes(TableMetadataPersistence.SweepStrategy.NOTHING),
            CONSERVATIVE, metadataBytes(TableMetadataPersistence.SweepStrategy.CONSERVATIVE),
            CONSERVATIVE2, metadataBytes(TableMetadataPersistence.SweepStrategy.CONSERVATIVE),
            THOROUGH, metadataBytes(TableMetadataPersistence.SweepStrategy.THOROUGH));

    private KeyValueService mockKvs = mock(KeyValueService.class);
    private WriteInfoPartitioner partitioner;
    private int numShards = 128;

    @Before
    public void setup() {
        partitioner = new WriteInfoPartitioner(mockKvs, () -> numShards);
        when(mockKvs.getMetadataForTable(any(TableReference.class))).thenAnswer(args -> {
            TableReference tableRef = args.getArgument(0);
            return METADATA_MAP.getOrDefault(tableRef, AtlasDbConstants.EMPTY_TABLE_METADATA);
        });
    }

    @Test
    public void getStrategyThrowsForIllegalMetadata() {
        when(mockKvs.getMetadataForTable(any())).thenReturn(AtlasDbConstants.EMPTY_TABLE_METADATA);
        assertThatThrownBy(() -> partitioner.getStrategy(getWriteInfoWithFixedShard(getTableRef("a"), 0, numShards)))
                .isInstanceOf(UncheckedExecutionException.class);
    }

    @Test
    public void getStrategyThrowsOnUncheckedException() {
        RuntimeException cause = new RuntimeException("cause");
        when(mockKvs.getMetadataForTable(any())).thenThrow(cause);
        assertThatThrownBy(() -> partitioner.getStrategy(getWriteInfoWithFixedShard(getTableRef("a"), 0, numShards)))
                .isInstanceOf(UncheckedExecutionException.class)
                .hasCause(cause);
    }

    @Test
    public void getStrategyReturnsCorrectStrategy() {
        assertThat(partitioner.getStrategy(getWriteInfoWithFixedShard(NOTHING, 0, numShards)))
                .isEmpty();
        assertThat(partitioner.getStrategy(getWriteInfoWithFixedShard(CONSERVATIVE, 10, numShards)))
                .contains(SweeperStrategy.CONSERVATIVE);
        assertThat(partitioner.getStrategy(getWriteInfoWithFixedShard(THOROUGH, 100, numShards)))
                .contains(SweeperStrategy.THOROUGH);
    }

    @Test
    public void getStrategyQueriesKvsOnlyOnceForEachTable() {
        for (int i = 0; i < 5; i++) {
            partitioner.getStrategy(getWriteInfoWithFixedShard(NOTHING, i, numShards));
            partitioner.getStrategy(getWriteInfoWithFixedShard(CONSERVATIVE, i, numShards));
        }
        verify(mockKvs, times(1)).getMetadataForTable(NOTHING);
        verify(mockKvs, times(1)).getMetadataForTable(CONSERVATIVE);
        verifyNoMoreInteractions(mockKvs);
    }

    @Test
    public void filterOutUnsweepableRemovesWritesWithStrategyNothing() {
        List<WriteInfo> writes = ImmutableList.of(
                getWriteInfoWithFixedShard(CONSERVATIVE, 0, numShards),
                getWriteInfoWithFixedShard(NOTHING, 1, numShards),
                getWriteInfoWithFixedShard(CONSERVATIVE, 0, numShards),
                getWriteInfoWithFixedShard(CONSERVATIVE2, 1, numShards),
                getWriteInfoWithFixedShard(THOROUGH, 2, numShards),
                getWriteInfoWithFixedShard(NOTHING, 0, numShards));

        assertThat(partitioner.filterOutUnsweepableTables(writes))
                .containsExactly(
                        getWriteInfoWithFixedShard(CONSERVATIVE, 0, numShards),
                                getWriteInfoWithFixedShard(CONSERVATIVE, 0, numShards),
                        getWriteInfoWithFixedShard(CONSERVATIVE2, 1, numShards),
                                getWriteInfoWithFixedShard(THOROUGH, 2, numShards));
    }

    @Test
    public void partitionWritesByShardStrategyTimestampPartitionsIntoSeparatePartitions() {
        List<WriteInfo> writes = ImmutableList.of(
                getWriteInfo(CONSERVATIVE, 0, 0, 100L),
                getWriteInfo(CONSERVATIVE, 1, 0, 100L),
                getWriteInfo(CONSERVATIVE, 0, 3, 100L),
                getWriteInfo(CONSERVATIVE, 0, 0, 200L),
                getWriteInfo(CONSERVATIVE2, 0, 0, 100L),
                getWriteInfo(THOROUGH, 0, 0, 100L));

        Map<PartitionInfo, List<WriteInfo>> partitions = partitioner.partitionWritesByShardStrategyTimestamp(writes);
        assertThat(partitions).hasSize(6);
    }

    @Test
    public void partitionWritesByShardStrategyTimestampGroupsOnShardClash() {
        List<WriteInfo> writes = new ArrayList<>();
        for (int i = 0; i <= numShards; i++) {
            writes.add(getWriteInfoWithFixedShard(CONSERVATIVE, i, numShards));
        }
        Map<PartitionInfo, List<WriteInfo>> partitions = partitioner.partitionWritesByShardStrategyTimestamp(writes);
        assertThat(partitions.keySet())
                .containsExactly(PartitionInfo.of(writes.get(0).toShard(numShards), true, 1L));
        assertThat(Iterables.getOnlyElement(partitions.values())).isEqualTo(writes);
    }

    @Test
    public void changingNumberOfPartitionsIsReflectedInPartitionInfo() {
        WriteInfo write = getWriteInfo(CONSERVATIVE, 1, 1, 100L);
        PartitionInfo partition1 = Iterables.getOnlyElement(partitioner
                .partitionWritesByShardStrategyTimestamp(ImmutableList.of(write))
                .keySet());
        numShards += 1;
        PartitionInfo partition2 = Iterables.getOnlyElement(partitioner
                .partitionWritesByShardStrategyTimestamp(ImmutableList.of(write))
                .keySet());

        assertThat(partition1.isConservative()).isEqualTo(partition2.isConservative());
        assertThat(partition1.timestamp()).isEqualTo(partition2.timestamp());
        assertThat(partition1.shard()).isNotEqualTo(partition2.shard());
    }

    @Test
    public void cellsWithSameRowAndColumnNamesGetAssignedToShardsUniformly() {
        int writes = 100_000;
        Map<Integer, Long> result = IntStream.range(0, writes)
                .mapToObj(index -> getWriteInfo(TABLE_CONS, index, index, 1L))
                .map(writeInfo -> writeInfo.toShard(numShards))
                .collect(Collectors.groupingBy(shard -> shard, Collectors.counting()));

        assertThat(result).hasSize(numShards);
        result.values().forEach(count -> assertThat((double) count)
                .isCloseTo(writes / (double) numShards, withinPercentage(10)));
    }

    private static TableReference getTableRef(String tableName) {
        return TableReference.createFromFullyQualifiedName("test." + tableName);
    }

    private WriteInfo getWriteInfoWithFixedShard(TableReference tableRef, int cellIndex, int numShards) {
        return IntStream.iterate(0, i -> i + 1)
                .mapToObj(index -> getWriteInfo(tableRef, cellIndex, index, 1L))
                .filter(writeInfo -> writeInfo.toShard(numShards) == 0)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Infinite stream had no cell possibilities :("));
    }

    private WriteInfo getWriteInfo(TableReference tableRef, int rowIndex, int colIndex, long timestamp) {
        Cell cell = Cell.create(PtBytes.toBytes(rowIndex), PtBytes.toBytes(colIndex));
        return WriteInfo.write(tableRef, cell, timestamp);
    }
}
