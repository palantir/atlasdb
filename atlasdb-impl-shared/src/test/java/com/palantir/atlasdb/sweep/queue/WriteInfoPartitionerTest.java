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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;


public class WriteInfoPartitionerTest {
    private static final TableReference NOTHING = getTableRef("nothing");
    private static final TableReference CONSERVATIVE = getTableRef("conservative");
    private static final TableReference CONSERVATIVE2 = getTableRef("conservative2");
    private static final TableReference THOROUGH = getTableRef("thorough");
    private static final Map<TableReference, byte[]> METADATA_MAP = ImmutableMap.of(
            NOTHING, metadataBytes(TableMetadataPersistence.SweepStrategy.NOTHING),
            CONSERVATIVE, metadataBytes(TableMetadataPersistence.SweepStrategy.CONSERVATIVE),
            CONSERVATIVE2, metadataBytes(TableMetadataPersistence.SweepStrategy.CONSERVATIVE),
            THOROUGH, metadataBytes(TableMetadataPersistence.SweepStrategy.THOROUGH));

    private KeyValueService mockKvs = mock(KeyValueService.class);
    private WriteInfoPartitioner partitioner;

    @Before
    public void setup() {
        partitioner = new WriteInfoPartitioner(mockKvs);
        when(mockKvs.getMetadataForTable(any(TableReference.class))).thenAnswer(args ->
                METADATA_MAP.getOrDefault(args.getArguments()[0], AtlasDbConstants.EMPTY_TABLE_METADATA));
    }

    @Test
    public void getStrategyThrowsOnIllegalMetadata() {
        when(mockKvs.getMetadataForTable(any())).thenReturn(AtlasDbConstants.EMPTY_TABLE_METADATA);
        assertThatThrownBy(() ->
                partitioner.getStrategy(getWriteInfo(TableReference.createFromFullyQualifiedName("test.a"), 0)))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void getStrategyReturnsCorrectStrategy() {
        assertThat(partitioner.getStrategy(getWriteInfo(NOTHING, 0)))
                .isEqualTo(TableMetadataPersistence.SweepStrategy.NOTHING);
        assertThat(partitioner.getStrategy(getWriteInfo(CONSERVATIVE, 10)))
                .isEqualTo(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
        assertThat(partitioner.getStrategy(getWriteInfo(THOROUGH, 100)))
                .isEqualTo(TableMetadataPersistence.SweepStrategy.THOROUGH);
    }

    @Test
    public void getStrategyQueriesKvsOnlyOnceForEachTable() {
        for (int i = 0; i < 5; i++) {
            partitioner.getStrategy(getWriteInfo(NOTHING, i));
            partitioner.getStrategy(getWriteInfo(CONSERVATIVE, i));
        }
        verify(mockKvs, times(2)).getMetadataForTable(any());
    }

    @Test
    public void filterOutUnsweepableRemovesWritesWithStrategyNothing(){
        List<WriteInfo> writes = ImmutableList.of(
                getWriteInfo(CONSERVATIVE, 0),
                getWriteInfo(NOTHING, 1),
                getWriteInfo(CONSERVATIVE, 0),
                getWriteInfo(CONSERVATIVE2, 1),
                getWriteInfo(THOROUGH, 2),
                getWriteInfo(NOTHING, 0));

        assertThat(partitioner.filterOutUnsweepableTables(writes))
                .containsExactly(getWriteInfo(CONSERVATIVE, 0), getWriteInfo(CONSERVATIVE, 0),
                        getWriteInfo(CONSERVATIVE2, 1), getWriteInfo(THOROUGH, 2));
    }

    @Test
    public void partitionWritesByShardStrategyTimestampPartitionsIntoSeparatePartitions() {
        List<WriteInfo> writes = ImmutableList.of(
                getWriteInfo(CONSERVATIVE, 0, 100L),
                getWriteInfo(CONSERVATIVE, 1, 100L),
                getWriteInfo(CONSERVATIVE, 0, 200L),
                getWriteInfo(CONSERVATIVE2, 0, 100L),
                getWriteInfo(THOROUGH, 0, 100L));

        Map<PartitionInfo, List<WriteInfo>> partitions = partitioner.partitionWritesByShardStrategyTimestamp(writes);
        assertThat(partitions.size()).isEqualTo(4);
    }

    @Test
    public void partitionWritesByShardStrategyTimestampGroupsOnShardClash() {
        List<WriteInfo> writes = new ArrayList<>();
        for (int i = 0; i <= WriteInfoPartitioner.SHARDS; i++) {
            writes.add(getWriteInfo(CONSERVATIVE, i));
        }
        Map<PartitionInfo, List<WriteInfo>> partitions = partitioner.partitionWritesByShardStrategyTimestamp(writes);
        assertThat(partitions.size()).isLessThan(WriteInfoPartitioner.SHARDS);

        Optional<List<WriteInfo>> pigeonHole = partitions.values().stream().filter(list -> list.size() > 1).findFirst();
        assertThat(pigeonHole.isPresent()).isTrue();
    }

    private static byte[] metadataBytes(TableMetadataPersistence.SweepStrategy sweepStrategy) {
        return new TableMetadata(new NameMetadataDescription(),
                new ColumnMetadataDescription(),
                ConflictHandler.RETRY_ON_WRITE_WRITE,
                TableMetadataPersistence.CachePriority.WARM,
                false,
                0,
                false,
                sweepStrategy,
                false,
                TableMetadataPersistence.LogSafety.UNSAFE)
                .persistToBytes();
    }

    private static TableReference getTableRef(String tableName) {
        return TableReference.createFromFullyQualifiedName("test." + tableName);
    }


    private WriteInfo getWriteInfo(TableReference tableRef, int cellIndex) {
        return getWriteInfo(tableRef, cellIndex, 0L);
    }

    private WriteInfo getWriteInfo(TableReference tableRef, int cellIndex, long timestamp) {
        Cell cell = Cell.create(PtBytes.toBytes(cellIndex), PtBytes.toBytes(cellIndex));
        return WriteInfo.of(tableRef, cell, timestamp);
    }
}
