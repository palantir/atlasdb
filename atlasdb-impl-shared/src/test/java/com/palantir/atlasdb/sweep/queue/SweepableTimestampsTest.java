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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.schema.generated.SweepableTimestampsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.table.description.TableMetadata;

public class SweepableTimestampsTest {
    KeyValueService mockKvs = mock(KeyValueService.class);
    KeyValueService kvs = new InMemoryKeyValueService(true);
    WriteInfoPartitioner partitioner;
    SweepableTimestampsWriter writer;
    SweepableTimestampsReader reader;


    private static final byte[] CONSERVATIVE = new TableMetadata().persistToBytes();
    private static final long TS = 1_000_000_100L;
    public static final long COARSE_TS = TS / SweepableTimestampsWriter.TS_COARSE_GRANULARITY;
    private static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("test.test");
    private static final TableReference TABLE_REF2 = TableReference.createFromFullyQualifiedName("test.test2");
    private static final byte[] ROW = new byte[] {'r'};
    private static final byte[] COL = new byte[] {'c'};
    private static final WriteInfo WRITE = WriteInfo.of(TABLE_REF, Cell.create(ROW, COL), TS);
    private static final int SHARD = WriteInfoPartitioner.getShard(WRITE);



    @Before
    public void setup() {
        when(mockKvs.getMetadataForTable(any())).thenReturn(CONSERVATIVE);
        partitioner = new WriteInfoPartitioner(mockKvs);
        writer = new SweepableTimestampsWriter(kvs, TargetedSweepTableFactory.of(), partitioner);
        reader = new SweepableTimestampsReader(kvs, TargetedSweepTableFactory.of());
        writer.enqueue(ImmutableList.of(WRITE));
    }

    @Test
    public void canReadOnlyWrite() {
        Set<SweepableTimestampsTable.SweepableTimestampsColumnValue> result = reader.getColumns(
                WriteInfoPartitioner.getShard(WRITE), COARSE_TS, true);
        SweepableTimestampsTable.SweepableTimestampsColumnValue columnValue = Iterables.getOnlyElement(result);
        assertThat(columnValue.getColumnName().getTimestampModulus()).isEqualTo(SweepableCellsWriter.tsMod(TS));
    }

    @Test
    public void cannotReadFromDifferentShard() {
        Set<SweepableTimestampsTable.SweepableTimestampsColumnValue> result1 = reader.getColumns(
                SHARD + 1, COARSE_TS, true);
        Set<SweepableTimestampsTable.SweepableTimestampsColumnValue> result2 = reader.getColumns(
                SHARD - 1, COARSE_TS, true);
        assertThat(result1).isEmpty();
        assertThat(result2).isEmpty();
    }

    @Test
    public void cannotReadFromDifferentTimestampBucket() {
        Set<SweepableTimestampsTable.SweepableTimestampsColumnValue> result1 = reader.getColumns(
                SHARD, COARSE_TS + 1, true);
        Set<SweepableTimestampsTable.SweepableTimestampsColumnValue> result2 = reader.getColumns(
                SHARD, TS / COARSE_TS - 1, true);
        assertThat(result1).isEmpty();
        assertThat(result2).isEmpty();
    }

    @Test
    public void cannotReadForDifferentSweepStrategy() {
        Set<SweepableTimestampsTable.SweepableTimestampsColumnValue> result = reader.getColumns(
                WriteInfoPartitioner.getShard(WRITE), COARSE_TS, false);
        assertThat(result).isEmpty();
    }

    @Test
    public void canReadMultipleWrites() {
        List<WriteInfo> writes = ImmutableList.of(WriteInfo.of(TABLE_REF, Cell.create(ROW, COL), TS + 1),
                WriteInfo.of(TABLE_REF, Cell.create(ROW, ROW), TS + 1),
                WriteInfo.of(TABLE_REF2, Cell.create(ROW, COL), TS + 1));

        writer.enqueue(writes);
        writer.enqueue(ImmutableList.of(WriteInfo.of(TABLE_REF, Cell.create(ROW, COL), TS + 2)));

        Set<SweepableTimestampsTable.SweepableTimestampsColumnValue> result = reader.getColumns(
                SHARD, COARSE_TS, true);

        List<Long> timestamps = result.stream()
                .map(colVal -> colVal.getColumnName().getTimestampModulus())
                .collect(Collectors.toList());
        assertThat(timestamps).containsExactlyInAnyOrder(SweepableCellsWriter.tsMod(TS),
                SweepableCellsWriter.tsMod(TS + 1), SweepableCellsWriter.tsMod(TS + 2));
    }

    @Test
    public void canReadSweepMarker() {
        writer.markLastSwept(SHARD, TS, true);
        writer.markLastSwept(SHARD, TS - 1, true);

        Set<SweepableTimestampsTable.SweepableTimestampsColumnValue> result = reader.getSweptColumns(SHARD, COARSE_TS,
                true);

        List<Long> timestamps = result.stream()
                .map(colVal -> colVal.getColumnName().getTimestampModulus())
                .collect(Collectors.toList());
        assertThat(timestamps).containsExactlyInAnyOrder(SweepableCellsWriter.tsMod(TS),
                SweepableCellsWriter.tsMod(TS - 1));

    }


}
