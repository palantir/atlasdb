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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public abstract class SweepQueueReadWriteTest {
    static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("test.test");
    static final TableReference TABLE_REF2 = TableReference.createFromFullyQualifiedName("test.test2");
    static final byte[] ROW = new byte[] {'r'};
    static final byte[] COL = new byte[] {'c'};
    static final long TS = 1_000_000_100L;
    static final long TS2 = 2 * TS;
    static final long TS_REF = tsPartitionFine(TS);
    static final long TS2_REF = tsPartitionFine(TS2);

    protected KeyValueService mockKvs = mock(KeyValueService.class);
    protected KeyValueService kvs = new InMemoryKeyValueService(true);
    protected WriteInfoPartitioner partitioner;
    protected KvsSweepQueueWriter writer;

    @Before
    public void setup() {
        when(mockKvs.getMetadataForTable(TABLE_REF))
                .thenReturn(SweepQueueReadWriteTest.metadataBytes(TableMetadataPersistence.SweepStrategy.CONSERVATIVE));
        when(mockKvs.getMetadataForTable(TABLE_REF2))
                .thenReturn(SweepQueueReadWriteTest.metadataBytes(TableMetadataPersistence.SweepStrategy.THOROUGH));
        partitioner = new WriteInfoPartitioner(mockKvs);
    }

    public static byte[] metadataBytes(TableMetadataPersistence.SweepStrategy sweepStrategy) {
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

    public static int writeTs(KvsSweepQueueWriter writer, long timestamp, boolean conservative) {
        return writeTsToCell(writer, timestamp, Cell.create(ROW, COL), conservative);
    }

    public static int writeTsToCell(KvsSweepQueueWriter writer, long timestamp, Cell cell, boolean conservative) {
        WriteInfo write = WriteInfo.of(conservative ? TABLE_REF : TABLE_REF2, cell, timestamp);
        writer.enqueue(ImmutableList.of(write));
        return WriteInfoPartitioner.getShard(write);
    }

    public static List<WriteInfo> writeToUniqueCellsInSameShard(KvsSweepQueueWriter writer, long timestamp, int number, boolean conservative) {
        TableReference tableRef = conservative ? TABLE_REF : TABLE_REF2;
        List<WriteInfo> result = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            Cell cell = getCellWithFixedHash(i);
            result.add(WriteInfo.of(tableRef, cell, timestamp));
        }
        writer.enqueue(result);
        return result;
    }

    public static Cell getCellWithFixedHash(int seed) {
        return Cell.create(PtBytes.toBytes(seed), PtBytes.toBytes(seed));
    }
}
