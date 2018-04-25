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

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;

public class SweepableCellsReadWriteTest extends SweepQueueReadWriteTest {
    private static final long TS = 1_000_000_100L;
    private static final long TS2 = 2 * TS;
    private static final long TS_REF = tsPartitionFine(TS);
    private static final long TS2_REF = tsPartitionFine(TS2);
    private static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("test.test");
    private static final TableReference TABLE_REF2 = TableReference.createFromFullyQualifiedName("test.test2");
    private static final byte[] ROW = new byte[] {'r'};
    private static final byte[] COL = new byte[] {'c'};

    private KeyValueService mockKvs = mock(KeyValueService.class);
    private KeyValueService kvs = new InMemoryKeyValueService(true);
    private WriteInfoPartitioner partitioner;
    private SweepableCellsWriter writer;
    private SweepableCellsReader reader;
    private SweepTimestampProvider provider;
    private KvsSweepQueueProgress progress;

    int shard;
    int shard2;

    @Before
    public void setup() {
        when(mockKvs.getMetadataForTable(TABLE_REF))
                .thenReturn(SweepQueueReadWriteTest.metadataBytes(TableMetadataPersistence.SweepStrategy.CONSERVATIVE));
        when(mockKvs.getMetadataForTable(TABLE_REF2))
                .thenReturn(SweepQueueReadWriteTest.metadataBytes(TableMetadataPersistence.SweepStrategy.THOROUGH));
        partitioner = new WriteInfoPartitioner(mockKvs);

        writer = new SweepableCellsWriter(kvs, partitioner);
        reader = new SweepableCellsReader(kvs);

        shard = writeTs(writer, TS, true);
        shard2 = writeTs(writer, TS2, false);
    }

    @Test
    public void canReadSingleEntryInSingleShard() {
        assertThat(reader.getTombstonesToWrite(TS_REF, shard, true))
                .containsExactly(WriteInfo.of(TABLE_REF, Cell.create(ROW, COL), TS));
        assertThat(reader.getTombstonesToWrite(TS2_REF, shard2, false))
                .containsExactly(WriteInfo.of(TABLE_REF2, Cell.create(ROW, COL), TS2));
    }

    @Test
    public void getOnlyMostRecentTimestampForCellAndTableRef() {
        writeTs(writer, TS - 1, true);
        writeTs(writer, TS + 2, true);
        writeTs(writer, TS - 2, true);
        writeTs(writer, TS + 1, true);
        assertThat(reader.getTombstonesToWrite(TS_REF, shard, true))
                .containsExactly(WriteInfo.of(TABLE_REF, Cell.create(ROW, COL), TS + 2));
    }

    @Test
    public void canReadMultipleEntriesInSingleShard() {
        int fixedShard = writeTsToCell(writer, TS, getCellWithFixedHash(1), true);
        assertThat(writeTsToCell(writer, TS + 1, getCellWithFixedHash(2), true)).isEqualTo(fixedShard);
        assertThat(reader.getTombstonesToWrite(TS_REF, fixedShard, true))
                .containsExactlyInAnyOrder(WriteInfo.of(TABLE_REF, getCellWithFixedHash(1), TS),
                        WriteInfo.of(TABLE_REF, getCellWithFixedHash(2), TS + 1));
    }
}
