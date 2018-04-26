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

import static com.palantir.atlasdb.sweep.queue.ShardAndStrategy.conservative;
import static com.palantir.atlasdb.sweep.queue.ShardAndStrategy.thorough;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.Cell;

public class SweepableCellsReadWriteTest extends SweepQueueReadWriteTest {
    private SweepableCellsReader reader;

    int shard;
    int shard2;

    @Before
    public void setup() {
        super.setup();
        writer = new SweepableCellsWriter(kvs, partitioner);
        reader = new SweepableCellsReader(kvs);

        shard = writeTs(writer, TS, true);
        shard2 = writeTs(writer, TS2, false);
    }

    @Test
    public void canReadSingleEntryInSingleShard() {
        assertThat(reader.getLatestWrites(TS_REF, conservative(shard)))
                .containsExactly(WriteInfo.of(TABLE_REF, Cell.create(ROW, COL), TS));
        assertThat(reader.getLatestWrites(TS2_REF, thorough(shard2)))
                .containsExactly(WriteInfo.of(TABLE_REF2, Cell.create(ROW, COL), TS2));
    }

    @Test
    public void getOnlyMostRecentTimestampForCellAndTableRef() {
        writeTs(writer, TS - 1, true);
        writeTs(writer, TS + 2, true);
        writeTs(writer, TS - 2, true);
        writeTs(writer, TS + 1, true);
        assertThat(reader.getLatestWrites(TS_REF, conservative(shard)))
                .containsExactly(WriteInfo.of(TABLE_REF, Cell.create(ROW, COL), TS + 2));
    }

    @Test
    public void canReadMultipleEntriesInSingleShardDifferentTransactions() {
        int fixedShard = writeTsToCell(writer, TS, getCellWithFixedHash(1), true);
        assertThat(writeTsToCell(writer, TS + 1, getCellWithFixedHash(2), true)).isEqualTo(fixedShard);
        assertThat(reader.getLatestWrites(TS_REF, conservative(fixedShard))).containsExactlyInAnyOrder(
                WriteInfo.of(TABLE_REF, getCellWithFixedHash(1), TS),
                WriteInfo.of(TABLE_REF, getCellWithFixedHash(2), TS + 1));
    }

    @Test
    public void canReadMultipleEntriesInSingleShardSameTransactionNotDedicated() {
        List<WriteInfo> writes = writeToUniqueCellsInSameShard(writer, TS, 10, true);
        ShardAndStrategy fixedShardAndStrategy = conservative(WriteInfoPartitioner.getShard(writes.get(0)));
        assertThat(writes.size()).isEqualTo(10);
        assertThat(reader.getLatestWrites(TS_REF, fixedShardAndStrategy)).hasSameElementsAs(writes);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardSameTransactionDedicated() {
        List<WriteInfo> writes = writeToUniqueCellsInSameShard(writer, TS, 257, true);
        ShardAndStrategy fixedShardAndStrategy = conservative(WriteInfoPartitioner.getShard(writes.get(0)));
        assertThat(writes.size()).isEqualTo(257);
        assertThat(reader.getLatestWrites(TS_REF, fixedShardAndStrategy)).hasSameElementsAs(writes);
    }
}
