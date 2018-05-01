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
import static com.palantir.atlasdb.sweep.queue.SweepableCells.MAX_CELLS_DEDICATED;
import static com.palantir.atlasdb.sweep.queue.SweepableCells.MAX_CELLS_GENERIC;
import static com.palantir.atlasdb.sweep.queue.WriteInfoPartitioner.SHARDS;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class SweepableCellsTest extends SweepQueueTablesTest {
    private SweepableCells sweepableCells;

    int shard;
    int shard2;

    @Before
    public void setup() {
        super.setup();
        sweepableCells = new SweepableCells(kvs, partitioner);

        shard = writeToDefault(sweepableCells, TS, true);
        shard2 = writeToDefault(sweepableCells, TS2, false);
    }

    @Test
    public void canReadSingleEntryInSingleShard() {
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, conservative(shard)))
                .containsExactly(WriteInfo.write(TABLE_REF, DEFAULT_CELL, TS));
        assertThat(sweepableCells.getWritesFromPartition(TS2_REF, thorough(shard2)))
                .containsExactly(WriteInfo.write(TABLE_REF2, DEFAULT_CELL, TS2));
    }

    @Test
    public void canReadSingleTombstoneInSameShard() {
        int tombstoneShard = putTombstone(sweepableCells, TS + 1, DEFAULT_CELL, true);
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, conservative(tombstoneShard)))
                .containsExactly(WriteInfo.write(TABLE_REF, DEFAULT_CELL, TS + 1));
    }

    @Test
    public void getOnlyMostRecentTimestampForCellAndTableRef() {
        writeToDefault(sweepableCells, TS - 1, true);
        writeToDefault(sweepableCells, TS + 2, true);
        writeToDefault(sweepableCells, TS - 2, true);
        writeToDefault(sweepableCells, TS + 1, true);
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, conservative(shard)))
                .containsExactly(WriteInfo.write(TABLE_REF, DEFAULT_CELL, TS + 2));
    }

    @Test
    public void canReadMultipleEntriesInSingleShardDifferentTransactions() {
        int fixedShard = writeToCell(sweepableCells, TS, getCellWithFixedHash(1), true);
        assertThat(writeToCell(sweepableCells, TS + 1, getCellWithFixedHash(2), true)).isEqualTo(fixedShard);
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, conservative(fixedShard))).containsExactlyInAnyOrder(
                WriteInfo.write(TABLE_REF, getCellWithFixedHash(1), TS),
                WriteInfo.write(TABLE_REF, getCellWithFixedHash(2), TS + 1));
    }

    @Test
    public void canReadMultipleEntriesInSingleShardSameTransactionNotDedicated() {
        List<WriteInfo> writes = writeToUniqueCellsInSameShard(sweepableCells, TS, 10, true);
        ShardAndStrategy fixedShardAndStrategy = conservative(writes.get(0).toShard(SHARDS));
        assertThat(writes.size()).isEqualTo(10);
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, fixedShardAndStrategy)).hasSameElementsAs(writes);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardSameTransactionOneDedicated() {
        List<WriteInfo> writes = writeToUniqueCellsInSameShard(sweepableCells, TS, MAX_CELLS_GENERIC * 2 + 1, true);
        ShardAndStrategy fixedShardAndStrategy = conservative(writes.get(0).toShard(SHARDS));
        assertThat(writes.size()).isEqualTo(MAX_CELLS_GENERIC * 2 + 1);
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, fixedShardAndStrategy)).hasSameElementsAs(writes);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardMultipleTransactionsCombined() {
        List<WriteInfo> writesFirst = writeToUniqueCellsInSameShard(sweepableCells, TS, MAX_CELLS_GENERIC * 2 + 1, true);
        List<WriteInfo> writesLast = writeToUniqueCellsInSameShard(sweepableCells, TS + 2, 1, true);
        List<WriteInfo> writesMiddle = writeToUniqueCellsInSameShard(sweepableCells, TS + 1, MAX_CELLS_GENERIC + 1, true);
        List<WriteInfo> expectedResult = new ArrayList<>(writesLast);
        expectedResult.addAll(writesMiddle.subList(writesLast.size(), writesMiddle.size()));
        expectedResult.addAll(writesFirst.subList(writesMiddle.size(), writesFirst.size()));

        ShardAndStrategy fixedShardAndStrategy = conservative(writesFirst.get(0).toShard(SHARDS));
        List<WriteInfo> result = sweepableCells.getWritesFromPartition(TS_REF, fixedShardAndStrategy);
        assertThat(result).hasSameElementsAs(expectedResult);
    }

    @Test
    @Ignore("This test takes 53 minutes to complete. Need to see how it performs with CassandraKVS")
    public void canReadMultipleEntriesInSingleShardSameTransactionMultipleDedicated() {
        List<WriteInfo> writes = writeToUniqueCellsInSameShard(sweepableCells, TS, MAX_CELLS_DEDICATED + 1, true);
        ShardAndStrategy fixedShardAndStrategy = conservative(writes.get(0).toShard(SHARDS));
        assertThat(writes.size()).isEqualTo(MAX_CELLS_DEDICATED + 1);
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, fixedShardAndStrategy)).hasSameElementsAs(writes);
    }
}
