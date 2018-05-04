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

    int shardCons;
    int shardThor;

    @Before
    public void setup() {
        super.setup();
        sweepableCells = new SweepableCells(kvs, partitioner);

        shardCons = writeToDefault(sweepableCells, TS, TABLE_CONS);
        shardThor = writeToDefault(sweepableCells, TS2, TABLE_THOR);
    }

    @Test
    public void canReadSingleEntryInSingleShard() {
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, conservative(shardCons)))
                .containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS));
        assertThat(sweepableCells.getWritesFromPartition(TS2_REF, thorough(shardThor)))
                .containsExactly(WriteInfo.write(TABLE_THOR, DEFAULT_CELL, TS2));
    }

    @Test
    public void canReadSingleTombstoneInSameShard() {
        int tombstoneShard = putTombstone(sweepableCells, TS + 1, DEFAULT_CELL, TABLE_CONS);
        List<WriteInfo> latestWrites = sweepableCells.getWritesFromPartition(TS_REF, conservative(tombstoneShard));
        assertThat(latestWrites).containsExactly(WriteInfo.tombstone(TABLE_CONS, DEFAULT_CELL, TS + 1));
    }

    @Test
    public void getOnlyMostRecentTimestampForCellAndTableRef() {
        writeToDefault(sweepableCells, TS - 1, TABLE_CONS);
        writeToDefault(sweepableCells, TS + 2, TABLE_CONS);
        writeToDefault(sweepableCells, TS - 2, TABLE_CONS);
        writeToDefault(sweepableCells, TS + 1, TABLE_CONS);
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, conservative(shardCons)))
                .containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS + 2));
    }

    @Test
    public void canReadMultipleEntriesInSingleShardDifferentTransactions() {
        int fixedShard = writeToCell(sweepableCells, TS, getCellWithFixedHash(1), TABLE_CONS);
        assertThat(writeToCell(sweepableCells, TS + 1, getCellWithFixedHash(2), TABLE_CONS)).isEqualTo(fixedShard);
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, conservative(fixedShard))).containsExactlyInAnyOrder(
                WriteInfo.write(TABLE_CONS, getCellWithFixedHash(1), TS),
                WriteInfo.write(TABLE_CONS, getCellWithFixedHash(2), TS + 1));
    }

    @Test
    public void canReadMultipleEntriesInSingleShardSameTransactionNotDedicated() {

        List<WriteInfo> writes = writeToCellsInFixedShard(sweepableCells, TS, 10, TABLE_CONS);
        ShardAndStrategy fixedShardAndStrategy = conservative(writes.get(0).toShard(SHARDS));
        assertThat(writes.size()).isEqualTo(10);
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, fixedShardAndStrategy)).hasSameElementsAs(writes);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardSameTransactionOneDedicated() {
        List<WriteInfo> writes = writeToCellsInFixedShard(sweepableCells, TS, MAX_CELLS_GENERIC * 2 + 1, TABLE_CONS);
        ShardAndStrategy fixedShardAndStrategy = conservative(writes.get(0).toShard(SHARDS));
        assertThat(writes.size()).isEqualTo(MAX_CELLS_GENERIC * 2 + 1);
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, fixedShardAndStrategy)).hasSameElementsAs(writes);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardMultipleTransactionsCombined() {
        List<WriteInfo> first = writeToCellsInFixedShard(sweepableCells, TS, MAX_CELLS_GENERIC * 2 + 1, TABLE_CONS);
        List<WriteInfo> last = writeToCellsInFixedShard(sweepableCells, TS + 2, 1, TABLE_CONS);
        List<WriteInfo> middle = writeToCellsInFixedShard(sweepableCells, TS + 1, MAX_CELLS_GENERIC + 1, TABLE_CONS);
        // The expected list of latest writes contains:
        // ((0, 0), TS + 2
        // ((1, 1), TS + 1) .. ((MAX_CELLS_GENERIC, MAX_CELLS_GENERIC), TS + 1)
        // ((MAX_CELLS_GENERIC + 1, MAX_CELLS_GENERIC) + 1, TS) .. ((2 * MAX_CELLS_GENERIC, 2 * MAX_CELLS_GENERIC), TS)
        List<WriteInfo> expectedResult = new ArrayList<>(last);
        expectedResult.addAll(middle.subList(last.size(), middle.size()));
        expectedResult.addAll(first.subList(middle.size(), first.size()));

        ShardAndStrategy fixedShardAndStrategy = conservative(first.get(0).toShard(SHARDS));
        List<WriteInfo> result = sweepableCells.getWritesFromPartition(TS_REF, fixedShardAndStrategy);
        assertThat(result).hasSameElementsAs(expectedResult);
    }

    @Test
    @Ignore("This test takes 53 minutes to complete. Need to see how it performs with CassandraKVS")
    public void canReadMultipleEntriesInSingleShardSameTransactionMultipleDedicated() {
        List<WriteInfo> writes = writeToCellsInFixedShard(sweepableCells, TS, MAX_CELLS_DEDICATED + 1, TABLE_CONS);
        ShardAndStrategy fixedShardAndStrategy = conservative(writes.get(0).toShard(SHARDS));
        assertThat(writes.size()).isEqualTo(MAX_CELLS_DEDICATED + 1);
        assertThat(sweepableCells.getWritesFromPartition(TS_REF, fixedShardAndStrategy)).hasSameElementsAs(writes);
    }
}
