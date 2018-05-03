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
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;
import static com.palantir.atlasdb.sweep.queue.SweepableCells.MAX_CELLS_DEDICATED;
import static com.palantir.atlasdb.sweep.queue.SweepableCells.MAX_CELLS_GENERIC;
import static com.palantir.atlasdb.sweep.queue.WriteInfoPartitioner.SHARDS;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class SweepableCellsTest extends SweepQueueTablesTest {
    private static final long SWEEP_TS = TS + 200L;

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
    public void canReadSingleEntryInSingleShardForCorrectPartitionAndRange() {
        SweepBatch conservativeBatch = readConservative(shardCons, TS_REF, TS - 1, SWEEP_TS);
        assertThat(conservativeBatch.writes()).containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS));

        SweepBatch thoroughBatch = readThorough(TS2_REF, TS2 - 1, Long.MAX_VALUE);
        assertThat(thoroughBatch.writes()).containsExactly(WriteInfo.write(TABLE_THOR, DEFAULT_CELL, TS2));
    }

    @Test
    public void lastSweptTimestampIsMinimumOfSweepTsAndEndOfFinePartitionWhenThereAreMatches() {
        SweepBatch conservativeBatch = readConservative(shardCons, TS_REF, TS - 1, SWEEP_TS);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(SWEEP_TS - 1);

        conservativeBatch = readConservative(shardCons, TS_REF, TS - 1, Long.MAX_VALUE);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(endOfFinePartitionForTs(TS));
    }

    @Test
    public void cannotReadEntryForWrongPartition() {
        SweepBatch conservativeBatch = readConservative(shardCons + 1, TS_REF, TS - 1, SWEEP_TS);
        assertThat(conservativeBatch.writes()).isEmpty();
    }

    @Test
    public void lastSweptTimestampIsMinimumOfSweepTsAndEndOfFinePartitionWhenNoMatches() {
        SweepBatch conservativeBatch = readConservative(shardCons + 1, TS_REF, TS - 1, SWEEP_TS);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(SWEEP_TS - 1);

        conservativeBatch = readConservative(shardCons + 1, TS_REF, TS - 1, Long.MAX_VALUE);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(endOfFinePartitionForTs(TS));
    }

    @Test
    public void cannotReadEntryOutOfRange() {
        SweepBatch conservativeBatch = readConservative(shardCons, TS_REF, TS, SWEEP_TS);
        assertThat(conservativeBatch.writes()).isEmpty();

        conservativeBatch = readConservative(shardCons, TS_REF, 0L, TS);
        assertThat(conservativeBatch.writes()).isEmpty();
    }

    @Test
    public void inconsistentRangeReturnsNoWritesAndSameLastSweptTimestamp() {
        long lastSweptTs = 2 * TS;
        SweepBatch conservativeBatch = readConservative(shardCons, tsPartitionFine(lastSweptTs), lastSweptTs, TS);
        assertThat(conservativeBatch.writes()).isEmpty();
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(lastSweptTs);
    }

    @Test
    public void readTombstoneOnlyWhenLatestInShardAndRange() {
        int tombstoneShard = putTombstone(sweepableCells, TS + 1, DEFAULT_CELL, TABLE_CONS);
        SweepBatch batch = readConservative(tombstoneShard, TS_REF, TS - 1, SWEEP_TS);
        assertThat(batch.writes()).containsExactly(WriteInfo.tombstone(TABLE_CONS, DEFAULT_CELL, TS + 1));
    }

    @Test
    public void getOnlyMostRecentTimestampForRange() {
        writeToDefault(sweepableCells, TS - 1, TABLE_CONS);
        writeToDefault(sweepableCells, TS + 2, TABLE_CONS);
        writeToDefault(sweepableCells, TS - 2, TABLE_CONS);
        writeToDefault(sweepableCells, TS + 1, TABLE_CONS);
        SweepBatch conservativeBatch = readConservative(shardCons, TS_REF, TS - 3, TS);
        assertThat(conservativeBatch.writes()).containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS - 1));
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(TS - 1);

        conservativeBatch = readConservative(shardCons, TS_REF, TS - 3, SWEEP_TS);
        assertThat(conservativeBatch.writes()).containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS + 2));
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(SWEEP_TS - 1);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardDifferentTransactions() {
        int fixedShard = writeToCell(sweepableCells, TS, getCellWithFixedHash(1), TABLE_CONS);
        assertThat(writeToCell(sweepableCells, TS + 1, getCellWithFixedHash(2), TABLE_CONS)).isEqualTo(fixedShard);
        SweepBatch conservativeBatch = readConservative(fixedShard, TS_REF, TS - 1, TS + 2);
        assertThat(conservativeBatch.writes()).containsExactlyInAnyOrder(
                WriteInfo.write(TABLE_CONS, getCellWithFixedHash(1), TS),
                WriteInfo.write(TABLE_CONS, getCellWithFixedHash(2), TS + 1));
    }

    @Test
    public void canReadMultipleEntriesInSingleShardSameTransactionNotDedicated() {
        List<WriteInfo> writes = writeToCellsInFixedShard(sweepableCells, TS, 10, TABLE_CONS);
        SweepBatch conservativeBatch = readConservative(FIXED_SHARD, TS_REF, TS - 1, TS + 1);
        assertThat(conservativeBatch.writes().size()).isEqualTo(10);
        assertThat(conservativeBatch.writes()).hasSameElementsAs(writes);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardSameTransactionOneDedicated() {
        List<WriteInfo> writes = writeToCellsInFixedShard(sweepableCells, TS, MAX_CELLS_GENERIC * 2 + 1, TABLE_CONS);
        SweepBatch conservativeBatch = readConservative(FIXED_SHARD, TS_REF, TS - 1, TS + 1);
        assertThat(conservativeBatch.writes().size()).isEqualTo(MAX_CELLS_GENERIC * 2 + 1);
        assertThat(conservativeBatch.writes()).hasSameElementsAs(writes);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardMultipleTransactionsCombined() {
        List<WriteInfo> first = writeToCellsInFixedShard(sweepableCells, TS, MAX_CELLS_GENERIC * 2 + 1, TABLE_CONS);
        List<WriteInfo> last = writeToCellsInFixedShard(sweepableCells, TS + 2, 1, TABLE_CONS);
        List<WriteInfo> middle = writeToCellsInFixedShard(sweepableCells, TS + 1, MAX_CELLS_GENERIC + 1, TABLE_CONS);
        // The expected list of latest writes contains:
        // ((0, 0), TS + 2)
        // ((1, 1), TS + 1) .. ((MAX_CELLS_GENERIC, MAX_CELLS_GENERIC), TS + 1)
        // ((MAX_CELLS_GENERIC + 1, MAX_CELLS_GENERIC) + 1, TS) .. ((2 * MAX_CELLS_GENERIC, 2 * MAX_CELLS_GENERIC), TS)
        List<WriteInfo> expectedResult = new ArrayList<>(last);
        expectedResult.addAll(middle.subList(last.size(), middle.size()));
        expectedResult.addAll(first.subList(middle.size(), first.size()));

        SweepBatch conservativeBatch = readConservative(FIXED_SHARD, TS_REF, TS - 1, TS + 3);
        assertThat(conservativeBatch.writes().size()).isEqualTo(MAX_CELLS_GENERIC * 2 + 1);
        assertThat(conservativeBatch.writes()).hasSameElementsAs(expectedResult);
    }

    @Test
    public void returnWhenMoreThanBatchSizeNonDedicated() {
        for (int i = 0; i < 2 * SweepableCells.SWEEP_BATCH_SIZE; i++) {
            writeToCell(sweepableCells, i, getCellWithFixedHash(i), TABLE_CONS);
        }
        SweepBatch conservativeBatch = readConservative(FIXED_SHARD, 0L, -1L, SWEEP_TS);
        assertThat(conservativeBatch.writes().size()).isEqualTo((int) SweepableCells.SWEEP_BATCH_SIZE);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(SweepableCells.SWEEP_BATCH_SIZE - 1);
    }

    // We read 5 dedicated entries until we pass 1K, for a total of 1005 writes
    @Test
    public void returnWhenMoreThanBatchSizeDedicated() {
        for (int i = 0; i < 10; i++) {
            writeToCellsInFixedShardStartWith(sweepableCells, i, 201, TABLE_CONS, i * 201);
        }
        SweepBatch conservativeBatch = readConservative(FIXED_SHARD, 0L, -1L, SWEEP_TS);
        assertThat(conservativeBatch.writes().size()).isEqualTo(1005);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(4);
    }

    @Test
    @Ignore("This test takes 53 minutes to complete. Need to see how it performs with CassandraKVS")
    public void canReadMultipleEntriesInSingleShardSameTransactionMultipleDedicated() {
        List<WriteInfo> writes = writeToCellsInFixedShard(sweepableCells, TS, MAX_CELLS_DEDICATED + 1, TABLE_CONS);
        int fixedShard = writes.get(0).toShard(SHARDS);
        SweepBatch conservativeBatch = readConservative(fixedShard, TS_REF, TS - 1, TS + 1);
        assertThat(conservativeBatch.writes()).hasSameElementsAs(writes);
    }

    private SweepBatch readConservative(int shard, long partition, long minExclusive, long maxExclusive) {
        return sweepableCells.getBatchForPartition(conservative(shard), partition, minExclusive, maxExclusive);
    }

    private SweepBatch readThorough(long partition, long minExclusive, long maxExclusive) {
        return sweepableCells.getBatchForPartition(thorough(shardThor), partition, minExclusive, maxExclusive);
    }

    private long endOfFinePartitionForTs(long timestamp) {
        return SweepQueueUtils.maxForFinePartition(tsPartitionFine(timestamp));
    }
}
