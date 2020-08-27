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

import static com.palantir.atlasdb.sweep.queue.ShardAndStrategy.conservative;
import static com.palantir.atlasdb.sweep.queue.ShardAndStrategy.thorough;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.MAX_CELLS_DEDICATED;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.MAX_CELLS_GENERIC;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.SWEEP_BATCH_SIZE;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableTargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable.SweepableCellsRow;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.sweep.metrics.SweepMetricsAssert;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.lock.v2.TimelockService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SweepableCellsTest extends AbstractSweepQueueTest {
    private static final long SMALL_SWEEP_TS = TS + 200L;
    private static final TableReference SWEEP_QUEUE_TABLE = TargetedSweepTableFactory.of()
            .getSweepableCellsTable(null)
            .getTableRef();

    private TargetedSweepMetrics metrics;
    private SweepableCells sweepableCells;

    @Before
    @Override
    public void setup() {
        super.setup();
        metrics = TargetedSweepMetrics.create(metricsManager, mock(TimelockService.class), spiedKvs,
                TargetedSweepMetrics.MetricsConfiguration.builder().millisBetweenRecomputingMetrics(1).build());
        sweepableCells = new SweepableCells(spiedKvs, partitioner, metrics, txnService);

        shardCons = writeToDefaultCellCommitted(sweepableCells, TS, TABLE_CONS);
        shardThor = writeToDefaultCellCommitted(sweepableCells, TS2, TABLE_THOR);
    }

    @Test
    public void canReadSingleEntryInSingleShardForCorrectPartitionAndRange() {
        SweepBatch conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.writes()).containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS));

        SweepBatch thoroughBatch = readThorough(TS2_FINE_PARTITION, TS2 - 1, Long.MAX_VALUE);
        assertThat(thoroughBatch.writes()).containsExactly(WriteInfo.write(TABLE_THOR, DEFAULT_CELL, TS2));

        SweepMetricsAssert.assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(1);
        SweepMetricsAssert.assertThat(metricsManager).hasEnqueuedWritesThoroughEqualTo(1);
    }

    @Test
    public void readDoesNotReturnValuesFromAbortedTransactions() {
        writeToDefaultCellAborted(sweepableCells, TS + 1, TABLE_CONS);
        SweepBatch conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.writes()).containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS));
    }

    @Test
    public void readDeletesValuesFromAbortedTransactions() {
        writeToDefaultCellAborted(sweepableCells, TS + 1, TABLE_CONS);
        readConservative(shardCons, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS);

        Multimap<Cell, Long> expectedDeletes = HashMultimap.create();
        expectedDeletes.put(DEFAULT_CELL, TS + 1);
        assertDeleted(TABLE_CONS, expectedDeletes);
        SweepMetricsAssert.assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(1);
    }

    @Test
    public void readDoesNotReturnValuesFromUncommittedTransactionsAndAbortsThem() {
        writeToDefaultCellUncommitted(sweepableCells, TS + 1, TABLE_CONS);
        assertThat(!isTransactionAborted(TS + 1));

        SweepBatch conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS);
        assertThat(isTransactionAborted(TS + 1));
        assertThat(conservativeBatch.writes()).containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS));
    }

    @Test
    public void readDeletesValuesFromUncommittedTransactions() {
        writeToDefaultCellUncommitted(sweepableCells, TS + 1, TABLE_CONS);
        readConservative(shardCons, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS);

        Multimap<Cell, Long> expectedDeletes = HashMultimap.create();
        expectedDeletes.put(DEFAULT_CELL, TS + 1);
        assertDeleted(TABLE_CONS, expectedDeletes);
        SweepMetricsAssert.assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(1);
    }

    @Test
    public void readDoesNotReturnValuesFromTransactionsCommittedAfterSweepTs() {
        writeToDefaultCellCommitedAt(sweepableCells, TS + 1, SMALL_SWEEP_TS, TABLE_CONS);
        SweepBatch conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.writes()).containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS));
    }

    @Test
    public void lastSweptTimestampIsMinimumOfSweepTsAndEndOfFinePartitionWhenThereAreMatches() {
        SweepBatch conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(SMALL_SWEEP_TS - 1);

        conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION, TS - 1, Long.MAX_VALUE);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(endOfFinePartitionForTs(TS));

    }

    @Test
    public void cannotReadEntryForWrongShard() {
        SweepBatch conservativeBatch = readConservative(shardCons + 1, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.writes()).isEmpty();
    }

    @Test
    public void cannotReadEntryForWrongPartition() {
        SweepBatch conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION - 1, 0L, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.writes()).isEmpty();

        conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION + 1, TS - 1, Long.MAX_VALUE);
        assertThat(conservativeBatch.writes()).isEmpty();
    }

    @Test
    public void cannotReadEntryOutOfRange() {
        SweepBatch conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION, TS, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.writes()).isEmpty();

        conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION, 0L, TS);
        assertThat(conservativeBatch.writes()).isEmpty();
    }

    @Test
    public void inconsistentPartitionAndRangeThrows() {
        assertThatThrownBy(() -> readConservative(shardCons, TS_FINE_PARTITION - 1, TS - 1, SMALL_SWEEP_TS))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> readConservative(shardCons, TS_FINE_PARTITION + 1, TS - 1, SMALL_SWEEP_TS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void lastSweptTimestampIsMinimumOfSweepTsAndEndOfFinePartitionWhenNoMatches() {
        SweepBatch conservativeBatch = readConservative(shardCons + 1, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(SMALL_SWEEP_TS - 1);

        conservativeBatch = readConservative(shardCons + 1, TS_FINE_PARTITION, TS - 1, Long.MAX_VALUE);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(endOfFinePartitionForTs(TS));
    }

    @Test
    public void readOnlyTombstoneWhenLatestInShardAndRange() {
        putTombstoneToDefaultCommitted(sweepableCells, TS + 1, TABLE_CONS);
        SweepBatch batch = readConservative(CONS_SHARD, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS);
        assertThat(batch.writes()).containsExactly(WriteInfo.tombstone(TABLE_CONS, DEFAULT_CELL, TS + 1));
    }

    @Test
    public void readOnlyMostRecentTimestampForRange() {
        writeToDefaultCellCommitted(sweepableCells, TS - 1, TABLE_CONS);
        writeToDefaultCellCommitted(sweepableCells, TS + 2, TABLE_CONS);
        writeToDefaultCellCommitted(sweepableCells, TS - 2, TABLE_CONS);
        writeToDefaultCellCommitted(sweepableCells, TS + 1, TABLE_CONS);
        SweepBatch conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION, TS - 3, TS);
        assertThat(conservativeBatch.writes()).containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS - 1));
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(TS - 1);

        conservativeBatch = readConservative(shardCons, TS_FINE_PARTITION, TS - 3, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.writes()).containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS + 2));
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(SMALL_SWEEP_TS - 1);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardDifferentTransactions() {
        writeToCellCommitted(sweepableCells, TS, getCellWithFixedHash(1), TABLE_CONS);
        writeToCellCommitted(sweepableCells, TS + 1, getCellWithFixedHash(2), TABLE_CONS);
        SweepBatch conservativeBatch = readConservative(FIXED_SHARD, TS_FINE_PARTITION, TS - 1, TS + 2);
        assertThat(conservativeBatch.writes()).containsExactlyInAnyOrder(
                WriteInfo.write(TABLE_CONS, getCellWithFixedHash(1), TS),
                WriteInfo.write(TABLE_CONS, getCellWithFixedHash(2), TS + 1));
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(TS + 1);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardSameTransactionNotDedicated() {
        List<WriteInfo> writes = writeToCellsInFixedShard(sweepableCells, TS, 10, TABLE_CONS);
        SweepBatch conservativeBatch = readConservative(FIXED_SHARD, TS_FINE_PARTITION, TS - 1, TS + 1);
        assertThat(conservativeBatch.writes().size()).isEqualTo(10);
        assertThat(conservativeBatch.writes()).hasSameElementsAs(writes);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardSameTransactionOneDedicated() {
        List<WriteInfo> writes = writeToCellsInFixedShard(sweepableCells, TS, MAX_CELLS_GENERIC * 2 + 1, TABLE_CONS);
        SweepBatch conservativeBatch = readConservative(FIXED_SHARD, TS_FINE_PARTITION, TS - 1, TS + 1);
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

        SweepBatch conservativeBatch = readConservative(FIXED_SHARD, TS_FINE_PARTITION, TS - 1, TS + 3);
        assertThat(conservativeBatch.writes().size()).isEqualTo(MAX_CELLS_GENERIC * 2 + 1);
        assertThat(conservativeBatch.writes()).hasSameElementsAs(expectedResult);
    }

    @Test
    public void changingNumberOfShardsDoesNotAffectExistingWritesButAffectsFuture() {
        useSingleShard();
        assertThat(readConservative(0, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS).writes()).isEmpty();

        writeToDefaultCellCommitted(sweepableCells, TS, TABLE_CONS);
        assertThat(readConservative(0, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS).writes())
                .containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, TS));
    }

    // We read 5 dedicated entries until we pass SWEEP_BATCH_SIZE, for a total of SWEEP_BATCH_SIZE + 5 writes
    @Test
    public void returnWhenMoreThanSweepBatchSize() {
        useSingleShard();
        long iterationWrites = 1 + SWEEP_BATCH_SIZE / 5;
        for (int i = 1; i <= 10; i++) {
            writeCommittedConservativeRowForTimestamp(i, iterationWrites);
        }
        SweepBatch conservativeBatch = readConservative(0, 0L, -1L, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.writes().size()).isEqualTo(SWEEP_BATCH_SIZE + 5);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(5);
        SweepMetricsAssert.assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(
                10 * iterationWrites + 1);
        SweepMetricsAssert.assertThat(metricsManager).hasEntriesReadConservativeEqualTo(
                5 * iterationWrites);
        SweepMetricsAssert.assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(0);
    }

    @Test
    public void returnWhenMoreThanSweepBatchSizeWithRepeatsHasFewerEntries() {
        useSingleShard();
        int iterationWrites = 1 + SWEEP_BATCH_SIZE / 5;
        for (int i = 1; i <= 10; i++) {
            writeCommittedConservativeRowZero(i, iterationWrites);
        }
        SweepBatch conservativeBatch = readConservative(0, 0L, -1L, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.writes().size()).isEqualTo(iterationWrites);
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(5);
        SweepMetricsAssert.assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(
                10 * iterationWrites + 1);
        SweepMetricsAssert.assertThat(metricsManager).hasEntriesReadConservativeEqualTo(
                5 * iterationWrites);
        SweepMetricsAssert.assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(0);
    }

    @Test
    public void returnNothingWhenMoreThanSweepBatchUncommitted() {
        useSingleShard();
        int iterationWrites = 1 + SWEEP_BATCH_SIZE / 5;
        for (int i = 1; i <= 10; i++) {
            writeWithoutCommitConservative(i, i, iterationWrites);
        }
        writeCommittedConservativeRowForTimestamp(10, iterationWrites);
        SweepBatch conservativeBatch = readConservative(0, 0L, -1L, SMALL_SWEEP_TS);
        assertThat(conservativeBatch.writes()).isEmpty();
        assertThat(conservativeBatch.lastSweptTimestamp()).isEqualTo(5);
        SweepMetricsAssert.assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(
                11 * iterationWrites + 1);
        SweepMetricsAssert.assertThat(metricsManager).hasEntriesReadConservativeEqualTo(
                5 * iterationWrites);
        SweepMetricsAssert.assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(
                5 * iterationWrites);
    }

    @Test
    public void canReadMultipleEntriesInSingleShardSameTransactionMultipleDedicated() {
        useSingleShard();
        List<WriteInfo> writes = writeCommittedConservativeRowForTimestamp(TS + 1, MAX_CELLS_DEDICATED + 1);

        SweepBatch conservativeBatch = readConservative(0, TS_FINE_PARTITION, TS, TS + 2);
        assertThat(conservativeBatch.writes().size()).isEqualTo(writes.size());
        assertThat(conservativeBatch.writes()).contains(writes.get(0), writes.get(writes.size() - 1));
    }

    @Test
    public void uncommittedWritesInDedicatedRowsGetDeleted() {
        useSingleShard();
        writeWithoutCommitConservative(TS + 1, 0L, MAX_CELLS_DEDICATED + 1);

        SweepBatch conservativeBatch = readConservative(0, TS_FINE_PARTITION, TS, TS + 2);
        assertThat(conservativeBatch.writes()).isEmpty();

        assertDeletedNumber(TABLE_CONS, MAX_CELLS_DEDICATED + 1);
        SweepMetricsAssert.assertThat(metricsManager)
                .hasAbortedWritesDeletedConservativeEquals(MAX_CELLS_DEDICATED + 1);
    }

    @Test
    public void cleanupNonDedicatedRow() {
        useSingleShard();
        writeCommittedConservativeRowForTimestamp(TS + 1, MAX_CELLS_GENERIC);
        writeCommittedConservativeRowForTimestamp(TS + 3, MAX_CELLS_GENERIC);
        writeCommittedConservativeRowForTimestamp(TS + 5, MAX_CELLS_GENERIC);

        sweepableCells.deleteNonDedicatedRows(ShardAndStrategy.conservative(0), ImmutableList.of(TS_FINE_PARTITION));
        SweepableCellsRow row = SweepableCellsRow.of(TS_FINE_PARTITION,
                ImmutableTargetedSweepMetadata.builder()
                        .conservative(true)
                        .dedicatedRow(false)
                        .shard(0)
                        .dedicatedRowNumber(0)
                        .build()
                        .persistToBytes());

        verifyRowsDeletedFromSweepQueue(ImmutableList.of(row));
    }

    @Test
    public void cleanupMultipleDedicatedRows() {
        useSingleShard();

        long timestamp = TS + 1;
        DedicatedRows dedicatedRows = DedicatedRows.of(LongStream.range(0, 3)
                .mapToObj(rowNumber -> ImmutableTargetedSweepMetadata.builder()
                        .conservative(true)
                        .dedicatedRow(true)
                        .dedicatedRowNumber(rowNumber)
                        .shard(0)
                        .build()
                        .persistToBytes())
                .map(metadata -> SweepableCellsRow.of(timestamp, metadata))
                .collect(Collectors.toList()));

        writeCommittedConservativeRowForTimestamp(timestamp, MAX_CELLS_DEDICATED * 2 + 1);

        sweepableCells.deleteDedicatedRows(dedicatedRows);

        verifyRowsDeletedFromSweepQueue(dedicatedRows.getDedicatedRows());
        verify(spiedKvs, times(1)).deleteRows(eq(SWEEP_QUEUE_TABLE), any());
    }

    @Test
    public void getBatchReturnsDedicatedRowsSeen() {
        useSingleShard();

        long timestamp = TS + 1;

        writeCommittedConservativeRowForTimestamp(timestamp, MAX_CELLS_DEDICATED * 2 + 1);

        DedicatedRows expectedDedicatedRows = DedicatedRows.of(LongStream.range(0, 3)
                .mapToObj(rowNumber -> ImmutableTargetedSweepMetadata.builder()
                        .conservative(true)
                        .dedicatedRow(true)
                        .dedicatedRowNumber(rowNumber)
                        .shard(0)
                        .build()
                        .persistToBytes())
                .map(metadata -> SweepableCellsRow.of(timestamp, metadata))
                .collect(Collectors.toList()));

        assertThat(readConservative(0, TS_FINE_PARTITION, TS - 1, SMALL_SWEEP_TS).dedicatedRows())
                .isEqualTo(expectedDedicatedRows);
    }

    private void verifyRowsDeletedFromSweepQueue(List<SweepableCellsRow> rows) {
        ArgumentCaptor<List<byte[]>> captor = ArgumentCaptor.forClass(List.class);
        verify(spiedKvs, atLeast(0)).deleteRows(eq(SWEEP_QUEUE_TABLE), captor.capture());

        List<byte[]> expectedValuesToDelete = rows.stream()
                .map(SweepableCellsRow::persistToBytes)
                .collect(Collectors.toList());
        assertThat(captor.getAllValues().stream().flatMap(Collection::stream).collect(Collectors.toList()))
                .hasSameElementsAs(expectedValuesToDelete);
    }

    private SweepBatch readConservative(int shard, long partition, long minExclusive, long maxExclusive) {
        return sweepableCells.getBatchForPartition(conservative(shard), partition, minExclusive, maxExclusive);
    }

    private SweepBatch readThorough(long partition, long minExclusive, long maxExclusive) {
        return sweepableCells.getBatchForPartition(thorough(shardThor), partition, minExclusive, maxExclusive);
    }

    private long endOfFinePartitionForTs(long timestamp) {
        return SweepQueueUtils.maxTsForFinePartition(tsPartitionFine(timestamp));
    }

    private void useSingleShard() {
        numShards = 1;
    }

    private List<WriteInfo> writeCommittedConservativeRowForTimestamp(long timestamp, long numWrites) {
        putTimestampIntoTransactionTable(timestamp, timestamp);
        return writeWithoutCommitConservative(timestamp, timestamp, numWrites);
    }

    private List<WriteInfo> writeCommittedConservativeRowZero(long timestamp, long numWrites) {
        putTimestampIntoTransactionTable(timestamp, timestamp);
        return writeWithoutCommitConservative(timestamp, 0, numWrites);
    }

    private List<WriteInfo> writeWithoutCommitConservative(long timestamp, long row, long numWrites) {
        List<WriteInfo> writes = new ArrayList<>();
        for (long i = 0; i < numWrites; i++) {
            Cell cell = Cell.create(PtBytes.toBytes(row), PtBytes.toBytes(i));
            writes.add(WriteInfo.write(TABLE_CONS, cell, timestamp));
        }
        sweepableCells.enqueue(writes);
        return writes;
    }

    private void assertDeleted(TableReference tableRef, Multimap<Cell, Long> expectedDeletes) {
        ArgumentCaptor<Multimap> argumentCaptor = ArgumentCaptor.forClass(Multimap.class);
        verify(spiedKvs).delete(eq(tableRef), argumentCaptor.capture());

        Multimap<Cell, Long> actual = argumentCaptor.getValue();
        assertThat(actual.keySet()).containsExactlyElementsOf(expectedDeletes.keySet());
        actual.keySet().forEach(key -> assertThat(actual.get(key)).containsExactlyElementsOf(expectedDeletes.get(key)));
    }

    private void assertDeletedNumber(TableReference tableRef, int expectedDeleted) {
        ArgumentCaptor<Multimap> argumentCaptor = ArgumentCaptor.forClass(Multimap.class);
        verify(spiedKvs).delete(eq(tableRef), argumentCaptor.capture());

        Multimap<Cell, Long> actual = argumentCaptor.getValue();
        assertThat(actual.size()).isEqualTo(expectedDeleted);
    }
}
