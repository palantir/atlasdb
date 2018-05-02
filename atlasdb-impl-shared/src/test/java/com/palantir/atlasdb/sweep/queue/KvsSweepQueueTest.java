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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy.CONSERVATIVE;
import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy.NOTHING;
import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy.THOROUGH;
import static com.palantir.atlasdb.sweep.queue.SweepQueueTablesTest.metadataBytes;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_COARSE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_FINE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;
import static com.palantir.atlasdb.sweep.queue.WriteInfoPartitioner.SHARDS;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.sweep.Sweeper;

public class KvsSweepQueueTest {
    private static final TableReference TABLE_CONSERVATIVE = TableReference.createFromFullyQualifiedName("test.cons");
    private static final TableReference TABLE_THOROUGH = TableReference.createFromFullyQualifiedName("test.thor");
    private static final TableReference TABLE_NOTHING = TableReference.createFromFullyQualifiedName("test.noth");
    private static final Cell DEFAULT_CELL = Cell.create(new byte[] {'r'}, new byte[] {'c'});
    private static final int CONS_SHARD = WriteInfo.tombstone(TABLE_CONSERVATIVE, DEFAULT_CELL, 0).toShard(SHARDS);
    private static final int THOR_SHARD = WriteInfo.tombstone(TABLE_THOROUGH, DEFAULT_CELL, 0).toShard(SHARDS);
    private static final long TS = 10L;
    private static final long TS2 = 2 * TS;

    KeyValueService kvs;
    KvsSweepQueue sweepQueue = KvsSweepQueue.createUninitialized(() -> SHARDS);
    long unreadableTs;
    long immutableTs;

    SweepTimestampProvider provider = new SweepTimestampProvider(() -> unreadableTs, () -> immutableTs);

    @Before
    public void setup() {
        kvs = spy(new InMemoryKeyValueService(false));
        unreadableTs = TS_COARSE_GRANULARITY * 5;
        immutableTs = TS_COARSE_GRANULARITY * 5;
        sweepQueue.initialize(provider, kvs);
        kvs.createTable(TABLE_CONSERVATIVE, metadataBytes(CONSERVATIVE));
        kvs.createTable(TABLE_THOROUGH, metadataBytes(THOROUGH));
        kvs.createTable(TABLE_NOTHING, metadataBytes(NOTHING));
    }

    @Test
    public void sweepStrategyNothingDoesNotPersistAnything() {
        enqueueWrite(TABLE_NOTHING, TS);
        enqueueWrite(TABLE_NOTHING, TS2);
        verify(kvs, times(2)).put(any(TableReference.class), anyMap(), anyLong());
    }

    @Test
    public void conservativeSweepAddsSentinelAndLeavesSingleValue() {
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        assertReadAtTimestampReturnsNothing(TABLE_CONSERVATIVE, TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS + 1, TS);
    }

    @Test
    public void thoroughSweepDoesNotAddSentinelAndLeavesSingleValue() {
        enqueueWrite(TABLE_THOROUGH, TS);
        assertReadAtTimestampReturnsNothing(TABLE_THOROUGH, TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOROUGH, TS);
        assertReadAtTimestampReturnsValue(TABLE_THOROUGH, TS + 1, TS);
    }

    @Test
    public void conservativeSweepDeletesLowerValue() {
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        enqueueWrite(TABLE_CONSERVATIVE, TS2);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS + 1, TS);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);
    }

    @Test
    public void thoroughSweepDeletesLowerValue() {
        enqueueWrite(TABLE_THOROUGH, TS);
        enqueueWrite(TABLE_THOROUGH, TS2);
        assertReadAtTimestampReturnsValue(TABLE_THOROUGH, TS + 1, TS);
        assertReadAtTimestampReturnsValue(TABLE_THOROUGH, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOROUGH, TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_THOROUGH, TS2 + 1, TS2);
    }

    @Test
    public void sweepDeletesAllButLatestWithSingleDeleteAllTimestamps() {
        long numWrites = 1000L;
        for (long i = 1; i <= numWrites; i++) {
            enqueueWrite(TABLE_CONSERVATIVE, i);
        }
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, numWrites);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, numWrites + 1, numWrites);
        verify(kvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
    }

    @Test
    public void onlySweepsOneBatchAtATime() {
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        enqueueWrite(TABLE_CONSERVATIVE, TS2);
        enqueueWrite(TABLE_CONSERVATIVE, TS_FINE_GRANULARITY);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS_FINE_GRANULARITY + 1, TS_FINE_GRANULARITY);
    }

    @Test
    public void sweepDeletesWritesWhenTombstoneHasHigherTimestamp() {
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        enqueueTombstone(TABLE_CONSERVATIVE, TS2);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS + 1, TS);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONSERVATIVE, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONSERVATIVE, TS2 + 1, TS2);
    }

    @Test
    public void thoroughSweepDeletesTombstoneIfLatestWrite() {
        enqueueTombstone(TABLE_THOROUGH, TS);
        enqueueTombstone(TABLE_THOROUGH, TS2);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_THOROUGH, TS + 1, TS);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_THOROUGH, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOROUGH, TS + 1);
        assertReadAtTimestampReturnsNothing(TABLE_THOROUGH, TS2 + 1);
    }

    @Test
    public void sweepDeletesTombstonesWhenWriteHasHigherTimestamp() {
        enqueueTombstone(TABLE_CONSERVATIVE, TS);
        enqueueWrite(TABLE_CONSERVATIVE, TS2);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONSERVATIVE, TS + 1, TS);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);
    }

    @Test
    public void sweepProgressesAndSkipsEmptyFinePartitions() {
        long tsFineTwo = TS + TS_FINE_GRANULARITY;
        long tsFineFour = TS + 3 * TS_FINE_GRANULARITY;
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        enqueueWrite(TABLE_CONSERVATIVE, tsFineTwo);
        enqueueWrite(TABLE_CONSERVATIVE, tsFineFour);
        enqueueWrite(TABLE_CONSERVATIVE, tsFineFour + 1L);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS + 1, TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, tsFineTwo);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, tsFineTwo + 1, tsFineTwo);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, tsFineFour + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, tsFineFour + 2, tsFineFour + 1);
    }

    @Test
    public void sweepProgressesAcrossCoarsePartitions() {
        long tsCoarseTwo = TS + TS_FINE_GRANULARITY + TS_COARSE_GRANULARITY;
        long tsCoarseFour = TS + 3 * TS_COARSE_GRANULARITY;
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        enqueueWrite(TABLE_CONSERVATIVE, tsCoarseTwo);
        enqueueWrite(TABLE_CONSERVATIVE, tsCoarseFour);
        enqueueWrite(TABLE_CONSERVATIVE, tsCoarseFour + 1L);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS + 1, TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, tsCoarseTwo);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, tsCoarseTwo + 1, tsCoarseTwo);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, tsCoarseFour + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, tsCoarseFour + 2, tsCoarseFour + 1);
    }

    @Test
    public void sweepProgressesToPartitionBeforeSweepTsWhenNothingToSweep() {
        long finePartitionForSweepTs = tsPartitionFine(provider.getSweepTimestamp(Sweeper.CONSERVATIVE));
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));

        assertProgressUpdatedToFineTimestampPartition(finePartitionForSweepTs - 1L);
    }

    @Test
    public void sweepProgressesToPartitionOfJustSwept() {
        long finePartitionForSweepTs = tsPartitionFine(provider.getSweepTimestamp(Sweeper.CONSERVATIVE));
        long writeTs = provider.getSweepTimestamp(Sweeper.CONSERVATIVE) - 3 * TS_FINE_GRANULARITY;
        enqueueWrite(TABLE_CONSERVATIVE, writeTs);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertProgressUpdatedToFineTimestampPartition(tsPartitionFine(writeTs));

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertProgressUpdatedToFineTimestampPartition(finePartitionForSweepTs - 1L);

        assertThat(tsPartitionFine(writeTs)).isLessThan(finePartitionForSweepTs);
    }

    @Test
    public void sweepCellOnlyOnceWhenInLastPartitionBeforeSweepTs() {
        immutableTs = 2 * TS_COARSE_GRANULARITY - TS_FINE_GRANULARITY;
        verify(kvs, never()).deleteAllTimestamps(any(TableReference.class), anyMap());

        enqueueWrite(TABLE_CONSERVATIVE, immutableTs - 1);
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        verify(kvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        verify(kvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
    }

    @Test
    public void sweepableTimestampsGetsScrubbedWhenNoMoreToSweepButSweepTsInNewCoarsePartition() {
        long tsSecondPartitionFine = TS + TS_FINE_GRANULARITY;
        long largestFirstPartitionCoarse = TS_COARSE_GRANULARITY - 1L;
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        enqueueWrite(TABLE_CONSERVATIVE, tsSecondPartitionFine);
        enqueueWrite(TABLE_CONSERVATIVE, largestFirstPartitionCoarse);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(TS));
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(TS));
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(TS));

        // next sweep goes into a new coarse partition
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertNoEntriesInSweepableTimestampsBeforeImmutableTimestamp();
    }

    @Test
    public void sweepableTimestampsGetsScrubbedWhenLastSweptProgressesInNewCoarsePartition2() {
        long tsSecondPartitionFine = TS + TS_FINE_GRANULARITY;
        long largestFirstPartitionCoarse = TS_COARSE_GRANULARITY - 1L;
        long thirdPartitionCoarse = 2 * TS_COARSE_GRANULARITY;
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        enqueueWrite(TABLE_CONSERVATIVE, tsSecondPartitionFine);
        enqueueWrite(TABLE_CONSERVATIVE, largestFirstPartitionCoarse);
        enqueueWrite(TABLE_CONSERVATIVE, thirdPartitionCoarse);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(TS));
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(TS));
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(TS));

        // next sweep goes into a new coarse partition
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(thirdPartitionCoarse));
    }

    @Test
    public void sweepableCellsGetsScrubbedAfterEachBatch() {
        long tsSecondPartitionFine = TS + TS_FINE_GRANULARITY;
        long largestBeforeSweepTs = provider.getSweepTimestamp(Sweeper.CONSERVATIVE) - 1L;
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        enqueueWrite(TABLE_CONSERVATIVE, TS + 1L);
        enqueueWrite(TABLE_CONSERVATIVE, tsSecondPartitionFine);
        enqueueWrite(TABLE_CONSERVATIVE, largestBeforeSweepTs);

        assertSweepableCellsHasEntryForTimestamp(TS + 1);
        assertSweepableCellsHasEntryForTimestamp(tsSecondPartitionFine);
        assertSweepableCellsHasEntryForTimestamp(largestBeforeSweepTs);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSweepableCellsHasNoEntriesBeforeTimestamp(TS + 1);
        assertSweepableCellsHasEntryForTimestamp(tsSecondPartitionFine);
        assertSweepableCellsHasEntryForTimestamp(largestBeforeSweepTs);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSweepableCellsHasNoEntriesBeforeTimestamp(TS + 1);
        assertSweepableCellsHasNoEntriesBeforeTimestamp(tsSecondPartitionFine);
        assertSweepableCellsHasEntryForTimestamp(largestBeforeSweepTs);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSweepableCellsHasNoEntriesBeforeTimestamp(TS + 1);
        assertSweepableCellsHasNoEntriesBeforeTimestamp(tsSecondPartitionFine);
        assertSweepableCellsHasNoEntriesBeforeTimestamp(largestBeforeSweepTs);
    }

    private void assertSweepableCellsHasEntryForTimestamp(long timestamp) {
        SweepBatch batch = sweepQueue.tables.sweepableCells.getWritesFromPartition(
                ShardAndStrategy.conservative(CONS_SHARD), tsPartitionFine(timestamp), -1L, timestamp + 1);
        assertThat(batch.writes()).containsExactly(WriteInfo.write(TABLE_CONSERVATIVE, DEFAULT_CELL, timestamp));
    }

    private void assertSweepableCellsHasNoEntriesBeforeTimestamp(long timestamp) {
        SweepBatch batch = sweepQueue.tables.sweepableCells.getWritesFromPartition(
                ShardAndStrategy.conservative(CONS_SHARD), tsPartitionFine(timestamp), -1L, timestamp + 1);
                assertThat(batch.writes()).isEmpty();
    }

    private void enqueueWrite(TableReference tableRef, long ts) {
        sweepQueue.enqueue(writeToDefaultCell(tableRef, ts), ts);
    }

    private void enqueueTombstone(TableReference tableRef, long ts) {
        sweepQueue.enqueue(tombstoneToDefaultCell(tableRef, ts), ts);
    }

    private Map<TableReference, ? extends Map<Cell, byte[]>> writeToDefaultCell(TableReference tableRef, long ts) {
        Map<Cell, byte[]> singleWrite = ImmutableMap.of(DEFAULT_CELL, PtBytes.toBytes(ts));
        kvs.put(tableRef, singleWrite, ts);
        return ImmutableMap.of(tableRef, singleWrite);
    }

    private Map<TableReference, ? extends Map<Cell, byte[]>> tombstoneToDefaultCell(TableReference tableRef, long ts) {
        Map<Cell, byte[]> singleWrite = ImmutableMap.of(DEFAULT_CELL, PtBytes.EMPTY_BYTE_ARRAY);
        kvs.put(tableRef, singleWrite, ts);
        return ImmutableMap.of(tableRef, singleWrite);
    }

    private void assertReadAtTimestampReturnsValue(TableReference tableRef, long readTs, long value) {
        assertThat(readValueFromDefaultCell(tableRef, readTs)).isEqualTo(value);
    }

    private long readValueFromDefaultCell(TableReference tableRef, long ts) {
        return PtBytes.toLong(Iterables.getOnlyElement(readFromDefaultCell(tableRef, ts).values()).getContents());
    }

    private void assertReadAtTimestampReturnsSentinel(TableReference tableRef, long readTs) {
        assertReadAtTimestampReturnsTombstoneAtTimestamp(tableRef, readTs, -1L);
    }

    private void assertReadAtTimestampReturnsTombstoneAtTimestamp(TableReference tableRef, long readTs, long tombTs) {
        Value readValue = Iterables.getOnlyElement(readFromDefaultCell(tableRef, readTs).values());
        assertThat(readValue.getTimestamp()).isEqualTo(tombTs);
        assertThat(readValue.getContents()).isEmpty();
    }

    private void assertReadAtTimestampReturnsNothing(TableReference tableRef, long readTs) {
        assertThat(readFromDefaultCell(tableRef, readTs)).isEmpty();
    }

    private Map<Cell, Value> readFromDefaultCell(TableReference tableRef, long ts) {
        Map<Cell, Long> singleRead = ImmutableMap.of(DEFAULT_CELL, ts);
        return kvs.get(tableRef, singleRead);
    }

    private void assertProgressUpdatedToFineTimestampPartition(long partitionFine) {
        assertThat(sweepQueue.tables.progress.getLastSweptTimestamp(ShardAndStrategy.conservative(CONS_SHARD)))
                .isEqualTo(partitionFine);
    }

    private void assertLowestFinePartitionInSweepableTimestampsEquals(long partitionFine) {
        assertThat(sweepQueue.tables.sweepableTimestamps
                .nextSweepableTimestampPartition(ShardAndStrategy.conservative(CONS_SHARD), -1L, immutableTs))
                .contains(partitionFine);
    }

    private void assertNoEntriesInSweepableTimestampsBeforeImmutableTimestamp() {
        assertThat(sweepQueue.tables.sweepableTimestamps
                .nextSweepableTimestampPartition(ShardAndStrategy.conservative(CONS_SHARD), -1L, immutableTs))
                .isEmpty();
    }
}
