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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_COARSE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_FINE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.maxTsForFinePartition;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;

public class TargetedSweeperTest extends AbstractSweepQueueTest {
    private static final long LOW_TS = 10L;
    private static final long LOW_TS2 = 2 * LOW_TS;

    private TargetedSweeper sweepQueue = TargetedSweeper.createUninitialized(() -> true, () -> DEFAULT_SHARDS, 0, 0);
    private ShardProgress progress;
    private SweepableTimestamps sweepableTimestamps;
    private SweepableCells sweepableCells;

    @Before
    public void setup() {
        super.setup();
        sweepQueue.initialize(timestampsSupplier, spiedKvs);

        progress = new ShardProgress(spiedKvs);
        sweepableTimestamps = new SweepableTimestamps(spiedKvs, partitioner);
        sweepableCells = new SweepableCells(spiedKvs, partitioner);
    }

    @Test
    public void sweepStrategyNothingDoesNotPersistAnything() {
        enqueueWrite(TABLE_NOTH, LOW_TS);
        enqueueWrite(TABLE_NOTH, LOW_TS2);
        verify(spiedKvs, times(2)).put(eq(TABLE_NOTH), anyMap(), anyLong());
        verify(spiedKvs, times(2)).put(any(TableReference.class), anyMap(), anyLong());
    }

    @Test
    public void conservativeSweepAddsSentinelAndLeavesSingleValue() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        assertReadAtTimestampReturnsNothing(TABLE_CONS, LOW_TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS);
        assertReadAtTimestampReturnsValue(TABLE_CONS, LOW_TS + 1, LOW_TS);
    }

    @Test
    public void thoroughSweepDoesNotAddSentinelAndLeavesSingleValue() {
        enqueueWrite(TABLE_THOR, LOW_TS);
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS);
        assertReadAtTimestampReturnsValue(TABLE_THOR, LOW_TS + 1, LOW_TS);
    }

    @Test
    public void conservativeSweepDeletesLowerValue() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, LOW_TS2);
        assertReadAtTimestampReturnsValue(TABLE_CONS, LOW_TS + 1, LOW_TS);
        assertReadAtTimestampReturnsValue(TABLE_CONS, LOW_TS2 + 1, LOW_TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONS, LOW_TS2 + 1, LOW_TS2);
    }

    @Test
    public void thoroughSweepDeletesLowerValue() {
        enqueueWrite(TABLE_THOR, LOW_TS);
        enqueueWrite(TABLE_THOR, LOW_TS2);
        assertReadAtTimestampReturnsValue(TABLE_THOR, LOW_TS + 1, LOW_TS);
        assertReadAtTimestampReturnsValue(TABLE_THOR, LOW_TS2 + 1, LOW_TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_THOR, LOW_TS2 + 1, LOW_TS2);
    }

    @Test
    public void sweepDeletesAllButLatestWithSingleDeleteAllTimestamps() {
        long lastWriteTs = TS_FINE_GRANULARITY - 1;
        for (long i = 0; i <= lastWriteTs; i++) {
            enqueueWrite(TABLE_CONS, i);
        }
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, lastWriteTs);
        assertReadAtTimestampReturnsValue(TABLE_CONS, lastWriteTs + 1, lastWriteTs);
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
    }

    @Test
    public void onlySweepsOneBatchAtATime() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, LOW_TS2);
        enqueueWrite(TABLE_CONS, TS_FINE_GRANULARITY);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONS, LOW_TS2 + 1, LOW_TS2);
        assertReadAtTimestampReturnsValue(TABLE_CONS, TS_FINE_GRANULARITY + 1, TS_FINE_GRANULARITY);
    }

    @Test
    public void sweepDeletesWritesWhenTombstoneHasHigherTimestamp() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueTombstone(TABLE_CONS, LOW_TS2);
        assertReadAtTimestampReturnsValue(TABLE_CONS, LOW_TS + 1, LOW_TS);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONS, LOW_TS2 + 1, LOW_TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + 1);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONS, LOW_TS2 + 1, LOW_TS2);
    }

    @Test
    public void thoroughSweepDeletesTombstoneIfLatestWrite() {
        enqueueTombstone(TABLE_THOR, LOW_TS);
        enqueueTombstone(TABLE_THOR, LOW_TS2);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_THOR, LOW_TS + 1, LOW_TS);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_THOR, LOW_TS2 + 1, LOW_TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS + 1);
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS2 + 1);
    }

    @Test
    public void sweepDeletesTombstonesWhenWriteHasHigherTimestamp() {
        enqueueTombstone(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, LOW_TS2);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONS, LOW_TS + 1, LOW_TS);
        assertReadAtTimestampReturnsValue(TABLE_CONS, LOW_TS2 + 1, LOW_TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONS, LOW_TS2 + 1, LOW_TS2);
    }

    @Test
    public void sweepProgressesAndSkipsEmptyFinePartitions() {
        long tsFineTwo = LOW_TS + TS_FINE_GRANULARITY;
        long tsFineFour = LOW_TS + 3 * TS_FINE_GRANULARITY;
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, tsFineTwo);
        enqueueWrite(TABLE_CONS, tsFineFour);
        enqueueWrite(TABLE_CONS, tsFineFour + 1L);

        // first sweep effectively only writes a sentinel
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS);
        assertReadAtTimestampReturnsValue(TABLE_CONS, LOW_TS + 1, LOW_TS);

        // second sweep deletes first entry
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, tsFineTwo);
        assertReadAtTimestampReturnsValue(TABLE_CONS, tsFineTwo + 1, tsFineTwo);

        // third sweep deletes all but last entry
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, tsFineFour + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONS, tsFineFour + 2, tsFineFour + 1);
    }

    @Test
    public void sweepProgressesAcrossCoarsePartitions() {
        long tsCoarseTwo = LOW_TS + TS_FINE_GRANULARITY + TS_COARSE_GRANULARITY;
        long tsCoarseFour = LOW_TS + 3 * TS_COARSE_GRANULARITY;
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, tsCoarseTwo);
        enqueueWrite(TABLE_CONS, tsCoarseFour);
        enqueueWrite(TABLE_CONS, tsCoarseFour + 1L);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS);
        assertReadAtTimestampReturnsValue(TABLE_CONS, LOW_TS + 1, LOW_TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, tsCoarseTwo);
        assertReadAtTimestampReturnsValue(TABLE_CONS, tsCoarseTwo + 1, tsCoarseTwo);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, tsCoarseFour + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONS, tsCoarseFour + 2, tsCoarseFour + 1);
    }

    @Test
    public void sweepProgressesToJustBeforeSweepTsWhenNothingToSweep() {
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertProgressUpdatedToTimestamp(getSweepTsCons() - 1L);
    }

    @Test
    public void sweepProgressesToEndOfPartitionWhenFewValuesAndSweepTsLarge() {
        long writeTs = getSweepTsCons() - 3 * TS_FINE_GRANULARITY;
        enqueueWrite(TABLE_CONS, writeTs);
        enqueueWrite(TABLE_CONS, writeTs + 5);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertProgressUpdatedToTimestamp(maxTsForFinePartition(tsPartitionFine(writeTs)));

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertProgressUpdatedToTimestamp(getSweepTsCons() - 1L);
    }

    @Test
    public void sweepCellOnlyOnceWhenInLastPartitionBeforeSweepTs() {
        immutableTs = 2 * TS_COARSE_GRANULARITY - TS_FINE_GRANULARITY;
        verify(spiedKvs, never()).deleteAllTimestamps(any(TableReference.class), anyMap());

        enqueueWrite(TABLE_CONS, immutableTs - 1);
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
    }

    @Test
    public void sweepableTimestampsGetsScrubbedWhenNoMoreToSweepButSweepTsInNewCoarsePartition() {
        long tsSecondPartitionFine = LOW_TS + TS_FINE_GRANULARITY;
        long largestFirstPartitionCoarse = TS_COARSE_GRANULARITY - 1L;
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, tsSecondPartitionFine);
        enqueueWrite(TABLE_CONS, largestFirstPartitionCoarse);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(LOW_TS));
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(LOW_TS));

        // after this sweep we progress to sweepTsConservative - 1
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertNoEntriesInSweepableTimestampsBeforeSweepTimestamp();
    }

    @Test
    public void sweepableTimestampsGetsScrubbedWhenLastSweptProgressesInNewCoarsePartition2() {
        long tsSecondPartitionFine = LOW_TS + TS_FINE_GRANULARITY;
        long largestFirstPartitionCoarse = TS_COARSE_GRANULARITY - 1L;
        long thirdPartitionCoarse = 2 * TS_COARSE_GRANULARITY;
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, tsSecondPartitionFine);
        enqueueWrite(TABLE_CONS, largestFirstPartitionCoarse);
        enqueueWrite(TABLE_CONS, thirdPartitionCoarse);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(LOW_TS));
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(LOW_TS));

        // after this sweep we progress to thirdPartitionCoarse - 1
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertLowestFinePartitionInSweepableTimestampsEquals(tsPartitionFine(thirdPartitionCoarse));
    }

    @Test
    public void sweepableCellsGetsScrubbedWheneverLastSweptInNewPartition() {
        long tsSecondPartitionFine = LOW_TS + TS_FINE_GRANULARITY;
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, LOW_TS + 1L);
        enqueueWrite(TABLE_CONS, tsSecondPartitionFine);
        enqueueWrite(TABLE_CONS, getSweepTsCons());

        // last swept timestamp: TS_FINE_GRANULARITY - 1
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSweepableCellsHasEntryForTimestamp(LOW_TS + 1);
        assertSweepableCellsHasEntryForTimestamp(tsSecondPartitionFine);
        assertSweepableCellsHasEntryForTimestamp(getSweepTsCons());

        // last swept timestamp: 2 * TS_FINE_GRANULARITY - 1
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSweepableCellsHasNoEntriesBeforeTimestamp(LOW_TS + 1);
        assertSweepableCellsHasEntryForTimestamp(tsSecondPartitionFine);
        assertSweepableCellsHasEntryForTimestamp(getSweepTsCons());

        // last swept timestamp: largestBeforeSweepTs
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSweepableCellsHasNoEntriesBeforeTimestamp(LOW_TS + 1);
        assertSweepableCellsHasNoEntriesBeforeTimestamp(tsSecondPartitionFine);
        assertSweepableCellsHasEntryForTimestamp(getSweepTsCons());
    }

    private void enqueueWrite(TableReference tableRef, long ts) {
        putTimestampIntoTransactionTable(ts, ts);
        sweepQueue.enqueue(writeToDefaultCell(tableRef, ts), ts);
    }

    private void enqueueTombstone(TableReference tableRef, long ts) {
        putTimestampIntoTransactionTable(ts, ts);
        sweepQueue.enqueue(tombstoneToDefaultCell(tableRef, ts), ts);
    }

    private Map<TableReference, ? extends Map<Cell, byte[]>> writeToDefaultCell(TableReference tableRef, long ts) {
        return writeToCell(tableRef, ts, DEFAULT_CELL);
    }

    private Map<TableReference, ? extends Map<Cell, byte[]>> writeToCell(TableReference tableRef, long ts, Cell cell) {
        Map<Cell, byte[]> singleWrite = ImmutableMap.of(cell, PtBytes.toBytes(ts));
        spiedKvs.put(tableRef, singleWrite, ts);
        return ImmutableMap.of(tableRef, singleWrite);
    }

    private Map<TableReference, ? extends Map<Cell, byte[]>> tombstoneToDefaultCell(TableReference tableRef, long ts) {
        Map<Cell, byte[]> singleWrite = ImmutableMap.of(DEFAULT_CELL, PtBytes.EMPTY_BYTE_ARRAY);
        spiedKvs.put(tableRef, singleWrite, ts);
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

    private void assertSweepableCellsHasEntryForTimestamp(long timestamp) {
        SweepBatch batch = sweepableCells.getBatchForPartition(
                ShardAndStrategy.conservative(CONS_SHARD), tsPartitionFine(timestamp), -1L, timestamp + 1);
        assertThat(batch.writes()).containsExactly(WriteInfo.write(TABLE_CONS, DEFAULT_CELL, timestamp));
    }

    private void assertSweepableCellsHasNoEntriesBeforeTimestamp(long timestamp) {
        SweepBatch batch = sweepableCells.getBatchForPartition(
                ShardAndStrategy.conservative(CONS_SHARD), tsPartitionFine(timestamp), -1L, timestamp + 1);
        assertThat(batch.writes()).isEmpty();
    }

    private Map<Cell, Value> readFromDefaultCell(TableReference tableRef, long ts) {
        Map<Cell, Long> singleRead = ImmutableMap.of(DEFAULT_CELL, ts);
        return spiedKvs.get(tableRef, singleRead);
    }

    private void assertProgressUpdatedToTimestamp(long ts) {
        assertProgressUpdatedToTimestamp(ts, CONS_SHARD);
    }

    private void assertProgressUpdatedToTimestamp(long ts, int shard) {
        assertThat(progress.getLastSweptTimestamp(ShardAndStrategy.conservative(shard))).isEqualTo(ts);
    }


    private void assertLowestFinePartitionInSweepableTimestampsEquals(long partitionFine) {
        assertThat(sweepableTimestamps
                .nextSweepableTimestampPartition(ShardAndStrategy.conservative(CONS_SHARD), -1L, getSweepTsCons()))
                .contains(partitionFine);
    }

    private void assertNoEntriesInSweepableTimestampsBeforeSweepTimestamp() {
        assertThat(sweepableTimestamps
                .nextSweepableTimestampPartition(ShardAndStrategy.conservative(CONS_SHARD), -1L, getSweepTsCons()))
                .isEmpty();
    }
}
