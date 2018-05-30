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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_COARSE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_FINE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.maxTsForFinePartition;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;

import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetricsTest;
import com.palantir.exception.NotInitializedException;

public class TargetedSweeperTest extends AbstractSweepQueueTest {
    private static final long LOW_TS = 10L;
    private static final long LOW_TS2 = 2 * LOW_TS;
    private static final long LOW_TS3 = 3 * LOW_TS;

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
        sweepableCells = new SweepableCells(spiedKvs, partitioner, null);
    }

    @Test
    public void callingEnqueueAndSweepOnUnitializedSweeperThrows() {
        TargetedSweeper uninitializedSweeper = TargetedSweeper.createUninitialized(null, null, 0, 0);
        assertThatThrownBy(() -> uninitializedSweeper.enqueue(ImmutableList.of()))
                .isInstanceOf(NotInitializedException.class)
                .hasMessageContaining("Targeted Sweeper");
        assertThatThrownBy(() -> uninitializedSweeper.sweepNextBatch(ShardAndStrategy.conservative(0)))
                .isInstanceOf(NotInitializedException.class)
                .hasMessageContaining("Targeted Sweeper");
    }

    @Test
    public void initializingWithUninitializedKvsThrows() {
        KeyValueService uninitializedKvs = mock(KeyValueService.class);
        when(uninitializedKvs.isInitialized()).thenReturn(false);
        TargetedSweeper sweeper = TargetedSweeper.createUninitialized(null, null, 0, 0);
        assertThatThrownBy(() -> sweeper.initialize(null, uninitializedKvs))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void sweepStrategyNothingDoesNotPersistAnything() {
        enqueueWrite(TABLE_NOTH, LOW_TS);
        enqueueWrite(TABLE_NOTH, LOW_TS2);
        verify(spiedKvs, times(2)).put(eq(TABLE_NOTH), anyMap(), anyLong());
        verify(spiedKvs, times(2)).put(any(TableReference.class), anyMap(), anyLong());
    }

    @Test
    public void sweepStrategyNothingDoesNotUpdateMetrics() {
        enqueueWrite(TABLE_NOTH, LOW_TS);
        enqueueWrite(TABLE_NOTH, LOW_TS2);
        TargetedSweepMetricsTest.assertEnqueuedWritesConservativeEquals(0);
        TargetedSweepMetricsTest.assertEnqueuedWritesThoroughEquals(0);
    }

    @Test
    public void conservativeSweepAddsSentinelAndLeavesSingleValue() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        assertReadAtTimestampReturnsNothing(TABLE_CONS, LOW_TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS);
    }

    @Test
    public void sweepWithSingleEntryUpdatesMetrics() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));

        TargetedSweepMetricsTest.assertTombstonesPutConservativeEquals(1);
        TargetedSweepMetricsTest.assertLastSweptConservativeEquals(maxTsForFinePartition(0));
        TargetedSweepMetricsTest.assertSweepTimestampConservativeEquals(getSweepTsCons());
    }

    @Test
    public void thoroughSweepDoesNotAddSentinelAndLeavesSingleValue() {
        enqueueWrite(TABLE_THOR, LOW_TS);
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_THOR, LOW_TS);
    }

    @Test
    public void thoroughSweepDeletesExistingSentinel() {
        spiedKvs.addGarbageCollectionSentinelValues(TABLE_THOR, ImmutableList.of(DEFAULT_CELL));
        assertReadAtTimestampReturnsSentinel(TABLE_THOR, 0L);
        enqueueWrite(TABLE_THOR, 10L);
        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, 0L);
    }

    @Test
    public void conservativeSweepDeletesLowerValue() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, LOW_TS2);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS2);
    }

    @Test
    public void thoroughSweepDeletesLowerValue() {
        enqueueWrite(TABLE_THOR, LOW_TS);
        enqueueWrite(TABLE_THOR, LOW_TS2);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_THOR, LOW_TS);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_THOR, LOW_TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, LOW_TS + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_THOR, LOW_TS2);
    }

    @Test
    public void conservativeSweepDeletesAllButLatestWithSingleDeleteAllTimestamps() {
        long lastWriteTs = TS_FINE_GRANULARITY - 1;
        for (long i = 0; i <= lastWriteTs; i++) {
            enqueueWrite(TABLE_CONS, i);
        }
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, lastWriteTs);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, lastWriteTs);
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
    }

    @Test
    public void thoroughSweepDeletesAllButLatestWithSingleDeleteAllTimestampsIncludingSentinels() {
        long lastWriteTs = TS_FINE_GRANULARITY - 1;
        for (long i = 0; i <= lastWriteTs; i++) {
            enqueueWrite(TABLE_THOR, i);
        }
        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOR, lastWriteTs);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_THOR, lastWriteTs);
//        verify(spiedKvs, times(1)).deleteAllTimestampsAndSentinels(any(TableReference.class), anyMap());
    }

    @Test
    public void onlySweepsOneBatchAtATime() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, LOW_TS2);
        enqueueWrite(TABLE_CONS, TS_FINE_GRANULARITY);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS2);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, TS_FINE_GRANULARITY);
    }

    @Test
    public void sweepDeletesWritesWhenTombstoneHasHigherTimestamp() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueTombstone(TABLE_CONS, LOW_TS2);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS);
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
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS2);
    }

    @Test
    public void sweepHandlesSequencesOfDeletesAndReadditionsInOneShot() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueTombstone(TABLE_CONS, LOW_TS + 2);
        enqueueWrite(TABLE_CONS, LOW_TS + 4);
        enqueueTombstone(TABLE_CONS, LOW_TS + 6);
        enqueueWrite(TABLE_CONS, LOW_TS + 8);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        for (int i = 0; i < 10; i = i + 2) {
            assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS + i);
        }
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS + 8);
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(), any());

        TargetedSweepMetricsTest.assertTombstonesPutConservativeEquals(1);
        TargetedSweepMetricsTest.assertLastSweptConservativeEquals(maxTsForFinePartition(0));
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
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS);
        TargetedSweepMetricsTest.assertTombstonesPutConservativeEquals(1);

        // second sweep deletes first entry
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, tsFineTwo);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, tsFineTwo);
        TargetedSweepMetricsTest.assertTombstonesPutConservativeEquals(2);

        // third sweep deletes all but last entry
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, tsFineFour + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, tsFineFour + 1);
        TargetedSweepMetricsTest.assertTombstonesPutConservativeEquals(3);
        TargetedSweepMetricsTest.assertEntriesReadConservativeEquals(4);
        TargetedSweepMetricsTest.assertLastSweptConservativeEquals(maxTsForFinePartition(3));
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
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, tsCoarseTwo);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, tsCoarseTwo);

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
    public void sweepableTimestampsGetsScrubbedWhenLastSweptProgressesInNewCoarsePartition() {
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

    @Test
    public void doesNotSweepBeyondSweepTimestamp() {
        writeValuesAroundSweepTimestampAndSweepAndCheck(getSweepTsCons(), 1);
    }

    @Test
    public void doesNotTransitivelyRetainWritesFromBeforeSweepTimestamp() {
        long sweepTimestamp = getSweepTsCons();
        enqueueWrite(TABLE_CONS, sweepTimestamp - 10);
        enqueueTombstone(TABLE_CONS, sweepTimestamp - 5);
        enqueueWrite(TABLE_CONS, sweepTimestamp + 5);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, sweepTimestamp - 5);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONS, sweepTimestamp - 5 + 1, sweepTimestamp - 5);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, sweepTimestamp + 5);
        TargetedSweepMetricsTest.assertTombstonesPutConservativeEquals(1);
        TargetedSweepMetricsTest.assertLastSweptConservativeEquals(sweepTimestamp - 1);
    }

    @Test
    public void sweepsOnlySweepableSegmentOfFinePartitions() {
        long sweepTs = TS_FINE_GRANULARITY + 1337;
        runWithConservativeSweepTimestamp(() -> writeValuesAroundSweepTimestampAndSweepAndCheck(sweepTs, 1), sweepTs);
    }

    @Test
    public void sweepsOnlySweepableSegmentAcrossFinePartitionBoundary() {
        long sweepTs = TS_FINE_GRANULARITY + 7;

        // Need 2 because need to cross a partition boundary
        runWithConservativeSweepTimestamp(() -> writeValuesAroundSweepTimestampAndSweepAndCheck(sweepTs, 2), sweepTs);
    }

    @Test
    public void sweepsOnlySweepableSegmentAcrossCoarsePartitionBoundary() {
        long sweepTs = TS_COARSE_GRANULARITY + 7;

        // Need 2 because need to cross a partition boundary
        runWithConservativeSweepTimestamp(() -> writeValuesAroundSweepTimestampAndSweepAndCheck(sweepTs, 2), sweepTs);
    }

    @Test
    public void remembersProgressWhenSweepTimestampAdvances() {
        long baseSweepTs = getSweepTsCons();
        long oldPartitionTs = baseSweepTs - 5;
        long newPartitionFirstTs = baseSweepTs + 5;
        long newPartitionSecondTs = baseSweepTs + 10;

        enqueueWrite(TABLE_CONS, oldPartitionTs);
        enqueueWrite(TABLE_CONS, newPartitionFirstTs);
        enqueueWrite(TABLE_CONS, newPartitionSecondTs);

        assertReadAtTimestampReturnsNothing(TABLE_CONS, oldPartitionTs);

        runConservativeSweepAtTimestamp(baseSweepTs + 7);
        runConservativeSweepAtTimestamp(baseSweepTs + 7);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, oldPartitionTs + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, newPartitionFirstTs);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, newPartitionSecondTs);

        runConservativeSweepAtTimestamp(newPartitionSecondTs + 1);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, newPartitionSecondTs);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, newPartitionSecondTs);
    }

    @Test
    public void doesNotGoBackwardsEvenIfSweepTimestampRegressesWithinBucket() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, LOW_TS2);
        enqueueWrite(TABLE_CONS, LOW_TS3);

        runConservativeSweepAtTimestamp(LOW_TS2 + 5);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS2);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS2);
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
        assertProgressUpdatedToTimestamp(LOW_TS2 + 5 - 1);

        runConservativeSweepAtTimestamp(LOW_TS2 - 5);
        verify(spiedKvs, times(1)).deleteAllTimestamps(any(TableReference.class), anyMap());
        assertProgressUpdatedToTimestamp(LOW_TS2 + 5 - 1);
    }

    @Test
    public void canSweepAtMinimumTimeWithNoWrites() {
        runConservativeSweepAtTimestamp(Long.MIN_VALUE);
        assertProgressUpdatedToTimestamp(SweepQueueUtils.INITIAL_TIMESTAMP);
    }

    @Test
    public void canSweepAtMinimumTime() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, LOW_TS2);
        enqueueWrite(TABLE_CONS, LOW_TS3);

        runConservativeSweepAtTimestamp(Long.MIN_VALUE);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS);
        assertProgressUpdatedToTimestamp(SweepQueueUtils.INITIAL_TIMESTAMP);
    }

    @Test
    public void canSweepAtMaximumTime() {
        enqueueWrite(TABLE_CONS, LOW_TS);
        enqueueWrite(TABLE_CONS, LOW_TS2);
        enqueueWrite(TABLE_CONS, LOW_TS3);

        runConservativeSweepAtTimestamp(Long.MAX_VALUE);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, LOW_TS3);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, LOW_TS3);
    }

    @Test
    public void doesNotGoBackwardsEvenIfSweepTimestampRegressesAcrossBoundary() {
        long coarseBoundary = TS_COARSE_GRANULARITY;
        enqueueWrite(TABLE_CONS, coarseBoundary - 5);
        enqueueWrite(TABLE_CONS, coarseBoundary + 5);
        enqueueWrite(TABLE_CONS, coarseBoundary + 15);

        // Need 2 sweeps to get through the first coarse bucket
        runConservativeSweepAtTimestamp(coarseBoundary + 8);
        TargetedSweepMetricsTest.assertSweepTimestampConservativeEquals(coarseBoundary + 8);
        runConservativeSweepAtTimestamp(coarseBoundary + 8);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, coarseBoundary - 5 + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, coarseBoundary + 5);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, coarseBoundary + 15);

        // Now regresses (e.g. clock drift on unreadable)
        runConservativeSweepAtTimestamp(coarseBoundary - 3);
        TargetedSweepMetricsTest.assertSweepTimestampConservativeEquals(coarseBoundary - 3);

        // And advances again
        runConservativeSweepAtTimestamp(coarseBoundary + 18);
        TargetedSweepMetricsTest.assertSweepTimestampConservativeEquals(coarseBoundary + 18);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, coarseBoundary - 5 + 1);
        assertReadAtTimestampReturnsSentinel(TABLE_CONS, coarseBoundary + 5 + 1);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, coarseBoundary + 15);
    }

    @Test
    public void testSweepTimestampMetric() {
        unreadableTs = 17;
        immutableTs = 40;
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(0));
        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(0));

        TargetedSweepMetricsTest.assertSweepTimestampConservativeEquals(17);
        TargetedSweepMetricsTest.assertSweepTimestampThoroughEquals(40);

        immutableTs = 5;
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(0));
        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(0));

        TargetedSweepMetricsTest.assertSweepTimestampConservativeEquals(5);
        TargetedSweepMetricsTest.assertSweepTimestampThoroughEquals(5);
    }

    private void writeValuesAroundSweepTimestampAndSweepAndCheck(long sweepTimestamp, int sweepIterations) {
        enqueueWrite(TABLE_CONS, sweepTimestamp - 10);
        enqueueWrite(TABLE_CONS, sweepTimestamp - 5);
        enqueueWrite(TABLE_CONS, sweepTimestamp + 5);

        IntStream.range(0, sweepIterations)
                .forEach(unused -> sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD)));

        assertReadAtTimestampReturnsSentinel(TABLE_CONS, sweepTimestamp - 5);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, sweepTimestamp - 5);
        assertTestValueEnqueuedAtGivenTimestampStillPresent(TABLE_CONS, sweepTimestamp + 5);
    }

    private void runConservativeSweepAtTimestamp(long desiredSweepTimestamp) {
        runWithConservativeSweepTimestamp(() -> sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD)),
                desiredSweepTimestamp);
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

    private void assertTestValueEnqueuedAtGivenTimestampStillPresent(TableReference tableRef, long timestamp) {
        assertReadAtTimestampReturnsValue(tableRef, timestamp + 1, timestamp);
    }

    private long readValueFromDefaultCell(TableReference tableRef, long ts) {
        byte[] cellContents = Iterables.getOnlyElement(readFromDefaultCell(tableRef, ts).values()).getContents();
        assertThat(cellContents).as("Reading value from table %s at timestamp %s", tableRef, ts).isNotEmpty();
        return PtBytes.toLong(cellContents);
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
