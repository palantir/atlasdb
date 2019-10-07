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
package com.palantir.atlasdb.sweep.metrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static com.palantir.atlasdb.sweep.metrics.SweepMetricsAssert.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;

public class TargetedSweepMetricsTest {
    private static final long RECOMPUTE_MILLIS = 10;
    private static final ShardAndStrategy CONS_ZERO = ShardAndStrategy.conservative(0);
    private static final ShardAndStrategy CONS_ONE = ShardAndStrategy.conservative(1);
    private static final ShardAndStrategy CONS_TWO = ShardAndStrategy.conservative(2);
    private static final ShardAndStrategy THOR_ZERO = ShardAndStrategy.thorough(0);

    private MetricsManager metricsManager;
    private long clockTime;
    private KeyValueService kvs;
    private PuncherStore puncherStore;
    private TargetedSweepMetrics metrics;

    @Before
    public void setup() {
        clockTime = 100;
        kvs = Mockito.spy(new InMemoryKeyValueService(true));
        puncherStore = KeyValueServicePuncherStore.create(kvs, false);
        metricsManager = MetricsManagers.createForTests();
        metrics = TargetedSweepMetrics.createWithClock(metricsManager, kvs, () -> clockTime, RECOMPUTE_MILLIS);
    }

    @Test
    public void initialMetricsAreNormalized() {
        assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(0);
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(0);
        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(0);
        assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(0);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(null);
        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(null);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(null);
        assertThat(metricsManager).containsEntriesReadInBatchConservative();
        assertThat(metricsManager).hasEntriesReadInBatchMeanConservativeEqualTo(0.0);
    }

    @Test
    public void canUpdateConservativeMetrics() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 10);
        metrics.updateEntriesRead(CONS_ZERO, 21);
        metrics.updateNumberOfTombstones(CONS_ZERO, 1);
        metrics.updateAbortedWritesDeleted(CONS_ZERO, 2);
        metrics.updateSweepTimestamp(CONS_ZERO, 7);
        metrics.updateProgressForShard(CONS_ZERO, 4);
        metrics.registerEntriesReadInBatch(CONS_ZERO, 100);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(10);
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(21);
        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(1);
        assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(2);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(7L);
        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(4L);
        assertThat(metricsManager).containsEntriesReadInBatchConservative(100L);
        assertThat(metricsManager).hasEntriesReadInBatchMeanConservativeEqualTo(100.0);

        puncherStore.put(3, 2);
        puncherStore.put(4, 15);
        puncherStore.put(5, 40);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 15);
    }

    @Test
    public void canUpdateThoroughMetrics() {
        metrics.updateEnqueuedWrites(THOR_ZERO, 11);
        metrics.updateEntriesRead(THOR_ZERO, 30);
        metrics.updateNumberOfTombstones(THOR_ZERO, 2);
        metrics.updateAbortedWritesDeleted(THOR_ZERO, 3);
        metrics.updateSweepTimestamp(THOR_ZERO, 9);
        metrics.updateProgressForShard(THOR_ZERO, 6);
        metrics.registerEntriesReadInBatch(THOR_ZERO, 50);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasEnqueuedWritesThoroughEqualTo(11);
        assertThat(metricsManager).hasEntriesReadThoroughEqualTo(30);
        assertThat(metricsManager).hasTombstonesPutThoroughEqualTo(2);
        assertThat(metricsManager).hasAbortedWritesDeletedThoroughEqualTo(3);
        assertThat(metricsManager).hasSweepTimestampThoroughEqualTo(9);
        assertThat(metricsManager).hasLastSweptTimestampThoroughEqualTo(6);
        assertThat(metricsManager).containsEntriesReadInBatchThorough(50L);
        assertThat(metricsManager).hasEntriesReadInBatchMeanThoroughEqualTo(50.0);

        puncherStore.put(5, 1);
        puncherStore.put(6, 9);
        puncherStore.put(7, 16);
        assertThat(metricsManager).hasMillisSinceLastSweptThoroughEqualTo(clockTime - 9);
    }

    @Test
    public void enqueuedWritesAccumulatesOverShards() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateEnqueuedWrites(CONS_ONE, 3);
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);

        assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(5);
    }

    @Test
    public void entriesReadAccumulatesOverShards() {
        metrics.updateEntriesRead(CONS_ZERO, 1);
        metrics.updateEntriesRead(CONS_ONE, 4);
        metrics.updateEntriesRead(CONS_ZERO, 1);

        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(6);
    }

    @Test
    public void numberOfTombstonesAccumulatesOverShards() {
        metrics.updateNumberOfTombstones(CONS_ZERO, 1);
        metrics.updateNumberOfTombstones(CONS_TWO, 1);
        metrics.updateNumberOfTombstones(CONS_ZERO, 1);

        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(3);
    }

    @Test
    public void abortedWritesDeletedAccumulatesOverShards() {
        metrics.updateAbortedWritesDeleted(CONS_ZERO, 1);
        metrics.updateAbortedWritesDeleted(CONS_ONE, 2);
        metrics.updateAbortedWritesDeleted(CONS_ZERO, 1);

        assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(4);
    }

    @Test
    public void sweepTimestampGetsLastValueOverShards() {
        metrics.updateSweepTimestamp(CONS_ZERO, 1);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(1L);

        metrics.updateSweepTimestamp(CONS_ONE, 5);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(5L);

        metrics.updateSweepTimestamp(CONS_ZERO, 3);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(3L);
    }

    @Test
    public void lastSweptGetsMinAcrossShards() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateProgressForShard(CONS_ZERO, 100);
        metrics.updateProgressForShard(CONS_ONE, 1);
        metrics.updateProgressForShard(CONS_TWO, 1000);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(1L);

        puncherStore.put(0, 5);
        puncherStore.put(2, 500);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 5);
    }

    @Test
    public void millisSinceLastSweptUpdatesAsClockUpdatesAfterWaiting() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateProgressForShard(CONS_ZERO, 100);
        puncherStore.put(0, 50);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 50);

        for (int i = 0; i < 10; i++) {
            clockTime = 100 + i;
            waitForProgressToRecompute();
            assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 50);
        }
    }

    @Test
    public void secondMetricsInstanceUsesSameMetrics() {
        TargetedSweepMetrics secondMetrics = TargetedSweepMetrics
                .createWithClock(metricsManager, kvs, () -> clockTime, RECOMPUTE_MILLIS);

        metrics.updateEnqueuedWrites(CONS_ZERO, 10);
        metrics.updateEntriesRead(CONS_ZERO, 21);
        metrics.updateNumberOfTombstones(CONS_ZERO, 1);
        metrics.updateAbortedWritesDeleted(CONS_ZERO, 2);
        metrics.updateSweepTimestamp(CONS_ZERO, 7);
        metrics.registerEntriesReadInBatch(CONS_ZERO, 20);

        assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(10);
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(21);
        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(1);
        assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(2);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(7L);
        assertThat(metricsManager).containsEntriesReadInBatchConservative(20L);

        secondMetrics.updateEnqueuedWrites(CONS_ZERO, 5);
        secondMetrics.updateEntriesRead(CONS_ZERO, 5);
        secondMetrics.updateNumberOfTombstones(CONS_ZERO, 5);
        secondMetrics.updateAbortedWritesDeleted(CONS_ZERO, 5);
        secondMetrics.updateSweepTimestamp(CONS_ZERO, 5);
        secondMetrics.registerEntriesReadInBatch(CONS_ZERO, 15);

        assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(10 + 5);
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(21 + 5);
        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(1 + 5);
        assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(2 + 5);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(5L);
        assertThat(metricsManager).containsEntriesReadInBatchConservative(20L, 15L);
    }

    @Test
    public void writeTimestampsAreSharedAcrossMetricsInstances() {
        TargetedSweepMetrics secondMetrics = TargetedSweepMetrics
                .createWithClock(metricsManager, kvs, () -> clockTime, RECOMPUTE_MILLIS);

        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        secondMetrics.updateProgressForShard(CONS_ZERO, 100);

        puncherStore.put(0, 50);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 50);

        waitForProgressToRecompute();
        clockTime += 1;
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 50);
    }

    @Test
    public void millisSinceLastSweptDoesNotUpdateWithoutWaiting() {
        metricsManager = MetricsManagers.createForTests();
        metrics = TargetedSweepMetrics.createWithClock(metricsManager, kvs, () -> clockTime, 1_000);

        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateProgressForShard(CONS_ZERO, 100);
        puncherStore.put(0, 50);

        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(50L);

        waitForProgressToRecompute();
        clockTime += 1;
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(50L);
        clockTime += 100;
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(50L);

        clockTime += 1000; // clock is now 1201
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(1151L);
    }

    @Test
    public void millisSinceLastSweptReadsPuncherAgainAfterWaiting() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateProgressForShard(CONS_ZERO, 10);
        puncherStore.put(0, 5);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 5);
        puncherStore.put(1, 10);
        waitForProgressToRecompute();
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 10);
    }

    @Test
    public void millisSinceLastSweptDoesNotRangeScanForGivenTimestampIfSweepTsTooFarInThePast() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateProgressForShard(CONS_ZERO, 10);

        // there was a greater timestamp than sweep progress punched more than a week ago
        clockTime = TimeUnit.DAYS.toMillis(14L);
        puncherStore.put(15, 1);

        // return the time from a week ago and only range scan for looking up the timestamp for the time a week ago
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - TimeUnit.DAYS.toMillis(7L));
        verify(kvs, times(1)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), eq(Long.MAX_VALUE));
        verify(kvs, times(1)).getRange(eq(AtlasDbConstants.PUNCH_TABLE), any(RangeRequest.class), anyLong());
    }

    @Test
    public void lastSweptGoesDownIfNewInformationBecomesAvailable() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateProgressForShard(CONS_ZERO, 9);
        waitForProgressToRecompute();
        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(9L);
        puncherStore.put(9, 9);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 9);

        metrics.updateProgressForShard(CONS_ONE, 2);
        waitForProgressToRecompute();
        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(2L);
        puncherStore.put(2, 2);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 2);
    }

    @Test
    public void lastSweptIncreasesWhenSmallestShardIncreases() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateProgressForShard(CONS_ZERO, 10);
        metrics.updateProgressForShard(CONS_ONE, 1);
        metrics.updateProgressForShard(CONS_TWO, 1000);
        waitForProgressToRecompute();

        metrics.updateProgressForShard(CONS_ONE, 15);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(10L);
        puncherStore.put(1, 1);
        puncherStore.put(10, 7);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 7);
    }

    @Test
    public void lastSweptDoesNotGetConfusedWhenMultipleShardsHaveSameValue() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateProgressForShard(CONS_ZERO, 10);
        metrics.updateProgressForShard(CONS_ONE, 10);
        metrics.updateProgressForShard(CONS_TWO, 10);
        waitForProgressToRecompute();

        metrics.updateProgressForShard(CONS_ZERO, 30);
        metrics.updateProgressForShard(CONS_TWO, 50);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(10L);
        puncherStore.put(10, 10);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 10);

        metrics.updateProgressForShard(CONS_ONE, 40);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(30L);
        puncherStore.put(30, 30);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 30);
    }

    @Test
    public void enqueuedWritesDoesNotClashAcrossStrategies() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateEnqueuedWrites(THOR_ZERO, 10);
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);

        assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(2);
        assertThat(metricsManager).hasEnqueuedWritesThoroughEqualTo(10);
    }

    @Test
    public void numberOfTombstonesDoesNotClashAcrossStrategies() {
        metrics.updateNumberOfTombstones(CONS_ONE, 1);
        metrics.updateNumberOfTombstones(THOR_ZERO, 10);
        metrics.updateNumberOfTombstones(CONS_TWO, 2);

        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(3);
        assertThat(metricsManager).hasTombstonesPutThoroughEqualTo(10);
    }

    @Test
    public void abortedWritesDeletedDoesNotClashAcrossStrategies() {
        metrics.updateAbortedWritesDeleted(CONS_ONE, 10);
        metrics.updateAbortedWritesDeleted(THOR_ZERO, 5);
        metrics.updateAbortedWritesDeleted(CONS_TWO, 20);

        assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(30);
        assertThat(metricsManager).hasAbortedWritesDeletedThoroughEqualTo(5);
    }

    @Test
    public void sweepTimestampDoesNotClashAcrossStrategies() {
        metrics.updateSweepTimestamp(CONS_ZERO, 1);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(1L);

        metrics.updateSweepTimestamp(THOR_ZERO, 5);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(1L);
        assertThat(metricsManager).hasSweepTimestampThoroughEqualTo(5);

        metrics.updateSweepTimestamp(CONS_ZERO, 3);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(3L);
        assertThat(metricsManager).hasSweepTimestampThoroughEqualTo(5);
    }

    @Test
    public void lastSweptDoesNotClashAcrossStrategies() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateEnqueuedWrites(THOR_ZERO, 1);
        metrics.updateProgressForShard(CONS_ZERO, 1);
        metrics.updateProgressForShard(THOR_ZERO, 50);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(1L);
        assertThat(metricsManager).hasLastSweptTimestampThoroughEqualTo(50);
        puncherStore.put(1, 1);
        puncherStore.put(50, 50);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 1);
        assertThat(metricsManager).hasMillisSinceLastSweptThoroughEqualTo(clockTime - 50);

        metrics.updateProgressForShard(CONS_ZERO, 10);
        metrics.updateProgressForShard(CONS_ONE, 5);
        metrics.updateProgressForShard(THOR_ZERO, 5);
        waitForProgressToRecompute();

        puncherStore.put(5, 5);
        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(5L);
        assertThat(metricsManager).hasLastSweptTimestampThoroughEqualTo(5);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 5);
        assertThat(metricsManager).hasMillisSinceLastSweptThoroughEqualTo(clockTime - 5);
    }

    @Test
    public void entriesReadInBatchAccumulatesAcrossShards() {
        metrics.registerEntriesReadInBatch(CONS_ZERO, 5);
        metrics.registerEntriesReadInBatch(CONS_ZERO, 10);
        metrics.registerEntriesReadInBatch(CONS_ONE, 15);

        assertThat(metricsManager).containsEntriesReadInBatchConservative(5L, 10L, 15L);
        assertThat(metricsManager).hasEntriesReadInBatchMeanConservativeEqualTo(10.0);
    }

    @Test
    public void entriesReadInBatchDoesNotClashAcrossStrategies() {
        metrics.registerEntriesReadInBatch(CONS_ZERO, 5);
        metrics.registerEntriesReadInBatch(CONS_ZERO, 10);
        metrics.registerEntriesReadInBatch(THOR_ZERO, 15);
        metrics.registerEntriesReadInBatch(THOR_ZERO, 25);

        assertThat(metricsManager).containsEntriesReadInBatchConservative(5L, 10L);
        assertThat(metricsManager).containsEntriesReadInBatchThorough(15L, 25L);
        assertThat(metricsManager).hasEntriesReadInBatchMeanConservativeEqualTo(7.5);
        assertThat(metricsManager).hasEntriesReadInBatchMeanThoroughEqualTo(20.0);
    }

    private static void waitForProgressToRecompute() {
        try {
            Thread.sleep(RECOMPUTE_MILLIS + 1);
        } catch (InterruptedException e) {
            throw new RuntimeException("Sad times");
        }
    }
}
