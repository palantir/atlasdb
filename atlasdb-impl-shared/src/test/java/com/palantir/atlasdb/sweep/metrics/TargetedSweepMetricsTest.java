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

package com.palantir.atlasdb.sweep.metrics;

import static com.palantir.atlasdb.sweep.metrics.SweepMetricsAssert.assertThat;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
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

    private static final MetricsManager metricsManager = MetricsManagers.createForTests();
    private long clockTime;
    private KeyValueService kvs;
    private PuncherStore puncherStore;
    private TargetedSweepMetrics metrics;

    @Before
    public void setup() {
        clockTime = 100;
        kvs = new InMemoryKeyValueService(true);
        puncherStore = KeyValueServicePuncherStore.create(kvs, false);
        metrics = TargetedSweepMetrics.createWithClock(metricsManager, kvs, () -> clockTime, RECOMPUTE_MILLIS);
    }

    @Test
    public void canUpdateConservativeMetrics() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 10);
        metrics.updateEntriesRead(CONS_ZERO, 21);
        metrics.updateNumberOfTombstones(CONS_ZERO, 1);
        metrics.updateAbortedWritesDeleted(CONS_ZERO, 2);
        metrics.updateSweepTimestamp(CONS_ZERO, 7);
        metrics.updateProgressForShard(CONS_ZERO, 4);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasEnqueuedWritesConservativeEqualTo(10);
        assertThat(metricsManager).hasEntriesReadConservativeEqualTo(21);
        assertThat(metricsManager).hasTombstonesPutConservativeEqualTo(1);
        assertThat(metricsManager).hasAbortedWritesDeletedConservativeEquals(2);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(7);
        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(4);

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
        waitForProgressToRecompute();

        assertThat(metricsManager).hasEnqueuedWritesThoroughEqualTo(11);
        assertThat(metricsManager).hasEntriesReadThoroughEqualTo(30);
        assertThat(metricsManager).hasTombstonesPutThoroughEqualTo(2);
        assertThat(metricsManager).hasAbortedWritesDeletedThoroughEqualTo(3);
        assertThat(metricsManager).hasSweepTimestampThoroughEqualTo(9);
        assertThat(metricsManager).hasLastSweptTimestampThoroughEqualTo(6);

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
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(1);

        metrics.updateSweepTimestamp(CONS_ONE, 5);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(5);

        metrics.updateSweepTimestamp(CONS_ZERO, 3);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(3);
    }

    @Test
    public void lastSweptGetsMinAcrossShards() {
        metrics.updateProgressForShard(CONS_ZERO, 100);
        metrics.updateProgressForShard(CONS_ONE, 1);
        metrics.updateProgressForShard(CONS_TWO, 1000);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(1);

        puncherStore.put(0, 5);
        puncherStore.put(2, 500);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 5);
    }

    @Test
    public void millisSinceLastSweptUpdatesAsClockUpdatesAfterWaiting() {
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
    public void millisSinceLastSweptDoesNotUpdateWithoutWaiting() {
        metrics = TargetedSweepMetrics.createWithClock(metricsManager, kvs, () -> clockTime, 1_000_000);
        metrics.updateProgressForShard(CONS_ZERO, 100);

        puncherStore.put(0, 50);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(50L);

        clockTime += 1;
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(50L);

        clockTime += 100;
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(50L);
    }

    @Test
    public void millisSinceLastSweptReadsPuncherAgainAfterWaiting() {
        metrics.updateProgressForShard(CONS_ZERO, 10);
        puncherStore.put(0, 5);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 5);
        puncherStore.put(1, 10);
        waitForProgressToRecompute();
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 10);
    }

    @Test
    public void lastSweptGoesDownIfNewInformationBecomesAvailable() {
        metrics.updateProgressForShard(CONS_ZERO, 9);
        waitForProgressToRecompute();
        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(9);
        puncherStore.put(9, 9);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 9);

        metrics.updateProgressForShard(CONS_ONE, 2);
        waitForProgressToRecompute();
        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(2);
        puncherStore.put(2, 2);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 2);
    }

    @Test
    public void lastSweptIncreasesWhenSmallestShardIncreases() {
        metrics.updateProgressForShard(CONS_ZERO, 10);
        metrics.updateProgressForShard(CONS_ONE, 1);
        metrics.updateProgressForShard(CONS_TWO, 1000);
        waitForProgressToRecompute();

        metrics.updateProgressForShard(CONS_ONE, 15);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(10);
        puncherStore.put(1, 1);
        puncherStore.put(10, 7);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 7);
    }

    @Test
    public void lastSweptDoesNotGetConfusedWhenMultipleShardsHaveSameValue() {
        metrics.updateProgressForShard(CONS_ZERO, 10);
        metrics.updateProgressForShard(CONS_ONE, 10);
        metrics.updateProgressForShard(CONS_TWO, 10);
        waitForProgressToRecompute();

        metrics.updateProgressForShard(CONS_ZERO, 30);
        metrics.updateProgressForShard(CONS_TWO, 50);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(10);
        puncherStore.put(10, 10);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 10);

        metrics.updateProgressForShard(CONS_ONE, 40);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(30);
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
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(1);

        metrics.updateSweepTimestamp(THOR_ZERO, 5);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(1);
        assertThat(metricsManager).hasSweepTimestampThoroughEqualTo(5);

        metrics.updateSweepTimestamp(CONS_ZERO, 3);
        assertThat(metricsManager).hasSweepTimestampConservativeEqualTo(3);
        assertThat(metricsManager).hasSweepTimestampThoroughEqualTo(5);
    }

    @Test
    public void lastSweptDoesNotClashAcrossStrategies() {
        metrics.updateProgressForShard(CONS_ZERO, 1);
        metrics.updateProgressForShard(THOR_ZERO, 50);
        waitForProgressToRecompute();

        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(1);
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
        assertThat(metricsManager).hasLastSweptTimestampConservativeEqualTo(5);
        assertThat(metricsManager).hasLastSweptTimestampThoroughEqualTo(5);
        assertThat(metricsManager).hasMillisSinceLastSweptConservativeEqualTo(clockTime - 5);
        assertThat(metricsManager).hasMillisSinceLastSweptThoroughEqualTo(clockTime - 5);
    }

    private static void waitForProgressToRecompute() {
        try {
            Thread.sleep(RECOMPUTE_MILLIS + 1);
        } catch (InterruptedException e) {
            throw new RuntimeException("Sad times");
        }
    }
}
