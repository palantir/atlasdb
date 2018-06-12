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

import static org.assertj.core.api.Assertions.assertThat;

import static com.palantir.atlasdb.sweep.queue.SweepQueue.REFRESH_INTERVAL;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.tritium.metrics.registry.MetricName;

public class TargetedSweepMetricsTest {
    private static final long RECOMPUTE_MILLIS = 10;
    private static final ShardAndStrategy CONS_ZERO = ShardAndStrategy.conservative(0);
    private static final ShardAndStrategy CONS_ONE = ShardAndStrategy.conservative(1);
    private static final ShardAndStrategy CONS_TWO = ShardAndStrategy.conservative(2);
    private static final ShardAndStrategy THOR_ZERO = ShardAndStrategy.thorough(0);
    private PuncherStore puncherStore;
    private long clockTime;
    private TargetedSweepMetrics metrics;

    @Before
    public void setup() {
        clockTime = 100;
        KeyValueService kvs = new InMemoryKeyValueService(true);
        puncherStore = KeyValueServicePuncherStore.create(kvs, false);
        metrics = TargetedSweepMetrics.createWithClock(kvs, () -> clockTime, RECOMPUTE_MILLIS);
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

        assertEnqueuedWritesConservativeEquals(10);
        assertEntriesReadConservativeEquals(21);
        assertTombstonesPutConservativeEquals(1);
        assertAbortedWritesDeletedConservativeEquals(2);
        assertSweepTimestampConservativeEquals(7);
        assertLastSweptTimestampConservativeEquals(4);

        puncherStore.put(3, 2);
        puncherStore.put(4, 15);
        puncherStore.put(5, 40);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(15);
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

        assertEnqueuedWritesThoroughEquals(11);
        assertEntriesReadThoroughEquals(30);
        assertTombstonesPutThoroughEquals(2);
        assertAbortedWritesDeletedThoroughEquals(3);
        assertSweepTimestampThoroughEquals(9);
        assertLastSweptTimestampThoroughEquals(6);

        puncherStore.put(5, 1);
        puncherStore.put(6, 9);
        puncherStore.put(7, 16);
        assertMillisSinceLastSweptThoroughEqualsClockTimeMinus(9);
    }

    @Test
    public void enqueuedWritesAccumulatesOverShards() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateEnqueuedWrites(CONS_ONE, 3);
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);

        assertEnqueuedWritesConservativeEquals(5);
    }

    @Test
    public void entriesReadAccumulatesOverShards() {
        metrics.updateEntriesRead(CONS_ZERO, 1);
        metrics.updateEntriesRead(CONS_ONE, 4);
        metrics.updateEntriesRead(CONS_ZERO, 1);

        assertEntriesReadConservativeEquals(6);
    }

    @Test
    public void numberOfTombstonesAccumulatesOverShards() {
        metrics.updateNumberOfTombstones(CONS_ZERO, 1);
        metrics.updateNumberOfTombstones(CONS_TWO, 1);
        metrics.updateNumberOfTombstones(CONS_ZERO, 1);

        assertTombstonesPutConservativeEquals(3);
    }

    @Test
    public void abortedWritesDeletedAccumulatesOverShards() {
        metrics.updateAbortedWritesDeleted(CONS_ZERO, 1);
        metrics.updateAbortedWritesDeleted(CONS_ONE, 2);
        metrics.updateAbortedWritesDeleted(CONS_ZERO, 1);

        assertAbortedWritesDeletedConservativeEquals(4);
    }

    @Test
    public void sweepTimestampGetsLastValueOverShards() {
        metrics.updateSweepTimestamp(CONS_ZERO, 1);
        assertSweepTimestampConservativeEquals(1);

        metrics.updateSweepTimestamp(CONS_ONE, 5);
        assertSweepTimestampConservativeEquals(5);

        metrics.updateSweepTimestamp(CONS_ZERO, 3);
        assertSweepTimestampConservativeEquals(3);
    }

    @Test
    public void lastSweptGetsMinAcrossShards() {
        metrics.updateProgressForShard(CONS_ZERO, 100);
        metrics.updateProgressForShard(CONS_ONE, 1);
        metrics.updateProgressForShard(CONS_TWO, 1000);
        waitForProgressToRecompute();

        assertLastSweptTimestampConservativeEquals(1);

        puncherStore.put(0, 5);
        puncherStore.put(2, 500);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(5);
    }

    @Test
    public void millisSinceLastSweptUpdatesAsClockUpdates() {
        metrics.updateProgressForShard(CONS_ZERO, 100);
        waitForProgressToRecompute();

        puncherStore.put(0, 50);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(50);

        for (int i = 0; i < 10; i++) {
            clockTime = 100 + i;
            assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(50);
        }
    }

    @Test
    public void millisSinceLastSweptDoesNotReadPuncherAgainUntilLastSweptChanges() {
        metrics.updateProgressForShard(CONS_ZERO, 10);
        waitForProgressToRecompute();

        puncherStore.put(0, 5);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(5);
        puncherStore.put(1, 10);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(5);

        metrics.updateProgressForShard(CONS_ZERO, 11);
        waitForProgressToRecompute();
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(10);
    }

    @Test
    public void lastSweptGoesDownIfNewInformationBecomesAvailable() {
        metrics.updateProgressForShard(CONS_ZERO, 9);
        waitForProgressToRecompute();
        assertLastSweptTimestampConservativeEquals(9);
        puncherStore.put(9, 9);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(9);

        metrics.updateProgressForShard(CONS_ONE, 2);
        waitForProgressToRecompute();
        assertLastSweptTimestampConservativeEquals(2);
        puncherStore.put(2, 2);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(2);
    }

    @Test
    public void lastSweptIncreasesWhenSmallestShardIncreases() {
        metrics.updateProgressForShard(CONS_ZERO, 10);
        metrics.updateProgressForShard(CONS_ONE, 1);
        metrics.updateProgressForShard(CONS_TWO, 1000);
        waitForProgressToRecompute();

        metrics.updateProgressForShard(CONS_ONE, 15);
        waitForProgressToRecompute();

        assertLastSweptTimestampConservativeEquals(10);
        puncherStore.put(1, 1);
        puncherStore.put(10, 10);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(10);
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

        assertLastSweptTimestampConservativeEquals(10);
        puncherStore.put(10, 10);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(10);

        metrics.updateProgressForShard(CONS_ONE, 40);
        waitForProgressToRecompute();

        assertLastSweptTimestampConservativeEquals(30);
        puncherStore.put(30, 30);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(30);
    }


    @Test
    public void enqueuedWritesDoesNotClashAcrossStrategies() {
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);
        metrics.updateEnqueuedWrites(THOR_ZERO, 10);
        metrics.updateEnqueuedWrites(CONS_ZERO, 1);

        assertEnqueuedWritesConservativeEquals(2);
        assertEnqueuedWritesThoroughEquals(10);
    }

    @Test
    public void numberOfTombstonesDoesNotClashAcrossStrategies() {
        metrics.updateNumberOfTombstones(CONS_ONE, 1);
        metrics.updateNumberOfTombstones(THOR_ZERO, 10);
        metrics.updateNumberOfTombstones(CONS_TWO, 2);

        assertTombstonesPutConservativeEquals(3);
        assertTombstonesPutThoroughEquals(10);
    }

    @Test
    public void abortedWritesDeletedDoesNotClashAcrossStrategies() {
        metrics.updateAbortedWritesDeleted(CONS_ONE, 10);
        metrics.updateAbortedWritesDeleted(THOR_ZERO, 5);
        metrics.updateAbortedWritesDeleted(CONS_TWO, 20);

        assertAbortedWritesDeletedConservativeEquals(30);
        assertAbortedWritesDeletedThoroughEquals(5);
    }

    @Test
    public void sweepTimestampDoesNotClashAcrossStrategies() {
        metrics.updateSweepTimestamp(CONS_ZERO, 1);
        assertSweepTimestampConservativeEquals(1);

        metrics.updateSweepTimestamp(THOR_ZERO, 5);
        assertSweepTimestampConservativeEquals(1);
        assertSweepTimestampThoroughEquals(5);

        metrics.updateSweepTimestamp(CONS_ZERO, 3);
        assertSweepTimestampConservativeEquals(3);
        assertSweepTimestampThoroughEquals(5);
    }

    @Test
    public void lastSweptDoesNotClashAcrossStrategies() {
        metrics.updateProgressForShard(CONS_ZERO, 1);
        metrics.updateProgressForShard(THOR_ZERO, 50);
        waitForProgressToRecompute();

        assertLastSweptTimestampConservativeEquals(1);
        assertLastSweptTimestampThoroughEquals(50);
        puncherStore.put(1, 1);
        puncherStore.put(50, 50);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(1);
        assertMillisSinceLastSweptThoroughEqualsClockTimeMinus(50);

        metrics.updateProgressForShard(CONS_ZERO, 10);
        metrics.updateProgressForShard(CONS_ONE, 5);
        metrics.updateProgressForShard(THOR_ZERO, 5);
        waitForProgressToRecompute();

        puncherStore.put(5, 5);
        assertLastSweptTimestampConservativeEquals(5);
        assertLastSweptTimestampThoroughEquals(5);
        assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(5);
        assertMillisSinceLastSweptThoroughEqualsClockTimeMinus(5);
    }

    @Test
    public void millisSinceLastSweptAtLeastRecomputeMillis() {
        metrics.updateProgressForShard(CONS_ZERO, 1);
        waitForProgressToRecompute();
        puncherStore.put(1, clockTime - 2 * RECOMPUTE_MILLIS);

        assertMillisSinceLastSweptConservativeEquals(2 * RECOMPUTE_MILLIS);

        metrics.updateProgressForShard(CONS_ZERO, 2);
        waitForProgressToRecompute();
        puncherStore.put(2, clockTime - RECOMPUTE_MILLIS);

        assertMillisSinceLastSweptConservativeEquals(RECOMPUTE_MILLIS);

        metrics.updateProgressForShard(CONS_ZERO, 3);
        waitForProgressToRecompute();
        puncherStore.put(3, clockTime);

        assertMillisSinceLastSweptConservativeEquals(RECOMPUTE_MILLIS);
    }

    private static void waitForProgressToRecompute() {
        try {
            Thread.sleep(RECOMPUTE_MILLIS + 1);
        } catch (InterruptedException e) {
            throw new RuntimeException("Sad times");
        }
    }

    public static void assertEnqueuedWritesConservativeEquals(long value) {
        assertThat(getGaugeConservative(AtlasDbMetricNames.ENQUEUED_WRITES).getValue()).isEqualTo(value);
    }

    public static void assertEntriesReadConservativeEquals(long value) {
        assertThat(getGaugeConservative(AtlasDbMetricNames.ENTRIES_READ).getValue()).isEqualTo(value);
    }

    public static void assertTombstonesPutConservativeEquals(long value) {
        assertThat(getGaugeConservative(AtlasDbMetricNames.TOMBSTONES_PUT).getValue()).isEqualTo(value);
    }

    public static void assertAbortedWritesDeletedConservativeEquals(long value) {
        assertThat(getGaugeConservative(AtlasDbMetricNames.ABORTED_WRITES_DELETED).getValue()).isEqualTo(value);
    }

    public static void assertSweepTimestampConservativeEquals(long value) {
        assertThat(getGaugeConservative(AtlasDbMetricNames.SWEEP_TS).getValue()).isEqualTo(value);
    }

    public static void assertLastSweptTimestampConservativeEquals(long value) {
        assertThat(getGaugeConservative(AtlasDbMetricNames.LAST_SWEPT_TS).getValue()).isEqualTo(value);
    }

    private void assertMillisSinceLastSweptConservativeEqualsClockTimeMinus(long lastSwept) {
        assertMillisSinceLastSweptConservativeEquals(clockTime - lastSwept);
    }

    private void assertMillisSinceLastSweptConservativeEquals(long millis) {
        assertThat(getGaugeConservative(AtlasDbMetricNames.LAG_MILLIS).getValue()).isEqualTo(millis);
    }

    public static void assertMillisSinceLastSweptConservativeExactlyRefreshTime() {
        assertThat(getGaugeConservative(AtlasDbMetricNames.LAG_MILLIS).getValue()).isEqualTo(REFRESH_INTERVAL);
    }

    private static Gauge<Long> getGaugeConservative(String name) {
        return getGauge(AtlasDbMetricNames.TAG_CONSERVATIVE, name);
    }

    public static void assertEnqueuedWritesThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.ENQUEUED_WRITES).getValue()).isEqualTo(value);
    }

    private static void assertEntriesReadThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.ENTRIES_READ).getValue()).isEqualTo(value);
    }

    private static void assertTombstonesPutThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.TOMBSTONES_PUT).getValue()).isEqualTo(value);
    }

    private static void assertAbortedWritesDeletedThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.ABORTED_WRITES_DELETED).getValue()).isEqualTo(value);
    }

    public static void assertSweepTimestampThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.SWEEP_TS).getValue()).isEqualTo(value);
    }

    private static void assertLastSweptTimestampThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.LAST_SWEPT_TS).getValue()).isEqualTo(value);
    }

    private void assertMillisSinceLastSweptThoroughEqualsClockTimeMinus(long lastSwept) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.LAG_MILLIS).getValue()).isEqualTo(clockTime - lastSwept);
    }

    private static Gauge<Long> getGaugeThorough(String name) {
        return getGauge(AtlasDbMetricNames.TAG_THOROUGH, name);
    }

    private static Gauge<Long> getGauge(String strategy, String name) {
        Map<String, String> tag = ImmutableMap.of(AtlasDbMetricNames.TAG_STRATEGY, strategy);
        MetricName metricName = MetricName.builder()
                .safeName(MetricRegistry.name(TargetedSweepMetrics.class, name))
                .safeTags(tag)
                .build();

        return (Gauge<Long>) AtlasDbMetrics.getTaggedMetricRegistry().getMetrics().get(metricName);
    }
}
