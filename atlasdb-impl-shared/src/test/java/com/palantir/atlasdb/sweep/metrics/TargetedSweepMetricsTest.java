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

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.tritium.metrics.registry.MetricName;

public class TargetedSweepMetricsTest {
    private static final ShardAndStrategy CONS_ZERO = ShardAndStrategy.conservative(0);
    private static final ShardAndStrategy CONS_ONE = ShardAndStrategy.conservative(1);
    private static final ShardAndStrategy CONS_TWO = ShardAndStrategy.conservative(2);
    private static final ShardAndStrategy THOR_ZERO = ShardAndStrategy.thorough(0);
    private TargetedSweepMetrics metrics;

    @Before
    public void setup() {
        metrics = TargetedSweepMetrics.withRecomputingInterval(1);
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
        assertLastSweptConservativeEquals(4);
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
        assertLastSweptThoroughEquals(6);
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

        assertLastSweptConservativeEquals(1);
    }

    @Test
    public void lastSweptGoesDownIfNewInformationBecomesAvailable() {
        metrics.updateProgressForShard(CONS_ZERO, 999);
        waitForProgressToRecompute();
        assertLastSweptConservativeEquals(999);

        metrics.updateProgressForShard(CONS_ONE, 200);
        waitForProgressToRecompute();
        assertLastSweptConservativeEquals(200);
    }

    @Test
    public void lastSweptIncreasesWhenSmallestShardIncreases() {
        metrics.updateProgressForShard(CONS_ZERO, 100);
        metrics.updateProgressForShard(CONS_ONE, 1);
        metrics.updateProgressForShard(CONS_TWO, 1000);
        waitForProgressToRecompute();

        metrics.updateProgressForShard(CONS_ONE, 150);
        waitForProgressToRecompute();

        assertLastSweptConservativeEquals(100);
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

        assertLastSweptConservativeEquals(10);

        metrics.updateProgressForShard(CONS_ONE, 40);
        waitForProgressToRecompute();

        assertLastSweptConservativeEquals(30);
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

        assertLastSweptConservativeEquals(1);
        assertLastSweptThoroughEquals(50);

        metrics.updateProgressForShard(CONS_ZERO, 10);
        metrics.updateProgressForShard(CONS_ONE, 5);
        metrics.updateProgressForShard(THOR_ZERO, 5);
        waitForProgressToRecompute();

        assertLastSweptConservativeEquals(5);
        assertLastSweptThoroughEquals(5);
    }

    public static void waitForProgressToRecompute() {
        try {
            Thread.sleep(2);
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

    public static void assertLastSweptConservativeEquals(long value) {
        assertThat(getGaugeConservative(AtlasDbMetricNames.SWEPT_TS).getValue()).isEqualTo(value);
    }

    public static Gauge<Long> getGaugeConservative(String name) {
        return getGauge(AtlasDbMetricNames.TAG_CONSERVATIVE, name);
    }

    public static void assertEnqueuedWritesThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.ENQUEUED_WRITES).getValue()).isEqualTo(value);
    }

    public static void assertEntriesReadThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.ENTRIES_READ).getValue()).isEqualTo(value);
    }

    public static void assertTombstonesPutThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.TOMBSTONES_PUT).getValue()).isEqualTo(value);
    }

    public static void assertAbortedWritesDeletedThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.ABORTED_WRITES_DELETED).getValue()).isEqualTo(value);
    }

    public static void assertSweepTimestampThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.SWEEP_TS).getValue()).isEqualTo(value);
    }

    public static void assertLastSweptThoroughEquals(long value) {
        assertThat(getGaugeThorough(AtlasDbMetricNames.SWEPT_TS).getValue()).isEqualTo(value);
    }

    public static Gauge<Long> getGaugeThorough(String name) {
        return getGauge(AtlasDbMetricNames.TAG_THOROUGH, name);
    }

    public static Gauge<Long> getGauge(String strategy, String name) {
        Map<String, String> tag = ImmutableMap.of(AtlasDbMetricNames.TAG_STRATEGY, strategy);
        MetricName metricName = MetricName.builder()
                .safeName(MetricRegistry.name(TargetedSweepMetrics.class, name))
                .safeTags(tag)
                .build();

        return (Gauge<Long>) AtlasDbMetrics.getTaggedMetricRegistry().getMetrics().get(metricName);
    }
}
