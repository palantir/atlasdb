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
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_FINE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.maxTsForFinePartition;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.minTsForFinePartition;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.sweep.Sweeper;

public class SweepableTimestampsTest extends AbstractSweepQueueTablesTest {
    private SweepableTimestamps sweepableTimestamps;
    private SweepTimestampProvider provider;
    private KvsSweepQueueProgress progress;

    private int shardCons;
    private int shardThor;
    private long immutableTs;
    private long unreadableTs;

    @Before
    @Override
    public void setup() {
        super.setup();
        provider = new SweepTimestampProvider(() -> unreadableTs, () -> immutableTs);
        progress = new KvsSweepQueueProgress(kvs);
        sweepableTimestamps = new SweepableTimestamps(kvs, partitioner);

        shardCons = writeToDefault(sweepableTimestamps, TS, TABLE_CONS);
        shardThor = writeToDefault(sweepableTimestamps, TS2, TABLE_THOR);

        immutableTs = 10 * TS;
        unreadableTs = 10 * TS;
    }

    @Test
    public void canReadNextTimestampWhenSweepTsIsLarge() {
        assertThat(readConservative(shardCons)).contains(TS_FINE_PARTITION);
        assertThat(readThorough(shardThor)).contains(TS2_FINE_PARTITION);
    }

    @Test
    public void cannotReadForWrongSweepStrategy() {
        assertThat(readThorough(shardCons)).isEmpty();
        assertThat(readConservative(shardThor)).isEmpty();
    }

    @Test
    public void cannotReadForWrongShard() {
        assertThat(readConservative(shardCons + 1)).isEmpty();
        assertThat(readThorough(shardThor + 1)).isEmpty();
    }

    @Test
    public void noNextTimestampWhenImmutableTsInSmallerPartitionForEitherSweepStrategy() {
        immutableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(immutableTs)).isLessThan(TS_FINE_PARTITION);
        assertThat(readConservative(shardCons)).isEmpty();

        assertThat(tsPartitionFine(immutableTs)).isLessThan(TS2_FINE_PARTITION);
        assertThat(readThorough(shardThor)).isEmpty();
    }

    @Test
    public void noNextTimestampWhenUnreadableTsInSmallerPartitionForConservativeOnly() {
        unreadableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(TS_FINE_PARTITION);
        assertThat(readConservative(shardCons)).isEmpty();

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(TS2_FINE_PARTITION);
        assertThat(readThorough(shardThor)).contains(TS2_FINE_PARTITION);
    }

    @Test
    public void canReadNextTimestampWhenSweepTimestampInSamePartitionAndGreater() {
        long sweepTs = setSweepTimestampAndGet(maxTsForFinePartition(TS_FINE_PARTITION));

        assertThat(tsPartitionFine(sweepTs)).isEqualTo(TS_FINE_PARTITION);
        assertThat(sweepTs).isGreaterThan(TS);
        assertThat(readConservative(shardCons)).contains(TS_FINE_PARTITION);
    }

    @Test
    public void noNextTimestampWhenWhenSweepTimestampInSamePartitionAndLower() {
        long sweepTs = setSweepTimestampAndGet(minTsForFinePartition(TS_FINE_PARTITION));

        assertThat(tsPartitionFine(sweepTs)).isEqualTo(TS_FINE_PARTITION);
        assertThat(sweepTs).isLessThan(TS);
        assertThat(readConservative(shardCons)).isEmpty();
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForConservative() {
        progress.updateLastSweptTimestamp(conservative(shardCons), TS - 1);
        assertThat(readConservative(shardCons)).contains(TS_FINE_PARTITION);
    }

    @Test
    public void canReadNextTimestampIfProgressedBeyondButInSamePartitionForConservative() {
        progress.updateLastSweptTimestamp(conservative(shardCons), maxTsForFinePartition(TS_FINE_PARTITION) - 1);
        assertThat(readConservative(shardCons)).contains(TS_FINE_PARTITION);
    }

    @Test
    public void noNextTimestampIfProgressedToEndOfPartitionForConservative() {
        progress.updateLastSweptTimestamp(conservative(shardCons), maxTsForFinePartition(TS_FINE_PARTITION));
        assertThat(readConservative(shardCons)).isEmpty();
    }

    @Test
    public void canReadNextWhenOtherShardsAndStrategiesProgressToEndOfPartitionForConservative() {
        progress.updateLastSweptTimestamp(thorough(shardCons), maxTsForFinePartition(TS_FINE_PARTITION));
        progress.updateLastSweptTimestamp(conservative(shardThor), maxTsForFinePartition(TS_FINE_PARTITION));
        progress.updateLastSweptTimestamp(thorough(shardThor), maxTsForFinePartition(TS_FINE_PARTITION));
        assertThat(readConservative(shardCons)).contains(TS_FINE_PARTITION);
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForThorough() {
        progress.updateLastSweptTimestamp(thorough(shardThor), TS2 - 1);
        assertThat(readThorough(shardThor)).contains(TS2_FINE_PARTITION);
    }

    @Test
    public void canReadNextTimestampIfProgressedBeyondButInSamePartitionForForThorough() {
        progress.updateLastSweptTimestamp(thorough(shardThor), maxTsForFinePartition(TS2_FINE_PARTITION) - 1);
        assertThat(readThorough(shardThor)).contains(TS2_FINE_PARTITION);
    }

    @Test
    public void noNextTimestampIfProgressedToEndOfPartitionForThorough() {
        progress.updateLastSweptTimestamp(thorough(shardThor), maxTsForFinePartition(TS2_FINE_PARTITION));
        assertThat(readThorough(shardThor)).isEmpty();
    }

    @Test
    public void canReadNextWhenOtherShardsAndStrategiesProgressToEndOfPartitionForThorough() {
        progress.updateLastSweptTimestamp(thorough(shardCons), maxTsForFinePartition(TS2_FINE_PARTITION));
        progress.updateLastSweptTimestamp(conservative(shardThor), maxTsForFinePartition(TS2_FINE_PARTITION));
        progress.updateLastSweptTimestamp(conservative(shardCons), maxTsForFinePartition(TS2_FINE_PARTITION));
        assertThat(readThorough(shardThor)).contains(TS2_FINE_PARTITION);
    }

    @Test
    public void getCorrectNextTimestampWhenMultipleCandidates() {
        for (long timestamp = 1000L; tsPartitionFine(timestamp) < 10L; timestamp += TS_FINE_GRANULARITY / 5) {
            writeToDefault(sweepableTimestamps, timestamp, TABLE_CONS);
        }
        assertThat(readConservative(shardCons)).contains(tsPartitionFine(1000L));

        progress.updateLastSweptTimestamp(conservative(shardCons), 2L * TS_FINE_GRANULARITY);
        assertThat(readConservative(shardCons)).contains(tsPartitionFine(2L * TS_FINE_GRANULARITY + 1000L));

        setSweepTimestampAndGet(4 * TS_FINE_GRANULARITY);
        assertThat(readConservative(shardCons)).contains(tsPartitionFine(2L * TS_FINE_GRANULARITY + 1000L));
    }

    private Optional<Long> readConservative(int shardNumber) {
        return sweepableTimestamps.nextSweepableTimestampPartition(
                conservative(shardNumber),
                progress.getLastSweptTimestamp(ShardAndStrategy.conservative(shardNumber)),
                provider.getSweepTimestamp(Sweeper.CONSERVATIVE));
    }

    private Optional<Long> readThorough(int shardNumber) {
        return sweepableTimestamps.nextSweepableTimestampPartition(
                thorough(shardNumber),
                progress.getLastSweptTimestamp(ShardAndStrategy.thorough(shardNumber)),
                provider.getSweepTimestamp(Sweeper.THOROUGH));
    }

    private long setSweepTimestampAndGet(long timestamp) {
        immutableTs = timestamp;
        return provider.getSweepTimestamp(Sweeper.CONSERVATIVE);
    }
}
