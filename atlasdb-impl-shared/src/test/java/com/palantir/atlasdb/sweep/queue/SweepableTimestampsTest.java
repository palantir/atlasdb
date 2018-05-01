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
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionCoarse;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.sweep.Sweeper;

public class SweepableTimestampsTest extends SweepQueueTablesTest {
    private SweepableTimestamps sweepableTimestamps;
    private SweepTimestampProvider provider;
    private KvsSweepQueueProgress progress;

    private int shard;
    private int shard2;
    private long immutableTs;
    private long unreadableTs;

    @Before
    @Override
    public void setup() {
        super.setup();
        provider = new SweepTimestampProvider(() -> unreadableTs, () -> immutableTs);
        progress = new KvsSweepQueueProgress(kvs);
        sweepableTimestamps = new SweepableTimestamps(kvs, partitioner, progress);

        shard = writeToDefault(sweepableTimestamps, TS, true);
        shard2 = writeToDefault(sweepableTimestamps, TS2, false);

        immutableTs = 10 * TS;
        unreadableTs = 10 * TS;
    }

    @Test
    public void canReadNextTimestampWhenSweepTsIsLarge() {
        assertThat(readConservative(shard)).contains(TS_REF);
        assertThat(readThorough(shard2)).contains(TS2_REF);
    }

    @Test
    public void cannotReadForWrongSweepStrategy() {
        assertThat(readThorough(shard)).isEmpty();
        assertThat(readConservative(shard2)).isEmpty();
    }

    @Test
    public void cannotReadForWrongShard() {
        assertThat(readConservative(shard + 1)).isEmpty();
        assertThat(readThorough(shard2 + 1)).isEmpty();
    }

    @Test
    public void noNextTimestampWhenImmutableTsInSmallerPartitionForEitherSweepStrategy() {
        immutableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(immutableTs)).isLessThan(tsPartitionFine(TS));
        assertThat(readConservative(shard)).isEmpty();

        assertThat(tsPartitionFine(immutableTs)).isLessThan(tsPartitionFine(TS2));
        assertThat(readThorough(shard2)).isEmpty();
    }

    @Test
    public void noNextTimestampWhenUnreadableTsInSmallerPartitionForConservativeOnly() {
        unreadableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(tsPartitionFine(TS));
        assertThat(readConservative(shard)).isEmpty();

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(tsPartitionFine(TS2));
        assertThat(readThorough(shard2)).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampWhenSweepTimestampInSamePartitionAndGreater() {
        immutableTs = TS + TS_FINE_GRANULARITY - TS % TS_FINE_GRANULARITY - 1;

        assertThat(tsPartitionFine(immutableTs)).isEqualTo(tsPartitionFine(TS));
        assertThat(immutableTs).isGreaterThan(TS);
        assertThat(readThorough(shard)).isEmpty();
    }

    @Test
    public void canReadNextTimestampWhenSweepTimestampInNextPartitionAndSameBucket() {
        immutableTs = TS + TS_FINE_GRANULARITY - TS % TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(immutableTs)).isGreaterThan(tsPartitionFine(TS));
        assertThat(tsPartitionCoarse(immutableTs)).isEqualTo(tsPartitionCoarse(TS));
        assertThat(readConservative(shard)).contains(TS_REF);
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForConservative() {
        progress.updateLastSweptTimestampPartition(thorough(shard), TS_REF);
        assertThat(readConservative(shard)).contains(TS_REF);

        progress.updateLastSweptTimestampPartition(conservative(shard), TS_REF - 1);
        assertThat(readConservative(shard)).contains(TS_REF);
        assertThat(readThorough(shard2)).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampIfProgressedBeyondForConservative() {
        progress.updateLastSweptTimestampPartition(conservative(shard), TS_REF);
        assertThat(readConservative(shard)).isEmpty();
        assertThat(readThorough(shard2)).contains(TS2_REF);
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForThorough() {
        progress.updateLastSweptTimestampPartition(conservative(shard2), TS2_REF);
        assertThat(readThorough(shard2)).contains(TS2_REF);

        progress.updateLastSweptTimestampPartition(thorough(shard), TS2_REF - 1);
        assertThat(readConservative(shard)).contains(TS_REF);
        assertThat(readThorough(shard2)).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampIfProgressedBeyondForThorough() {
        progress.updateLastSweptTimestampPartition(thorough(shard2), TS2_REF);
        assertThat(readConservative(shard)).contains(TS_REF);
        assertThat(readThorough(shard2)).isEmpty();
    }

    @Test
    public void getCorrectNextTimestampWhenMultipleCandidates() {
        for (long timestamp = 1000L; tsPartitionFine(timestamp) < 10L; timestamp += TS_FINE_GRANULARITY / 5) {
            writeToDefault(sweepableTimestamps, timestamp, true);
        }
        assertThat(readConservative(shard)).contains(tsPartitionFine(1000L));

        progress.updateLastSweptTimestampPartition(conservative(shard), 2L);
        assertThat(readConservative(shard)).contains(3L);

        immutableTs = 4 * TS_FINE_GRANULARITY;
        assertThat(readConservative(shard)).contains(3L);

        immutableTs = 4 * TS_FINE_GRANULARITY - 1;
        assertThat(readConservative(shard)).isEmpty();
    }

    private Optional<Long> readConservative(int shardNumber) {
        return sweepableTimestamps.nextSweepableTimestampPartition(conservative(shardNumber),
                provider.getSweepTimestamp(Sweeper.CONSERVATIVE));
    }

    private Optional<Long> readThorough(int shardNumber) {
        return sweepableTimestamps.nextSweepableTimestampPartition(thorough(shardNumber),
                provider.getSweepTimestamp(Sweeper.THOROUGH));
    }
}
