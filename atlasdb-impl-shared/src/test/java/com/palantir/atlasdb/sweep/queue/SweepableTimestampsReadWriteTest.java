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

import org.junit.Before;
import org.junit.Test;

public class SweepableTimestampsReadWriteTest extends SweepQueueReadWriteTest{
    private SweepableTimestampsReader reader;
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
        writer = new SweepableTimestampsWriter(kvs, partitioner);
        reader = new SweepableTimestampsReader(kvs, provider);
        progress = new KvsSweepQueueProgress(kvs);

        shard = writeTs(writer, TS, true);
        shard2 = writeTs(writer, TS2, false);

        immutableTs = 10 * TS;
        unreadableTs = 10 * TS;
    }

    @Test
    public void canReadNextTimestampWhenSweepTsIsLarge() {
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(thorough(shard2))).contains(TS2_REF);
    }

    @Test
    public void cannotReadForWrongSweepStrategy() {
        assertThat(reader.nextSweepableTimestampPartition(thorough(shard))).isEmpty();
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard2))).isEmpty();
    }

    @Test
    public void cannotReadForWrongShard() {
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard + 1))).isEmpty();
        assertThat(reader.nextSweepableTimestampPartition(thorough(shard2 + 1))).isEmpty();
    }

    @Test
    public void noNextTimestampWhenImmutableTsInSmallerPartitionForEitherSweepStrategy() {
        immutableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(immutableTs)).isLessThan(tsPartitionFine(TS));
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).isEmpty();

        assertThat(tsPartitionFine(immutableTs)).isLessThan(tsPartitionFine(TS2));
        assertThat(reader.nextSweepableTimestampPartition(thorough(shard2))).isEmpty();
    }

    @Test
    public void noNextTimestampWhenUnreadableTsInSmallerPartitionForConservativeOnly() {
        unreadableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(tsPartitionFine(TS));
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).isEmpty();

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(tsPartitionFine(TS2));
        assertThat(reader.nextSweepableTimestampPartition(thorough(shard2))).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampWhenSweepTimestampInSamePartitionAndGreater() {
        immutableTs = TS + TS_FINE_GRANULARITY - TS % TS_FINE_GRANULARITY - 1;

        assertThat(tsPartitionFine(immutableTs)).isEqualTo(tsPartitionFine(TS));
        assertThat(immutableTs).isGreaterThan(TS);
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).isEmpty();
    }

    @Test
    public void canReadNextTimestampWhenSweepTimestampInNextPartitionAndSameBucket() {
        immutableTs = TS + TS_FINE_GRANULARITY - TS % TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(immutableTs)).isGreaterThan(tsPartitionFine(TS));
        assertThat(tsPartitionCoarse(immutableTs)).isEqualTo(tsPartitionCoarse(TS));
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).contains(TS_REF);
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForConservative() {
        progress.updateLastSweptTimestampPartition(thorough(shard), TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).contains(TS_REF);

        progress.updateLastSweptTimestampPartition(conservative(shard), TS_REF - 1);
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(thorough(shard2))).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampIfProgressedBeyondForConservative() {
        progress.updateLastSweptTimestampPartition(conservative(shard), TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).isEmpty();
        assertThat(reader.nextSweepableTimestampPartition(thorough(shard2))).contains(TS2_REF);
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForThorough() {
        progress.updateLastSweptTimestampPartition(conservative(shard2), TS2_REF);
        assertThat(reader.nextSweepableTimestampPartition(thorough(shard2))).contains(TS2_REF);

        progress.updateLastSweptTimestampPartition(thorough(shard), TS2_REF - 1);
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(thorough(shard2))).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampIfProgressedBeyondForThorough() {
        progress.updateLastSweptTimestampPartition(thorough(shard2), TS2_REF);
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(thorough(shard2))).isEmpty();
    }

    @Test
    public void getCorrectNextTimestampWhenMultipleCandidates() {
        for (long timestamp = 1000L; tsPartitionFine(timestamp) < 10L; timestamp += TS_FINE_GRANULARITY / 5) {
            writeTs(writer, timestamp, true);
        }
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard)))
                .contains(tsPartitionFine(1000L));

        progress.updateLastSweptTimestampPartition(conservative(shard), 2L);
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).contains(3L);

        immutableTs = 4 * TS_FINE_GRANULARITY;
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).contains(3L);

        immutableTs = 4 * TS_FINE_GRANULARITY - 1;
        assertThat(reader.nextSweepableTimestampPartition(conservative(shard))).isEmpty();
    }
}
