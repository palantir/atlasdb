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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_FINE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionCoarse;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.tsPartitionFine;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;

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
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(shard2, false)).contains(TS2_REF);
    }

    @Test
    public void cannotReadForWrongSweepStrategy() {
        assertThat(reader.nextSweepableTimestampPartition(shard, false)).isEmpty();
        assertThat(reader.nextSweepableTimestampPartition(shard2, true)).isEmpty();
    }

    @Test
    public void cannotReadForWrongShard() {
        assertThat(reader.nextSweepableTimestampPartition(shard + 1, true)).isEmpty();
        assertThat(reader.nextSweepableTimestampPartition(shard2 + 1, false)).isEmpty();
    }

    @Test
    public void noNextTimestampWhenImmutableTsInSmallerPartitionForEitherSweepStrategy() {
        immutableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(immutableTs)).isLessThan(tsPartitionFine(TS));
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).isEmpty();

        assertThat(tsPartitionFine(immutableTs)).isLessThan(tsPartitionFine(TS2));
        assertThat(reader.nextSweepableTimestampPartition(shard2, false)).isEmpty();
    }

    @Test
    public void noNextTimestampWhenUnreadableTsInSmallerPartitionForConservativeOnly() {
        unreadableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(tsPartitionFine(TS));
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).isEmpty();

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(tsPartitionFine(TS2));
        assertThat(reader.nextSweepableTimestampPartition(shard2, false)).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampWhenSweepTimestampInSamePartitionAndGreater() {
        immutableTs = TS + TS_FINE_GRANULARITY - TS % TS_FINE_GRANULARITY - 1;

        assertThat(tsPartitionFine(immutableTs)).isEqualTo(tsPartitionFine(TS));
        assertThat(immutableTs).isGreaterThan(TS);
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).isEmpty();
    }

    @Test
    public void canReadNextTimestampWhenSweepTimestampInNextPartitionAndSameBucket() {
        immutableTs = TS + TS_FINE_GRANULARITY - TS % TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(immutableTs)).isGreaterThan(tsPartitionFine(TS));
        assertThat(tsPartitionCoarse(immutableTs)).isEqualTo(tsPartitionCoarse(TS));
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).contains(TS_REF);
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForConservative() {
        progress.updateLastSweptTimestampPartition(shard, false, TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).contains(TS_REF);

        progress.updateLastSweptTimestampPartition(shard, true, TS_REF - 1);
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(shard2, false)).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampIfProgressedBeyondForConservative() {
        progress.updateLastSweptTimestampPartition(shard, true, TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).isEmpty();
        assertThat(reader.nextSweepableTimestampPartition(shard2, false)).contains(TS2_REF);
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForThorough() {
        progress.updateLastSweptTimestampPartition(shard2, true, TS2_REF);
        assertThat(reader.nextSweepableTimestampPartition(shard2, false)).contains(TS2_REF);

        progress.updateLastSweptTimestampPartition(shard, false, TS2_REF - 1);
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(shard2, false)).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampIfProgressedBeyondForThorough() {
        progress.updateLastSweptTimestampPartition(shard2, false, TS2_REF);
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(shard2, false)).isEmpty();
    }

    @Test
    public void getCorrectNextTimestampWhenMultipleCandidates() {
        for (long timestamp = 1000L; tsPartitionFine(timestamp) < 10L; timestamp += TS_FINE_GRANULARITY / 5) {
            writeTs(writer, timestamp, true);
        }
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).contains(tsPartitionFine(1000L));

        progress.updateLastSweptTimestampPartition(shard, true, 2L);
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).contains(3L);

        immutableTs = 4 * TS_FINE_GRANULARITY;
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).contains(3L);

        immutableTs = 4 * TS_FINE_GRANULARITY - 1;
        assertThat(reader.nextSweepableTimestampPartition(shard, true)).isEmpty();
    }


}
