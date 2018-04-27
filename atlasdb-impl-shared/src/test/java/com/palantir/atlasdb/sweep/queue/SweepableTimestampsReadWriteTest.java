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

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;

public class SweepableTimestampsReadWriteTest {
    private static final long TS = 1_000_000_100L;
    private static final long TS2 = 2 * TS;
    private static final long TS_REF = tsPartitionFine(TS);
    private static final long TS2_REF = tsPartitionFine(TS2);
    private static final TableReference TABLE_REF = TableReference.createFromFullyQualifiedName("test.test");
    private static final TableReference TABLE_REF2 = TableReference.createFromFullyQualifiedName("test.test2");
    private static final byte[] ROW = new byte[] {'r'};
    private static final byte[] COL = new byte[] {'c'};

    private KeyValueService mockKvs = mock(KeyValueService.class);
    private KeyValueService kvs = new InMemoryKeyValueService(true);
    private WriteInfoPartitioner partitioner;
    private SweepableTimestampsWriter writer;
    private SweepableTimestampsReader reader;
    private SweepTimestampProvider provider;
    private KvsSweepQueueProgress progress;

    private int shard;
    private int shard2;
    private long immutableTs;
    private long unreadableTs;

    @Before
    public void setup() {
        when(mockKvs.getMetadataForTable(TABLE_REF))
                .thenReturn(SweepQueueTestUtils.metadataBytes(TableMetadataPersistence.SweepStrategy.CONSERVATIVE));
        when(mockKvs.getMetadataForTable(TABLE_REF2))
                .thenReturn(SweepQueueTestUtils.metadataBytes(TableMetadataPersistence.SweepStrategy.THOROUGH));
        partitioner = new WriteInfoPartitioner(mockKvs);

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
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.thorough(shard2))).contains(TS2_REF);
    }

    @Test
    public void cannotReadForWrongSweepStrategy() {
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.thorough(shard))).isEmpty();
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard2))).isEmpty();
    }

    @Test
    public void cannotReadForWrongShard() {
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard + 1))).isEmpty();
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.thorough(shard2 + 1))).isEmpty();
    }

    @Test
    public void noNextTimestampWhenImmutableTsInSmallerPartitionForEitherSweepStrategy() {
        immutableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(immutableTs)).isLessThan(tsPartitionFine(TS));
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).isEmpty();

        assertThat(tsPartitionFine(immutableTs)).isLessThan(tsPartitionFine(TS2));
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.thorough(shard2))).isEmpty();
    }

    @Test
    public void noNextTimestampWhenUnreadableTsInSmallerPartitionForConservativeOnly() {
        unreadableTs = TS - TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(tsPartitionFine(TS));
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).isEmpty();

        assertThat(tsPartitionFine(unreadableTs)).isLessThan(tsPartitionFine(TS2));
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.thorough(shard2))).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampWhenSweepTimestampInSamePartitionAndGreater() {
        immutableTs = TS + TS_FINE_GRANULARITY - TS % TS_FINE_GRANULARITY - 1;

        assertThat(tsPartitionFine(immutableTs)).isEqualTo(tsPartitionFine(TS));
        assertThat(immutableTs).isGreaterThan(TS);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).isEmpty();
    }

    @Test
    public void canReadNextTimestampWhenSweepTimestampInNextPartitionAndSameBucket() {
        immutableTs = TS + TS_FINE_GRANULARITY - TS % TS_FINE_GRANULARITY;

        assertThat(tsPartitionFine(immutableTs)).isGreaterThan(tsPartitionFine(TS));
        assertThat(tsPartitionCoarse(immutableTs)).isEqualTo(tsPartitionCoarse(TS));
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).contains(TS_REF);
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForConservative() {
        progress.updateLastSweptTimestampPartition(ShardAndStrategy.thorough(shard), TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).contains(TS_REF);

        progress.updateLastSweptTimestampPartition(ShardAndStrategy.conservative(shard), TS_REF - 1);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.thorough(shard2))).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampIfProgressedBeyondForConservative() {
        progress.updateLastSweptTimestampPartition(ShardAndStrategy.conservative(shard), TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).isEmpty();
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.thorough(shard2))).contains(TS2_REF);
    }

    @Test
    public void canReadNextIfNotProgressedBeyondForThorough() {
        progress.updateLastSweptTimestampPartition(ShardAndStrategy.conservative(shard2), TS2_REF);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.thorough(shard2))).contains(TS2_REF);

        progress.updateLastSweptTimestampPartition(ShardAndStrategy.thorough(shard), TS2_REF - 1);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.thorough(shard2))).contains(TS2_REF);
    }

    @Test
    public void noNextTimestampIfProgressedBeyondForThorough() {
        progress.updateLastSweptTimestampPartition(ShardAndStrategy.thorough(shard2), TS2_REF);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).contains(TS_REF);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.thorough(shard2))).isEmpty();
    }

    @Test
    public void getCorrectNextTimestampWhenMultipleCandidates() {
        for (long timestamp = 1000L; tsPartitionFine(timestamp) < 10L; timestamp += TS_FINE_GRANULARITY / 5) {
            writeTs(writer, timestamp, true);
        }
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard)))
                .contains(tsPartitionFine(1000L));

        progress.updateLastSweptTimestampPartition(ShardAndStrategy.conservative(shard), 2L);
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).contains(3L);

        immutableTs = 4 * TS_FINE_GRANULARITY;
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).contains(3L);

        immutableTs = 4 * TS_FINE_GRANULARITY - 1;
        assertThat(reader.nextSweepableTimestampPartition(ShardAndStrategy.conservative(shard))).isEmpty();
    }

    private static int writeTs(SweepableTimestampsWriter writer, long timestamp, boolean conservative) {
        WriteInfo write = WriteInfo.of(conservative ? TABLE_REF : TABLE_REF2, Cell.create(ROW, COL), timestamp);
        writer.enqueue(ImmutableList.of(write));
        return WriteInfoPartitioner.getShard(write);
    }
}
