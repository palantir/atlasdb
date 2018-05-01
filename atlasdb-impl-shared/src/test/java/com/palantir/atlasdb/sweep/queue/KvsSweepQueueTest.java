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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy.CONSERVATIVE;
import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy.NOTHING;
import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy.THOROUGH;
import static com.palantir.atlasdb.sweep.queue.SweepQueueTablesTest.metadataBytes;
import static com.palantir.atlasdb.sweep.queue.SweepQueueUtils.TS_FINE_GRANULARITY;
import static com.palantir.atlasdb.sweep.queue.WriteInfoPartitioner.SHARDS;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;

public class KvsSweepQueueTest {
    private static final TableReference TABLE_CONSERVATIVE = TableReference.createFromFullyQualifiedName("test.cons");
    private static final TableReference TABLE_THOROUGH = TableReference.createFromFullyQualifiedName("test.thor");
    private static final TableReference TABLE_NOTHING = TableReference.createFromFullyQualifiedName("test.noth");
    private static final Cell DEFAULT_CELL = Cell.create(new byte[] {'r'}, new byte[] {'c'});
    private static final int CONS_SHARD = WriteInfo.tombstone(TABLE_CONSERVATIVE, DEFAULT_CELL, 0).toShard(SHARDS);
    private static final int THOR_SHARD = WriteInfo.tombstone(TABLE_THOROUGH, DEFAULT_CELL, 0).toShard(SHARDS);
    private static final long TS = 10L;
    private static final long TS2 = 2 * TS;

    KeyValueService kvs;
    KvsSweepQueue sweepQueue = KvsSweepQueue.createUninitialized(() -> SHARDS);
    long unreadableTs;
    long immutableTs;

    SweepTimestampProvider provider = new SweepTimestampProvider(() -> unreadableTs, () -> immutableTs);

    @Before
    public void setup() {
        kvs = spy(new InMemoryKeyValueService(false));
        unreadableTs = SweepQueueUtils.TS_COARSE_GRANULARITY;
        immutableTs = SweepQueueUtils.TS_COARSE_GRANULARITY;
        sweepQueue.initialize(provider, kvs);
        kvs.createTable(TABLE_CONSERVATIVE, metadataBytes(CONSERVATIVE));
        kvs.createTable(TABLE_THOROUGH, metadataBytes(THOROUGH));
        kvs.createTable(TABLE_NOTHING, metadataBytes(NOTHING));
    }

    @Test
    public void sweepStrategyNothingDoesNotPersistAntything() {
        sweepQueue.enqueue(writeToDefaultCell(TABLE_NOTHING, TS), TS);
        sweepQueue.enqueue(writeToDefaultCell(TABLE_NOTHING, TS2), TS2);
        verify(kvs, times(0)).put(eq(TargetedSweepTableFactory.of().getSweepableCellsTable(null).getTableRef()),
                anyMap(), anyLong());
        verify(kvs, times(0)).put(eq(TargetedSweepTableFactory.of().getSweepableTimestampsTable(null).getTableRef()),
                anyMap(), anyLong());
        verify(kvs, times(0)).put(eq(TargetedSweepTableFactory.of().getSweepShardProgressTable(null).getTableRef()),
                anyMap(), anyLong());
    }

    @Test
    public void conservativeSweepAddsSentinelAndLeavesSingleValue() {
        sweepQueue.enqueue(writeToDefaultCell(TABLE_CONSERVATIVE, TS), TS);
        assertNothing(TABLE_CONSERVATIVE, TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSentinel(TABLE_CONSERVATIVE, TS);
        assertValue(TABLE_CONSERVATIVE, TS + 1, TS);
    }

    @Test
    public void thoroughSweepDoesNotAddSentinelAndLeavesSingleValue() {
        sweepQueue.enqueue(writeToDefaultCell(TABLE_THOROUGH, TS), TS);
        assertNothing(TABLE_THOROUGH, TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertNothing(TABLE_THOROUGH, TS);
        assertValue(TABLE_THOROUGH, TS + 1, TS);
    }

    @Test
    public void conservativeSweepDeletesLowerValue() {
        sweepQueue.enqueue(writeToDefaultCell(TABLE_CONSERVATIVE, TS), TS);
        sweepQueue.enqueue(writeToDefaultCell(TABLE_CONSERVATIVE, TS2), TS2);
        assertValue(TABLE_CONSERVATIVE, TS + 1, TS);
        assertValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);
    }

    @Test
    public void thoroughSweepDeletesLowerValue() {
        sweepQueue.enqueue(writeToDefaultCell(TABLE_THOROUGH, TS), TS);
        sweepQueue.enqueue(writeToDefaultCell(TABLE_THOROUGH, TS2), TS2);
        assertValue(TABLE_THOROUGH, TS + 1, TS);
        assertValue(TABLE_THOROUGH, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertNothing(TABLE_THOROUGH, TS + 1);
        assertValue(TABLE_THOROUGH, TS2 + 1, TS2);
    }

    @Test
    public void sweepDeletesAllButLatestWithSingleDeleteAllTimestamps() {
        long numWrites = 1000L;
        for (long i = 1; i <= numWrites; i++) {
            sweepQueue.enqueue(writeToDefaultCell(TABLE_CONSERVATIVE, i), i);
        }
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSentinel(TABLE_CONSERVATIVE, numWrites);
        assertValue(TABLE_CONSERVATIVE, numWrites + 1, numWrites);
        verify(kvs).deleteAllTimestamps(eq(TABLE_CONSERVATIVE), anyMap());
    }

    @Test
    public void nextBatchOfSweepIgnoresWritesWithLargeTimestamps() {
        sweepQueue.enqueue(writeToDefaultCell(TABLE_CONSERVATIVE, TS), TS);
        sweepQueue.enqueue(writeToDefaultCell(TABLE_CONSERVATIVE, TS2), TS2);
        sweepQueue.enqueue(writeToDefaultCell(TABLE_CONSERVATIVE, TS_FINE_GRANULARITY), TS_FINE_GRANULARITY);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);
        assertValue(TABLE_CONSERVATIVE, TS_FINE_GRANULARITY + 1, TS_FINE_GRANULARITY);
    }



    @Test
    public void sweepDeletesWritesWhenTombstoneHasHigherTimestamp() {
        sweepQueue.enqueue(writeToDefaultCell(TABLE_CONSERVATIVE, TS), TS);
        sweepQueue.enqueue(tombstoneToDefaultCell(TABLE_CONSERVATIVE, TS2), TS2);
        assertValue(TABLE_CONSERVATIVE, TS + 1, TS);
        assertTombstone(TABLE_CONSERVATIVE, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertTombstone(TABLE_CONSERVATIVE, TS2 + 1, TS2);
    }

    @Test
    public void thoroughSweepDeletesTombstoneIfLatestWrite() {
        sweepQueue.enqueue(tombstoneToDefaultCell(TABLE_THOROUGH, TS), TS);
        sweepQueue.enqueue(tombstoneToDefaultCell(TABLE_THOROUGH, TS2), TS2);
        assertTombstone(TABLE_THOROUGH, TS + 1, TS);
        assertTombstone(TABLE_THOROUGH, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertNothing(TABLE_THOROUGH, TS + 1);
        assertNothing(TABLE_THOROUGH, TS2 + 1);
    }

    @Test
    public void sweepDeletesTombstonesWhenWriteHasHigherTimestamp() {
        sweepQueue.enqueue(tombstoneToDefaultCell(TABLE_CONSERVATIVE, TS), TS);
        sweepQueue.enqueue(writeToDefaultCell(TABLE_CONSERVATIVE, TS2), TS2);
        assertTombstone(TABLE_CONSERVATIVE, TS + 1, TS);
        assertValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);
    }

    private Map<TableReference, ? extends Map<Cell, byte[]>> writeToDefaultCell(TableReference tableRef, long ts) {
        Map<Cell, byte[]> singleWrite = ImmutableMap.of(DEFAULT_CELL, PtBytes.toBytes(ts));
        kvs.put(tableRef, singleWrite, ts);
        return ImmutableMap.of(tableRef, singleWrite);
    }

    private Map<TableReference, ? extends Map<Cell, byte[]>> tombstoneToDefaultCell(TableReference tableRef, long ts) {
        Map<Cell, byte[]> singleWrite = ImmutableMap.of(DEFAULT_CELL, PtBytes.EMPTY_BYTE_ARRAY);
        kvs.put(tableRef, singleWrite, ts);
        return ImmutableMap.of(tableRef, singleWrite);
    }

    private void assertValue(TableReference tableRef, long ts, long value) {
        assertThat(readValueFromDefaultCell(tableRef, ts)).isEqualTo(value);
    }

    private long readValueFromDefaultCell(TableReference tableRef, long ts) {
        return PtBytes.toLong(Iterables.getOnlyElement(readFromDefaultCell(tableRef, ts).values()).getContents());
    }

    private void assertSentinel(TableReference tableRef, long ts) {
        assertTombstone(tableRef, ts, -1L);
    }

    private void assertTombstone(TableReference tableRef, long readTs, long tombTs) {
        Value readValue = Iterables.getOnlyElement(readFromDefaultCell(tableRef, readTs).values());
        assertThat(readValue.getTimestamp()).isEqualTo(tombTs);
        assertThat(readValue.getContents()).isEmpty();
    }

    private void assertNothing(TableReference tableRef, long ts) {
        assertThat(readFromDefaultCell(tableRef, ts)).isEmpty();
    }

    private Map<Cell, Value> readFromDefaultCell(TableReference tableRef, long ts) {
        Map<Cell, Long> singleRead = ImmutableMap.of(DEFAULT_CELL, ts);
        return kvs.get(tableRef, singleRead);
    }
}
