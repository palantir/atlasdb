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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy.CONSERVATIVE;
import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy.NOTHING;
import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy.THOROUGH;
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
    KvsSweepQueue sweepQueue = KvsSweepQueue.createUninitialized(() -> true, () -> SHARDS);
    KvsSweepQueueScrubber scrubber = mock(KvsSweepQueueScrubber.class);
    long unreadableTs;
    long immutableTs;

    SweepTimestampProvider provider = new SweepTimestampProvider(() -> unreadableTs, () -> immutableTs);

    @Before
    public void setup() {
        kvs = spy(new InMemoryKeyValueService(false));
        unreadableTs = SweepQueueUtils.TS_COARSE_GRANULARITY;
        immutableTs = SweepQueueUtils.TS_COARSE_GRANULARITY;
        sweepQueue.initialize(provider, kvs);
        sweepQueue.setScrubbers(scrubber);
        kvs.createTable(TABLE_CONSERVATIVE, SweepQueueTestUtils.metadataBytes(CONSERVATIVE));
        kvs.createTable(TABLE_THOROUGH, SweepQueueTestUtils.metadataBytes(THOROUGH));
        kvs.createTable(TABLE_NOTHING, SweepQueueTestUtils.metadataBytes(NOTHING));
    }

    // todo(gmaretic): scrubber interface needs to be changed, because we also need to progress if there is nothing to sweep
    @Test
    public void scrubberCalledEvenIfNothingToSweep() {
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        verify(scrubber).scrub(anyCollection());
    }

    @Test
    public void sweepStrategyNothingDoesNotPersistAntything() {
        enqueueWrite(TABLE_NOTHING, TS);
        enqueueWrite(TABLE_NOTHING, TS2);
        verify(kvs, times(2)).put(any(TableReference.class), anyMap(), anyLong());
    }

    @Test
    public void conservativeSweepAddsSentinelAndLeavesSingleValue() {
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        assertReadAtTimestampReturnsNothing(TABLE_CONSERVATIVE, TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS + 1, TS);
    }

    @Test
    public void thoroughSweepDoesNotAddSentinelAndLeavesSingleValue() {
        enqueueWrite(TABLE_THOROUGH, TS);
        assertReadAtTimestampReturnsNothing(TABLE_THOROUGH, TS);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOROUGH, TS);
        assertReadAtTimestampReturnsValue(TABLE_THOROUGH, TS + 1, TS);
    }

    @Test
    public void conservativeSweepDeletesLowerValue() {
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        enqueueWrite(TABLE_CONSERVATIVE, TS2);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS + 1, TS);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);
    }

    @Test
    public void thoroughSweepDeletesLowerValue() {
        enqueueWrite(TABLE_THOROUGH, TS);
        enqueueWrite(TABLE_THOROUGH, TS2);
        assertReadAtTimestampReturnsValue(TABLE_THOROUGH, TS + 1, TS);
        assertReadAtTimestampReturnsValue(TABLE_THOROUGH, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOROUGH, TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_THOROUGH, TS2 + 1, TS2);
    }

    @Test
    public void sweepDeletesAllButLatestWithSingleDeleteAllTimestamps() {
        long numWrites = 1000L;
        for (long i = 1; i <= numWrites; i++) {
            enqueueWrite(TABLE_CONSERVATIVE, i);
        }
        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, numWrites);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, numWrites + 1, numWrites);
        verify(kvs, times(1)).deleteAllTimestamps(eq(TABLE_CONSERVATIVE), anyMap());
    }

    @Test
    public void onlySweepsOneBatchAtATime() {
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        enqueueWrite(TABLE_CONSERVATIVE, TS2);
        enqueueWrite(TABLE_CONSERVATIVE, TS_FINE_GRANULARITY);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS_FINE_GRANULARITY + 1, TS_FINE_GRANULARITY);
    }

    @Test
    public void sweepDeletesWritesWhenTombstoneHasHigherTimestamp() {
        enqueueWrite(TABLE_CONSERVATIVE, TS);
        enqueueTombstone(TABLE_CONSERVATIVE, TS2);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS + 1, TS);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONSERVATIVE, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONSERVATIVE, TS2 + 1, TS2);
    }

    @Test
    public void thoroughSweepDeletesTombstoneIfLatestWrite() {
        enqueueTombstone(TABLE_THOROUGH, TS);
        enqueueTombstone(TABLE_THOROUGH, TS2);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_THOROUGH, TS + 1, TS);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_THOROUGH, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.thorough(THOR_SHARD));
        assertReadAtTimestampReturnsNothing(TABLE_THOROUGH, TS + 1);
        assertReadAtTimestampReturnsNothing(TABLE_THOROUGH, TS2 + 1);
    }

    @Test
    public void sweepDeletesTombstonesWhenWriteHasHigherTimestamp() {
        enqueueTombstone(TABLE_CONSERVATIVE, TS);
        enqueueWrite(TABLE_CONSERVATIVE, TS2);
        assertReadAtTimestampReturnsTombstoneAtTimestamp(TABLE_CONSERVATIVE, TS + 1, TS);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);

        sweepQueue.sweepNextBatch(ShardAndStrategy.conservative(CONS_SHARD));
        assertReadAtTimestampReturnsSentinel(TABLE_CONSERVATIVE, TS + 1);
        assertReadAtTimestampReturnsValue(TABLE_CONSERVATIVE, TS2 + 1, TS2);
    }

    private void enqueueWrite(TableReference tableRef, long ts) {
        sweepQueue.enqueue(writeToDefaultCell(tableRef, ts), ts);
    }

    private void enqueueTombstone(TableReference tableRef, long ts) {
        sweepQueue.enqueue(tombstoneToDefaultCell(tableRef, ts), ts);
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

    private void assertReadAtTimestampReturnsValue(TableReference tableRef, long readTs, long value) {
        assertThat(readValueFromDefaultCell(tableRef, readTs)).isEqualTo(value);
    }

    private long readValueFromDefaultCell(TableReference tableRef, long ts) {
        return PtBytes.toLong(Iterables.getOnlyElement(readFromDefaultCell(tableRef, ts).values()).getContents());
    }

    private void assertReadAtTimestampReturnsSentinel(TableReference tableRef, long readTs) {
        assertReadAtTimestampReturnsTombstoneAtTimestamp(tableRef, readTs, -1L);
    }

    private void assertReadAtTimestampReturnsTombstoneAtTimestamp(TableReference tableRef, long readTs, long tombTs) {
        Value readValue = Iterables.getOnlyElement(readFromDefaultCell(tableRef, readTs).values());
        assertThat(readValue.getTimestamp()).isEqualTo(tombTs);
        assertThat(readValue.getContents()).isEmpty();
    }

    private void assertReadAtTimestampReturnsNothing(TableReference tableRef, long readTs) {
        assertThat(readFromDefaultCell(tableRef, readTs)).isEmpty();
    }

    private Map<Cell, Value> readFromDefaultCell(TableReference tableRef, long ts) {
        Map<Cell, Long> singleRead = ImmutableMap.of(DEFAULT_CELL, ts);
        return kvs.get(tableRef, singleRead);
    }
}
