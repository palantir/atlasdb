/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.sweep.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TargetedSweepTest extends AtlasDbTestCase {
    private static final TableReference TABLE_CONS = TableReference.createFromFullyQualifiedName("test.1");
    private static final TableReference TABLE_THOR = TableReference.createFromFullyQualifiedName("test.2");
    private static final TableReference TABLE_NONAMESPACE = TableReference.createWithEmptyNamespace("empty");
    private static final Cell TEST_CELL = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("col1"));
    private static final byte[] TEST_DATA = new byte[] {1};
    private static final WriteReference SINGLE_WRITE = WriteReference.write(TABLE_CONS, TEST_CELL);
    private static final WriteReference SINGLE_DELETE = WriteReference.tombstone(TABLE_CONS, TEST_CELL);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        keyValueService.createTable(TABLE_CONS, AtlasDbConstants.GENERIC_TABLE_METADATA);
        keyValueService.createTable(
                TABLE_THOR,
                TableMetadataPersistence.TableMetadata.newBuilder(TableMetadataPersistence.TableMetadata.parseFrom(
                                AtlasDbConstants.GENERIC_TABLE_METADATA))
                        .setSweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH)
                        .build()
                        .toByteArray());
        keyValueService.createTable(TABLE_NONAMESPACE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void committedWritesAreAddedToSweepQueue() {
        List<WriteReference> table1Writes = ImmutableList.of(
                WriteReference.write(
                        TABLE_CONS,
                        Cell.create("a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8))),
                WriteReference.write(
                        TABLE_CONS,
                        Cell.create("a".getBytes(StandardCharsets.UTF_8), "c".getBytes(StandardCharsets.UTF_8))),
                WriteReference.tombstone(
                        TABLE_CONS,
                        Cell.create("a".getBytes(StandardCharsets.UTF_8), "d".getBytes(StandardCharsets.UTF_8))),
                WriteReference.write(
                        TABLE_CONS,
                        Cell.create("b".getBytes(StandardCharsets.UTF_8), "d".getBytes(StandardCharsets.UTF_8))));
        List<WriteReference> table2Writes = ImmutableList.of(
                WriteReference.write(
                        TABLE_THOR,
                        Cell.create("w".getBytes(StandardCharsets.UTF_8), "x".getBytes(StandardCharsets.UTF_8))),
                WriteReference.write(
                        TABLE_THOR,
                        Cell.create("y".getBytes(StandardCharsets.UTF_8), "z".getBytes(StandardCharsets.UTF_8))),
                WriteReference.tombstone(
                        TABLE_THOR,
                        Cell.create("z".getBytes(StandardCharsets.UTF_8), "z".getBytes(StandardCharsets.UTF_8))));

        long startTimestamp = txManager.runTaskWithRetry(txn -> {
            table1Writes.forEach(write -> put(txn, write));
            table2Writes.forEach(write -> put(txn, write));
            return txn.getTimestamp();
        });

        List<WriteInfo> expectedWrites = Stream.concat(table1Writes.stream(), table2Writes.stream())
                .map(writeRef -> WriteInfo.of(writeRef, startTimestamp))
                .collect(Collectors.toList());

        assertThat(getEnqueuedWritesNumber(1)).hasSameElementsAs(expectedWrites);
        verify(sweepQueue, times(1)).enqueue(anyList());
    }

    @Test
    public void writesAddedToSweepQueueOnNoConflict() {
        WriteReference firstWrite = WriteReference.write(TABLE_CONS, TEST_CELL);
        WriteReference secondWrite = WriteReference.write(TABLE_THOR, TEST_CELL);

        Transaction t1 = txManager.createNewTransaction();
        Transaction t2 = txManager.createNewTransaction();

        put(t1, firstWrite);
        put(t2, secondWrite);

        t1.commit();
        assertThat(getEnqueuedWritesNumber(1)).containsExactly(WriteInfo.of(firstWrite, t1.getTimestamp()));
        assertLatestEntryForCellInKvsAtTimestamp(TABLE_CONS, TEST_CELL, t1.getTimestamp());

        t2.commit();
        assertThat(getEnqueuedWritesNumber(2)).containsExactly(WriteInfo.of(secondWrite, t2.getTimestamp()));
        assertLatestEntryForCellInKvsAtTimestamp(TABLE_THOR, TEST_CELL, t2.getTimestamp());

        verify(sweepQueue, times(2)).enqueue(anyList());
    }

    @Test
    public void writesAddedToSweepQueueAndKvsOnPreCommitConditionFailure() {
        long startTs = putWriteAndFailOnPreCommitConditionReturningStartTimestamp(SINGLE_WRITE);

        assertThat(getEnqueuedWritesNumber(1)).containsExactly(WriteInfo.of(SINGLE_WRITE, startTs));
        verify(sweepQueue, times(1)).enqueue(anyList());
        assertLatestEntryForCellInKvsAtTimestamp(TABLE_CONS, TEST_CELL, startTs);
    }

    @Test
    public void writesNotAddedToSweepQueueOrKvsOnWriteWriteConflict() {
        Transaction t1 = txManager.createNewTransaction();
        Transaction t2 = txManager.createNewTransaction();

        put(t1, SINGLE_WRITE);
        put(t2, SINGLE_WRITE);

        t1.commit();
        assertThatThrownBy(t2::commit).isInstanceOf(TransactionConflictException.class);

        verify(sweepQueue, times(1)).enqueue(anyList());
        assertLatestEntryForCellInKvsAtTimestamp(TABLE_CONS, TEST_CELL, t1.getTimestamp());
    }

    @Test
    public void writesNotAddedToSweepQueueOrKvsOnException() {
        assertThatThrownBy(() -> txManager.runTaskWithRetry(txn -> {
                    txn.put(TABLE_CONS, ImmutableMap.of(TEST_CELL, TEST_DATA));
                    throw new RuntimeException("test");
                }))
                .isInstanceOf(RuntimeException.class);

        verify(sweepQueue, times(0)).enqueue(anyList());
        assertNoEntryForCellInKvs(TABLE_CONS, TEST_CELL);
    }

    @Test
    public void sweepDeletesValuesWrittenOnPreCommitConditionFailure() {
        useOneSweepQueueShard();
        long startTs = putWriteAndFailOnPreCommitConditionReturningStartTimestamp(SINGLE_WRITE);

        serializableTxManager.setUnreadableTimestamp(startTs + 1);
        waitForImmutableTimestampToBeStrictlyGreaterThan(startTs);
        sweepNextBatch(ShardAndStrategy.conservative(0));
        assertNoEntryForCellInKvs(TABLE_CONS, TEST_CELL);
    }

    /**
     * After this method returns successfully, there has existed some point in time when the immutable timestamp from
     * the {@link #serializableTxManager} was strictly greater than the timestamp parameter that was passed. In the
     * absence of concurrent transaction starts, this also means that the immutable timestamp will be
     * strictly greater than the timestamp parameter going forward.
     *
     * Note that in the general case, this does not guarantee that the immutable timestamp is currently greater than
     * the timestamp parameter in the presence of concurrent transaction starts, since under certain scheduling
     * conditions it may go backwards.
     *
     * The purpose of this method is to ensure that, for tests that assert that Sweep has deleted values written in a
     * given transaction, that that transaction has released its immutable timestamp lock. This happens
     * asynchronously, and thus an explicit wait is needed to avoid race conditions.
     */
    private void waitForImmutableTimestampToBeStrictlyGreaterThan(long timestampToBePassed) {
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .until(() -> timestampToBePassed < serializableTxManager.getImmutableTimestamp());
    }

    @Test
    public void sweepRetainsLatestVersion() {
        useOneSweepQueueShard();

        writeInTransactionAndGetStartTimestamp(SINGLE_WRITE);
        putWriteAndFailOnPreCommitConditionReturningStartTimestamp(SINGLE_WRITE);
        long freshCommittedTs = writeInTransactionAndGetStartTimestamp(SINGLE_WRITE);
        putWriteAndFailOnPreCommitConditionReturningStartTimestamp(SINGLE_DELETE);
        long freshFailedTs = putWriteAndFailOnPreCommitConditionReturningStartTimestamp(SINGLE_WRITE);

        serializableTxManager.setUnreadableTimestamp(freshFailedTs + 1);
        sweepNextBatch(ShardAndStrategy.conservative(0));
        assertLatestEntryForCellInKvsAtTimestamp(TABLE_CONS, TEST_CELL, freshCommittedTs);
        assertOnlySentinelBeforeTs(TABLE_CONS, TEST_CELL, freshCommittedTs);
    }

    @Test
    public void sweepDeletesAllIfLatestCommittedTombstoneInThorough() {
        useOneSweepQueueShard();
        WriteReference writeToThorough = WriteReference.write(TABLE_THOR, TEST_CELL);
        WriteReference tombstoneToThorough = WriteReference.tombstone(TABLE_THOR, TEST_CELL);
        long firstStart = writeInTransactionAndGetStartTimestamp(writeToThorough);
        long secondStart = writeInTransactionAndGetStartTimestamp(tombstoneToThorough);

        assertLatestEntryBeforeTsIs(Long.MAX_VALUE, TABLE_THOR, TEST_CELL, PtBytes.EMPTY_BYTE_ARRAY, secondStart);
        assertLatestEntryBeforeTsIs(secondStart, TABLE_THOR, TEST_CELL, TEST_DATA, firstStart);

        serializableTxManager.setUnreadableTimestamp(secondStart + 1);
        sweepNextBatch(ShardAndStrategy.thorough(0));
        assertNoEntryForCellInKvs(TABLE_THOR, TEST_CELL);
    }

    @Test
    public void sweepThrowsAwayWritesForDroppedTables() {
        useOneSweepQueueShard();
        writeInTransactionAndGetStartTimestamp(SINGLE_WRITE);
        Long startTimestamp = writeInTransactionAndGetStartTimestamp(SINGLE_WRITE);

        keyValueService.dropTable(TABLE_CONS);

        serializableTxManager.setUnreadableTimestamp(startTimestamp + 1);
        sweepNextBatch(ShardAndStrategy.conservative(0));
    }

    @Test
    public void sweepDoesntErrorOnDroppedTableWithSentinelsEnqueued() {
        useOneSweepQueueShard();
        Long startTimestamp = putWriteAndFailOnPreCommitConditionReturningStartTimestamp(SINGLE_WRITE);

        keyValueService.dropTable(TABLE_CONS);

        serializableTxManager.setUnreadableTimestamp(startTimestamp + 1);
        sweepNextBatch(ShardAndStrategy.conservative(0));
    }

    @Test
    public void sweepHandlesEmptyNamespace() {
        useOneSweepQueueShard();
        WriteReference write = WriteReference.write(TABLE_NONAMESPACE, TEST_CELL);
        writeInTransactionAndGetStartTimestamp(write);
        long timestamp = writeInTransactionAndGetStartTimestamp(write);

        serializableTxManager.setUnreadableTimestamp(timestamp + 1);
        sweepNextBatch(ShardAndStrategy.conservative(0));
    }

    private void put(Transaction txn, WriteReference write) {
        if (write.isTombstone()) {
            txn.delete(write.tableRef(), ImmutableSet.of(write.cell()));
        } else {
            txn.put(write.tableRef(), ImmutableMap.of(write.cell(), TEST_DATA));
        }
    }

    private List<WriteInfo> getEnqueuedWritesNumber(int index) {
        ArgumentCaptor<List<WriteInfo>> writes = ArgumentCaptor.forClass(List.class);
        verify(sweepQueue, atLeast(index)).enqueue(writes.capture());
        return writes.getAllValues().get(index - 1);
    }

    private void assertLatestEntryForCellInKvsAtTimestamp(TableReference tableRef, Cell cell, long ts) {
        assertLatestEntryBeforeTsIs(Long.MAX_VALUE, tableRef, cell, TEST_DATA, ts);
    }

    private void assertLatestEntryBeforeTsIs(long getTs, TableReference tableRef, Cell cell, byte[] data, long ts) {
        Value expected = Value.create(data, ts);
        assertThat(keyValueService.get(tableRef, ImmutableMap.of(cell, getTs))).containsEntry(cell, expected);
    }

    private long putWriteAndFailOnPreCommitConditionReturningStartTimestamp(WriteReference writeRef) {
        AtomicLong startTs = new AtomicLong(0);
        assertThatThrownBy(() -> serializableTxManager.runTaskWithConditionWithRetry(
                        FailingPreCommitCondition::new, (txn, ignore) -> {
                            put(txn, writeRef);
                            startTs.set(txn.getTimestamp());
                            return null;
                        }))
                .isInstanceOf(RuntimeException.class);
        return startTs.get();
    }

    private void assertNoEntryForCellInKvs(TableReference tableRef, Cell cell) {
        assertThat(keyValueService.get(tableRef, ImmutableMap.of(cell, Long.MAX_VALUE)))
                .doesNotContainKey(cell);
    }

    private void useOneSweepQueueShard() {
        sweepQueueShards = 1;
    }

    private void assertOnlySentinelBeforeTs(TableReference tableRef, Cell cell, long ts) {
        Value sentinel = Value.create(PtBytes.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP);
        assertThat(keyValueService.get(tableRef, ImmutableMap.of(cell, ts))).containsEntry(cell, sentinel);
    }

    private Long writeInTransactionAndGetStartTimestamp(WriteReference writeRef) {
        return txManager.runTaskWithRetry(txn -> {
            put(txn, writeRef);
            return txn.getTimestamp();
        });
    }

    private static final class FailingPreCommitCondition implements PreCommitCondition {
        @Override
        public void throwIfConditionInvalid(long timestamp) {
            throw new RuntimeException("test");
        }

        @Override
        public void cleanup() {}
    }

    private void sweepNextBatch(ShardAndStrategy shardStrategy) {
        sweepQueue.sweepNextBatch(shardStrategy, Sweeper.of(shardStrategy).getSweepTimestamp(sweepTimestampSupplier));
    }
}
