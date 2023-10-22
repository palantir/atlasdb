/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class LocalWriteBufferTest {
    private static final TableReference TABLE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "test-table");
    private static final TableReference TABLE_2 = TableReference.create(Namespace.DEFAULT_NAMESPACE, "test-table2");
    private static final Cell CELL_1 = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("col1"));
    private static final Cell CELL_2 = Cell.create(PtBytes.toBytes("row2"), PtBytes.toBytes("col2"));
    private static final byte[] VALUE_1 = PtBytes.toBytes(1L);
    private static final byte[] VALUE_2 = PtBytes.toBytes(2L);
    private static final ChangeMetadata METADATA_1 = ChangeMetadata.deleted(PtBytes.toBytes(1L));
    private static final ChangeMetadata METADATA_2 = ChangeMetadata.created(PtBytes.toBytes(2L));

    private static final ExecutorService DIRECT_EXECUTOR = MoreExecutors.newDirectExecutorService();

    private final LocalWriteBuffer buffer = new LocalWriteBuffer();

    @Test
    public void canPutValueWithoutMetadata() {
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of());

        assertThat(buffer.getLocalWritesForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, VALUE_1));
        assertThat(buffer.getChangeMetadataForTable(TABLE)).isEmpty();
    }

    @Test
    public void canPutValueWithMetadata() {
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of(CELL_1, METADATA_1));

        assertThat(buffer.getLocalWritesForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, VALUE_1));
        assertThat(buffer.getChangeMetadataForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, METADATA_1));
    }

    @Test
    public void canPutMetadataForSubsetOfWrites() {
        buffer.putLocalWritesAndMetadata(
                TABLE, ImmutableMap.of(CELL_1, VALUE_1, CELL_2, VALUE_2), ImmutableMap.of(CELL_1, METADATA_1));

        assertThat(buffer.getLocalWritesForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, VALUE_1, CELL_2, VALUE_2));
        assertThat(buffer.getChangeMetadataForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, METADATA_1));
    }

    @Test
    public void canOverwriteValueAndMetadata() {
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of(CELL_1, METADATA_1));
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_2), ImmutableMap.of(CELL_1, METADATA_2));

        assertThat(buffer.getLocalWritesForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, VALUE_2));
        assertThat(buffer.getChangeMetadataForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, METADATA_2));
    }

    @Test
    public void writingWithoutMetadataRemovesMetadata() {
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of(CELL_1, METADATA_1));
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_2), ImmutableMap.of());

        assertThat(buffer.getLocalWritesForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, VALUE_2));
        assertThat(buffer.getChangeMetadataForTable(TABLE)).isEmpty();
    }

    @Test
    public void canStoreValuesAndMetadataForMultipleCellsAndTables() {
        buffer.putLocalWritesAndMetadata(
                TABLE,
                ImmutableMap.of(CELL_1, VALUE_1, CELL_2, VALUE_2),
                ImmutableMap.of(CELL_1, METADATA_1, CELL_2, METADATA_2));
        buffer.putLocalWritesAndMetadata(
                TABLE_2,
                ImmutableMap.of(CELL_1, VALUE_1, CELL_2, VALUE_2),
                ImmutableMap.of(CELL_1, METADATA_1, CELL_2, METADATA_2));

        assertThat(buffer.getLocalWritesForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, VALUE_1, CELL_2, VALUE_2));
        assertThat(buffer.getLocalWritesForTable(TABLE_2))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, VALUE_1, CELL_2, VALUE_2));
        assertThat(buffer.getChangeMetadataForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, METADATA_1, CELL_2, METADATA_2));
        assertThat(buffer.getChangeMetadataForTable(TABLE_2))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, METADATA_1, CELL_2, METADATA_2));
    }

    @Test
    public void cannotPutMetadataWithoutAnyWrite() {
        assertThatPutThrowsForAllMetadataDueToMissingWrite(ImmutableMap.of(), ImmutableMap.of(CELL_1, METADATA_1));
    }

    @Test
    public void cannotPutMetadataWithoutAssociatedWrite() {
        assertThatPutThrowsForAllMetadataDueToMissingWrite(
                ImmutableMap.of(CELL_2, VALUE_2), ImmutableMap.of(CELL_1, METADATA_1));
    }

    /**
     * This test would fail for a previous synchronization model for {@link LocalWriteBuffer#putLocalWritesAndMetadata},
     * where we used {@link ConcurrentSkipListMap#compute} in the following way:
     *
     * <pre> {@code
     *  writes.compute(cell, (k, oldVal) -> {
     *                 if (hasMetadata) {
     *                     metadataForWrites.put(cell, metadata.get(cell));
     *                 } else {
     *                     metadataForWrites.remove(cell);
     *                 }
     *                 return val;
     *             });
     * }</pre>
     * <p>
     * {@link ConcurrentSkipListMap#compute} uses {@link AtomicReference#compareAndSet} to test whether another thread
     * has set the value in the meantime. It uses the old value for the key read before calling
     * {@link ConcurrentSkipListMap#compute} to decide whether it should retry, including re-computation of the value.
     * <p>
     * Let Cell C hold value A and no metadata and the following sequence involving two threads occurs:
     * <pre> {@code
     * |                   T1                   |                    T2                   |
     * |:--------------------------------------:|:---------------------------------------:|
     * | read A as old value                    |                                         |
     * | putMetadata(C, X)                      |                                         |
     * |                                        | read A as old value                     |
     * |                                        | putMetadata(C, Y)                       |
     * |                                        | compareAndSet(expected: A, newValue: A) |
     * | compareAndSet(expected:A, newValue: B) |                                         |
     * }</pre>
     * Because T2 did not modify the value of C, the {@link AtomicReference#compareAndSet} in T1 succeeds.
     * At the end of this sequence, C has value B and metadata Y, which violates the atomicity guarantee.
     */
    @Test
    public void writingValueAndMetadataIsAtomicWhenOverwritingWithSameValue()
            throws InterruptedException, ExecutionException, TimeoutException {
        int numIterations = 1000;
        long randomSeed = System.currentTimeMillis();
        Random random = new Random(randomSeed);

        for (int i = 0; i < numIterations; i++) {
            buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of());

            CountDownLatch latch = new CountDownLatch(2);
            Callable<Void> task1 = () -> {
                latch.countDown();
                buffer.putLocalWritesAndMetadata(
                        TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of(CELL_1, METADATA_1));
                return null;
            };
            Callable<Void> task2 = () -> {
                latch.countDown();
                buffer.putLocalWritesAndMetadata(
                        TABLE, ImmutableMap.of(CELL_1, VALUE_2), ImmutableMap.of(CELL_1, METADATA_2));
                return null;
            };

            ListeningExecutorService concurrentExecutor =
                    MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(2));

            List<ListenableFuture<Void>> futures = (random.nextBoolean()
                            ? ImmutableList.of(task1, task2)
                            : ImmutableList.of(task2, task1))
                    .stream().map(concurrentExecutor::submit).collect(Collectors.toUnmodifiableList());
            Futures.whenAllSucceed(futures).call(() -> null, DIRECT_EXECUTOR).get(20, TimeUnit.SECONDS);

            byte[] currentValue = buffer.getLocalWritesForTable(TABLE).get(CELL_1);
            ChangeMetadata currentMetadata =
                    buffer.getChangeMetadataForTable(TABLE).get(CELL_1);
            assertThat((Arrays.equals(currentValue, VALUE_1) && currentMetadata.equals(METADATA_1))
                            || (Arrays.equals(currentValue, VALUE_2) && currentMetadata.equals(METADATA_2)))
                    .as("Atomicity guarantee violated on iteration %d with seed %d", i, randomSeed)
                    .isTrue();
        }
    }

    @Test
    public void valueByteCountIsUpdatedCorrectlyWhenOverWritingValue() {
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of());
        assertThat(buffer.getValuesByteCount()).isEqualTo(Cells.getApproxSizeOfCell(CELL_1) + VALUE_1.length);

        byte[] otherValue = PtBytes.toBytes("some long string");
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, otherValue), ImmutableMap.of());
        assertThat(buffer.getValuesByteCount()).isEqualTo(Cells.getApproxSizeOfCell(CELL_1) + otherValue.length);
    }

    @Test
    public void valueByteCountIsUpdatedCorrectlyWhenDeletingValue() {
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of());
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, new byte[0]), ImmutableMap.of());

        assertThat(buffer.getValuesByteCount()).isEqualTo(Cells.getApproxSizeOfCell(CELL_1));
    }

    @Test
    public void valueByteCountIsMaintainedAcrossMultipleTables() {
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of());
        buffer.putLocalWritesAndMetadata(TABLE_2, ImmutableMap.of(CELL_2, VALUE_2), ImmutableMap.of());

        assertThat(buffer.getValuesByteCount())
                .isEqualTo(Cells.getApproxSizeOfCell(CELL_1)
                        + VALUE_1.length
                        + Cells.getApproxSizeOfCell(CELL_2)
                        + VALUE_2.length);
    }

    @Test
    public void changeMetadataCountIsCorrect() {
        buffer.putLocalWritesAndMetadata(
                TABLE,
                ImmutableMap.of(CELL_1, VALUE_1, CELL_2, VALUE_2),
                ImmutableMap.of(CELL_1, METADATA_1, CELL_2, METADATA_2));
        buffer.putLocalWritesAndMetadata(TABLE_2, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of());

        assertThat(buffer.changeMetadataCount()).isEqualTo(2);
    }

    private void assertThatPutThrowsForAllMetadataDueToMissingWrite(
            Map<Cell, byte[]> values, Map<Cell, ChangeMetadata> metadata) {
        assertThatLoggableExceptionThrownBy(() -> buffer.putLocalWritesAndMetadata(TABLE, values, metadata))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("Every metadata we put must be associated with a write")
                .hasExactlyArgs(LoggingArgs.tableRef(TABLE), UnsafeArg.of("cellsWithOnlyMetadata", metadata.keySet()));
    }
}
