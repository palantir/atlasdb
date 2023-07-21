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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class LocalWriteBufferTest {
    private static final TableReference TABLE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "test-table");
    private static final Cell CELL_1 = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("col1"));
    private static final byte[] VALUE_1 = PtBytes.toBytes(1L);
    private static final byte[] VALUE_2 = PtBytes.toBytes(2L);
    private static final ChangeMetadata METADATA_1 = ChangeMetadata.deleted(PtBytes.toBytes(1L));
    private static final ChangeMetadata METADATA_2 = ChangeMetadata.created(PtBytes.toBytes(2L));

    private final LocalWriteBuffer buffer = new LocalWriteBuffer();

    @Test
    public void canPutValueWithoutMetadata() {
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of());

        assertThat(buffer.getLocalWritesForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, VALUE_1));
        assertThat(buffer.getChangeMetadataForWritesToTable(TABLE)).isEmpty();
    }

    @Test
    public void canPutValueWithMetadata() {
        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of(CELL_1, METADATA_1));

        assertThat(buffer.getLocalWritesForTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, VALUE_1));
        assertThat(buffer.getChangeMetadataForWritesToTable(TABLE))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(CELL_1, METADATA_1));
    }

    @Test
    public void cannotPutMetadataWithoutAssociatedWrite() {
        assertThatLoggableExceptionThrownBy(() ->
                        buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(), ImmutableMap.of(CELL_1, METADATA_1)))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("Every metadata we put must be associated with a write")
                .hasExactlyArgs(
                        UnsafeArg.of("tableRef", TABLE),
                        UnsafeArg.of("cellsWithOnlyMetadata", ImmutableSet.of(CELL_1)));
    }

    /**
     * This test would fail for a previous synchronization model for {@link LocalWriteBuffer#putLocalWritesAndMetadata},
     * where we used {@link ConcurrentSkipListMap#compute} in the following way:
     *
     * <pre> {@code
     *  writes.compute(cell, (k, oldVal) -> {
     *                 previousValue.set(oldVal);
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
     * has set the value in the meantime. If the comparison against the value read before calling
     * {@link ConcurrentSkipListMap#compute} fails, we retry.
     * <p>
     * Now, imagine Cell C holds value A and no metadata and the following sequence involving two threads occurs:
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
     * At the end, C has value B and metadata Y, which violates the atomicity guarantee.
     */
    @Test
    public void writingValueAndMetadataIsAtomicWhenOverwritingWithSameValue() throws InterruptedException {
        int numIterations = 1000;
        long randomSeed = System.currentTimeMillis();
        Random random = new Random(randomSeed);

        for (int i = 0; i < numIterations; i++) {
            buffer.putLocalWritesAndMetadata(TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of());
            Thread writeWithoutMetadata = new Thread(() -> {
                // Sleep is needed to trigger the AAB problem reliably (if it can happen)
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                buffer.putLocalWritesAndMetadata(
                        TABLE, ImmutableMap.of(CELL_1, VALUE_1), ImmutableMap.of(CELL_1, METADATA_1));
            });
            Thread writeWithMetadata = new Thread(() -> {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                buffer.putLocalWritesAndMetadata(
                        TABLE, ImmutableMap.of(CELL_1, VALUE_2), ImmutableMap.of(CELL_1, METADATA_2));
            });

            if (random.nextBoolean()) {
                writeWithMetadata.start();
                writeWithoutMetadata.start();
            } else {
                writeWithoutMetadata.start();
                writeWithMetadata.start();
            }
            writeWithMetadata.join();
            writeWithoutMetadata.join();

            byte[] currentValue = buffer.getLocalWritesForTable(TABLE).get(CELL_1);
            ChangeMetadata currentMetadata =
                    buffer.getChangeMetadataForWritesToTable(TABLE).get(CELL_1);
            assertThat((Arrays.equals(currentValue, VALUE_1) && currentMetadata.equals(METADATA_1))
                            || (Arrays.equals(currentValue, VALUE_2) && currentMetadata.equals(METADATA_2)))
                    .as("Atomicity guarantee violated on iteration %d with seed %d", i, randomSeed)
                    .isTrue();
        }
    }
}
