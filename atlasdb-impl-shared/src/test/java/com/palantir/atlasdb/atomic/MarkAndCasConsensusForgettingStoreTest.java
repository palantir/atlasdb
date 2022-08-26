/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.atomic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.junit.Test;

public class MarkAndCasConsensusForgettingStoreTest {
    private static final byte[] SAD = PtBytes.toBytes("sad");
    private static final byte[] HAPPY = PtBytes.toBytes("happy");

    private static final ByteBuffer BUFFERED_SAD = ByteBuffer.wrap(SAD);
    private static final ByteBuffer BUFFERED_HAPPY = ByteBuffer.wrap(HAPPY);

    private static final Cell CELL = Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("col1"));
    private static final Cell CELL_2 = Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("col2"));
    public static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");

    private static final byte[] IN_PROGRESS_MARKER = new byte[] {0};
    private static final ByteBuffer BUFFERED_IN_PROGRESS_MARKER = ByteBuffer.wrap(IN_PROGRESS_MARKER);

    private final InMemoryKeyValueService kvs = spy(new InMemoryKeyValueService(true));
    private final MarkAndCasConsensusForgettingStore store =
            new MarkAndCasConsensusForgettingStore(IN_PROGRESS_MARKER, kvs, TABLE);

    @Test
    public void canMarkCell() throws ExecutionException, InterruptedException {
        store.mark(CELL);
        assertThat(store.get(CELL).get()).hasValue(IN_PROGRESS_MARKER);
        assertThat(kvs.getAllTimestamps(TABLE, ImmutableSet.of(CELL), Long.MAX_VALUE)
                        .size())
                .isEqualTo(1);
    }

    @Test
    public void updatesMarkedCell() throws ExecutionException, InterruptedException {
        store.mark(CELL);
        store.processBatch(
                kvs, TABLE, ImmutableList.of(TestBatchElement.of(CELL, BUFFERED_IN_PROGRESS_MARKER, BUFFERED_HAPPY)));
        assertThat(store.get(CELL).get()).hasValue(HAPPY);
    }

    @Test
    public void updatesMultipleMarkedCells() throws ExecutionException, InterruptedException {
        store.mark(ImmutableSet.of(CELL, CELL_2));

        store.atomicUpdate(ImmutableMap.of(CELL, HAPPY, CELL_2, HAPPY));
        TestBatchElement elem1 = TestBatchElement.of(CELL, BUFFERED_IN_PROGRESS_MARKER, BUFFERED_HAPPY);
        TestBatchElement elem2 = TestBatchElement.of(CELL_2, BUFFERED_IN_PROGRESS_MARKER, BUFFERED_SAD);

        store.processBatch(kvs, TABLE, ImmutableList.of(elem1, elem2));
        assertThat(store.get(CELL).get()).hasValue(elem1.argument().update().array());
        assertThat(store.get(CELL_2).get()).hasValue(elem2.argument().update().array());
    }

    @Test
    public void cannotUpdateUnmarkedCell() throws ExecutionException, InterruptedException {
        TestBatchElement element = TestBatchElement.of(CELL, BUFFERED_IN_PROGRESS_MARKER, BUFFERED_HAPPY);
        store.processBatch(kvs, TABLE, ImmutableList.of(element));
        assertThatThrownBy(() -> element.result().get())
                .hasCauseInstanceOf(CheckAndSetException.class)
                .hasMessageContaining(
                        "Unexpected value observed in table test.table. If this is happening repeatedly, your program"
                                + " may be out of sync with the database");
        assertThat(store.get(CELL).get()).isEmpty();
    }

    @Test
    public void coalescesMultipleMcasCalls() throws ExecutionException, InterruptedException {
        store.mark(CELL);

        List<BatchElement<MarkAndCasConsensusForgettingStore.CasRequest, Void>> requests = IntStream.range(0, 100)
                .mapToObj(idx -> TestBatchElement.of(CELL, BUFFERED_IN_PROGRESS_MARKER, BUFFERED_HAPPY))
                .collect(Collectors.toList());
        store.processBatch(kvs, TABLE, requests);
        verify(kvs).multiCheckAndSet(any());
        assertThat(store.get(CELL).get()).hasValue(HAPPY);
        requests.forEach(req -> assertThatCode(() -> req.result().get()).doesNotThrowAnyException());
    }

    @Test
    public void choosesCommitOverAbort() throws ExecutionException, InterruptedException {
        store.mark(CELL);
        ByteBuffer commitVal = ByteBuffer.wrap(new byte[] {9, 23, 45, 27, 0});
        TestBatchElement commit = TestBatchElement.of(CELL, BUFFERED_IN_PROGRESS_MARKER, commitVal);
        TestBatchElement abort = TestBatchElement.of(
                CELL,
                BUFFERED_IN_PROGRESS_MARKER,
                ByteBuffer.wrap(TransactionConstants.TICKETS_ENCODING_ABORTED_TRANSACTION_VALUE));

        store.processBatch(kvs, TABLE, ImmutableList.of(commit, abort));
        verify(kvs).multiCheckAndSet(any());
        assertThat(store.get(CELL).get()).hasValue(commitVal.array());
    }

    @SuppressWarnings("immutables:subtype")
    @org.immutables.value.Value.Immutable
    interface TestBatchElement extends BatchElement<MarkAndCasConsensusForgettingStore.CasRequest, Void> {
        @org.immutables.value.Value.Parameter
        @Nullable
        @Override
        MarkAndCasConsensusForgettingStore.CasRequest argument();

        @org.immutables.value.Value.Parameter
        @Override
        DisruptorAutobatcher.DisruptorFuture<Void> result();

        static TestBatchElement of(Cell cell, ByteBuffer expected, ByteBuffer update) {
            return ImmutableTestBatchElement.builder()
                    .argument(ImmutableCasRequest.of(cell, expected, update))
                    .result(new DisruptorAutobatcher.DisruptorFuture<>("test"))
                    .build();
        }
    }
}
