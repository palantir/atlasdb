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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import org.junit.Test;

public class MarkAndCasConsensusForgettingStoreTest {
    private static final byte[] SAD = PtBytes.toBytes("sad");
    private static final byte[] HAPPY = PtBytes.toBytes("happy");
    private static final Cell CELL = Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("col1"));
    private static final Cell CELL_2 = Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("col2"));
    public static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final byte[] IN_PROGRESS_MARKER = new byte[] {0};

    private final InMemoryKeyValueService kvs = spy(new InMemoryKeyValueService(true));
    private final MarkAndCasConsensusForgettingStore store =
            new MarkAndCasConsensusForgettingStore(IN_PROGRESS_MARKER, kvs, TABLE);
//    private final CasCoalescingFunction casCoalescingFunction =

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
        store.atomicUpdate(CELL, HAPPY);
        assertThat(store.get(CELL).get()).hasValue(HAPPY);
    }

    @Test
    public void updatesMultipleMarkedCells() throws ExecutionException, InterruptedException {
        store.mark(ImmutableSet.of(CELL, CELL_2));

        store.atomicUpdate(ImmutableMap.of(CELL, HAPPY, CELL_2, HAPPY));
        assertThat(store.get(CELL).get()).hasValue(HAPPY);
        assertThat(store.get(CELL_2).get()).hasValue(HAPPY);
    }

    @Test
    public void cannotUpdateUnmarkedCell() throws ExecutionException, InterruptedException {
        assertThatThrownBy(() -> store.atomicUpdate(CELL, SAD))
                .isInstanceOf(CheckAndSetException.class)
                .hasMessageContaining(
                        "Unexpected value observed in table test.table. If this is happening repeatedly, your program"
                                + " may be out of sync with the database");
        assertThat(store.get(CELL).get()).isEmpty();
    }

    @Test
    public void coalescesMultipleMcasCalls() throws ExecutionException, InterruptedException {
        KeyValueService kvs = mock(KeyValueService.class);
        MarkAndCasConsensusForgettingStore store =
                new MarkAndCasConsensusForgettingStore(IN_PROGRESS_MARKER, kvs, TABLE);

        ExecutorService service = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(100);
        store.mark(CELL);
        when(kvs.get(any(), any())).thenReturn(ImmutableMap.of(CELL, Value.create(IN_PROGRESS_MARKER, 0)));

        byte[] stagingCommitTs = {1, 1, 1, 0};

        for (int i = 0; i < 100; i++) {
            service.execute(() -> {
                try {
                    latch.await();
                    store.atomicUpdate(CELL, stagingCommitTs);
                } catch (InterruptedException e) {
                    System.out.println(e);
                } finally {
                    done.countDown();
                }
            });
        }
        latch.countDown();
        done.await();
        verify(kvs).multiCheckAndSet(any());
        assertThat(store.get(CELL).get()).hasValue(IN_PROGRESS_MARKER);
    }

    @Test
    public void choosesCommitOverAbort() throws ExecutionException, InterruptedException {
        store.mark(CELL);
        ExecutorService service = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        byte[] stagingCommitTs = {1, 1, 1, 0};

        service.execute(() -> {
            try {
                latch.await();
                store.atomicUpdate(CELL, stagingCommitTs);
            } catch (InterruptedException e) {
                // Do nothing
            } finally {
                done.countDown();
            }
        });

//        service.execute(() -> {
//            try {
//                latch.await();
//                store.atomicUpdate(CELL, TransactionConstants.TICKETS_ENCODING_ABORTED_TRANSACTION_VALUE);
//            } catch (InterruptedException e) {
//                // Do nothing
//            } finally {
//                done.countDown();
//            }
//        });

        latch.countDown();
        done.await();
//        verify(kvs).multiCheckAndSet(any());
        assertThat(store.get(CELL).get()).hasValue(stagingCommitTs);
    }
}
