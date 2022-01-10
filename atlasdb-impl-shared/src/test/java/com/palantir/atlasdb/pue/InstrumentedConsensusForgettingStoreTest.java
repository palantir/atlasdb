/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.pue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class InstrumentedConsensusForgettingStoreTest {
    private static final Cell CELL = Cell.create(PtBytes.toBytes("roh"), PtBytes.toBytes("kaulem-won"));
    private static final Cell CELL_2 = Cell.create(PtBytes.toBytes("roh"), PtBytes.toBytes("kaulem-too"));
    private static final byte[] VALUE = PtBytes.toBytes("valyew");

    private final ConsensusForgettingStore delegate = mock(ConsensusForgettingStore.class);
    private final TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
    private final ConsensusForgettingStoreMetrics metrics = ConsensusForgettingStoreMetrics.of(registry);
    private final AtomicInteger concurrentOperationTracker = new AtomicInteger(0);
    private final ConsensusForgettingStore consensusForgettingStore =
            new InstrumentedConsensusForgettingStore(delegate, metrics, concurrentOperationTracker);

    @Before
    public void before() {
        metrics.concurrentCheckAndTouches(concurrentOperationTracker::get);
    }

    @Test
    public void forwardsSimpleCallsToDelegate() throws ExecutionException, InterruptedException {
        consensusForgettingStore.putUnlessExists(CELL, VALUE);
        verify(delegate).putUnlessExists(CELL, VALUE);

        when(delegate.get(CELL)).thenReturn(Futures.immediateFuture(Optional.of(VALUE)));
        assertThat(consensusForgettingStore.get(CELL).get()).contains(VALUE);
        verify(delegate).get(CELL);
    }

    @Test
    public void timesCheckAndTouchOperations() {
        consensusForgettingStore.checkAndTouch(CELL, VALUE);
        assertThat(metrics.checkAndTouch().getCount()).isEqualTo(1);

        consensusForgettingStore.checkAndTouch(CELL, VALUE);
        assertThat(metrics.checkAndTouch().getCount()).isEqualTo(2);

        doAnswer(invocation -> {
                    Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(100));
                    return null;
                })
                .when(delegate)
                .checkAndTouch(any(), any());
        consensusForgettingStore.checkAndTouch(CELL, VALUE);
        assertThat(metrics.checkAndTouch().getCount()).isEqualTo(3);

        // Metric registries use microseconds internally
        assertThat(metrics.checkAndTouch().getSnapshot().getMax())
                .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toMicros(100));
    }

    @Test
    public void tracksConcurrentCheckAndTouchOperations() throws BrokenBarrierException, InterruptedException {
        int iterations = 5;
        CountDownLatch latchEnsuringAllTasksAreConcurrent = new CountDownLatch(iterations);
        CyclicBarrier barrierEnsuringOperationHasBegun = new CyclicBarrier(2);

        doAnswer(invocation -> {
                    latchEnsuringAllTasksAreConcurrent.countDown();
                    barrierEnsuringOperationHasBegun.await();
                    latchEnsuringAllTasksAreConcurrent.await();
                    return null;
                })
                .when(delegate)
                .checkAndTouch(any(), any());

        ExecutorService executorService = PTExecutors.newFixedThreadPool(iterations);
        try {
            for (int iteration = 1; iteration <= iterations; iteration++) {
                executorService.execute(() -> consensusForgettingStore.checkAndTouch(CELL, VALUE));
                barrierEnsuringOperationHasBegun.await();
                if (iteration != iterations) {
                    // On the last iteration, they may have completed the task after clearing the barrier.
                    // This can be solved by introducing an additional barrier (and/or making the latch have iteration +
                    // 1 counts and counting down here), but seems unnecessarily complex.
                    assertThat(concurrentOperationTracker.get()).isEqualTo(iteration);
                }
            }
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void batchedCheckAndTouchIsConsideredAnOngoingOperation() throws InterruptedException {
        CountDownLatch latchEnsuringTaskStarted = new CountDownLatch(1);
        CountDownLatch latchEnsuringTaskStillRunning = new CountDownLatch(1);
        doAnswer(invocation -> {
                    latchEnsuringTaskStarted.countDown();
                    latchEnsuringTaskStillRunning.await();
                    return null;
                })
                .when(delegate)
                .checkAndTouch(any());

        ExecutorService executorService = PTExecutors.newSingleThreadExecutor();
        try {
            executorService.execute(
                    () -> consensusForgettingStore.checkAndTouch(ImmutableMap.of(CELL, VALUE, CELL_2, VALUE)));
            latchEnsuringTaskStarted.await();
            assertThat(concurrentOperationTracker.get()).isEqualTo(1);
            latchEnsuringTaskStillRunning.countDown();
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void recognisesExceptionAsAbortingOperation() {
        RuntimeException failure = new RuntimeException("nonono");
        doThrow(failure).when(delegate).checkAndTouch(any(), any());

        assertThatThrownBy(() -> consensusForgettingStore.checkAndTouch(CELL, VALUE))
                .isEqualTo(failure);
        assertThat(concurrentOperationTracker.get()).isEqualTo(0);
    }

    @Test
    public void sizeOfBatchesIsTracked() {
        consensusForgettingStore.checkAndTouch(ImmutableMap.of(CELL, VALUE, CELL_2, VALUE));
        assertThat(metrics.batchedCheckAndTouchSize().getCount()).isEqualTo(1);
        assertThat(metrics.batchedCheckAndTouchSize().getSnapshot().getMax()).isEqualTo(2);

        consensusForgettingStore.checkAndTouch(ImmutableMap.of(CELL, VALUE));
        consensusForgettingStore.checkAndTouch(
                KeyedStream.of(IntStream.range(0, 50).boxed())
                        .mapKeys(index -> Cell.create(PtBytes.toBytes("ruh roh"), PtBytes.toBytes(index)))
                        .map(_unused -> VALUE)
                        .collectToMap());
        assertThat(metrics.batchedCheckAndTouchSize().getCount()).isEqualTo(3);
        assertThat(metrics.batchedCheckAndTouchSize().getSnapshot().getValues()).containsExactlyInAnyOrder(1, 2, 50);
    }
}
