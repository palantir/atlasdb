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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

public class TimedConsensusForgettingStoreTest {
    private static final Cell CELL = Cell.create(PtBytes.toBytes("roh"), PtBytes.toBytes("kaulem"));
    private static final byte[] VALUE = PtBytes.toBytes("valyew");

    private final ConsensusForgettingStore delegate = mock(ConsensusForgettingStore.class);
    private final TaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
    private final ConsensusForgettingStoreMetrics metrics = ConsensusForgettingStoreMetrics.of(registry);
    private final AtomicInteger concurrentOperationTracker = new AtomicInteger(0);
    private final ConsensusForgettingStore consensusForgettingStore =
            new TimedConsensusForgettingStore(delegate, metrics, concurrentOperationTracker);

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
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
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
        CountDownLatch latch = new CountDownLatch(iterations);
        CyclicBarrier barrier = new CyclicBarrier(2);

        doAnswer(invocation -> {
                    latch.countDown();
                    barrier.await();
                    latch.await();
                    return null;
                })
                .when(delegate)
                .checkAndTouch(any(), any());

        ExecutorService executorService = PTExecutors.newFixedThreadPool(iterations);
        for (int iteration = 1; iteration <= iterations; iteration++) {
            executorService.submit(() -> consensusForgettingStore.checkAndTouch(CELL, VALUE));
            barrier.await();
            if (iteration != iterations) {
                // On the last iteration, they may have completed the task after clearing the barrier.
                // This can be solved by introducing an additional barrier (and/or making the latch have iteration +
                // 1 counts and countign down here), but seems unnecessarily complex.
                assertThat(concurrentOperationTracker.get()).isEqualTo(iteration);
            }
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
}
