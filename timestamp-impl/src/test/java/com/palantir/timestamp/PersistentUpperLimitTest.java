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
package com.palantir.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;

public class PersistentUpperLimitTest {
    private static final long TIMESTAMP = 12345L;
    private static final long INITIAL_UPPER_LIMIT = 10L;
    private static final long BUFFER = PersistentUpperLimit.BUFFER;

    private TimestampBoundStore boundStore;
    private PersistentUpperLimit upperLimit;

    @Before
    public void setup() {
        boundStore = mock(TimestampBoundStore.class);
        when(boundStore.getUpperLimit()).thenReturn(INITIAL_UPPER_LIMIT);
        upperLimit = new PersistentUpperLimit(boundStore);
    }

    @Test
    public void shouldStartWithTheCurrentStoredLimit() {
        when(boundStore.getUpperLimit()).thenReturn(TIMESTAMP);

        PersistentUpperLimit brandNewUpperLimit = new PersistentUpperLimit(boundStore);

        assertThat(brandNewUpperLimit.get()).isEqualTo(TIMESTAMP);
    }

    @Test
    public void shouldOnlyMakeOneGetCallToTheUnderlyingStore() {
        upperLimit.get();
        upperLimit.get();

        verify(boundStore, times(1)).getUpperLimit();
    }

    @Test
    public void shouldIncreaseTheUpperLimitIfTheNewLimitIsBigger() {
        upperLimit.increaseToAtLeast(TIMESTAMP);

        long biggerLimit = upperLimit.get() + 1000;
        upperLimit.increaseToAtLeast(biggerLimit);
        assertThat(upperLimit.get()).isEqualTo(biggerLimit + BUFFER);
    }

    @Test
    public void shouldNotIncreaseTheUpperLimitWithBufferIfTheNewLimitIsEqual() {
        upperLimit.increaseToAtLeast(TIMESTAMP);

        upperLimit.increaseToAtLeast(upperLimit.get());
        assertThat(upperLimit.get()).isEqualTo(TIMESTAMP + BUFFER);
    }

    @Test
    public void shouldNotIncreaseTheUpperLimitIfTheNewLimitIsSmaller() {
        upperLimit.increaseToAtLeast(TIMESTAMP);

        upperLimit.increaseToAtLeast(TIMESTAMP - 1000);
        assertThat(upperLimit.get()).isEqualTo(TIMESTAMP + BUFFER);
    }

    @Test
    public void shouldNotIncreaseTheUpperLimitIfWouldOverflow() {
        assertThatExceptionOfType(ArithmeticException.class)
                .isThrownBy(() -> upperLimit.increaseToAtLeast(Long.MAX_VALUE - BUFFER + 1));
    }

    @Test
    public void shouldPersistAnIncreasedTimestamp() {
        upperLimit.increaseToAtLeast(TIMESTAMP);

        long biggerLimit = upperLimit.get() + 1000;
        upperLimit.increaseToAtLeast(biggerLimit);
        verify(boundStore).storeUpperLimit(biggerLimit + BUFFER);
    }

    @Test
    public void shouldNotChangeTheCurrentUpperLimitIfItFailsToPersist() {
        doThrow(RuntimeException.class).when(boundStore).storeUpperLimit(anyLong());

        try {
            upperLimit.increaseToAtLeast(INITIAL_UPPER_LIMIT + 10);
        } catch (Exception e) {
            // We expect this to throw
        }

        assertThat(upperLimit.get()).isEqualTo(INITIAL_UPPER_LIMIT);
    }

    @Test
    public void shouldSurviveStressTest() throws Exception {
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newWorkStealingPool());
        try {
            Stopwatch timer = Stopwatch.createStarted();
            AtomicLong expected = new AtomicLong(TIMESTAMP);
            upperLimit.increaseToAtLeast(expected.get());
            assertThat(upperLimit.get()).isGreaterThanOrEqualTo(TIMESTAMP);

            final int iterations = 1_000_000;
            List<ListenableFuture<?>> futures = new ArrayList<>(iterations);
            for (int i = 0; i < iterations; i++) {
                futures.add(executor.submit(() -> {
                    long newLimit = expected.addAndGet(123);
                    upperLimit.increaseToAtLeast(newLimit);
                    assertThat(upperLimit.get()).isGreaterThanOrEqualTo(newLimit);
                }));
            }
            executor.shutdown();
            assertThat(Futures.allAsList(futures).get()).hasSize(iterations);
            assertThat(upperLimit.get()).isGreaterThanOrEqualTo(expected.get());
            assertThat(timer.stop().elapsed()).isLessThan(Duration.ofSeconds(10));
        } finally {
            executor.shutdown();
            assertThat(executor.shutdownNow()).isEmpty();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS))
                    .as("PersistentTimestamp increase tasks did not finish")
                    .isTrue();
        }
    }
}
