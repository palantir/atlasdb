/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.common.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockMakers;

public class CoalescingSupplierTest {
    private static final String PARAMETERIZED_TEST_NAME = "{0}";

    private static final int DEFAULT_VALUE = 123;

    private final Supplier<Integer> delegate =
            mock(Supplier.class, withSettings().mockMaker(MockMakers.SUBCLASS));
    private final FreezableSupplier freezableDelegate = new FreezableSupplier(delegate);
    private final CoalescingSupplier<Integer> coalescing = new CoalescingSupplier<>(freezableDelegate);

    public static List<Arguments> namesAndFactories() {
        return List.of(
                createArguments("blocking", test -> test.coalescing.get()),
                createArguments("async", test -> unwrap(test.coalescing.getAsync())));
    }

    private static Arguments createArguments(String description, Function<CoalescingSupplierTest, Integer> function) {
        return Arguments.of(description, function);
    }

    private static <T> T unwrap(ListenableFuture<T> future) {
        return AtlasFutures.getUnchecked(future);
    }

    @BeforeEach
    public void before() {
        when(delegate.get()).thenReturn(DEFAULT_VALUE);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("namesAndFactories")
    public void delegatesToDelegate(String name, Function<CoalescingSupplierTest, Integer> factory) {
        Supplier<Integer> supplier = () -> factory.apply(this);
        assertThat(supplier.get()).isEqualTo(DEFAULT_VALUE);

        verify(delegate).get();
        verifyNoMoreInteractions(delegate);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("namesAndFactories")
    public void batchesConcurrentRequests(String name, Function<CoalescingSupplierTest, Integer> factory) {
        Supplier<Integer> supplier = () -> factory.apply(this);
        freezableDelegate.freeze();
        getConcurrently(1, supplier);
        AsyncTasks batch = getConcurrently(5, supplier);
        freezableDelegate.unfreeze();

        batch.await();

        // At least some of these requests should be batched. We can't guarantee it will always be 2 though, if
        // we get really unlucky with scheduling.
        verify(delegate, atLeast(2)).get();
        verify(delegate, atMost(5)).get();
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("namesAndFactories")
    @SuppressWarnings("ReturnValueIgnored") // Test relating to properties of a Supplier
    public void doesNotBatchSerialRequests(String name, Function<CoalescingSupplierTest, Integer> factory) {
        Supplier<Integer> supplier = () -> factory.apply(this);
        supplier.get();
        supplier.get();
        supplier.get();

        verify(delegate, times(3)).get();
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("namesAndFactories")
    public void requestsDoNotRecieveOldResults(String name, Function<CoalescingSupplierTest, Integer> factory) {
        Supplier<Integer> supplier = () -> factory.apply(this);
        assertThat(supplier.get()).isEqualTo(DEFAULT_VALUE);

        int value2 = 2;
        when(delegate.get()).thenReturn(value2);
        assertThat(supplier.get()).isEqualTo(value2);
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("namesAndFactories")
    public void exceptionsArePropagated(String name, Function<CoalescingSupplierTest, Integer> factory) {
        Supplier<Integer> supplier = () -> factory.apply(this);
        RuntimeException expected = new RuntimeException("foo");
        when(delegate.get()).thenThrow(expected);

        assertThatThrownBy(supplier::get).hasMessage(expected.getMessage());
    }

    @ParameterizedTest(name = PARAMETERIZED_TEST_NAME)
    @MethodSource("namesAndFactories")
    public void exceptionsArePropagatedForCoalescedCalls(
            String name, Function<CoalescingSupplierTest, Integer> factory) {
        Supplier<Integer> supplier = () -> factory.apply(this);
        RuntimeException expected = new RuntimeException("foo");
        when(delegate.get()).thenThrow(expected);

        freezableDelegate.freeze();
        AsyncTasks tasks = getConcurrently(5, supplier);
        freezableDelegate.unfreeze();

        tasks.assertAllFailed(expected);
    }

    @Test
    public void stressTest() {
        int poolSize = 1024;
        ListeningExecutorService executorService =
                MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(poolSize));
        AtomicLong counter = new AtomicLong(0);
        Supplier<Long> supplier = new CoalescingSupplier<>(() -> {
            sleep(2);
            return counter.incrementAndGet();
        });
        List<ListenableFuture<?>> futures = IntStream.range(0, poolSize)
                .mapToObj(index -> executorService.submit(() -> assertIncreasing(supplier)))
                .collect(Collectors.toList());
        executorService.shutdown();
        Futures.getUnchecked(Futures.allAsList(futures));
    }

    @Test
    public void canCancelReturnedFuture() {
        SettableFuture<Long> future = SettableFuture.create();
        CoalescingSupplier<Long> supplier = new CoalescingSupplier<>(() -> Futures.getUnchecked(future));
        ListenableFuture<Long> returned = supplier.getAsync();
        returned.cancel(true);
        ListenableFuture<Long> after = supplier.getAsync();
        future.set(1L);
        assertThat(Futures.getUnchecked(after)).isOne();
    }

    private static void assertIncreasing(Supplier<Long> supplier) {
        long last = supplier.get();
        for (int i = 0; i < 128; i++) {
            long current = supplier.get();
            if (current > last) {
                last = current;
            } else {
                throw new SafeIllegalStateException(
                        "current > last", SafeArg.of("current", current), SafeArg.of("last", last));
            }
        }
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("ReturnValueIgnored") // Test relating to properties of a Supplier
    private AsyncTasks getConcurrently(int count, Supplier<Integer> supplier) {
        return AsyncTasks.runInParallel(supplier::get, count);
    }

    private static class AsyncTasks {

        private final List<Future<?>> futures;

        AsyncTasks(List<Future<?>> futures) {
            this.futures = futures;
        }

        static AsyncTasks runInParallel(Runnable task, int count) {
            ExecutorService executor = Executors.newCachedThreadPool();
            List<Future<?>> futures = IntStream.range(0, count)
                    .mapToObj(i -> executor.submit(task))
                    .collect(Collectors.toList());

            // give the threads a chance to start
            Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(20));
            return new AsyncTasks(futures);
        }

        public void await() {
            try {
                for (Future<?> future : futures) {
                    future.get();
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        void assertAllFailed(Throwable expectedError) {
            for (Future<?> future : futures) {
                assertThatThrownBy(future::get).hasCause(expectedError);
            }
        }
    }

    private static class FreezableSupplier implements Supplier<Integer> {

        private volatile boolean isFrozen;
        private final Supplier<Integer> delegate;

        FreezableSupplier(Supplier<Integer> delegate) {
            this.delegate = delegate;
        }

        void freeze() {
            isFrozen = true;
        }

        void unfreeze() {
            isFrozen = false;
        }

        @Override
        public Integer get() {
            while (isFrozen) {
                Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(5));
            }

            return delegate.get();
        }
    }
}
