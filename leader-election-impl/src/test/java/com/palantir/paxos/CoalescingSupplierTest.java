/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.paxos;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

public class CoalescingSupplierTest {

    private static final int DEFAULT_VALUE = 123;

    private final DeterministicScheduler executor = new DeterministicScheduler();
    private final Supplier<Integer> delegate = mock(Supplier.class);
    private final FreezableSupplier freezableDelegate = new FreezableSupplier(delegate);
    private final CoalescingSupplier<Integer> supplier = new CoalescingSupplier<>(freezableDelegate);

    @Before
    public void before() {
        when(delegate.get()).thenReturn(DEFAULT_VALUE);
    }

    @Test
    public void delegates_to_delegate() {
        assertThat(supplier.get()).isEqualTo(DEFAULT_VALUE);
    }

    @Test
    public void batchesConcurrentRequests() throws InterruptedException {
        freezableDelegate.freeze();
        AsyncTasks tasks = getConcurrently(5);
        freezableDelegate.unfreeze();

        tasks.await();

        verify(delegate, times(2)).get();
    }


    @Test
    public void doesNotBatchSerialRequests() {
        supplier.get();
        supplier.get();
        supplier.get();

        verify(delegate, times(3)).get();
    }

    @Test
    public void requestsDoNotRecieveOldResults() {
        assertThat(supplier.get()).isEqualTo(DEFAULT_VALUE);

        int value2 = 2;
        when(delegate.get()).thenReturn(value2);
        assertThat(supplier.get()).isEqualTo(value2);
    }

    @Test
    public void exceptionsArePropagated() {
        RuntimeException expected = new RuntimeException("foo");
        when(delegate.get()).thenThrow(expected);

        assertThatThrownBy(() -> supplier.get()).isEqualTo(expected);
    }

    @Test
    public void exceptionsArePropagatedForCoalescedCalls() {
        RuntimeException expected = new RuntimeException("foo");
        when(delegate.get()).thenThrow(expected);

        freezableDelegate.freeze();
        AsyncTasks tasks = getConcurrently(5);
        freezableDelegate.unfreeze();

        tasks.assertAllFailed(expected);
    }

//    @Test
//    public void perf() throws InterruptedException {
//
//        Supplier<Long> actualSupplier = () -> {
//            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
//            return 123L;
//        };
//
//        CoalescingSupplier<Long> coalescingSupplier = new CoalescingSupplier<>(
//                actualSupplier,
//                Executors.newSingleThreadExecutor());
//
//        ExecutorService executor = Executors.newCachedThreadPool();
//        int concurrency = 4;
//        while(true) {
//            Stopwatch timer = Stopwatch.createStarted();
//            CountDownLatch latch = new CountDownLatch(10_000);
//            for (int i = 0; i < concurrency; i++) {
//                executor.submit(() -> {
//                    while (latch.getCount() > 0) {
//                        try {
//                            coalescingSupplier.get().get();
//                        } catch (Throwable e) {
//                            e.printStackTrace();
//                        }
//                        latch.countDown();
//                    }
//                });
//            }
//            latch.await();
//            System.out.println("concurrency = " + concurrency + "; time = " + timer.elapsed(TimeUnit.MILLISECONDS));
//        }
//
//    }

    private AsyncTasks getConcurrently(int count) {
        return AsyncTasks.runInParallel(supplier::get, count);
    }

    private static class AsyncTasks {

        private final List<Future<?>> futures;

        public AsyncTasks(List<Future<?>> futures) {
            this.futures = futures;
        }

        public static AsyncTasks runInParallel(Runnable task, int count) {
            ExecutorService executor = Executors.newCachedThreadPool();
            List<Future<?>> futures = IntStream.range(0, count)
                    .mapToObj(i -> executor.submit(task))
                    .collect(Collectors.toList());
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

        public void assertAllFailed(Throwable expectedError) {
            for (Future<?> future : futures) {
                assertThatThrownBy(future::get).hasCause(expectedError);
            }
        }
    }

    private static class FreezableSupplier implements Supplier<Integer> {

        private volatile boolean isFrozen;
        private final Supplier<Integer> delegate;

        public FreezableSupplier(Supplier<Integer> delegate) {
            this.delegate = delegate;
        }

        public synchronized void freeze() {
            isFrozen = true;
        }

        public synchronized void unfreeze() {
            isFrozen = false;
            notifyAll();
        }

        @Override
        public synchronized Integer get() {
            while(isFrozen) {
                try {
                    wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }

            return delegate.get();
        }
    }
}
