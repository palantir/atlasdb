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
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

public class CoalescingSupplierTest {
    private static final int DEFAULT_VALUE = 123;

    private final Supplier<Integer> delegate = mock(Supplier.class);
    private final FreezableSupplier freezableDelegate = new FreezableSupplier(delegate);
    private final CoalescingSupplier<Integer> supplier = new CoalescingSupplier<>(freezableDelegate);

    @Before
    public void before() {
        when(delegate.get()).thenReturn(DEFAULT_VALUE);
    }

    @Test
    public void delegatesToDelegate() {
        assertThat(supplier.get()).isEqualTo(DEFAULT_VALUE);

        verify(delegate).get();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void batchesConcurrentRequests() throws InterruptedException {
        freezableDelegate.freeze();
        AsyncTasks initialTask = getConcurrently(1);
        AsyncTasks batch = getConcurrently(5);
        freezableDelegate.unfreeze();

        batch.await();

        // At least some of these requests should be batched. We can't guarantee it will always be 2 though, if
        // we get really unlucky with scheduling.
        verify(delegate, atLeast(2)).get();
        verify(delegate, atMost(5)).get();
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

    private AsyncTasks getConcurrently(int count) {
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
            Uninterruptibles.sleepUninterruptibly(20, TimeUnit.MILLISECONDS);
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
                Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
            }

            return delegate.get();
        }
    }
}
