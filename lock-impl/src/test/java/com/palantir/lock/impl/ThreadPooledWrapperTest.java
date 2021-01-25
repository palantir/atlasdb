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
package com.palantir.lock.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.google.common.collect.Lists;
import com.palantir.common.base.FunctionCheckedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

public class ThreadPooledWrapperTest {
    private static final Waiter WAITER = new Waiter();

    private static CountDownLatch countDownLatch;

    private static final class Waiter {
        int await() throws InterruptedException {
            countDownLatch.await();
            return 0;
        }
    }

    @Before
    public void resetLatch() {
        countDownLatch = new CountDownLatch(1);
    }

    @Test
    public void emptyPoolDoesNotExecuteMethod() throws InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Semaphore sharedThreadPool = new Semaphore(0);
        List<Future> futures = getFuturesForNewClient(1, executorService, 0, sharedThreadPool, w -> {
            fail("Wasn't expecting to execute function");
            return null;
        });

        assertBlockedThreadsAreDone(futures, 1);
    }

    @Test
    public void localPoolCanExecuteMethod() throws InterruptedException, ExecutionException {
        assertSingleClientCanExecuteMethods(new Semaphore(0), 1, 1);
    }

    @Test
    public void sharedPoolCanExecuteMethod() throws InterruptedException, ExecutionException {
        assertSingleClientCanExecuteMethods(new Semaphore(1), 0, 1);
    }

    @Test
    public void singleClientCanUseLocalAndSharedPools() throws InterruptedException, ExecutionException {
        assertSingleClientCanExecuteMethods(new Semaphore(1), 1, 2);
    }

    private void assertSingleClientCanExecuteMethods(
            Semaphore sharedThreadPool, int localThreadPoolSize, int numThreads)
            throws InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future> futures =
                getFuturesForNewClient(numThreads, executorService, localThreadPoolSize, sharedThreadPool, w -> 1L);

        executorService.awaitTermination(1, TimeUnit.SECONDS);

        for (Future future : futures) {
            assertThat((long) future.get()).isEqualTo(1L);
        }
    }

    @Test
    public void singleClientCantOveruseLocalAndSharedPools() throws InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        List<Future> futures = getFuturesForNewClient(3, executorService, 1, new Semaphore(1), w -> {
            w.await();
            return 0L;
        });

        assertBlockedThreadsAreDone(futures, 1);
        countDownLatch.countDown();
        assertSuccessfulThreadsAreDone(futures, 2);
    }

    @Test
    public void twoClientsCanUseEachLocalThreadPools() throws InterruptedException, ExecutionException {
        assertTwoClientsCanExecuteMethods(new Semaphore(0), 2, 1);
    }

    @Test
    public void twoClientsCanUseLocalAndSharedPools() throws InterruptedException, ExecutionException {
        assertTwoClientsCanExecuteMethods(new Semaphore(2), 4, 1);
    }

    private void assertTwoClientsCanExecuteMethods(Semaphore sharedThreadPool, int numThreads, int localThreadPoolSize)
            throws InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future> futuresForClient1 =
                getFuturesForNewClient(1, executorService, localThreadPoolSize, sharedThreadPool, w -> 1L);
        List<Future> futuresForClient2 =
                getFuturesForNewClient(1, executorService, localThreadPoolSize, sharedThreadPool, w -> 2L);

        executorService.awaitTermination(1, TimeUnit.SECONDS);

        for (Future future : futuresForClient1) {
            assertThat(future.get()).isEqualTo(1L);
        }
        for (Future future : futuresForClient2) {
            assertThat(future.get()).isEqualTo(2L);
        }
    }

    @Test
    public void twoClientsCantReuseSharedPool() throws InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Semaphore sharedThreadPool = new Semaphore(1);
        Future future1 = getSingleFutureForNewClient(executorService, 0, sharedThreadPool, w -> {
            w.await();
            return 0L;
        });
        Future future2 = getSingleFutureForNewClient(executorService, 0, sharedThreadPool, w -> {
            w.await();
            return 0L;
        });

        assertBlockedThreadsAreDone(Lists.newArrayList(future1, future2), 1);
        countDownLatch.countDown();
        assertSuccessfulThreadsAreDone(Lists.newArrayList(future1, future2), 1);
    }

    private Future getSingleFutureForNewClient(
            ExecutorService executorService,
            int localThreadPoolSize,
            Semaphore sharedThreadPool,
            FunctionCheckedException<Waiter, ?, ?> function) {
        ThreadPooledWrapper<Waiter> client = new ThreadPooledWrapper<>(WAITER, localThreadPoolSize, sharedThreadPool);
        return executorService.submit(() -> client.applyWithPermit(function));
    }

    private List<Future> getFuturesForNewClient(
            int numberOfFutures,
            ExecutorService executorService,
            int localThreadPoolSize,
            Semaphore sharedThreadPool,
            FunctionCheckedException<Waiter, ?, ?> function) {
        List<Future> futures = new ArrayList<>();
        ThreadPooledWrapper<Waiter> client = new ThreadPooledWrapper<>(WAITER, localThreadPoolSize, sharedThreadPool);

        for (int i = 0; i < numberOfFutures; i++) {
            futures.add(executorService.submit(() -> client.applyWithPermit(function)));
        }

        return futures;
    }

    private void assertBlockedThreadsAreDone(List<Future> futures, int numberBlocked)
            throws InterruptedException, ExecutionException {
        AtomicInteger exceptions = new AtomicInteger(0);
        do {
            exceptions.set(0);
            futures.forEach(future -> {
                if (future.isDone()) {
                    try {
                        future.get();
                        fail("fail");
                    } catch (Exception e) {
                        assertThat(e)
                                .isInstanceOf(ExecutionException.class)
                                .hasMessageContaining("TooManyRequestsException");
                        exceptions.getAndIncrement();
                    }
                }
            });
        } while (exceptions.get() < numberBlocked);
        assertThat(exceptions.get()).isEqualTo(numberBlocked);
    }

    private void assertSuccessfulThreadsAreDone(List<Future> futures, int numberSuccessful)
            throws InterruptedException, ExecutionException {
        AtomicInteger successes = new AtomicInteger(0);
        futures.forEach(future -> {
            try {
                future.get();
                successes.getAndIncrement();
            } catch (Exception e) {
                assertThat(e).isInstanceOf(ExecutionException.class).hasMessageContaining("TooManyRequestsException");
            }
        });
        assertThat(successes.get()).isEqualTo(numberSuccessful);
    }
}
