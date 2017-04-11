/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.lock.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.StringLockDescriptor;

public class RateLimitingLockServiceTest {
    private static final LockRequest MOCK = LockRequest.builder(ImmutableSortedMap.of(StringLockDescriptor.of("mock"), LockMode.WRITE))
            .build();
    public static final int AVAILABLE_THREADS = 100;
    public static final int NUM_CLIENTS = 10;
    public static final int LOCAL_TC_LIMIT = AVAILABLE_THREADS / 2 / NUM_CLIENTS;
    public static final int GLOBAL_TC_LIMIT = AVAILABLE_THREADS - LOCAL_TC_LIMIT * NUM_CLIENTS;

    public static final int TWENTY = 20;
    public static final String TEST = "test";

    private static CountDownLatch countDownLatch;

    private static LockService slowLockService = mock(LockService.class);
    static {
        try {
            when(slowLockService.lock(TEST, MOCK)).thenAnswer(invocation -> {
                countDownLatch.await();
                return null;
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void resetLatch() {
        countDownLatch = new CountDownLatch(1);
    }


    @Test
    public void oneClientCanUseAllGlobalAndItsLocalThreads() throws InterruptedException, ExecutionException {
        List<Future<LockRefreshToken>> futures = requestLocks(1, LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT + TWENTY);
        assertBlocked(futures, TWENTY);
        countDownLatch.countDown();
        assertUnBlocked(futures, LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT);
    }

    @Test
    public void multipleClientsCanShareGlobalThreads() throws InterruptedException, ExecutionException {
        List<Future<LockRefreshToken>> futures = requestLocks(NUM_CLIENTS, LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT / NUM_CLIENTS);
        assertBlocked(futures, 0);
        countDownLatch.countDown();
        assertUnBlocked(futures, NUM_CLIENTS * LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT);
    }

    @Test
    public void multipleClientsCanExhaustGlobalThreadsTogether() throws InterruptedException, ExecutionException {
        List<Future<LockRefreshToken>> futures = requestLocks(5, LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT);
        assertBlocked(futures, 4 * GLOBAL_TC_LIMIT);
        countDownLatch.countDown();
        assertUnBlocked(futures, 5 * LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT);
    }

    @Test
    public void clientsCanUseLocalAllowanceWhenGlobalIsExhausted() throws InterruptedException, ExecutionException {
        List<Future<LockRefreshToken>> futures1 = requestLocks(1, LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT + 1);
        List<Future<LockRefreshToken>> futures2 = requestLocks(1, LOCAL_TC_LIMIT);
        List<Future<LockRefreshToken>> futures3 = requestLocks(1, LOCAL_TC_LIMIT + 1);
        List<Future<LockRefreshToken>> futures4 = requestLocks(1, LOCAL_TC_LIMIT);
        assertBlocked(futures1, 1);
        assertBlocked(futures2, 0);
        assertBlocked(futures3, 1);
        assertBlocked(futures4, 0);
        countDownLatch.countDown();
        assertUnBlocked(futures1, LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT);
        assertUnBlocked(futures2, LOCAL_TC_LIMIT);
        assertUnBlocked(futures3, LOCAL_TC_LIMIT);
        assertUnBlocked(futures4, LOCAL_TC_LIMIT);
    }

    private List<Future<LockRefreshToken>> requestLocks(int clients, int locks) {
        List<Future<LockRefreshToken>> futures = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(clients * locks);
        for (int i = 0; i < clients; i++) {
            LockService lockService = RateLimitingLockService.create(slowLockService, AVAILABLE_THREADS, NUM_CLIENTS);
            for (int j = 0; j < locks; j++) {
                futures.add(executorService.submit(() -> lockService.lock(TEST, MOCK)));
            }
        }
        return futures;
    }

    private void assertBlocked(List<Future<LockRefreshToken>> futures, int numberBlocked)
            throws InterruptedException, ExecutionException {
        AtomicInteger exceptions = new AtomicInteger(0);
        futures.forEach(future -> {
            if (future.isDone()) {
                try {
                    future.get();
                } catch (Exception e) {
                    assertThat(e).isInstanceOf(ExecutionException.class)
                            .hasMessageContaining("TooManyRequestsException");
                    exceptions.getAndIncrement();
                }
            }});
        assertThat(exceptions.get()).isEqualTo(numberBlocked);
    }

    private void assertUnBlocked(List<Future<LockRefreshToken>> futures, int numberSuccessful)
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
