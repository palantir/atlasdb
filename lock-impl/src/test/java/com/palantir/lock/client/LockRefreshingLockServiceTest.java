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
package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockRequest.Builder;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class LockRefreshingLockServiceTest {
    private LockServiceImpl internalServer;
    private LockRefreshingLockService server;
    private LockDescriptor lock1;

    @Before public void setUp() {
        internalServer = LockServiceImpl.create(LockServerOptions.builder()
                .isStandaloneServer(false)
                .build());
        server = LockRefreshingLockService.create(internalServer);
        lock1 = StringLockDescriptor.of("lock1");
    }

    @After public void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    public void testSimpleRefresh() throws InterruptedException {
        Builder builder = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE));
        builder.timeoutAfter(SimpleTimeDuration.of(5, TimeUnit.SECONDS));
        LockResponse lock = server.lockWithFullLockResponse(LockClient.ANONYMOUS, builder.build());
        Thread.sleep(10000);
        Set<HeldLocksToken> refreshTokens = server.refreshTokens(ImmutableList.of(lock.getToken()));
        Assert.assertEquals(1, refreshTokens.size());
    }

    @Test
    public void testClosed() {
        server.close();
        assertThatThrownBy(() -> server.currentTimeMillis())
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessage("LockRefreshingLockService is closed");
    }

    @Test
    public void testFailedRefreshCallback() throws InterruptedException {
        AtomicReference<Set<LockRefreshToken>> failedTokens = new AtomicReference<>();
        server.registerRefreshFailedCallback(failedTokens::set);
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                .timeoutAfter(SimpleTimeDuration.of(5, TimeUnit.SECONDS))
                .build();
        LockResponse lock = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
        // use the internal server to unlock (refreshing service will still try to refresh but fail)
        internalServer.unlock(lock.getToken());
        Awaitility.waitAtMost(20, TimeUnit.SECONDS)
                .until(() -> failedTokens.get() != null);
        assertThat(failedTokens.get()).containsExactlyInAnyOrder(lock.getLockRefreshToken());
    }

    @Test
    public void testFailedRefreshCallbackMultithreaded() throws InterruptedException {
        AtomicReference<Set<LockRefreshToken>> failedTokens = new AtomicReference<>();
        server.registerRefreshFailedCallback(failedTokens::set);
        // register a callback that simply waits and then increments a counter
        AtomicInteger callbackFinishedCount = new AtomicInteger(0);
        AtomicInteger callbackStartedCount = new AtomicInteger(0);
        CountDownLatch callbackLatch = new CountDownLatch(1);
        server.registerRefreshFailedCallback(unused -> {
            callbackStartedCount.incrementAndGet();
            try {
                callbackLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            callbackFinishedCount.incrementAndGet();
        });
        // create two locks
        LockRequest request1 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                .timeoutAfter(SimpleTimeDuration.of(5, TimeUnit.SECONDS))
                .build();
        LockResponse lock1 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request1);
        LockRequest request2 = LockRequest.builder(ImmutableSortedMap.of(StringLockDescriptor.of("lock2"), LockMode.WRITE))
                .timeoutAfter(SimpleTimeDuration.of(5, TimeUnit.SECONDS))
                .build();
        LockResponse lock2 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request2);
        // unlock lock1 on the internal server so refresh fails
        internalServer.unlock(lock1.getToken());
        Awaitility.waitAtMost(20, TimeUnit.SECONDS)
                .until(() -> failedTokens.get() != null);
        assertThat(failedTokens.get()).containsExactlyInAnyOrder(lock1.getLockRefreshToken());
        // now reset failedTokens and unlock lock2 so that it should also fail
        failedTokens.set(null);
        internalServer.unlock(lock2.getToken());
        Awaitility.waitAtMost(20, TimeUnit.SECONDS)
                .until(() -> failedTokens.get() != null);
        assertThat(failedTokens.get()).containsExactlyInAnyOrder(lock2.getLockRefreshToken());
        // by this point the latch callbacks should be still running and not finished
        assertThat(callbackFinishedCount.get()).isEqualTo(0);
        // make sure the callbacks have actually started (before we let either finish)
        // we want to assert that the callbacks are actually being run in parallel
        Awaitility.waitAtMost(2, TimeUnit.SECONDS)
                .until(() -> callbackStartedCount.get() == 2);
        // allow those callbacks to finish
        callbackLatch.countDown();
        Awaitility.waitAtMost(2, TimeUnit.SECONDS)
                .until(() -> callbackFinishedCount.get() == 2);
    }
}
