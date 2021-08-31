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
package com.palantir.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.palantir.common.concurrent.InterruptibleFuture;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.proxy.SimulatingServerProxy;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.logger.LockServiceTestUtils;
import com.palantir.util.Mutable;
import com.palantir.util.Mutables;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the Lock Server.
 *
 * @author jtamer, ddoan
 */
public abstract class LockServiceTest {
    private final ExecutorService executor = PTExecutors.newCachedThreadPool(LockServiceTest.class.getName());

    private LockService server;
    private LockClient client;
    private LockDescriptor lock1;
    private LockDescriptor lock2;
    private CyclicBarrier barrier;

    protected abstract LockService getLockService();

    @Before
    public void before() {
        new File("lock_server_timestamp.dat").delete();
        server = getLockService();
        client = LockClient.of("a client");
        lock1 = StringLockDescriptor.of("lock1");
        lock2 = StringLockDescriptor.of("lock2");
        barrier = new CyclicBarrier(2);
    }

    @After
    public void after() {
        executor.shutdownNow();
    }

    /**
     * Tests that RemoteLockService api (that internal forwards to LockService api) passes a sanity check.
     */
    @Test
    public void testRemoteLockServiceApi() throws InterruptedException {
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                .withLockedInVersionId(10)
                .doNotBlock()
                .build();

        assertThat(server.getMinLockedInVersionId(LockClient.ANONYMOUS.getClientId()))
                .isNull();
        LockRefreshToken token = server.lock(LockClient.ANONYMOUS.getClientId(), request);
        assertThat(server.getMinLockedInVersionId(LockClient.ANONYMOUS.getClientId()))
                .isEqualTo(10);
        assertThat(server.lock(LockClient.ANONYMOUS.getClientId(), request)).isNull();
        server.unlock(token);

        assertThat(server.getMinLockedInVersionId(LockClient.ANONYMOUS.getClientId()))
                .isNull();
        HeldLocksToken heldToken = server.lockAndGetHeldLocks(LockClient.ANONYMOUS.getClientId(), request);
        assertThat(server.getMinLockedInVersionId(LockClient.ANONYMOUS.getClientId()))
                .isEqualTo(10);
        assertThat(server.lockAndGetHeldLocks(LockClient.ANONYMOUS.getClientId(), request))
                .isNull();
        server.unlock(heldToken.getLockRefreshToken());
    }

    /**
     * Tests using doNotBlock() in the lock request.
     */
    @Test
    public void testDoNotBlock() throws InterruptedException {
        long lockTimeoutMs = LockRequest.getDefaultLockTimeout().toMillis();
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                .withLockedInVersionId(10)
                .doNotBlock()
                .build();
        long currentTimeMs = System.currentTimeMillis();
        LockResponse response = server.lockWithFullLockResponse(client, request);
        assertThat(response.success()).isTrue();
        assertThat(response.getLockHolders()).isEmpty();
        HeldLocksToken token1 = response.getToken();
        assertThat(token1).isNotNull();
        assertThat(token1.getClient()).isEqualTo(this.client);
        assertThat(token1.getVersionId()).isEqualTo(10);
        assertThat(token1.getLockDescriptors()).isEqualTo(request.getLockDescriptors());
        assertThat(token1.getExpirationDateMs())
                .isBetween(currentTimeMs + lockTimeoutMs, System.currentTimeMillis() + lockTimeoutMs);

        assertThat(server.lockWithFullLockResponse(
                                LockClient.ANONYMOUS,
                                LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                                        .doNotBlock()
                                        .build())
                        .getToken())
                .isNull();

        assertThat(server.lockWithFullLockResponse(
                                LockClient.ANONYMOUS,
                                LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                                        .doNotBlock()
                                        .build())
                        .getToken())
                .isNull();

        HeldLocksToken anonymousReadToken = server.lockWithFullLockResponse(
                        LockClient.ANONYMOUS,
                        LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                                .doNotBlock()
                                .build())
                .getToken();
        assertThat(anonymousReadToken).isNotNull();
        server.unlock(anonymousReadToken);

        HeldLocksToken token2 = server.lockWithFullLockResponse(
                        this.client,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                                .withLockedInVersionId(5)
                                .doNotBlock()
                                .build())
                .getToken();
        assertThat(token2).isNotNull();

        HeldLocksToken token3 = server.lockWithFullLockResponse(
                        this.client,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                                .doNotBlock()
                                .build())
                .getToken();
        assertThat(token3).isNotNull();

        assertThat(server.getTokens(this.client)).isEqualTo(ImmutableSet.of(token1, token2, token3));
        assertThat(server.getMinLockedInVersionId(this.client).longValue()).isEqualTo(5);

        server.unlock(token2);
        assertThat(server.getTokens(this.client)).isEqualTo(ImmutableSet.of(token1, token3));
        assertThat(server.getMinLockedInVersionId(this.client).longValue()).isEqualTo(10);

        server.unlock(token1);
        assertThat(server.refreshTokens(ImmutableSet.of(token1, token2, token3)))
                .isEqualTo(ImmutableSet.of(token3));
        assertThat(server.getMinLockedInVersionId(this.client)).isNull();

        server.unlock(token3);
        assertThat(server.getTokens(this.client)).isEmpty();
        assertThat(server.getMinLockedInVersionId(this.client)).isNull();
    }

    /**
     * Tests using blockForAtMost() in the lock request.
     */
    @Test
    public void testBlockForAtMost() throws Exception {
        long lockTimeoutMs = LockRequest.getDefaultLockTimeout().toMillis();
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                .withLockedInVersionId(10)
                .blockForAtMost(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                .build();
        long currentTimeMs = System.currentTimeMillis();
        LockResponse response = server.lockWithFullLockResponse(client, request);
        assertThat(response.success()).isTrue();
        assertThat(response.getLockHolders()).isEmpty();
        HeldLocksToken token1 = response.getToken();
        assertThat(token1).isNotNull();
        assertThat(token1.getClient()).isEqualTo(client);
        assertThat(token1.getLockDescriptors()).isEqualTo(request.getLockDescriptors());
        assertThat(token1.getExpirationDateMs())
                .isBetween(currentTimeMs + lockTimeoutMs, System.currentTimeMillis() + lockTimeoutMs);

        Future<?> future = executor.submit((Callable<Void>) () -> {
            LockResponse response1 = server.lockWithFullLockResponse(
                    LockClient.ANONYMOUS,
                    LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                            .blockForAtMost(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                            .build());
            assertThat(response1.success()).isFalse();
            assertThat(response1.getLockHolders()).isNotEmpty().isEqualTo(ImmutableSortedMap.of(lock2, client));
            assertThat(response1.getToken()).isNull();
            barrier.await();

            LockResponse response2 = server.lockWithFullLockResponse(
                    LockClient.ANONYMOUS,
                    LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                            .blockForAtMost(SimpleTimeDuration.of(100, TimeUnit.MILLISECONDS))
                            .build());
            assertThat(response2.success()).isTrue();
            assertThat(response2.getLockHolders()).isEmpty();
            HeldLocksToken validToken = response2.getToken();
            assertThat(validToken).isNotNull();
            server.unlock(validToken);

            return null;
        });

        barrier.await(15, TimeUnit.SECONDS);
        Thread.sleep(10);
        server.unlock(token1);
        future.get();
        token1 = server.lockWithFullLockResponse(client, request).getToken();

        response = server.lockWithFullLockResponse(
                LockClient.ANONYMOUS,
                LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                        .blockForAtMost(SimpleTimeDuration.of(0, TimeUnit.MILLISECONDS))
                        .build());
        assertThat(response.success()).isTrue();
        assertThat(response.getLockHolders()).isEmpty();
        HeldLocksToken anonymousReadToken = response.getToken();
        assertThat(anonymousReadToken).isNotNull();
        server.unlock(anonymousReadToken);

        response = server.lockWithFullLockResponse(
                client,
                LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                        .withLockedInVersionId(5)
                        .blockForAtMost(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                        .build());
        assertThat(response.success()).isTrue();
        assertThat(response.getLockHolders()).isEmpty();
        HeldLocksToken token2 = response.getToken();
        assertThat(token2).isNotNull();

        response = server.lockWithFullLockResponse(
                client,
                LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                        .blockForAtMost(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                        .build());
        assertThat(response.success()).isTrue();
        assertThat(response.getLockHolders()).isEmpty();
        HeldLocksToken token3 = response.getToken();
        assertThat(token3).isNotNull();

        assertThat(server.getTokens(client)).isEqualTo(ImmutableSet.of(token1, token2, token3));
        assertThat(server.getMinLockedInVersionId(client).longValue()).isEqualTo(5);
        assertThat(server.getMinLockedInVersionId(LockClient.ANONYMOUS)).isNull();

        server.unlock(token2);
        assertThat(server.getTokens(client)).isEqualTo(ImmutableSet.of(token1, token3));
        assertThat(server.getMinLockedInVersionId(client).longValue()).isEqualTo(10);
        assertThat(server.getMinLockedInVersionId(LockClient.ANONYMOUS)).isNull();

        server.unlock(token1);
        assertThat(server.refreshTokens(ImmutableSet.of(token1, token2, token3)))
                .isEqualTo(ImmutableSet.of(token3));
        assertThat(server.getMinLockedInVersionId(client)).isNull();

        server.unlock(token3);
        assertThat(server.getTokens(client)).isEmpty();
        assertThat(server.getMinLockedInVersionId(client)).isNull();
    }

    /**
     * Tests using block indefinitely mode
     */
    @Test
    public void testBlockIndefinitely() throws Exception {
        long lockTimeoutMs = LockRequest.getDefaultLockTimeout().toMillis();
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                .withLockedInVersionId(10)
                .build();
        long currentTimeMs = System.currentTimeMillis();
        LockResponse response = server.lockWithFullLockResponse(client, request);
        assertThat(response.success()).isTrue();
        assertThat(response.getLockHolders()).isEmpty();
        HeldLocksToken token1 = response.getToken();
        assertThat(token1).isNotNull();
        assertThat(token1.getClient()).isEqualTo(client);
        assertThat(token1.getLockDescriptors()).isEqualTo(request.getLockDescriptors());
        assertThat(token1.getExpirationDateMs())
                .isBetween(currentTimeMs + lockTimeoutMs, System.currentTimeMillis() + lockTimeoutMs);

        Future<?> future = executor.submit((Callable<Void>) () -> {
            barrier.await();
            HeldLocksToken validToken = server.lockWithFullLockResponse(
                            LockClient.ANONYMOUS,
                            LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                                    .build())
                    .getToken();
            assertThat(validToken).isNotNull();
            server.unlock(validToken);

            return null;
        });

        barrier.await();
        Thread.sleep(500);
        server.unlock(token1);
        future.get();
        token1 = server.lockWithFullLockResponse(client, request).getToken();

        HeldLocksToken anonymousReadToken = server.lockWithFullLockResponse(
                        LockClient.ANONYMOUS,
                        LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                                .build())
                .getToken();
        assertThat(anonymousReadToken).isNotNull();
        server.unlock(anonymousReadToken);

        HeldLocksToken token2 = server.lockWithFullLockResponse(
                        client,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                                .withLockedInVersionId(5)
                                .build())
                .getToken();
        assertThat(token2).isNotNull();

        HeldLocksToken token3 = server.lockWithFullLockResponse(
                        client,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                                .build())
                .getToken();
        assertThat(token3).isNotNull();

        assertThat(server.getTokens(client)).isEqualTo(ImmutableSet.of(token1, token2, token3));
        assertThat(server.getMinLockedInVersionId(client).longValue()).isEqualTo(5);
        assertThat(server.getMinLockedInVersionId(LockClient.ANONYMOUS)).isNull();

        server.unlock(token2);
        assertThat(server.getTokens(client)).isEqualTo(ImmutableSet.of(token1, token3));
        assertThat(server.getMinLockedInVersionId(client).longValue()).isEqualTo(10);
        assertThat(server.getMinLockedInVersionId(LockClient.ANONYMOUS)).isNull();

        server.unlock(token1);
        assertThat(server.refreshTokens(ImmutableSet.of(token1, token2, token3)))
                .isEqualTo(ImmutableSet.of(token3));
        assertThat(server.getMinLockedInVersionId(client)).isNull();

        server.unlock(token3);
        assertThat(server.getTokens(client)).isEmpty();
        assertThat(server.getMinLockedInVersionId(client)).isNull();
    }

    /**
     * Tests lockAndRelease
     */
    @Test
    public void testLockAndRelease() throws Exception {
        LockRequest hasLock2 = LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                .build();
        final LockRequest request = LockRequest.builder(
                        ImmutableSortedMap.of(lock1, LockMode.WRITE, lock2, LockMode.WRITE))
                .lockAndRelease()
                .build();

        LockResponse resp2 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, hasLock2);
        assertThat(resp2.success()).isTrue();

        Future<?> future = executor.submit((Callable<Void>) () -> {
            LockResponse resp = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
            assertThat(resp).isNotNull();
            assertThat(resp.success()).isTrue();
            return null;
        });

        assertThatThrownBy(() -> future.get(1, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);

        server.unlock(resp2.getToken());

        future.get(150, TimeUnit.SECONDS);

        resp2 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, hasLock2);
        server.unlock(resp2.getToken());
    }

    /**
     * Tests lockAndRelease with perf optimization
     */
    @Test
    public void testLockAndRelease2() throws Exception {
        LockRequest hasLock1 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                .build();
        LockRequest hasLock2 = LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                .build();
        final LockRequest request = LockRequest.builder(
                        ImmutableSortedMap.of(lock1, LockMode.WRITE, lock2, LockMode.WRITE))
                .lockAndRelease()
                .build();

        LockResponse resp2 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, hasLock2);
        assertThat(resp2.success()).isTrue();

        Future<?> future = executor.submit((Callable<Void>) () -> {
            LockResponse resp = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
            assertThat(resp).isNotNull();
            assertThat(resp.success()).isTrue();
            return null;
        });

        Thread.sleep(10);
        assertThatThrownBy(() -> future.get(1, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
        LockResponse resp1 = server.lockWithFullLockResponse(client, hasLock1);

        server.unlock(resp2.getToken());

        future.get(150, TimeUnit.SECONDS);

        server.unlock(resp1.getToken());

        resp2 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, hasLock2);
        server.unlock(resp2.getToken());
    }

    /**
     * Tests lockAsManyAsPossible()
     */
    @Test
    public void testLockAsManyAsPossible() throws InterruptedException {
        LockRequest request1 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                .doNotBlock()
                .build();
        LockRequest request2 = LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                .doNotBlock()
                .build();

        LockResponse response = server.lockWithFullLockResponse(client, request1);
        HeldLocksToken token1 = response.getToken();
        assertThat(token1).isNotNull();
        assertThat(token1.getClient()).isEqualTo(client);
        assertThat(token1.getLockDescriptors()).isEqualTo(request1.getLockDescriptors());

        response = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request2);
        HeldLocksToken token2 = response.getToken();
        System.out.println(response.getLockHolders());
        assertThat(token2).isNotNull();
        assertThat(token2.getClient()).isEqualTo(LockClient.ANONYMOUS);
        assertThat(token2.getLockDescriptors()).isEqualTo(request2.getLockDescriptors());

        LockRequest request3 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                .lockAsManyAsPossible()
                .blockForAtMost(SimpleTimeDuration.of(100, TimeUnit.MILLISECONDS))
                .build();
        response = server.lockWithFullLockResponse(client, request3);
        assertThat(response.success()).isTrue();
        HeldLocksToken token3 = response.getToken();
        assertThat(token3).isNotNull();
        assertThat(token3.getClient()).isEqualTo(client);
        assertThat(token3.getLockDescriptors())
                .isEqualTo(LockCollections.of(ImmutableSortedMap.of(lock1, LockMode.READ)));

        server.unlock(token1);
        server.unlock(token2);
        server.unlock(token3);
        assertThat(server.getTokens(client)).isEmpty();
    }

    /**
     * Tests against LockService.logCurrentState() long-block bug (QA-87074)
     */
    @Test
    public void testLogCurrentState() throws Exception {
        long lockTimeoutMs = LockRequest.getDefaultLockTimeout().toMillis();
        // Timeout in private LockServiceImpl.LOG_STATE_DEBUG_LOCK_WAIT_TIME_IN_MILLIS; test value is double that
        long logCurrentStateCallTimeoutMs = 2 * 5000L;

        // First lock request grabs a READ lock
        LockRequest request1 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                .doNotBlock()
                .build();
        long currentTimeMs = System.currentTimeMillis();
        LockResponse response1 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request1);
        assertThat(response1.success()).isTrue();
        assertThat(response1.getLockHolders()).isEmpty();
        HeldLocksToken token1 = response1.getToken();
        assertThat(token1).isNotNull();
        assertThat(token1.getClient()).isEqualTo(LockClient.ANONYMOUS);
        assertThat(token1.getLockDescriptors()).isEqualTo(request1.getLockDescriptors());
        assertThat(token1.getExpirationDateMs())
                .isBetween(currentTimeMs + lockTimeoutMs, System.currentTimeMillis() + lockTimeoutMs);

        // Second request grabs corresponding WRITE lock, will block inside LockServer until READ lock expires
        executor.submit((Callable<Void>) () -> {
            barrier.await();
            LockRequest request2 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                    .build();
            LockResponse response2 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request2);
            HeldLocksToken validToken = response2.getToken();
            assertThat(validToken).isNotNull();
            server.unlock(validToken);
            return null;
        });

        /* Now make the logCurrentState() request; with the WRITE lock request blocked inside LockServer.lock(),
         * this call should block until the first of these happens:
         * -The READ lock times out and the WRITE lock can be granted, thus freeing up the debugLock
         * -The logCurrentState tryLock() call times out after LOG_STATE_DEBUG_LOCK_WAIT_TIME_IN_MILLIS
         *   and the call moves on to logCurrentStateInconsistent()
         */
        barrier.await();
        Thread.sleep(500);
        Future<?> logCallFuture = executor.submit((Callable<Void>) () -> {
            server.logCurrentState();
            return null;
        });

        try {
            logCallFuture.get(logCurrentStateCallTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError("If we exceed the timeout, the call is hung and it's a failure", e);
        } finally {
            LockServiceTestUtils.cleanUpLogStateDir();
        }
    }

    @Test
    public void testGetLockState() throws Exception {
        LockState state0 = server.getLockState(lock1);
        assertThat(state0.isWriteLocked()).isFalse();
        assertThat(state0.exactCurrentLockHolders()).isEmpty();
        assertThat(state0.holders()).isEmpty();
        assertThat(state0.requesters()).isEmpty();

        LockRequest request1 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                .doNotBlock()
                .build();
        LockResponse response1 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request1);
        assertThat(response1.success()).isTrue();
        LockState state1 = server.getLockState(lock1);
        assertThat(state1.isWriteLocked()).isFalse();
        assertThat(ImmutableList.of(LockClient.ANONYMOUS)).isEqualTo(state1.exactCurrentLockHolders());
        assertThat(Thread.currentThread().getName())
                .isEqualTo(Iterables.getOnlyElement(state1.holders()).requestingThread());

        executor.submit((Callable<Void>) () -> {
            barrier.await();
            LockRequest request2 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                    .withLockedInVersionId(100L)
                    .build();
            LockResponse response2 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request2);
            HeldLocksToken validToken = response2.getToken();
            assertThat(validToken).isNotNull();
            server.unlock(validToken);
            return null;
        });

        barrier.await();
        Thread.sleep(500);
        LockState state2 = server.getLockState(lock1);
        assertThat(state2.requesters()).hasSize(1);
        assertThat(Iterables.getOnlyElement(state2.requesters()).versionId()).hasValue(100L);
    }

    @Test
    public void testGetLockStateOfReadWriteReentrancy() throws Exception {
        LockResponse response1 = server.lockWithFullLockResponse(
                client,
                LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                        .build());
        assertThat(response1.success()).isTrue();
        assertThat(server.getLockState(lock1).isWriteLocked()).isTrue();

        LockResponse response2 = server.lockWithFullLockResponse(
                client,
                LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ)).build());
        assertThat(response2.success()).isTrue();
        assertThat(server.getLockState(lock1).isWriteLocked()).isTrue();
    }

    /**
     * Tests lock responses
     */
    @Test
    public void testLockReponse() throws InterruptedException {
        LockDescriptor lock3 = StringLockDescriptor.of("third lock");
        LockDescriptor lock4 = StringLockDescriptor.of("fourth lock");

        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(
                        lock1, LockMode.READ, lock2, LockMode.READ, lock3, LockMode.WRITE, lock4, LockMode.WRITE))
                .doNotBlock()
                .build();
        LockResponse response = server.lockWithFullLockResponse(client, request);
        assertThat(response.success()).isTrue();
        assertThat(response.getLockHolders()).isEmpty();

        request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock3, LockMode.WRITE))
                .lockAsManyAsPossible()
                .blockForAtMost(SimpleTimeDuration.of(50, TimeUnit.MILLISECONDS))
                .build();
        response = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
        assertThat(response.success()).isTrue();
        assertThat(response.getLockHolders().isEmpty()).isFalse();
        assertThat(response.getLockHolders()).isEqualTo(ImmutableMap.of(lock3, client));
        HeldLocksToken token = response.getToken();
        assertThat(token.getClient()).isEqualTo(LockClient.ANONYMOUS);
        assertThat(token.getLockDescriptors())
                .isEqualTo(LockCollections.of(ImmutableSortedMap.of(lock1, LockMode.READ)));

        request = LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ, lock4, LockMode.WRITE))
                .lockAsManyAsPossible()
                .blockForAtMost(SimpleTimeDuration.of(50, TimeUnit.MILLISECONDS))
                .build();
        response = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
        assertThat(response.success()).isTrue();
        assertThat(response.getLockHolders().isEmpty()).isFalse();
        assertThat(response.getLockHolders()).isEqualTo(ImmutableMap.of(lock4, client));
        token = response.getToken();
        assertThat(token.getClient()).isEqualTo(LockClient.ANONYMOUS);
        assertThat(token.getLockDescriptors())
                .isEqualTo(LockCollections.of(ImmutableSortedMap.of(lock2, LockMode.READ)));

        request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.READ))
                .lockAsManyAsPossible()
                .blockForAtMost(SimpleTimeDuration.of(50, TimeUnit.MILLISECONDS))
                .build();
        response = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
        assertThat(response.success()).isTrue();
        assertThat(response.getLockHolders()).isEmpty();
        token = response.getToken();
        assertThat(token.getClient()).isEqualTo(LockClient.ANONYMOUS);
        assertThat(token.getLockDescriptors())
                .isEqualTo(LockCollections.of(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.READ)));

        request = LockRequest.builder(ImmutableSortedMap.of(lock3, LockMode.WRITE, lock4, LockMode.WRITE))
                .lockAsManyAsPossible()
                .blockForAtMost(SimpleTimeDuration.of(50, TimeUnit.MILLISECONDS))
                .build();
        response = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
        assertThat(response.success()).isFalse();
        assertThat(response.getLockHolders().isEmpty()).isFalse();
        assertThat(response.getLockHolders()).isEqualTo(ImmutableSortedMap.of(lock3, client, lock4, client));
        token = response.getToken();
        assertThat(token).isNull();
    }

    /**
     * Tests grants
     */
    @Test
    public void testGrants() throws Exception {
        LockRequest requestWrite = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                .doNotBlock()
                .build();
        LockRequest requestRead = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                .doNotBlock()
                .build();
        LockRequest requestTwoLocks = LockRequest.builder(
                        ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                .doNotBlock()
                .build();
        HeldLocksToken token1 =
                server.lockWithFullLockResponse(client, requestWrite).getToken();
        assertThat(token1).isNotNull();
        HeldLocksToken token2 =
                server.lockWithFullLockResponse(client, requestRead).getToken();
        assertThat(token2).isNotNull();
        HeldLocksToken finalToken = token1;
        assertThatThrownBy(() -> server.convertToGrant(finalToken))
                .describedAs("Expected: holding both read and write locks")
                .isInstanceOf(IllegalMonitorStateException.class);
        HeldLocksToken token3 =
                server.lockWithFullLockResponse(client, requestTwoLocks).getToken();
        assertThat(token3).isNotNull();
        assertThatThrownBy(() -> server.convertToGrant(token3))
                .describedAs("Expected: holding multiple locks")
                .isInstanceOf(IllegalMonitorStateException.class);
        server.unlock(token2);
        server.unlock(token3);

        LockClient client2 = LockClient.of("client2");
        LockResponse response = server.lockWithFullLockResponse(client2, requestWrite);
        assertThat(response.success()).isFalse();
        assertThat(response.getLockHolders()).isEqualTo(ImmutableMap.of(lock1, client));
        HeldLocksToken nullToken = response.getToken();
        assertThat(nullToken).isNull();

        HeldLocksGrant grantToken = server.convertToGrant(token1);
        assertThat(grantToken.getClient()).isNull();

        HeldLocksToken validToken = server.useGrant(client2, grantToken);
        assertThat(validToken).isNotNull();
        assertThat(validToken.getClient()).isEqualTo(client2);
        server.unlock(validToken);

        requestWrite = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                .build();
        token1 = server.lockWithFullLockResponse(client, requestWrite).getToken();
        assertThat(token1).isNotNull();
        assertThat(token1.getClient()).isEqualTo(client);
        Future<?> future = executor.submit((Callable<Void>) () -> {
            HeldLocksToken validToken1 = server.lockWithFullLockResponse(
                            LockClient.ANONYMOUS,
                            LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                                    .build())
                    .getToken();
            assertThat(validToken1).isNotNull();
            assertThat(validToken1.getClient()).isEqualTo(LockClient.ANONYMOUS);
            server.unlock(validToken1);

            return null;
        });
        grantToken = server.convertToGrant(token1);
        assertThat(grantToken.getClient()).isNull();

        validToken = server.useGrant(client2, grantToken.getGrantId());
        assertThat(validToken).isNotNull();
        assertThat(validToken.getClient()).isEqualTo(client2);

        Thread.sleep(100);
        server.unlock(validToken);
        future.get();
        assertThat(server.getTokens(client)).isEmpty();
        assertThat(server.getTokens(client2)).isEmpty();
    }

    /**
     * Tests for illegal actions
     */
    @Test
    public void testIllegalActions() throws InterruptedException {
        assertThatThrownBy(() -> LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                        .doNotBlock()
                        .doNotBlock()
                        .build())
                .describedAs("Expected: can't call doNotBlock() twice")
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                        .doNotBlock()
                        .blockForAtMost(SimpleTimeDuration.of(1, TimeUnit.MILLISECONDS))
                        .build())
                .describedAs("Expected: can't call both doNotBlock() and blockForAtMost()")
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                        .blockForAtMost(SimpleTimeDuration.of(1, TimeUnit.MILLISECONDS))
                        .blockForAtMost(SimpleTimeDuration.of(1, TimeUnit.MILLISECONDS))
                        .build())
                .describedAs("Expected: can't call blockForAtMost() twice")
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                        .timeoutAfter(SimpleTimeDuration.of(1, TimeUnit.MILLISECONDS))
                        .timeoutAfter(SimpleTimeDuration.of(1, TimeUnit.MILLISECONDS))
                        .build())
                .describedAs("Expected: can't call timeoutAfter() twice")
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                        .lockAsManyAsPossible()
                        .lockAsManyAsPossible()
                        .build())
                .describedAs("Expected: can't call lockAsManyAsPossible() twice")
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                        .lockAsManyAsPossible()
                        .build())
                .describedAs("Expected: lockAsManyAsPossible() requires" + " doNotBlock() or blockForAtMost() modes ")
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                        .withLockedInVersionId(1)
                        .withLockedInVersionId(2)
                        .build())
                .describedAs("Expected: can't call withLockedInVersionId() twice")
                .isInstanceOf(IllegalStateException.class);

        HeldLocksToken token = server.lockWithFullLockResponse(
                        client,
                        LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                                .doNotBlock()
                                .build())
                .getToken();

        assertThat(token).isNotNull();
        assertThat(server.getTokens(client)).isEqualTo(ImmutableSet.of(token));
        assertThatThrownBy(() -> server.getTokens(LockClient.ANONYMOUS))
                .describedAs("Expected: can't refresh an anonymous client")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> server.getTokens(LockClient.INTERNAL_LOCK_GRANT_CLIENT))
                .describedAs("Expected: can't refresh the internal lock grant client")
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> server.unlockAndFreeze(token))
                .describedAs("Expected: can't unlock and freeze read lock")
                .isInstanceOf(IllegalArgumentException.class);

        server.unlock(token);
        assertThat(server.getTokens(client)).isEmpty();
    }

    /**
     * Tests grabbing many locks with each request
     */
    @Test
    public void testNumerousLocksPerClient() throws InterruptedException {
        SortedMap<LockDescriptor, LockMode> lockMap = new TreeMap<>();
        int numLocks = 10000;

        for (int i = 0; i < numLocks; ++i) {
            if (i % 2 == 0) {
                lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.READ);
            } else {
                lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.WRITE);
            }
        }

        LockRequest requestAllLocks = LockRequest.builder(lockMap).doNotBlock().build();
        HeldLocksToken readWriteToken =
                server.lockWithFullLockResponse(client, requestAllLocks).getToken();
        assertThat(readWriteToken).isNotNull();
        assertThat(readWriteToken.getClient()).isEqualTo(client);
        assertThat(readWriteToken.getLockDescriptors())
                .isEqualTo(LockCollections.of(ImmutableSortedMap.copyOf(lockMap)));

        lockMap = new TreeMap<>();
        for (int i = 0; i < numLocks; ++i) {
            lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.READ);
        }
        requestAllLocks = LockRequest.builder(lockMap).doNotBlock().build();
        HeldLocksToken token =
                server.lockWithFullLockResponse(client, requestAllLocks).getToken();
        assertThat(token).isNotNull();
        assertThat(token.getClient()).isEqualTo(client);
        assertThat(token.getLockDescriptors()).isEqualTo(LockCollections.of(ImmutableSortedMap.copyOf(lockMap)));

        server.unlock(token);
        server.unlock(readWriteToken);
        lockMap = new TreeMap<>();
        for (int i = 0; i < numLocks; ++i) {
            if (i % 2 == 0) {
                lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.READ);
            }
        }
        requestAllLocks = LockRequest.builder(lockMap).doNotBlock().build();
        server.lockWithFullLockResponse(client, requestAllLocks);

        LockClient client2 = LockClient.of("another client");
        lockMap = new TreeMap<>();
        for (int i = 0; i < numLocks; ++i) {
            lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.WRITE);
        }
        requestAllLocks = LockRequest.builder(lockMap).doNotBlock().build();
        token = server.lockWithFullLockResponse(client2, requestAllLocks).getToken();
        assertThat(token).isNull();
        requestAllLocks =
                LockRequest.builder(lockMap).doNotBlock().lockAsManyAsPossible().build();
        token = server.lockWithFullLockResponse(client2, requestAllLocks).getToken();
        assertThat(token).isNotNull();
        assertThat(token.getClient()).isEqualTo(client2);
        lockMap = new TreeMap<>();
        for (int i = 0; i < numLocks; ++i) {
            if (i % 2 != 0) {
                lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.WRITE);
            }
        }
        assertThat(token.getLockDescriptors()).isEqualTo(LockCollections.of(ImmutableSortedMap.copyOf(lockMap)));
        server.unlock(token);

        lockMap = new TreeMap<>();
        for (int i = 0; i < numLocks; ++i) {
            if (i % 2 == 0) {
                lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.WRITE);
            } else {
                lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.READ);
            }
        }
        requestAllLocks =
                LockRequest.builder(lockMap).doNotBlock().lockAsManyAsPossible().build();
        token = server.lockWithFullLockResponse(client2, requestAllLocks).getToken();
        assertThat(token).isNotNull();
        assertThat(token.getClient()).isEqualTo(client2);
        lockMap = new TreeMap<>();
        for (int i = 0; i < numLocks; ++i) {
            if (i % 2 != 0) {
                lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.READ);
            }
        }
        assertThat(token.getLockDescriptors()).isEqualTo(LockCollections.of(ImmutableSortedMap.copyOf(lockMap)));
        server.unlock(token);
    }

    /**
     * Tests using multiple threads to grab the same locks
     */
    @Test
    public void testNumerousClientsPerLock() throws Exception {
        new File("lock_server_timestamp.dat").delete();
        server = SimulatingServerProxy.newProxyInstance(
                LockService.class,
                LockServiceImpl.create(LockServerOptions.builder()
                        .maxAllowedClockDrift(SimpleTimeDuration.of(0, TimeUnit.MILLISECONDS))
                        .build()),
                100);

        final int partitions = 5;
        final int[] partition = new int[partitions - 1];
        final int numThreads = partitions * 10;
        for (int i = 1; i < partitions; ++i) {
            partition[i - 1] = numThreads * i / partitions;
        }

        List<Future<?>> futures = new ArrayList<>();
        final Mutable<Integer> numSuccess = Mutables.newMutable(0);
        final Mutable<Integer> numFailure = Mutables.newMutable(0);
        for (int i = 0; i < numThreads; ++i) {
            final int clientID = i;
            InterruptibleFuture<?> future = new InterruptibleFuture<Void>() {
                @Override
                public Void call() throws InterruptedException {
                    try {
                        while (true) {
                            LockClient client = LockClient.of("client" + clientID);
                            LockRequest request;
                            HeldLocksToken token;

                            if (clientID < partition[0]) {
                                request = LockRequest.builder(
                                                ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                                        .timeoutAfter(SimpleTimeDuration.of(200, TimeUnit.MILLISECONDS))
                                        .doNotBlock()
                                        .build();
                            } else if (clientID >= partition[0] && clientID < partition[1]) {
                                request = LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                                        .timeoutAfter(SimpleTimeDuration.of(200, TimeUnit.MILLISECONDS))
                                        .blockForAtMost(SimpleTimeDuration.of(200, TimeUnit.MILLISECONDS))
                                        .build();
                            } else if (clientID >= partition[1] && clientID < partition[2]) {
                                request = LockRequest.builder(
                                                ImmutableSortedMap.of(lock1, LockMode.WRITE, lock2, LockMode.READ))
                                        .timeoutAfter(SimpleTimeDuration.of(200, TimeUnit.MILLISECONDS))
                                        .doNotBlock()
                                        .build();
                            } else if (clientID >= partition[2] && clientID < partition[3]) {
                                request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                                        .timeoutAfter(SimpleTimeDuration.of(200, TimeUnit.MILLISECONDS))
                                        .build();
                            } else {
                                request = LockRequest.builder(
                                                ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                                        .timeoutAfter(SimpleTimeDuration.of(200, TimeUnit.MILLISECONDS))
                                        .blockForAtMost(SimpleTimeDuration.of(2000, TimeUnit.MILLISECONDS))
                                        .build();
                            }
                            token = server.lockWithFullLockResponse(client, request)
                                    .getToken();
                            if (token == null) {
                                numFailure.set(numFailure.get() + 1);
                            } else {
                                numSuccess.set(numSuccess.get() + 1);
                                assertThat(token.getClient().getClientId()).isEqualTo(Integer.toString(clientID));
                                assertThat(token.getLockDescriptors()).isEqualTo(request.getLockDescriptors());
                                try {
                                    Thread.sleep(50);
                                } catch (InterruptedException e) {
                                    /* Intentionally swallow. */
                                }
                                System.out.println(System.currentTimeMillis() - token.getExpirationDateMs());
                                server.unlock(token);
                                assertThat(server.getTokens(client)).isEmpty();
                            }
                        }
                    } catch (RuntimeException e) {
                        e.printStackTrace();
                        throw e;
                    }
                }
            };
            futures.add(future);
            executor.execute(future);
        }
        Thread.sleep(5000);
        for (Future<?> future : futures) {
            future.cancel(true);
        }
        for (Future<?> future : futures) {
            assertThatThrownBy(future::get).isInstanceOf(ExecutionException.class);
        }
        System.out.println("Number of unsuccessfully acquired locks: " + numFailure.get());
        System.out.println("Number of successfully acquired locks: " + numSuccess.get());
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE, lock2, LockMode.WRITE))
                .build();
        HeldLocksToken token =
                server.lockWithFullLockResponse(LockClient.ANONYMOUS, request).getToken();
        assertThat(token).isNotNull();
        assertThat(token.getClient()).isEqualTo(LockClient.ANONYMOUS);
        assertThat(token.getLockDescriptors()).isEqualTo(request.getLockDescriptors());
        server.unlock(token);
    }

    /**
     * Tests expiring lock tokens and grants
     */
    @Test
    public void testExpiringTokensAndGrants() throws Exception {
        new File("lock_server_timestamp.dat").delete();
        server = SimulatingServerProxy.newProxyInstance(
                LockService.class,
                LockServiceImpl.create(LockServerOptions.builder()
                        .maxAllowedClockDrift(SimpleTimeDuration.of(0, TimeUnit.MILLISECONDS))
                        .build()),
                100);
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                .doNotBlock()
                .timeoutAfter(SimpleTimeDuration.of(500, TimeUnit.MILLISECONDS))
                .build();
        HeldLocksToken token = server.lockWithFullLockResponse(client, request).getToken();
        assertThat(token).isNotNull();
        assertThat(token.getClient()).isEqualTo(client);
        assertThat(token.getLockDescriptors()).isEqualTo(request.getLockDescriptors());
        Thread.sleep(51);
        assertThat(token.getExpirationDateMs() - System.currentTimeMillis()).isLessThan(450);
        HeldLocksToken nullToken =
                server.lockWithFullLockResponse(LockClient.ANONYMOUS, request).getToken();
        assertThat(nullToken).isNull();
        Thread.sleep(450);

        token = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request).getToken();
        assertThat(token).isNotNull();
        assertThat(token.getClient()).isEqualTo(LockClient.ANONYMOUS);
        assertThat(token.getLockDescriptors()).isEqualTo(request.getLockDescriptors());

        HeldLocksGrant grant = server.convertToGrant(token);
        assertThat(grant).isNotNull();
        assertThat(grant.getClient()).isNull();
        assertThat(grant.getLocks()).isEqualTo(request.getLockDescriptors());
        Thread.sleep(51);
        assertThat(grant.getExpirationDateMs() - System.currentTimeMillis()).isLessThan(450);
        grant = server.refreshGrant(grant);
        assertThat(grant).isNotNull();
        assertThat(grant.getExpirationDateMs() - System.currentTimeMillis()).isLessThan(500);
        nullToken =
                server.lockWithFullLockResponse(LockClient.ANONYMOUS, request).getToken();
        assertThat(nullToken).isNull();
        Thread.sleep(500);

        token = server.lockWithFullLockResponse(client, request).getToken();
        assertThat(token).isNotNull();
        assertThat(token.getClient()).isEqualTo(client);
        assertThat(token.getLockDescriptors()).isEqualTo(request.getLockDescriptors());
        server.unlock(token);
        assertThat(server.getTokens(client)).isEmpty();
    }

    /**
     * Convert a write lock to a read lock
     */
    @Test
    public void testConvertWriteToRead() throws Exception {
        final LockRequest request1 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                .build();
        final LockRequest request2 =
                LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ)).build();
        HeldLocksToken token1 =
                server.lockWithFullLockResponse(client, request1).getToken();
        assertThat(token1).isNotNull();
        assertThat(token1.getClient()).isEqualTo(client);
        assertThat(token1.getLockDescriptors()).isEqualTo(request1.getLockDescriptors());

        Future<?> future = executor.submit((Callable<Void>) () -> {
            barrier.await();
            HeldLocksToken validToken = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request2)
                    .getToken();
            assertThat(validToken).isNotNull();
            assertThat(validToken.getClient()).isEqualTo(LockClient.ANONYMOUS);
            assertThat(validToken.getLockDescriptors()).isEqualTo(request2.getLockDescriptors());
            assertThat(server.unlock(validToken)).isTrue();
            return null;
        });
        barrier.await();
        Thread.sleep(50);
        HeldLocksToken token2 =
                server.lockWithFullLockResponse(client, request2).getToken();
        assertThat(token2).isNotNull();
        assertThat(token2.getClient()).isEqualTo(client);
        assertThat(token2.getLockDescriptors()).isEqualTo(request2.getLockDescriptors());
        assertThat(server.unlock(token1)).isTrue();
        future.get();
        assertThat(server.unlock(token2)).isTrue();
        assertThat(server.getTokens(client)).isEmpty();
    }

    /**
     * Test bounds
     */
    @Test
    public void testBoundaryConditions() throws InterruptedException {
        char[] longChar = new char[1000000];
        for (int i = 0; i < 999999; ++i) {
            longChar[i] = 'x';
        }
        longChar[999999] = '\0';
        String longString = String.copyValueOf(longChar);

        assertThat(LockClient.of(null)).isEqualTo(LockClient.ANONYMOUS);
        assertThat(LockClient.of(LockClient.ANONYMOUS.getClientId())).isEqualTo(LockClient.ANONYMOUS);

        LockClient lockClient = LockClient.of(longString);
        assertThat(lockClient.getClientId()).isEqualTo(longString);
        assertThatThrownBy(() -> StringLockDescriptor.of(""))
                .describedAs("Should not create lock descriptor with empty string")
                .isInstanceOf(IllegalArgumentException.class);
        LockDescriptor lock = StringLockDescriptor.of(longString);
        assertThat(lock.getLockIdAsString()).isEqualTo(longString);

        LockRequest request =
                LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.READ)).build();
        HeldLocksToken token =
                server.lockWithFullLockResponse(lockClient, request).getToken();
        assertThat(token).isNotNull().extracting(HeldLocksToken::getClient).isEqualTo(lockClient);
        assertThat(token.getLockDescriptors())
                .isEqualTo(LockCollections.of(ImmutableSortedMap.of(lock, LockMode.READ)));

        LockRequest request2 = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.WRITE))
                .blockForAtMost(SimpleTimeDuration.of(0, TimeUnit.SECONDS))
                .build();
        assertThatThrownBy(() -> server.lockWithFullLockResponse(lockClient, request2))
                .isInstanceOf(IllegalMonitorStateException.class);
        LockClient client2 = LockClient.of("another client");
        HeldLocksToken token2 =
                server.lockWithFullLockResponse(client2, request2).getToken();
        assertThat(token2).isNull();

        LockRequest request3 = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.READ))
                .blockForAtMost(server.getLockServerOptions().getMaxAllowedBlockingDuration())
                .build();
        HeldLocksToken token3 =
                server.lockWithFullLockResponse(client2, request3).getToken();
        assertThat(token3).isNotNull();
        server.unlock(token3);

        TimeDuration beyondMaxDuration = SimpleTimeDuration.of(
                server.getLockServerOptions().getMaxAllowedBlockingDuration().toSeconds() + 10, TimeUnit.SECONDS);
        LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.READ))
                .blockForAtMost(beyondMaxDuration)
                .build();

        TimeDuration negativeDuration = SimpleTimeDuration.of(-10, TimeUnit.SECONDS);
        assertThatThrownBy(() -> LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.READ))
                        .blockForAtMost(negativeDuration)
                        .build())
                .describedAs("Expected: negative time duration")
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Tests unlock and freeze
     */
    @Test
    public void testUnlockAndFreeze() throws Exception {
        new File("lock_server_timestamp.dat").delete();
        server = SimulatingServerProxy.newProxyInstance(
                LockService.class,
                LockServiceImpl.create(LockServerOptions.builder()
                        .maxAllowedClockDrift(SimpleTimeDuration.of(0, TimeUnit.MILLISECONDS))
                        .build()),
                10);

        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                .timeoutAfter(SimpleTimeDuration.of(1, TimeUnit.SECONDS))
                .doNotBlock()
                .build();
        HeldLocksToken token =
                server.lockWithFullLockResponse(LockClient.ANONYMOUS, request).getToken();
        assertThat(token).isNotNull();
        assertThat(token.getClient()).isEqualTo(LockClient.ANONYMOUS);
        assertThat(token.getLockDescriptors())
                .isEqualTo(LockCollections.of(ImmutableSortedMap.of(lock1, LockMode.WRITE)));
        HeldLocksToken finalToken = token;
        assertThatThrownBy(() -> server.unlockAndFreeze(finalToken))
                .describedAs("Expected: anonymous clients can't unlock and freeze")
                .isInstanceOf(IllegalArgumentException.class);

        server.unlock(token);
        assertThat(server.getTokens(client)).isEmpty();

        token = server.lockWithFullLockResponse(client, request).getToken();
        HeldLocksToken token2 = server.lockWithFullLockResponse(client, request).getToken();
        assertThat(token2).isNotNull();
        assertThat(token2.getClient()).isEqualTo(client);
        assertThat(token2.getLockDescriptors())
                .isEqualTo(LockCollections.of(ImmutableSortedMap.of(lock1, LockMode.WRITE)));
        server.unlockAndFreeze(token2);
        token2 = server.lockWithFullLockResponse(client, request).getToken();
        assertThat(token2).isNull();
        server.unlockAndFreeze(token);
        assertThat(server.getTokens(client)).isEmpty();

        token = server.lockWithFullLockResponse(client, request).getToken();
        assertThat(token).isNotNull();
        token2 = server.lockWithFullLockResponse(client, request).getToken();
        assertThat(token2).isNotNull();
        HeldLocksToken token3 = server.lockWithFullLockResponse(client, request).getToken();
        assertThat(token3).isNotNull();
        server.unlockAndFreeze(token3);
        token3 = server.lockWithFullLockResponse(client, request).getToken();
        assertThat(token3).isNull();
        assertThat(server.getTokens(client)).isEmpty();
        HeldLocksToken token4 = server.lockWithFullLockResponse(
                        client,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                                .doNotBlock()
                                .build())
                .getToken();
        assertThat(token4).isNotNull();
        assertThat(server.getTokens(client)).isEqualTo(ImmutableSet.of(token4));
        token = server.lockWithFullLockResponse(client, request).getToken();
        assertThat(token).isNull();
        Thread.sleep(1000);
        token = server.lockWithFullLockResponse(client, request).getToken();
        assertThat(token).isNotNull();
    }

    /**
     * Tests identity operations
     */
    @Test
    public void testIdentity() {
        assertThat(client.getClientId()).isEqualTo("a client");
        assertThat(LockClient.ANONYMOUS.getClientId()).isEmpty();
        assertThat(lock1.getLockIdAsString()).isEqualTo("lock1");
        assertThat(lock2.getLockIdAsString()).isEqualTo("lock2");
    }

    @Test
    public void testReentrantReadRead() throws InterruptedException {
        testReentrancy(LockMode.READ, LockMode.READ);
    }

    @Test
    public void testReentrantReadWrite() {
        assertThatThrownBy(() -> testReentrancy(LockMode.READ, LockMode.WRITE))
                .isInstanceOf(IllegalMonitorStateException.class);
    }

    @Test
    public void testReentrantWriteRead() throws InterruptedException {
        testReentrancy(LockMode.WRITE, LockMode.READ);
    }

    @Test
    public void testReentrantWriteWrite() throws InterruptedException {
        testReentrancy(LockMode.WRITE, LockMode.WRITE);
    }

    private void testReentrancy(LockMode mode1, LockMode mode2) throws InterruptedException {
        LockResponse lockResponse1 = server.lockWithFullLockResponse(
                client, LockRequest.builder(ImmutableSortedMap.of(lock1, mode1)).build());
        LockResponse lockResponse2 = server.lockWithFullLockResponse(
                client, LockRequest.builder(ImmutableSortedMap.of(lock1, mode2)).build());
        server.unlockSimple(SimpleHeldLocksToken.fromHeldLocksToken(lockResponse1.getToken()));
        server.unlockSimple(SimpleHeldLocksToken.fromHeldLocksToken(lockResponse2.getToken()));
    }
}
