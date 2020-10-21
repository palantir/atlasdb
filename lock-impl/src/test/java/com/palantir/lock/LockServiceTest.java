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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the Lock Server.
 *
 * @author jtamer, ddoan
 */
public abstract class LockServiceTest {
    private static final ExecutorService executor = PTExecutors.newCachedThreadPool(LockServiceTest.class.getName());

    private LockService server;
    private LockClient client;
    private LockDescriptor lock1;
    private LockDescriptor lock2;
    private CyclicBarrier barrier;

    protected abstract LockService getLockService();

    @Before
    public void setUp() {
        new File("lock_server_timestamp.dat").delete();
        server = getLockService();
        client = LockClient.of("a client");
        lock1 = StringLockDescriptor.of("lock1");
        lock2 = StringLockDescriptor.of("lock2");
        barrier = new CyclicBarrier(2);
    }

    /** Tests that RemoteLockService api (that internal forwards to LockService api) passes a sanity check. */
    @Test
    public void testRemoteLockServiceApi() throws InterruptedException {
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                .withLockedInVersionId(10)
                .doNotBlock()
                .build();

        Assert.assertNull(server.getMinLockedInVersionId(LockClient.ANONYMOUS.getClientId()));
        LockRefreshToken token = server.lock(LockClient.ANONYMOUS.getClientId(), request);
        Assert.assertEquals(10, (long) server.getMinLockedInVersionId(LockClient.ANONYMOUS.getClientId()));
        Assert.assertNull(server.lock(LockClient.ANONYMOUS.getClientId(), request));
        server.unlock(token);

        Assert.assertNull(server.getMinLockedInVersionId(LockClient.ANONYMOUS.getClientId()));
        HeldLocksToken heldToken = server.lockAndGetHeldLocks(LockClient.ANONYMOUS.getClientId(), request);
        Assert.assertEquals(10, (long) server.getMinLockedInVersionId(LockClient.ANONYMOUS.getClientId()));
        Assert.assertNull(server.lockAndGetHeldLocks(LockClient.ANONYMOUS.getClientId(), request));
        server.unlock(heldToken.getLockRefreshToken());
    }

    /** Tests using doNotBlock() in the lock request. */
    @Test
    public void testDoNotBlock() throws InterruptedException {
        long lockTimeoutMs = LockRequest.getDefaultLockTimeout().toMillis();
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                .withLockedInVersionId(10)
                .doNotBlock()
                .build();
        long currentTimeMs = System.currentTimeMillis();
        LockResponse response = server.lockWithFullLockResponse(client, request);
        Assert.assertTrue(response.success());
        Assert.assertTrue(response.getLockHolders().isEmpty());
        HeldLocksToken token1 = response.getToken();
        Assert.assertNotNull(token1);
        Assert.assertEquals(client, token1.getClient());
        Assert.assertEquals(10, (long) token1.getVersionId());
        Assert.assertEquals(request.getLockDescriptors(), token1.getLockDescriptors());
        Assert.assertTrue(currentTimeMs + lockTimeoutMs <= token1.getExpirationDateMs());
        Assert.assertTrue(token1.getExpirationDateMs() <= System.currentTimeMillis() + lockTimeoutMs);

        HeldLocksToken nullToken = server.lockWithFullLockResponse(
                        LockClient.ANONYMOUS,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                                .doNotBlock()
                                .build())
                .getToken();
        Assert.assertNull(nullToken);

        nullToken = server.lockWithFullLockResponse(
                        LockClient.ANONYMOUS,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                                .doNotBlock()
                                .build())
                .getToken();
        Assert.assertNull(nullToken);

        HeldLocksToken anonymousReadToken = server.lockWithFullLockResponse(
                        LockClient.ANONYMOUS,
                        LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                                .doNotBlock()
                                .build())
                .getToken();
        Assert.assertNotNull(anonymousReadToken);
        server.unlock(anonymousReadToken);

        HeldLocksToken token2 = server.lockWithFullLockResponse(
                        client,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                                .withLockedInVersionId(5)
                                .doNotBlock()
                                .build())
                .getToken();
        Assert.assertNotNull(token2);

        HeldLocksToken token3 = server.lockWithFullLockResponse(
                        client,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                                .doNotBlock()
                                .build())
                .getToken();
        Assert.assertNotNull(token3);

        Assert.assertEquals(ImmutableSet.of(token1, token2, token3), server.getTokens(client));
        Assert.assertEquals(5, server.getMinLockedInVersionId(client).longValue());

        server.unlock(token2);
        Assert.assertEquals(ImmutableSet.of(token1, token3), server.getTokens(client));
        Assert.assertEquals(10, server.getMinLockedInVersionId(client).longValue());

        server.unlock(token1);
        Assert.assertEquals(ImmutableSet.of(token3), server.refreshTokens(ImmutableSet.of(token1, token2, token3)));
        Assert.assertNull(server.getMinLockedInVersionId(client));

        server.unlock(token3);
        Assert.assertTrue(server.getTokens(client).isEmpty());
        Assert.assertNull(server.getMinLockedInVersionId(client));
    }

    /** Tests using blockForAtMost() in the lock request. */
    @Test
    public void testBlockForAtMost() throws Exception {
        long lockTimeoutMs = LockRequest.getDefaultLockTimeout().toMillis();
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                .withLockedInVersionId(10)
                .blockForAtMost(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                .build();
        long currentTimeMs = System.currentTimeMillis();
        LockResponse response = server.lockWithFullLockResponse(client, request);
        Assert.assertTrue(response.success());
        Assert.assertTrue(response.getLockHolders().isEmpty());
        HeldLocksToken token1 = response.getToken();
        Assert.assertNotNull(token1);
        Assert.assertEquals(client, token1.getClient());
        Assert.assertEquals(request.getLockDescriptors(), token1.getLockDescriptors());
        Assert.assertTrue(currentTimeMs + lockTimeoutMs <= token1.getExpirationDateMs());
        Assert.assertTrue(token1.getExpirationDateMs() <= System.currentTimeMillis() + lockTimeoutMs);

        Future<?> future = executor.submit((Callable<Void>) () -> {
            LockResponse response1 = server.lockWithFullLockResponse(
                    LockClient.ANONYMOUS,
                    LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                            .blockForAtMost(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                            .build());
            Assert.assertFalse(response1.success());
            Assert.assertFalse(response1.getLockHolders().isEmpty());
            Assert.assertEquals(ImmutableSortedMap.of(lock2, client), response1.getLockHolders());
            HeldLocksToken nullToken = response1.getToken();
            Assert.assertNull(nullToken);
            barrier.await();

            response1 = server.lockWithFullLockResponse(
                    LockClient.ANONYMOUS,
                    LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                            .blockForAtMost(SimpleTimeDuration.of(100, TimeUnit.MILLISECONDS))
                            .build());
            Assert.assertTrue(response1.success());
            Assert.assertTrue(response1.getLockHolders().isEmpty());
            HeldLocksToken validToken = response1.getToken();
            Assert.assertNotNull(validToken);
            server.unlock(validToken);

            return null;
        });

        barrier.await();
        Thread.sleep(10);
        server.unlock(token1);
        future.get();
        token1 = server.lockWithFullLockResponse(client, request).getToken();

        response = server.lockWithFullLockResponse(
                LockClient.ANONYMOUS,
                LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                        .blockForAtMost(SimpleTimeDuration.of(0, TimeUnit.MILLISECONDS))
                        .build());
        Assert.assertTrue(response.success());
        Assert.assertTrue(response.getLockHolders().isEmpty());
        HeldLocksToken anonymousReadToken = response.getToken();
        Assert.assertNotNull(anonymousReadToken);
        server.unlock(anonymousReadToken);

        response = server.lockWithFullLockResponse(
                client,
                LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                        .withLockedInVersionId(5)
                        .blockForAtMost(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                        .build());
        Assert.assertTrue(response.success());
        Assert.assertTrue(response.getLockHolders().isEmpty());
        HeldLocksToken token2 = response.getToken();
        Assert.assertNotNull(token2);

        response = server.lockWithFullLockResponse(
                client,
                LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                        .blockForAtMost(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                        .build());
        Assert.assertTrue(response.success());
        Assert.assertTrue(response.getLockHolders().isEmpty());
        HeldLocksToken token3 = response.getToken();
        Assert.assertNotNull(token3);

        Assert.assertEquals(ImmutableSet.of(token1, token2, token3), server.getTokens(client));
        Assert.assertEquals(5, server.getMinLockedInVersionId(client).longValue());
        Assert.assertNull(server.getMinLockedInVersionId(LockClient.ANONYMOUS));

        server.unlock(token2);
        Assert.assertEquals(ImmutableSet.of(token1, token3), server.getTokens(client));
        Assert.assertEquals(10, server.getMinLockedInVersionId(client).longValue());
        Assert.assertNull(server.getMinLockedInVersionId(LockClient.ANONYMOUS));

        server.unlock(token1);
        Assert.assertEquals(ImmutableSet.of(token3), server.refreshTokens(ImmutableSet.of(token1, token2, token3)));
        Assert.assertNull(server.getMinLockedInVersionId(client));

        server.unlock(token3);
        Assert.assertTrue(server.getTokens(client).isEmpty());
        Assert.assertNull(server.getMinLockedInVersionId(client));
    }

    /** Tests using block indefinitely mode */
    @Test
    public void testBlockIndefinitely() throws Exception {
        long lockTimeoutMs = LockRequest.getDefaultLockTimeout().toMillis();
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                .withLockedInVersionId(10)
                .build();
        long currentTimeMs = System.currentTimeMillis();
        LockResponse response = server.lockWithFullLockResponse(client, request);
        Assert.assertTrue(response.success());
        Assert.assertTrue(response.getLockHolders().isEmpty());
        HeldLocksToken token1 = response.getToken();
        Assert.assertNotNull(token1);
        Assert.assertEquals(client, token1.getClient());
        Assert.assertEquals(request.getLockDescriptors(), token1.getLockDescriptors());
        Assert.assertTrue(currentTimeMs + lockTimeoutMs <= token1.getExpirationDateMs());
        Assert.assertTrue(token1.getExpirationDateMs() <= System.currentTimeMillis() + lockTimeoutMs);

        Future<?> future = executor.submit((Callable<Void>) () -> {
            barrier.await();
            HeldLocksToken validToken = server.lockWithFullLockResponse(
                            LockClient.ANONYMOUS,
                            LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                                    .build())
                    .getToken();
            Assert.assertNotNull(validToken);
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
        Assert.assertNotNull(anonymousReadToken);
        server.unlock(anonymousReadToken);

        HeldLocksToken token2 = server.lockWithFullLockResponse(
                        client,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ))
                                .withLockedInVersionId(5)
                                .build())
                .getToken();
        Assert.assertNotNull(token2);

        HeldLocksToken token3 = server.lockWithFullLockResponse(
                        client,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                                .build())
                .getToken();
        Assert.assertNotNull(token3);

        Assert.assertEquals(ImmutableSet.of(token1, token2, token3), server.getTokens(client));
        Assert.assertEquals(5, server.getMinLockedInVersionId(client).longValue());
        Assert.assertNull(server.getMinLockedInVersionId(LockClient.ANONYMOUS));

        server.unlock(token2);
        Assert.assertEquals(ImmutableSet.of(token1, token3), server.getTokens(client));
        Assert.assertEquals(10, server.getMinLockedInVersionId(client).longValue());
        Assert.assertNull(server.getMinLockedInVersionId(LockClient.ANONYMOUS));

        server.unlock(token1);
        Assert.assertEquals(ImmutableSet.of(token3), server.refreshTokens(ImmutableSet.of(token1, token2, token3)));
        Assert.assertNull(server.getMinLockedInVersionId(client));

        server.unlock(token3);
        Assert.assertTrue(server.getTokens(client).isEmpty());
        Assert.assertNull(server.getMinLockedInVersionId(client));
    }

    /** Tests lockAndRelease */
    @Test
    public void testLockAndRelease() throws Exception {
        LockRequest hasLock2 = LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                .build();
        final LockRequest request = LockRequest.builder(
                        ImmutableSortedMap.of(lock1, LockMode.WRITE, lock2, LockMode.WRITE))
                .lockAndRelease()
                .build();

        LockResponse resp2 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, hasLock2);
        Assert.assertTrue(resp2.success());

        Future<?> future = executor.submit((Callable<Void>) () -> {
            LockResponse resp = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
            Assert.assertNotNull(resp);
            Assert.assertTrue(resp.success());
            return null;
        });

        try {
            future.get(1, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (TimeoutException e) {
            // good
        }

        server.unlock(resp2.getToken());

        future.get(150, TimeUnit.SECONDS);

        resp2 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, hasLock2);
        server.unlock(resp2.getToken());
    }

    /** Tests lockAndRelease with perf optimization */
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
        Assert.assertTrue(resp2.success());

        Future<?> future = executor.submit((Callable<Void>) () -> {
            LockResponse resp = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
            Assert.assertNotNull(resp);
            Assert.assertTrue(resp.success());
            return null;
        });

        Thread.sleep(10);
        try {
            future.get(1, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (TimeoutException e) {
            // good
        }
        LockResponse resp1 = server.lockWithFullLockResponse(client, hasLock1);

        server.unlock(resp2.getToken());

        future.get(150, TimeUnit.SECONDS);

        server.unlock(resp1.getToken());

        resp2 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, hasLock2);
        server.unlock(resp2.getToken());
    }

    /** Tests lockAsManyAsPossible() */
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
        Assert.assertNotNull(token1);
        Assert.assertEquals(client, token1.getClient());
        Assert.assertEquals(request1.getLockDescriptors(), token1.getLockDescriptors());

        response = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request2);
        HeldLocksToken token2 = response.getToken();
        System.out.println(response.getLockHolders());
        Assert.assertNotNull(token2);
        Assert.assertEquals(LockClient.ANONYMOUS, token2.getClient());
        Assert.assertEquals(request2.getLockDescriptors(), token2.getLockDescriptors());

        LockRequest request3 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.WRITE))
                .lockAsManyAsPossible()
                .blockForAtMost(SimpleTimeDuration.of(100, TimeUnit.MILLISECONDS))
                .build();
        response = server.lockWithFullLockResponse(client, request3);
        Assert.assertTrue(response.success());
        HeldLocksToken token3 = response.getToken();
        Assert.assertNotNull(token3);
        Assert.assertEquals(client, token3.getClient());
        Assert.assertEquals(
                LockCollections.of(ImmutableSortedMap.of(lock1, LockMode.READ)), token3.getLockDescriptors());

        server.unlock(token1);
        server.unlock(token2);
        server.unlock(token3);
        Assert.assertTrue(server.getTokens(client).isEmpty());
    }

    /** Tests against LockService.logCurrentState() long-block bug (QA-87074) */
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
        Assert.assertTrue(response1.success());
        Assert.assertTrue(response1.getLockHolders().isEmpty());
        HeldLocksToken token1 = response1.getToken();
        Assert.assertNotNull(token1);
        Assert.assertEquals(LockClient.ANONYMOUS, token1.getClient());
        Assert.assertEquals(request1.getLockDescriptors(), token1.getLockDescriptors());
        Assert.assertTrue(currentTimeMs + lockTimeoutMs <= token1.getExpirationDateMs());
        Assert.assertTrue(token1.getExpirationDateMs() <= System.currentTimeMillis() + lockTimeoutMs);

        // Second request grabs corresponding WRITE lock, will block inside LockServer until READ lock expires
        executor.submit((Callable<Void>) () -> {
            barrier.await();
            LockRequest request2 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                    .build();
            LockResponse response2 = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request2);
            HeldLocksToken validToken = response2.getToken();
            Assert.assertNotNull(validToken);
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
            // If we exceed the timeout, the call is hung and it's a failure
            Assert.fail();
        } finally {
            LockServiceTestUtils.cleanUpLogStateDir();
        }
    }

    /** Tests lock responses */
    @Test
    public void testLockReponse() throws InterruptedException {
        LockDescriptor lock3 = StringLockDescriptor.of("third lock");
        LockDescriptor lock4 = StringLockDescriptor.of("fourth lock");

        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(
                        lock1, LockMode.READ, lock2, LockMode.READ, lock3, LockMode.WRITE, lock4, LockMode.WRITE))
                .doNotBlock()
                .build();
        LockResponse response = server.lockWithFullLockResponse(client, request);
        Assert.assertTrue(response.success());
        Assert.assertTrue(response.getLockHolders().isEmpty());

        request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock3, LockMode.WRITE))
                .lockAsManyAsPossible()
                .blockForAtMost(SimpleTimeDuration.of(50, TimeUnit.MILLISECONDS))
                .build();
        response = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
        Assert.assertTrue(response.success());
        Assert.assertFalse(response.getLockHolders().isEmpty());
        Assert.assertEquals(ImmutableMap.of(lock3, client), response.getLockHolders());
        HeldLocksToken token = response.getToken();
        Assert.assertEquals(LockClient.ANONYMOUS, token.getClient());
        Assert.assertEquals(
                LockCollections.of(ImmutableSortedMap.of(lock1, LockMode.READ)), token.getLockDescriptors());

        request = LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.READ, lock4, LockMode.WRITE))
                .lockAsManyAsPossible()
                .blockForAtMost(SimpleTimeDuration.of(50, TimeUnit.MILLISECONDS))
                .build();
        response = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
        Assert.assertTrue(response.success());
        Assert.assertFalse(response.getLockHolders().isEmpty());
        Assert.assertEquals(ImmutableMap.of(lock4, client), response.getLockHolders());
        token = response.getToken();
        Assert.assertEquals(LockClient.ANONYMOUS, token.getClient());
        Assert.assertEquals(
                LockCollections.of(ImmutableSortedMap.of(lock2, LockMode.READ)), token.getLockDescriptors());

        request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.READ))
                .lockAsManyAsPossible()
                .blockForAtMost(SimpleTimeDuration.of(50, TimeUnit.MILLISECONDS))
                .build();
        response = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
        Assert.assertTrue(response.success());
        Assert.assertTrue(response.getLockHolders().isEmpty());
        token = response.getToken();
        Assert.assertEquals(LockClient.ANONYMOUS, token.getClient());
        Assert.assertEquals(
                LockCollections.of(ImmutableSortedMap.of(lock1, LockMode.READ, lock2, LockMode.READ)),
                token.getLockDescriptors());

        request = LockRequest.builder(ImmutableSortedMap.of(lock3, LockMode.WRITE, lock4, LockMode.WRITE))
                .lockAsManyAsPossible()
                .blockForAtMost(SimpleTimeDuration.of(50, TimeUnit.MILLISECONDS))
                .build();
        response = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request);
        Assert.assertFalse(response.success());
        Assert.assertFalse(response.getLockHolders().isEmpty());
        Assert.assertEquals(ImmutableSortedMap.of(lock3, client, lock4, client), response.getLockHolders());
        token = response.getToken();
        Assert.assertNull(token);
    }

    /** Tests grants */
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
        Assert.assertNotNull(token1);
        HeldLocksToken token2 =
                server.lockWithFullLockResponse(client, requestRead).getToken();
        Assert.assertNotNull(token2);
        try {
            server.convertToGrant(token1);
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected: holding both read and write locks */
        }
        HeldLocksToken token3 =
                server.lockWithFullLockResponse(client, requestTwoLocks).getToken();
        Assert.assertNotNull(token3);
        try {
            server.convertToGrant(token3);
            Assert.fail();
        } catch (IllegalMonitorStateException expected) {
            /* Expected: holding multiple locks */
        }
        server.unlock(token2);
        server.unlock(token3);

        LockClient client2 = LockClient.of("client2");
        LockResponse response = server.lockWithFullLockResponse(client2, requestWrite);
        Assert.assertFalse(response.success());
        Assert.assertEquals(ImmutableMap.of(lock1, client), response.getLockHolders());
        HeldLocksToken nullToken = response.getToken();
        Assert.assertNull(nullToken);

        HeldLocksGrant grantToken = server.convertToGrant(token1);
        Assert.assertNull(grantToken.getClient());

        HeldLocksToken validToken = server.useGrant(client2, grantToken);
        Assert.assertNotNull(validToken);
        Assert.assertEquals(client2, validToken.getClient());
        server.unlock(validToken);

        requestWrite = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                .build();
        token1 = server.lockWithFullLockResponse(client, requestWrite).getToken();
        Assert.assertNotNull(token1);
        Assert.assertEquals(client, token1.getClient());
        Future<?> future = executor.submit((Callable<Void>) () -> {
            HeldLocksToken validToken1 = server.lockWithFullLockResponse(
                            LockClient.ANONYMOUS,
                            LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                                    .build())
                    .getToken();
            Assert.assertNotNull(validToken1);
            Assert.assertEquals(LockClient.ANONYMOUS, validToken1.getClient());
            server.unlock(validToken1);

            return null;
        });
        grantToken = server.convertToGrant(token1);
        Assert.assertNull(grantToken.getClient());

        validToken = server.useGrant(client2, grantToken.getGrantId());
        Assert.assertNotNull(validToken);
        Assert.assertEquals(client2, validToken.getClient());

        Thread.sleep(100);
        server.unlock(validToken);
        future.get();
        Assert.assertTrue(server.getTokens(client).isEmpty());
        Assert.assertTrue(server.getTokens(client2).isEmpty());
    }

    /** Tests for illegal actions */
    @Test
    public void testIllegalActions() throws InterruptedException {
        try {
            LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                    .doNotBlock()
                    .doNotBlock()
                    .build();
            Assert.fail();
        } catch (IllegalStateException expected) {
            /* Expected: can't call doNotBlock() twice */
        }
        try {
            LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                    .doNotBlock()
                    .blockForAtMost(SimpleTimeDuration.of(1, TimeUnit.MILLISECONDS))
                    .build();
            Assert.fail();
        } catch (IllegalStateException expected) {
            /* Expected: can't call both doNotBlock() and blockForAtMost() */
        }
        try {
            LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                    .blockForAtMost(SimpleTimeDuration.of(1, TimeUnit.MILLISECONDS))
                    .blockForAtMost(SimpleTimeDuration.of(1, TimeUnit.MILLISECONDS))
                    .build();
            Assert.fail();
        } catch (IllegalStateException expected) {
            /* Expected: can't call blockForAtMost() twice */
        }
        try {
            LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                    .timeoutAfter(SimpleTimeDuration.of(1, TimeUnit.MILLISECONDS))
                    .timeoutAfter(SimpleTimeDuration.of(1, TimeUnit.MILLISECONDS))
                    .build();
            Assert.fail();
        } catch (IllegalStateException expected) {
            /* Expected: can't call timeoutAfter() twice */
        }
        try {
            LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                    .lockAsManyAsPossible()
                    .lockAsManyAsPossible()
                    .build();
            Assert.fail();
        } catch (IllegalStateException expected) {
            /* Expected: can't call lockAsManyAsPossible() twice */
        }
        try {
            LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                    .lockAsManyAsPossible()
                    .build();
            Assert.fail();
        } catch (IllegalStateException expected) {
            /* Expected: lockAsManyAsPossible() requires doNotBlock() or blockForAtMost() modes */
        }
        try {
            LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                    .withLockedInVersionId(1)
                    .withLockedInVersionId(2)
                    .build();
            Assert.fail();
        } catch (IllegalStateException expected) {
            /* Expected: can't call withLockedInVersionId() twice */
        }
        HeldLocksToken token = server.lockWithFullLockResponse(
                        client,
                        LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ))
                                .doNotBlock()
                                .build())
                .getToken();
        Assert.assertNotNull(token);
        Assert.assertEquals(ImmutableSet.of(token), server.getTokens(client));
        try {
            server.getTokens(LockClient.ANONYMOUS);
            Assert.fail();
        } catch (IllegalArgumentException expected) {
            /* Expected: can't refresh an anonymous client */
        }
        try {
            server.getTokens(LockClient.INTERNAL_LOCK_GRANT_CLIENT);
            Assert.fail();
        } catch (IllegalArgumentException expected) {
            /* Expected: can't refresh the internal lock grant client */
        }
        try {
            server.unlockAndFreeze(token);
            Assert.fail();
        } catch (IllegalArgumentException expected) {
            /* Expected: can't unlock and freeze read lock */
        }
        server.unlock(token);
        Assert.assertTrue(server.getTokens(client).isEmpty());
    }

    /** Tests grabbing many locks with each request */
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
        Assert.assertNotNull(readWriteToken);
        Assert.assertEquals(client, readWriteToken.getClient());
        Assert.assertEquals(
                LockCollections.of(ImmutableSortedMap.copyOf(lockMap)), readWriteToken.getLockDescriptors());

        lockMap = new TreeMap<>();
        for (int i = 0; i < numLocks; ++i) {
            lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.READ);
        }
        requestAllLocks = LockRequest.builder(lockMap).doNotBlock().build();
        HeldLocksToken token =
                server.lockWithFullLockResponse(client, requestAllLocks).getToken();
        Assert.assertNotNull(token);
        Assert.assertEquals(client, token.getClient());
        Assert.assertEquals(LockCollections.of(ImmutableSortedMap.copyOf(lockMap)), token.getLockDescriptors());

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
        Assert.assertNull(token);
        requestAllLocks =
                LockRequest.builder(lockMap).doNotBlock().lockAsManyAsPossible().build();
        token = server.lockWithFullLockResponse(client2, requestAllLocks).getToken();
        Assert.assertNotNull(token);
        Assert.assertEquals(client2, token.getClient());
        lockMap = new TreeMap<>();
        for (int i = 0; i < numLocks; ++i) {
            if (i % 2 != 0) {
                lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.WRITE);
            }
        }
        Assert.assertEquals(LockCollections.of(ImmutableSortedMap.copyOf(lockMap)), token.getLockDescriptors());
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
        Assert.assertNotNull(token);
        Assert.assertEquals(client2, token.getClient());
        lockMap = new TreeMap<>();
        for (int i = 0; i < numLocks; ++i) {
            if (i % 2 != 0) {
                lockMap.put(StringLockDescriptor.of("lock " + i), LockMode.READ);
            }
        }
        Assert.assertEquals(LockCollections.of(ImmutableSortedMap.copyOf(lockMap)), token.getLockDescriptors());
        server.unlock(token);
    }

    /** Tests using multiple threads to grab the same locks */
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
                                Assert.assertEquals(
                                        Integer.toString(clientID),
                                        token.getClient().getClientId());
                                Assert.assertEquals(request.getLockDescriptors(), token.getLockDescriptors());
                                try {
                                    Thread.sleep(50);
                                } catch (InterruptedException e) {
                                    /* Intentionally swallow. */
                                }
                                System.out.println(System.currentTimeMillis() - token.getExpirationDateMs());
                                server.unlock(token);
                                Assert.assertTrue(server.getTokens(client).isEmpty());
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
            try {
                future.get();
                Assert.fail();
            } catch (ExecutionException expected) {
                /* expected */
            }
        }
        System.out.println("Number of unsuccessfully acquired locks: " + numFailure.get());
        System.out.println("Number of successfully acquired locks: " + numSuccess.get());
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE, lock2, LockMode.WRITE))
                .build();
        HeldLocksToken token =
                server.lockWithFullLockResponse(LockClient.ANONYMOUS, request).getToken();
        Assert.assertNotNull(token);
        Assert.assertEquals(LockClient.ANONYMOUS, token.getClient());
        Assert.assertEquals(request.getLockDescriptors(), token.getLockDescriptors());
        server.unlock(token);
    }

    /** Tests expiring lock tokens and grants */
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
        Assert.assertNotNull(token);
        Assert.assertEquals(client, token.getClient());
        Assert.assertEquals(request.getLockDescriptors(), token.getLockDescriptors());
        Thread.sleep(51);
        Assert.assertTrue(token.getExpirationDateMs() - System.currentTimeMillis() < 450);
        HeldLocksToken nullToken =
                server.lockWithFullLockResponse(LockClient.ANONYMOUS, request).getToken();
        Assert.assertNull(nullToken);
        Thread.sleep(450);

        token = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request).getToken();
        Assert.assertNotNull(token);
        Assert.assertEquals(LockClient.ANONYMOUS, token.getClient());
        Assert.assertEquals(request.getLockDescriptors(), token.getLockDescriptors());

        HeldLocksGrant grant = server.convertToGrant(token);
        Assert.assertNotNull(grant);
        Assert.assertNull(grant.getClient());
        Assert.assertEquals(request.getLockDescriptors(), grant.getLocks());
        Thread.sleep(51);
        Assert.assertTrue(grant.getExpirationDateMs() - System.currentTimeMillis() < 450);
        grant = server.refreshGrant(grant);
        Assert.assertTrue(grant.getExpirationDateMs() - System.currentTimeMillis() < 500);
        nullToken =
                server.lockWithFullLockResponse(LockClient.ANONYMOUS, request).getToken();
        Assert.assertNull(nullToken);
        Thread.sleep(500);

        token = server.lockWithFullLockResponse(client, request).getToken();
        Assert.assertNotNull(token);
        Assert.assertEquals(client, token.getClient());
        Assert.assertEquals(request.getLockDescriptors(), token.getLockDescriptors());
        server.unlock(token);
        Assert.assertTrue(server.getTokens(client).isEmpty());
    }

    /** Convert a write lock to a read lock */
    @Test
    public void testConvertWriteToRead() throws Exception {
        final LockRequest request1 = LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.WRITE))
                .build();
        final LockRequest request2 =
                LockRequest.builder(ImmutableSortedMap.of(lock1, LockMode.READ)).build();
        HeldLocksToken token1 =
                server.lockWithFullLockResponse(client, request1).getToken();
        Assert.assertNotNull(token1);
        Assert.assertEquals(client, token1.getClient());
        Assert.assertEquals(request1.getLockDescriptors(), token1.getLockDescriptors());

        Future<?> future = executor.submit((Callable<Void>) () -> {
            barrier.await();
            HeldLocksToken validToken = server.lockWithFullLockResponse(LockClient.ANONYMOUS, request2)
                    .getToken();
            Assert.assertNotNull(validToken);
            Assert.assertEquals(LockClient.ANONYMOUS, validToken.getClient());
            Assert.assertEquals(request2.getLockDescriptors(), validToken.getLockDescriptors());
            Assert.assertTrue(server.unlock(validToken));
            return null;
        });
        barrier.await();
        Thread.sleep(50);
        HeldLocksToken token2 =
                server.lockWithFullLockResponse(client, request2).getToken();
        Assert.assertNotNull(token2);
        Assert.assertEquals(client, token2.getClient());
        Assert.assertEquals(request2.getLockDescriptors(), token2.getLockDescriptors());
        Assert.assertTrue(server.unlock(token1));
        future.get();
        Assert.assertTrue(server.unlock(token2));
        Assert.assertTrue(server.getTokens(client).isEmpty());
    }

    /** Test bounds */
    @Test
    public void testBoundaryConditions() throws InterruptedException {
        LockClient client;
        LockDescriptor lock;

        char[] longChar = new char[1000000];
        for (int i = 0; i < 999999; ++i) {
            longChar[i] = 'x';
        }
        longChar[999999] = '\0';
        String longString = String.copyValueOf(longChar);

        Assert.assertEquals(LockClient.of(null), LockClient.ANONYMOUS);
        Assert.assertEquals(LockClient.of(LockClient.ANONYMOUS.getClientId()), LockClient.ANONYMOUS);

        client = LockClient.of(longString);
        Assert.assertEquals(longString, client.getClientId());
        try {
            lock = StringLockDescriptor.of("");
            Assert.fail();
        } catch (IllegalArgumentException expected) {
            /* Expected: empty string */
        }
        lock = StringLockDescriptor.of(longString);
        Assert.assertEquals(longString, lock.getLockIdAsString());

        LockRequest request =
                LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.READ)).build();
        HeldLocksToken token = server.lockWithFullLockResponse(client, request).getToken();
        Assert.assertEquals(client, token.getClient());
        Assert.assertEquals(LockCollections.of(ImmutableSortedMap.of(lock, LockMode.READ)), token.getLockDescriptors());

        LockRequest request2 = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.WRITE))
                .blockForAtMost(SimpleTimeDuration.of(0, TimeUnit.SECONDS))
                .build();
        try {
            server.lockWithFullLockResponse(client, request2);
            Assert.fail();
        } catch (IllegalMonitorStateException e) {
            // expected
        }
        LockClient client2 = LockClient.of("another client");
        HeldLocksToken token2 =
                server.lockWithFullLockResponse(client2, request2).getToken();
        Assert.assertNull(token2);
        request2 = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.READ))
                .blockForAtMost(server.getLockServerOptions().getMaxAllowedBlockingDuration())
                .build();
        token2 = server.lockWithFullLockResponse(client2, request2).getToken();
        Assert.assertNotNull(token2);
        server.unlock(token2);

        TimeDuration beyondMaxDuration = SimpleTimeDuration.of(
                server.getLockServerOptions().getMaxAllowedBlockingDuration().toSeconds() + 10, TimeUnit.SECONDS);
        request2 = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.READ))
                .blockForAtMost(beyondMaxDuration)
                .build();
        TimeDuration negativeDuration = SimpleTimeDuration.of(-10, TimeUnit.SECONDS);
        try {
            request2 = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.READ))
                    .blockForAtMost(negativeDuration)
                    .build();
            Assert.fail();
        } catch (IllegalArgumentException expected) {
            /* Expected: negative time duration */
        }
    }

    /** Tests unlock and freeze */
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
        Assert.assertNotNull(token);
        Assert.assertEquals(LockClient.ANONYMOUS, token.getClient());
        Assert.assertEquals(
                LockCollections.of(ImmutableSortedMap.of(lock1, LockMode.WRITE)), token.getLockDescriptors());
        try {
            server.unlockAndFreeze(token);
            Assert.fail();
        } catch (IllegalArgumentException expected) {
            /* Expected: anonymous clients can't unlock and freeze */
        }
        server.unlock(token);
        Assert.assertTrue(server.getTokens(client).isEmpty());

        token = server.lockWithFullLockResponse(client, request).getToken();
        HeldLocksToken token2 = server.lockWithFullLockResponse(client, request).getToken();
        Assert.assertNotNull(token2);
        Assert.assertEquals(client, token2.getClient());
        Assert.assertEquals(
                LockCollections.of(ImmutableSortedMap.of(lock1, LockMode.WRITE)), token2.getLockDescriptors());
        server.unlockAndFreeze(token2);
        token2 = server.lockWithFullLockResponse(client, request).getToken();
        Assert.assertNull(token2);
        server.unlockAndFreeze(token);
        Assert.assertTrue(server.getTokens(client).isEmpty());

        token = server.lockWithFullLockResponse(client, request).getToken();
        Assert.assertNotNull(token);
        token2 = server.lockWithFullLockResponse(client, request).getToken();
        Assert.assertNotNull(token2);
        HeldLocksToken token3 = server.lockWithFullLockResponse(client, request).getToken();
        Assert.assertNotNull(token3);
        server.unlockAndFreeze(token3);
        token3 = server.lockWithFullLockResponse(client, request).getToken();
        Assert.assertNull(token3);
        Assert.assertTrue(server.getTokens(client).isEmpty());
        HeldLocksToken token4 = server.lockWithFullLockResponse(
                        client,
                        LockRequest.builder(ImmutableSortedMap.of(lock2, LockMode.WRITE))
                                .doNotBlock()
                                .build())
                .getToken();
        Assert.assertNotNull(token4);
        Assert.assertEquals(ImmutableSet.of(token4), server.getTokens(client));
        token = server.lockWithFullLockResponse(client, request).getToken();
        Assert.assertNull(token);
        Thread.sleep(1000);
        token = server.lockWithFullLockResponse(client, request).getToken();
        Assert.assertNotNull(token);
    }

    /** Tests identity operations */
    @Test
    public void testIdentity() {
        Assert.assertEquals("a client", client.getClientId());
        Assert.assertEquals("", LockClient.ANONYMOUS.getClientId());
        Assert.assertEquals("lock1", lock1.getLockIdAsString());
        Assert.assertEquals("lock2", lock2.getLockIdAsString());
    }

    @Test
    public void testReentrantReadRead() throws InterruptedException {
        testReentrancy(LockMode.READ, LockMode.READ);
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testReentrantReadWrite() throws InterruptedException {
        testReentrancy(LockMode.READ, LockMode.WRITE);
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
