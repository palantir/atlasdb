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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartTransactionRequestV4;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;

public class AsyncTimelockServiceIntegrationTest extends AbstractAsyncTimelockServiceIntegrationTest {

    private static final LockDescriptor LOCK_A = StringLockDescriptor.of("a");
    private static final LockDescriptor LOCK_B = StringLockDescriptor.of("b");

    private static final long SHORT_TIMEOUT = 500L;
    private static final long TIMEOUT = 10_000L;
    private static final LockClient TEST_CLIENT = LockClient.of("test");
    private static final LockClient TEST_CLIENT_2 = LockClient.of("test2");
    private static final LockClient TEST_CLIENT_3 = LockClient.of("test3");

    @Test
    public void canLockRefreshAndUnlock() {
        LockToken token = cluster.lock(requestFor(LOCK_A)).getToken();
        boolean wasRefreshed = cluster.refreshLockLease(token);
        boolean wasUnlocked = cluster.unlock(token);

        assertTrue(wasRefreshed);
        assertTrue(wasUnlocked);
    }

    @Test
    public void locksAreExclusive() {
        LockToken token = cluster.lock(requestFor(LOCK_A)).getToken();
        Future<LockToken> futureToken = cluster.lockAsync(requestFor(LOCK_A))
                .thenApply(LockResponse::getToken);

        assertNotYetLocked(futureToken);

        cluster.unlock(token);

        assertLockedAndUnlock(futureToken);
    }

    @Test
    public void canLockImmutableTimestamp() {
        LockImmutableTimestampResponse response1 = cluster.timelockService()
                .lockImmutableTimestamp();
        LockImmutableTimestampResponse response2 = cluster.timelockService()
                .lockImmutableTimestamp();

        long immutableTs = cluster.timelockService().getImmutableTimestamp();
        assertThat(immutableTs).isEqualTo(response1.getImmutableTimestamp());

        cluster.unlock(response1.getLock());

        assertThat(immutableTs).isEqualTo(response2.getImmutableTimestamp());

        cluster.unlock(response2.getLock());
    }

    @Test
    public void batchedStartTransactionCallShouldLockImmutableTimestamp() {
        StartTransactionRequestV4 request = StartTransactionRequestV4.createForRequestor(
                UUID.randomUUID(),
                123);

        LockImmutableTimestampResponse response1 = cluster.timelockRpcClient()
                .startTransactions(request)
                .immutableTimestamp();

        LockImmutableTimestampResponse response2 = cluster.timelockRpcClient()
                .startTransactions(request)
                .immutableTimestamp();

        long immutableTs = cluster.timelockService().getImmutableTimestamp();
        assertThat(immutableTs).isEqualTo(response1.getImmutableTimestamp());

        cluster.timelockRpcClient().unlock(ImmutableSet.of(response1.getLock()));

        assertThat(immutableTs).isEqualTo(response2.getImmutableTimestamp());

        cluster.timelockRpcClient().unlock(ImmutableSet.of(response2.getLock()));

    }

    @Test
    public void immutableTimestampIsGreaterThanFreshTimestampWhenNotLocked() {
        long freshTs = cluster.getFreshTimestamp();
        long immutableTs = cluster.timelockService().getImmutableTimestamp();

        assertThat(immutableTs).isGreaterThan(freshTs);
    }

    @Test
    public void cannotRetrieveImmutableTimestampViaSyncApiForAsyncService() {
        if (cluster != CLUSTER_WITH_ASYNC) {
            // safety check only applies if the cluster is indeed async
            return;
        }
        // Catching any exception since this currently is an error deserialization exception
        // until we stop requiring http-remoting2 errors
        assertThatThrownBy(() -> cluster.lockService().getMinLockedInVersionId("foo"))
                .isInstanceOf(Exception.class);
    }

    @Test
    public void canRetrieveImmutableTimestampViaSyncApiForSyncServiceOrAsyncWithoutSafetyCheck() {
        if (cluster == CLUSTER_WITH_ASYNC) {
            // will fail - ensuring this fails is covered by cannotRetrieveImmutableTimestampViaSyncApiForAsyncService
            return;
        }
        cluster.lockService().getMinLockedInVersionId("foo");
    }

    @Test
    public void canWaitForLocks() {
        LockToken token = cluster.lock(requestFor(LOCK_A, LOCK_B)).getToken();

        CompletableFuture<WaitForLocksResponse> future = cluster.waitForLocksAsync(waitRequestFor(LOCK_A, LOCK_B));
        assertNotDone(future);

        cluster.unlock(token);

        assertDone(future);
        assertThat(future.join().wasSuccessful()).isTrue();
    }

    @Test
    public void canGetTimestamps() {
        List<Long> timestamps = ImmutableList.of(
                cluster.getFreshTimestamp(),
                cluster.timelockService().getFreshTimestamp(),
                cluster.getFreshTimestamp(),
                cluster.timelockService().getFreshTimestamp());

        long lastTs = -1;
        for (long ts : timestamps) {
            assertThat(ts).isGreaterThan(lastTs);
            lastTs = ts;
        }
    }

    @Test
    public void canGetBatchTimestamps() {
        TimestampRange range1 = cluster.getFreshTimestamps(5);
        TimestampRange range2 = cluster.timelockService().getFreshTimestamps(5);

        assertThat(range1.getUpperBound()).isLessThan(range2.getLowerBound());
    }

    @Test
    public void lockRequestCanTimeOut() {
        LockToken token = cluster.lock(requestFor(LOCK_A)).getToken();
        LockResponse token2 = cluster.lock(requestFor(SHORT_TIMEOUT, LOCK_A));

        assertThat(token2.wasSuccessful()).isFalse();
        cluster.unlock(token);
    }

    @Test
    public void waitForLocksRequestCanTimeOut() {
        LockToken token = cluster.lock(requestFor(LOCK_A)).getToken();
        WaitForLocksResponse response = cluster.waitForLocks(waitRequestFor(SHORT_TIMEOUT, LOCK_A));

        assertThat(response.wasSuccessful()).isFalse();
        cluster.unlock(token);
    }

    @Test
    public void canGetCurrentTimeMillis() {
        assertThat(cluster.lockService().currentTimeMillis()).isGreaterThan(0L);
        assertThat(cluster.timelockService().currentTimeMillis()).isGreaterThan(0L);
    }

    @Test
    public void canPerformLockAndUnlock() throws InterruptedException {
        HeldLocksToken token1 = lockWithFullResponse(requestForWriteLock(LOCK_A), TEST_CLIENT);
        cluster.lockService().unlock(token1);
        HeldLocksToken token2 = lockWithFullResponse(requestForWriteLock(LOCK_A), TEST_CLIENT);
        cluster.lockService().unlockSimple(SimpleHeldLocksToken.fromHeldLocksToken(token2));
    }

    @Test
    public void canPerformLockAndRefreshLock() throws InterruptedException {
        HeldLocksToken token1 = lockWithFullResponse(requestForWriteLock(LOCK_A), TEST_CLIENT);
        LockRefreshToken token = token1.getLockRefreshToken();
        Set<LockRefreshToken> lockRefreshTokens = cluster.lockService().refreshLockRefreshTokens(
                ImmutableList.of(token));
        assertThat(lockRefreshTokens).contains(token);

        unlock(token1);
    }

    @Test
    public void unlockAndFreezeDoesNotAllowRefreshes() throws InterruptedException {
        HeldLocksToken token = lockWithFullResponse(requestForWriteLock(LOCK_A), TEST_CLIENT);
        cluster.lockService().unlockAndFreeze(token);
        Set<LockRefreshToken> lockRefreshTokens = cluster.lockService().refreshLockRefreshTokens(
                ImmutableList.of(token.getLockRefreshToken()));
        assertThat(lockRefreshTokens).isEmpty();
    }

    @Test
    public void testGetAndRefreshTokens() throws InterruptedException {
        HeldLocksToken token1 = lockWithFullResponse(requestForWriteLock(LOCK_A), TEST_CLIENT);
        HeldLocksToken token2 = lockWithFullResponse(requestForWriteLock(LOCK_B), TEST_CLIENT);

        Set<HeldLocksToken> tokens = cluster.lockService().getTokens(TEST_CLIENT);
        assertThat(tokens.stream()
                .map(token -> Iterables.getOnlyElement(token.getLocks()).getLockDescriptor())
                .collect(Collectors.toList())).containsExactlyInAnyOrder(LOCK_A, LOCK_B);

        Set<HeldLocksToken> heldLocksTokens = cluster.lockService().refreshTokens(tokens);
        assertThat(heldLocksTokens.stream()
                .map(token -> Iterables.getOnlyElement(token.getLocks()).getLockDescriptor())
                .collect(Collectors.toList())).containsExactlyInAnyOrder(LOCK_A, LOCK_B);

        unlock(token1, token2);
    }

    @Test
    public void lockGrantsCanBeServedAndRefreshed() throws InterruptedException {
        HeldLocksToken heldLocksToken = lockWithFullResponse(requestForReadLock(LOCK_A), TEST_CLIENT);
        HeldLocksGrant heldLocksGrant = cluster.lockService().convertToGrant(heldLocksToken);

        assertThat(cluster.lockService().getTokens(TEST_CLIENT)).isEmpty();

        HeldLocksGrant refreshedLockGrant = cluster.lockService().refreshGrant(heldLocksGrant);
        assertThat(refreshedLockGrant).isEqualTo(heldLocksGrant);

        HeldLocksGrant refreshedLockGrant2 = cluster.lockService().refreshGrant(heldLocksGrant.getGrantId());
        assertThat(refreshedLockGrant2).isEqualTo(heldLocksGrant);

        cluster.lockService().useGrant(TEST_CLIENT_2, heldLocksGrant);
        assertThat(Iterables.getOnlyElement(cluster.lockService().getTokens(TEST_CLIENT_2)).getLockDescriptors())
                .contains(LOCK_A);

        // Catching any exception since this currently is an error deserialization exception
        // until we stop requiring http-remoting2 errors
        assertThatThrownBy(() -> cluster.lockService().useGrant(TEST_CLIENT_3, heldLocksGrant.getGrantId()))
                .isInstanceOf(Exception.class);
    }

    @Test
    public void getMinLockedInVersionIdReturnsNullIfNoVersionIdsAreSpecified() throws InterruptedException {
        HeldLocksToken token = lockWithFullResponse(requestForReadLock(LOCK_A), TEST_CLIENT);
        assertThat(cluster.lockService().getMinLockedInVersionId(TEST_CLIENT)).isNull();
        unlock(token);
    }

    @Test
    public void getMinLockedInVersionIdReturnsValidValues() throws InterruptedException {
        HeldLocksToken token1 = lockWithFullResponse(requestForReadLock(LOCK_A, 10L), TEST_CLIENT);
        HeldLocksToken token2 = lockWithFullResponse(requestForReadLock(LOCK_B, 12L), TEST_CLIENT);
        assertThat(cluster.lockService().getMinLockedInVersionId(TEST_CLIENT)).isEqualTo(10L);
        unlock(token1, token2);
    }

    @Test
    public void getMinLockedInVersionIdReturnsValidValuesForAnonymousClient() throws InterruptedException {
        HeldLocksToken token1 = lockWithFullResponse(requestForReadLock(LOCK_A, 10L), LockClient.ANONYMOUS);
        HeldLocksToken token2 = lockWithFullResponse(requestForReadLock(LOCK_B, 12L), LockClient.ANONYMOUS);
        assertThat(cluster.lockService().getMinLockedInVersionId()).isEqualTo(10L);
        unlock(token1, token2);
    }

    @Test
    public void testGetLockServerOptions() {
        assertThat(cluster.lockService().getLockServerOptions())
                .isEqualTo(LockServerOptions.builder().randomBitCount(127).build());
    }

    @Test
    public void testCurrentTimeMillis() {
        assertThat(cluster.lockService().currentTimeMillis()).isPositive();
    }

    @Test
    public void lockRequestsToRpcClientAreIdempotent() {
        IdentifiedLockRequest firstRequest = IdentifiedLockRequest.from(requestFor(LOCK_A));

        LockToken token = getToken(cluster.timelockRpcClient().lock(firstRequest));

        IdentifiedLockRequest secondRequest = IdentifiedLockRequest.from(requestFor(LOCK_A));
        CompletableFuture<LockResponseV2> responseFuture =
                cluster.runWithRpcClientAsync(rpcClient -> rpcClient.lock(secondRequest));
        CompletableFuture<LockResponseV2> duplicateResponseFuture =
                cluster.runWithRpcClientAsync(rpcClient -> rpcClient.lock(secondRequest));

        cluster.timelockRpcClient().unlock(ImmutableSet.of(token));

        LockResponseV2 response = responseFuture.join();
        LockResponseV2 duplicateResponse = duplicateResponseFuture.join();
        assertThat(response).isEqualTo(duplicateResponse);

        cluster.timelockRpcClient().unlock(ImmutableSet.of(getToken(response)));
    }

    private LockToken getToken(LockResponseV2 responseV2) {
        return responseV2.accept(LockResponseV2.Visitor.of(
                LockResponseV2.Successful::getToken,
                unsuccessful -> {
                    throw new RuntimeException("Unsuccessful lock request");
                }));
    }

    @Test
    public void lockSucceedsOnlyOnce() {
        LockRequest request = requestFor(SHORT_TIMEOUT, LOCK_A);

        LockResponse response = cluster.lock(request);
        LockResponse duplicateResponse = cluster.lock(request);

        assertThat(response.wasSuccessful()).isTrue();
        assertThat(duplicateResponse.wasSuccessful()).isFalse();

        cluster.unlock(response.getToken());
    }

    @Test
    public void waitForLockRequestsAreIdempotent() {
        LockToken token = cluster.lock(requestFor(LOCK_A)).getToken();

        WaitForLocksRequest request = waitRequestFor(LOCK_A);
        CompletableFuture<WaitForLocksResponse> response = cluster.waitForLocksAsync(request);
        CompletableFuture<WaitForLocksResponse> duplicateResponse = cluster.waitForLocksAsync(request);

        cluster.unlock(token);

        assertThat(response.join()).isEqualTo(duplicateResponse.join());
    }

    private HeldLocksToken lockWithFullResponse(com.palantir.lock.LockRequest request,
            LockClient client) throws InterruptedException {
        return cluster.lockService()
                .lockWithFullLockResponse(client, request)
                .getToken();
    }

    private com.palantir.lock.LockRequest requestForReadLock(LockDescriptor lock) {
        return com.palantir.lock.LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.READ))
                .timeoutAfter(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                .build();
    }

    private com.palantir.lock.LockRequest requestForReadLock(LockDescriptor lockA, long versionId) {
        return com.palantir.lock.LockRequest.builder(ImmutableSortedMap.of(lockA, LockMode.READ))
                .withLockedInVersionId(versionId)
                .timeoutAfter(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                .build();
    }

    private com.palantir.lock.LockRequest requestForWriteLock(LockDescriptor lock) {
        return com.palantir.lock.LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.WRITE))
                .timeoutAfter(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                .build();
    }

    private LockRequest requestFor(LockDescriptor... locks) {
        return LockRequest.of(ImmutableSet.copyOf(locks), TIMEOUT);
    }

    private LockRequest requestFor(long timeoutMs, LockDescriptor... locks) {
        return LockRequest.of(ImmutableSet.copyOf(locks), timeoutMs);
    }

    private WaitForLocksRequest waitRequestFor(LockDescriptor... locks) {
        return WaitForLocksRequest.of(ImmutableSet.copyOf(locks), TIMEOUT);
    }

    private WaitForLocksRequest waitRequestFor(long timeoutMs, LockDescriptor... locks) {
        return WaitForLocksRequest.of(ImmutableSet.copyOf(locks), timeoutMs);
    }

    private void assertNotYetLocked(Future<LockToken> futureToken) {
        assertNotDone(futureToken);
    }

    private void assertLockedAndUnlock(Future<LockToken> futureToken) {
        cluster.unlock(assertLocked(futureToken));
    }

    private LockToken assertLocked(Future<LockToken> futureToken) {
        return assertDone(futureToken);
    }

    private void assertNotDone(Future<?> future) {
        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    private <T> T assertDone(Future<T> future) {
        try {
            return future.get(1, TimeUnit.SECONDS);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void unlock(HeldLocksToken... tokens) {
        for (HeldLocksToken token : tokens) {
            cluster.lockService().unlock(token);
        }
    }

    @After
    public void after() {
        assertThat(cluster.lockService().getTokens(TEST_CLIENT)).isEmpty();
    }
}
