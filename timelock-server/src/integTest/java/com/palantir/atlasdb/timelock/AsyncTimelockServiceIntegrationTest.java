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

package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.BadRequestException;

import org.assertj.core.api.ThrowableAssert;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;

public class AsyncTimelockServiceIntegrationTest extends AbstractAsyncTimelockServiceIntegrationTest {

    private static final LockDescriptor LOCK_A = StringLockDescriptor.of("a");
    private static final LockDescriptor LOCK_B = StringLockDescriptor.of("b");

    private static final long SHORT_TIMEOUT = 500L;
    private static final long TIMEOUT = 10_000L;

    public AsyncTimelockServiceIntegrationTest(TestableTimelockCluster cluster) {
        super(cluster);
    }

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
        if (cluster == CLUSTER_WITH_SYNC_ADAPTER_WITH_CHECK) {
            // should fail - covered by the cannotLockImmutableTimestampIfQueryingAsyncServiceViaSyncApi test
            return;
        }
        LockImmutableTimestampResponse response1 = cluster.timelockService()
                .lockImmutableTimestamp(LockImmutableTimestampRequest.create());
        LockImmutableTimestampResponse response2 = cluster.timelockService()
                .lockImmutableTimestamp(LockImmutableTimestampRequest.create());

        long immutableTs = cluster.timelockService().getImmutableTimestamp();
        assertThat(immutableTs).isEqualTo(response1.getImmutableTimestamp());

        cluster.unlock(response1.getLock());

        assertThat(immutableTs).isEqualTo(response2.getImmutableTimestamp());

        cluster.unlock(response2.getLock());
    }

    @Test
    public void cannotLockImmutableTimestampIfQueryingAsyncServiceViaSyncApi() {
        if (cluster != CLUSTER_WITH_SYNC_ADAPTER_WITH_CHECK) {
            // should pass - covered by the canLockImmutableTimestamp test
            return;
        }
        assertBadRequest(() -> cluster.timelockService().lockImmutableTimestamp(
                LockImmutableTimestampRequest.create()));
    }

    @Test
    public void immutableTimestampIsGreaterThanFreshTimestampWhenNotLocked() {
        if (cluster == CLUSTER_WITH_SYNC_ADAPTER_WITH_CHECK) {
            // should fail - covered by the cannotRetrieveImmutableTimestampIfQueryingAsyncServiceViaSyncApi test
            return;
        }
        long freshTs = cluster.getFreshTimestamp();
        long immutableTs = cluster.timelockService().getImmutableTimestamp();

        assertThat(immutableTs).isGreaterThan(freshTs);
    }

    @Test
    public void cannotRetrieveImmutableTimestampIfQueryingAsyncServiceViaSyncApi() {
        if (cluster != CLUSTER_WITH_SYNC_ADAPTER_WITH_CHECK) {
            // should pass - covered by the immutableTimestampIsGreaterThanFreshTimestampWhenNotLocked test
            return;
        }
        assertBadRequest(() -> cluster.timelockService().getImmutableTimestamp());
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
        if (isUsingSyncAdapter(cluster)) {
            // legacy API does not support timeouts on this endpoint
            return;
        }

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
    public void lockRequestsAreIdempotent() {
        if (isUsingSyncAdapter(cluster)) {
            // legacy API does not support idempotence
            return;
        }

        LockToken token = cluster.lock(requestFor(LOCK_A)).getToken();

        LockRequest request = requestFor(LOCK_A);
        CompletableFuture<LockResponse> response = cluster.lockAsync(request);
        CompletableFuture<LockResponse> duplicateResponse = cluster.lockAsync(request);

        cluster.unlock(token);

        assertThat(response.join()).isEqualTo(duplicateResponse.join());

        cluster.unlock(response.join().getToken());
    }

    @Test
    public void waitForLockRequestsAreIdempotent() {
        if (isUsingSyncAdapter(cluster)) {
            // legacy API does not support idempotence
            return;
        }

        LockToken token = cluster.lock(requestFor(LOCK_A)).getToken();

        WaitForLocksRequest request = waitRequestFor(LOCK_A);
        CompletableFuture<WaitForLocksResponse> response = cluster.waitForLocksAsync(request);
        CompletableFuture<WaitForLocksResponse> duplicateResponse = cluster.waitForLocksAsync(request);

        cluster.unlock(token);

        assertThat(response.join()).isEqualTo(duplicateResponse.join());
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

    private static void assertBadRequest(ThrowableAssert.ThrowingCallable throwingCallable) {
        assertThatThrownBy(throwingCallable)
                .isInstanceOf(AtlasDbRemoteException.class)
                .satisfies(remoteException -> {
                    AtlasDbRemoteException atlasDbRemoteException = (AtlasDbRemoteException) remoteException;
                    assertThat(atlasDbRemoteException.getErrorName())
                            .isEqualTo(BadRequestException.class.getCanonicalName());
                    assertThat(atlasDbRemoteException.getStatus())
                            .isEqualTo(HttpStatus.BAD_REQUEST_400);
                });
    }
}
