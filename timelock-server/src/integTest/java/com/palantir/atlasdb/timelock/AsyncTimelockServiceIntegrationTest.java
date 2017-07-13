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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequestV2;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockTokenV2;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;

public class AsyncTimelockServiceIntegrationTest {
    private static final String CLIENT = "test";

    private static final TestableTimelockCluster CLUSTER = new TestableTimelockCluster(
            "http://localhost",
            CLIENT,
            "paxosSingleServer.yml");

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    private static final LockDescriptor LOCK_A = StringLockDescriptor.of("a");
    private static final LockDescriptor LOCK_B = StringLockDescriptor.of("b");

    private static final long SHORT_TIMEOUT = 500L;
    public static final long TIMEOUT = 10_000L;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void canLockRefreshAndUnlock() {
        LockTokenV2 token = CLUSTER.lock(requestFor(LOCK_A)).getToken();
        boolean wasRefreshed = CLUSTER.refreshLockLease(token);
        boolean wasUnlocked = CLUSTER.unlock(token);

        assertTrue(wasRefreshed);
        assertTrue(wasUnlocked);
    }

    @Test
    public void locksAreExclusive() {
        LockTokenV2 token = CLUSTER.lock(requestFor(LOCK_A)).getToken();
        Future<LockTokenV2> futureToken = CLUSTER.lockAsync(requestFor(LOCK_A))
                .thenApply(LockResponseV2::getToken);

        assertNotYetLocked(futureToken);

        CLUSTER.unlock(token);

        assertLockedAndUnlock(futureToken);
    }

    @Test
    public void canLockImmutableTimestamp() {
        LockImmutableTimestampResponse response1 = CLUSTER.timelockService()
                .lockImmutableTimestamp(LockImmutableTimestampRequest.create());
        LockImmutableTimestampResponse response2 = CLUSTER.timelockService()
                .lockImmutableTimestamp(LockImmutableTimestampRequest.create());

        long immutableTs = CLUSTER.timelockService().getImmutableTimestamp();
        assertThat(immutableTs).isEqualTo(response1.getImmutableTimestamp());

        CLUSTER.unlock(response1.getLock());

        assertThat(immutableTs).isEqualTo(response2.getImmutableTimestamp());

        CLUSTER.unlock(response2.getLock());
    }

    @Test
    public void immutableTimestampIsGreaterThanFreshTimestampWhenNotLocked() {
        long freshTs = CLUSTER.getFreshTimestamp();
        long immutableTs = CLUSTER.timelockService().getImmutableTimestamp();

        assertThat(immutableTs).isGreaterThan(freshTs);
    }

    @Test
    public void canWaitForLocks() {
        LockTokenV2 token = CLUSTER.lock(requestFor(LOCK_A, LOCK_B)).getToken();

        CompletableFuture<WaitForLocksResponse> future = CLUSTER.waitForLocksAsync(waitRequestFor(LOCK_A, LOCK_B));
        assertNotDone(future);

        CLUSTER.unlock(token);

        assertDone(future);
        assertThat(future.join().wasSuccessful()).isTrue();
    }

    @Test
    public void canGetTimestamps() {
        long ts1 = CLUSTER.timelockService().getFreshTimestamp();
        long ts2 = CLUSTER.getFreshTimestamp();

        assertThat(ts2).isGreaterThan(ts1);
    }

    @Test
    public void canGetBatchTimestamps() {
        TimestampRange range1 = CLUSTER.getFreshTimestamps(5);
        TimestampRange range2 = CLUSTER.timelockService().getFreshTimestamps(5);

        assertThat(range1.getUpperBound()).isLessThan(range2.getLowerBound());
    }

    @Test
    public void lockRequestCanTimeOut() {
        LockTokenV2 token = CLUSTER.lock(requestFor(LOCK_A)).getToken();
        LockResponseV2 token2 = CLUSTER.lock(requestFor(SHORT_TIMEOUT, LOCK_A));

        assertThat(token2.wasSuccessful()).isFalse();
        CLUSTER.unlock(token);
    }

    @Test
    public void waitForLocksRequestCanTimeOut() {
        LockTokenV2 token = CLUSTER.lock(requestFor(LOCK_A)).getToken();
        WaitForLocksResponse response = CLUSTER.waitForLocks(waitRequestFor(SHORT_TIMEOUT, LOCK_A));

        assertThat(response.wasSuccessful()).isFalse();
        CLUSTER.unlock(token);
    }

    private LockRequestV2 requestFor(LockDescriptor... locks) {
        return LockRequestV2.of(ImmutableSet.copyOf(locks), TIMEOUT);
    }

    private LockRequestV2 requestFor(long timeoutMs, LockDescriptor... locks) {
        return LockRequestV2.of(ImmutableSet.copyOf(locks), timeoutMs);
    }

    private WaitForLocksRequest waitRequestFor(LockDescriptor... locks) {
        return WaitForLocksRequest.of(ImmutableSet.copyOf(locks), TIMEOUT);
    }

    private WaitForLocksRequest waitRequestFor(long timeoutMs, LockDescriptor... locks) {
        return WaitForLocksRequest.of(ImmutableSet.copyOf(locks), timeoutMs);
    }

    private void assertNotYetLocked(Future<LockTokenV2> futureToken) {
        assertNotDone(futureToken);
    }

    private void assertLockedAndUnlock(Future<LockTokenV2> futureToken) {
        CLUSTER.unlock(assertLocked(futureToken));
    }

    private LockTokenV2 assertLocked(Future<LockTokenV2> futureToken) {
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

}
