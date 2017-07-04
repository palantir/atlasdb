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
import com.palantir.lock.v2.LockTokenV2;
import com.palantir.lock.v2.WaitForLocksRequest;

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

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void canLockRefreshAndUnlock() {
        LockTokenV2 token = CLUSTER.lock(requestFor(LOCK_A));
        boolean wasRefreshed = CLUSTER.refreshLockLease(token);
        boolean wasUnlocked = CLUSTER.unlock(token);

        assertTrue(wasRefreshed);
        assertTrue(wasUnlocked);
    }

    @Test
    public void locksAreExclusive() {
        LockTokenV2 token = CLUSTER.lock(requestFor(LOCK_A));
        Future<LockTokenV2> futureToken = executor.submit(() -> CLUSTER.lock(requestFor(LOCK_A)));

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

        long freshTs = CLUSTER.getFreshTimestamp();
        immutableTs = CLUSTER.timelockService().getImmutableTimestamp();
        assertThat(immutableTs).isGreaterThan(freshTs);
    }

    @Test
    public void canWaitForLocks() {
        LockTokenV2 token = CLUSTER.lock(requestFor(LOCK_A, LOCK_B));

        Future<?> future = executor.submit(
                () -> CLUSTER.timelockService().waitForLocks(waitRequestFor(LOCK_A, LOCK_B)));
        assertNotDone(future);

        CLUSTER.unlock(token);

        assertDone(future);
    }

    @Test
    public void canGetTimestamps() {
        long ts1 = CLUSTER.getFreshTimestamp();
        long ts2 = CLUSTER.getFreshTimestamp();

        assertThat(ts2).isGreaterThan(ts1);
    }

    private LockRequestV2 requestFor(LockDescriptor... locks) {
        return LockRequestV2.of(ImmutableSet.copyOf(locks));
    }

    private WaitForLocksRequest waitRequestFor(LockDescriptor... locks) {
        return WaitForLocksRequest.of(ImmutableSet.copyOf(locks));
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
