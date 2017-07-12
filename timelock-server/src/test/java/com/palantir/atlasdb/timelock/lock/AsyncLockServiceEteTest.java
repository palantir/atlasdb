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

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.common.time.Clock;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockTokenV2;

public class AsyncLockServiceEteTest {

    private static final UUID REQUEST_1 = UUID.randomUUID();
    private static final UUID REQUEST_2 = UUID.randomUUID();

    private static final String LOCK_A = "a";
    private static final String LOCK_B = "b";
    private static final String LOCK_C = "c";
    private static final String LOCK_D = "d";

    private static final Clock CLOCK = System::currentTimeMillis;

    private static final TimeLimit TIMEOUT = TimeLimit.of(10_000L);
    private static final TimeLimit SHORT_TIMEOUT = TimeLimit.of(500L);
    private static final TimeLimit LONG_TIMEOUT = TimeLimit.of(100_000L);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final AsyncLockService service = new AsyncLockService(
            new LockCollection(),
            new ImmutableTimestampTracker(),
            new LockAcquirer(Executors.newSingleThreadScheduledExecutor()),
            new HeldLocksCollection(),
            executor);

    @Test
    public void canLockAndUnlock() {
        LockTokenV2 token = lockSynchronously(REQUEST_1, LOCK_A);
        assertLocked(LOCK_A);

        assertTrue(service.unlock(token));
        assertNotLocked(LOCK_A);
    }

    @Test
    public void canLockAndUnlockMultipleLocks() {
        LockTokenV2 token = lockSynchronously(REQUEST_1, LOCK_A, LOCK_B, LOCK_C);

        assertTrue(service.unlock(token));
        assertNotLocked(LOCK_A);
        assertNotLocked(LOCK_B);
        assertNotLocked(LOCK_C);
    }

    @Test
    public void waitingRequestGetsTheLockAfterItIsUnlocked() {
        LockTokenV2 request1 = lockSynchronously(REQUEST_1, LOCK_A);

        AsyncResult<LockTokenV2> request2 = lock(REQUEST_2, LOCK_A);
        assertThat(request2.isComplete()).isFalse();

        service.unlock(request1);
        assertThat(request2.isCompletedSuccessfully()).isTrue();
    }

    @Test
    public void waitingRequestGetsTheLockAfterItIsUnlockedWithMultipleLocks() {
        LockTokenV2 request1 = lockSynchronously(REQUEST_1, LOCK_A, LOCK_C);

        AsyncResult<LockTokenV2> request2 = lock(REQUEST_2, LOCK_A, LOCK_B, LOCK_C, LOCK_D);
        assertThat(request2.isComplete()).isFalse();

        service.unlock(request1);
        assertThat(request2.isCompletedSuccessfully()).isTrue();
    }

    @Test
    public void requestsAreIdempotentDuringAcquisitionPhase() {
        LockTokenV2 currentHolder = lockSynchronously(REQUEST_1, LOCK_A);

        AsyncResult<LockTokenV2> tokenResult = lock(REQUEST_2, LOCK_A);
        AsyncResult<LockTokenV2> duplicateResult = lock(REQUEST_2, LOCK_A);

        service.unlock(currentHolder);

        assertThat(tokenResult.isCompletedSuccessfully()).isTrue();
        assertThat(duplicateResult.isCompletedSuccessfully()).isTrue();

        assertThat(tokenResult.get()).isEqualTo(duplicateResult.get());
    }

    @Test
    public void requestsAreIdempotentAfterBeingAcquired() {
        LockTokenV2 token = lockSynchronously(REQUEST_1, LOCK_A);
        LockTokenV2 duplicate = lockSynchronously(REQUEST_1, LOCK_A);

        assertThat(token).isEqualTo(duplicate);
    }

    @Test
    public void requestsAreIdempotentWithRespectToTimeout() {
        lockSynchronously(REQUEST_1, LOCK_A);
        service.lock(REQUEST_2, descriptors(LOCK_A), SHORT_TIMEOUT);
        AsyncResult<LockTokenV2> duplicate = service.lock(REQUEST_2, descriptors(LOCK_A), LONG_TIMEOUT);

        waitForTimeout(SHORT_TIMEOUT);

        assertThat(duplicate.isTimedOut()).isTrue();
    }

    @Test
    public void locksCanBeRefreshed() {
        LockTokenV2 token = lockSynchronously(REQUEST_1, LOCK_A);

        assertTrue(service.refresh(token));
    }

    @Test
    public void cannotRefreshAfterUnlocking() {
        LockTokenV2 token = lockSynchronously(REQUEST_1, LOCK_A);
        service.unlock(token);

        assertFalse(service.refresh(token));
    }

    @Test
    public void cannotUnlockAfterUnlocking() {
        LockTokenV2 token = lockSynchronously(REQUEST_1, LOCK_A);
        service.unlock(token);

        assertFalse(service.unlock(token));
    }

    @Test
    public void canUnlockAfterRefreshing() {
        LockTokenV2 token = lockSynchronously(REQUEST_1, LOCK_A);
        service.refresh(token);

        assertTrue(service.unlock(token));
    }

    @Test
    public void canLockAndUnlockImmutableTimestamp() {
        long timestamp = 123L;
        LockTokenV2 token = service.lockImmutableTimestamp(REQUEST_1, timestamp).get();

        assertThat(service.getImmutableTimestamp().get()).isEqualTo(123L);

        service.unlock(token);

        assertThat(service.getImmutableTimestamp()).isEqualTo(Optional.empty());
    }

    @Test
    public void canWaitForLock() {
        LockTokenV2 lockAHolder = lockSynchronously(REQUEST_1, LOCK_A);

        AsyncResult<Void> waitResult = waitForLocks(REQUEST_2, LOCK_A);
        assertThat(waitResult.isComplete()).isFalse();

        service.unlock(lockAHolder);

        assertThat(waitResult.isCompletedSuccessfully()).isTrue();
        assertNotLocked(LOCK_A);
    }

    @Test
    public void canWaitForMultipleLocks() {
        LockTokenV2 lockAHolder = lockSynchronously(REQUEST_1, LOCK_B, LOCK_C);

        AsyncResult<Void> waitResult = waitForLocks(REQUEST_2, LOCK_A, LOCK_B, LOCK_C);
        assertThat(waitResult.isComplete()).isFalse();
        assertNotLocked(LOCK_A);

        service.unlock(lockAHolder);

        assertThat(waitResult.isCompletedSuccessfully()).isTrue();
        assertNotLocked(LOCK_A);
        assertNotLocked(LOCK_C);
    }

    @Test
    public void lockRequestTimesOutWhenTimeoutPasses() {
        lockSynchronously(REQUEST_1, LOCK_A);
        AsyncResult<LockTokenV2> result = service.lock(REQUEST_2, descriptors(LOCK_A), SHORT_TIMEOUT);
        assertThat(result.isTimedOut()).isFalse();

        waitForTimeout(SHORT_TIMEOUT);

        assertThat(result.isTimedOut()).isTrue();
    }

    @Test
    public void waitForLocksRequestTimesOutWhenTimeoutPasses() {
        lockSynchronously(REQUEST_1, LOCK_A);
        AsyncResult<Void> result = service.waitForLocks(REQUEST_2, descriptors(LOCK_A), SHORT_TIMEOUT);
        assertThat(result.isTimedOut()).isFalse();

        waitForTimeout(SHORT_TIMEOUT);

        assertThat(result.isTimedOut()).isTrue();
    }

    @Test
    public void timedOutRequestDoesNotHoldLocks() {
        LockTokenV2 lockBToken = lockSynchronously(REQUEST_1, LOCK_B);
        service.lock(REQUEST_2, descriptors(LOCK_A, LOCK_B), SHORT_TIMEOUT);

        waitForTimeout(SHORT_TIMEOUT);

        assertNotLocked(LOCK_A);
        service.unlock(lockBToken);
        assertNotLocked(LOCK_B);
    }

    @Test
    public void outstandingRequestsReceiveNotCurrentLeaderExceptionOnClose() {
        lockSynchronously(REQUEST_1, LOCK_A);
        AsyncResult<LockTokenV2> request2 = lock(REQUEST_2, LOCK_A);

        service.close();

        assertThat(request2.isFailed()).isTrue();
        assertThat(request2.getError()).isInstanceOf(NotCurrentLeaderException.class);
    }

    @Test
    public void reaperIsShutDownOnClose() {
        service.close();

        assertThat(executor.isShutdown()).isTrue();
    }

    private void waitForTimeout(TimeLimit timeout) {
        Stopwatch timer = Stopwatch.createStarted();
        long buffer = 250L;
        while (timer.elapsed(TimeUnit.MILLISECONDS) < timeout.getTimeMillis() + buffer) {
            Uninterruptibles.sleepUninterruptibly(buffer, TimeUnit.MILLISECONDS);
        }
    }

    private LockTokenV2 lockSynchronously(UUID requestId, String... locks) {
        return lock(requestId, locks).get();
    }

    private AsyncResult<LockTokenV2> lock(UUID requestId, String... locks) {
        return service.lock(requestId, descriptors(locks), TIMEOUT);
    }

    private AsyncResult<Void> waitForLocks(UUID requestId, String... locks) {
        return service.waitForLocks(requestId, descriptors(locks), TIMEOUT);
    }

    private Set<LockDescriptor> descriptors(String... locks) {
        return Arrays.stream(locks)
                .map(StringLockDescriptor::of)
                .collect(Collectors.toSet());
    }

    private void assertNotLocked(String lock) {
        LockTokenV2 token = lockSynchronously(UUID.randomUUID(), lock);
        assertTrue(service.unlock(token));
    }

    private void assertLocked(String... locks) {
        AsyncResult<LockTokenV2> result = lock(UUID.randomUUID(), locks);
        assertFalse(result.isComplete());

        result.map(token -> service.unlock(token));
    }

}
