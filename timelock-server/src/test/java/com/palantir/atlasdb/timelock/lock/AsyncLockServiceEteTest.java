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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.junit.Test;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockTokenV2;

public class AsyncLockServiceEteTest {

    private static final UUID REQUEST_1 = UUID.randomUUID();
    private static final UUID REQUEST_2 = UUID.randomUUID();

    private static final String LOCK_A = "a";
    private static final String LOCK_B = "b";
    private static final String LOCK_C = "c";
    private static final String LOCK_D = "c";

    private final AsyncLockService service = new AsyncLockService(
            new LockCollection(),
            new ImmutableTimestampTracker(),
            new LockAcquirer(),
            new HeldLocksCollection(),
            Executors.newSingleThreadScheduledExecutor());

    @Test
    public void canLockAndUnlock() {
        LockTokenV2 token = lockSynchronously(REQUEST_1, LOCK_A);
        assertLocked(LOCK_A);

        assertTrue(service.unlock(token));
        assertNotLocked(LOCK_A);
    }

    @Test
    public void canLockAndUnlockMultipleLocks() {
        LockTokenV2 token = lockSynchronously(REQUEST_1, LOCK_A);

        assertTrue(service.unlock(token));
        assertNotLocked(LOCK_A);
    }

    @Test
    public void waitingRequestGetsTheLockAfterItIsUnlocked() {
        LockTokenV2 request1 = lockSynchronously(REQUEST_1, LOCK_A);

        CompletableFuture<LockTokenV2> request2 = lock(REQUEST_2, LOCK_A);
        assertNotDone(request2);

        service.unlock(request1);
        assertCompleteSuccessfully(request2);
    }

    @Test
    public void waitingRequestGetsTheLockAfterItIsUnlockedWithMultipleLocks() {
        LockTokenV2 request1 = lockSynchronously(REQUEST_1, LOCK_A, LOCK_C);

        CompletableFuture<LockTokenV2> request2 = lock(REQUEST_2, LOCK_A, LOCK_B, LOCK_C, LOCK_D);
        assertNotDone(request2);

        service.unlock(request1);
        assertCompleteSuccessfully(request2);
    }

    @Test
    public void requestsAreIdempotentDuringAcquisitionPhase() {
        LockTokenV2 currentHolder = lockSynchronously(REQUEST_1, LOCK_A);

        CompletableFuture<LockTokenV2> tokenFuture = lock(REQUEST_2, LOCK_A);
        CompletableFuture<LockTokenV2> duplicateFuture = lock(REQUEST_2, LOCK_A);

        service.unlock(currentHolder);

        LockTokenV2 token = assertCompleteSuccessfully(tokenFuture);
        LockTokenV2 duplicate = assertCompleteSuccessfully(duplicateFuture);

        assertThat(token).isEqualTo(duplicate);
    }

    @Test
    public void requestsAreIdempotentAfterBeingAcquired() {
        LockTokenV2 token = lockSynchronously(REQUEST_1, LOCK_A);
        LockTokenV2 duplicate = lockSynchronously(REQUEST_1, LOCK_A);

        assertThat(token).isEqualTo(duplicate);
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
        LockTokenV2 token = service.lockImmutableTimestamp(REQUEST_1, timestamp).join();

        assertThat(service.getImmutableTimestamp().get()).isEqualTo(123L);

        service.unlock(token);

        assertThat(service.getImmutableTimestamp()).isEqualTo(Optional.empty());
    }

    @Test
    public void canWaitForLock() {
        LockTokenV2 lockAHolder = lockSynchronously(REQUEST_1, LOCK_A);

        CompletableFuture<Void> waitFuture = waitForLocks(REQUEST_2, LOCK_A);
        assertNotDone(waitFuture);

        service.unlock(lockAHolder);

        assertCompleteSuccessfully(waitFuture);
        assertNotLocked(LOCK_A);
    }

    @Test
    public void canWaitForMultipleLocks() {
        LockTokenV2 lockAHolder = lockSynchronously(REQUEST_1, LOCK_B, LOCK_C);

        CompletableFuture<Void> waitFuture = waitForLocks(REQUEST_2, LOCK_A, LOCK_B, LOCK_C);
        assertNotDone(waitFuture);
        assertNotLocked(LOCK_A);

        service.unlock(lockAHolder);

        assertCompleteSuccessfully(waitFuture);
        assertNotLocked(LOCK_A);
        assertNotLocked(LOCK_C);
    }

    private void assertNotDone(CompletableFuture<?> request) {
        assertFalse(request.isDone());
    }

    private LockTokenV2 lockSynchronously(UUID requestId, String... locks) {
        return assertCompleteSuccessfully(lock(requestId, locks));
    }

    private CompletableFuture<LockTokenV2> lock(UUID requestId, String... locks) {
        return service.lock(requestId, descriptors(locks));
    }

    private CompletableFuture<Void> waitForLocks(UUID requestId, String... locks) {
        return service.waitForLocks(requestId, descriptors(locks));
    }

    private Set<LockDescriptor> descriptors(String... locks) {
        return Arrays.stream(locks)
                .map(StringLockDescriptor::of)
                .collect(Collectors.toSet());
    }

    private <T> T assertCompleteSuccessfully(CompletableFuture<T> future) {
        assertTrue(future.isDone());
        return future.join();
    }

    private void assertNotLocked(String lock) {
        LockTokenV2 token = lockSynchronously(UUID.randomUUID(), lock);
        assertTrue(service.unlock(token));
    }

    private void assertLocked(String... locks) {
        CompletableFuture<LockTokenV2> future = lock(UUID.randomUUID(), locks);
        assertFalse(future.isDone());

        future.thenAccept(token -> service.unlock(token));
    }

}
