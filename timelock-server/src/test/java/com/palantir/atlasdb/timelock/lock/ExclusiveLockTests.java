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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.junit.Test;

public class ExclusiveLockTests {

    private static final UUID REQUEST_1 = UUID.randomUUID();
    private static final UUID REQUEST_2 = UUID.randomUUID();
    private static final UUID REQUEST_3 = UUID.randomUUID();

    private final ExclusiveLock lock = new ExclusiveLock();

    @Test
    public void canLockAndUnlock() {
        lockSynchronously(REQUEST_1);
        lock.unlock(REQUEST_1);
    }

    @Test
    public void lockIsExclusive() {
        lockSynchronously(REQUEST_1);

        CompletableFuture<Void> future = lockAsync(REQUEST_2);
        assertNotComplete(future);
    }

    @Test
    public void lockCanBeObtainedAfterBeingUnlocked() {
        lockSynchronously(REQUEST_1);
        lock.unlock(REQUEST_1);

        lockSynchronously(REQUEST_2);
    }

    @Test
    public void queuedRequestObtainsLockAfterBeingUnlocked() {
        lockSynchronously(REQUEST_1);
        CompletableFuture<Void> future = lockAsync(REQUEST_2);

        unlock(REQUEST_1);

        assertCompleteSuccessfully(future);
    }

    @Test
    public void multipleQueuedRequestsCanObtainLock() {
        lockSynchronously(REQUEST_1);
        CompletableFuture<Void> future2 = lockAsync(REQUEST_2);
        CompletableFuture<Void> future3 = lockAsync(REQUEST_3);

        unlock(REQUEST_1);

        assertCompleteSuccessfully(future2);
        assertNotComplete(future3);

        unlock(REQUEST_2);

        assertCompleteSuccessfully(future3);
    }

    @Test
    public void unlockByNonHolderThrows() {
        lockSynchronously(REQUEST_1);

        assertThatThrownBy(() -> unlock(UUID.randomUUID()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(lock.getCurrentHolder()).isEqualTo(REQUEST_1);
    }

    @Test
    public void unlockByWaiterThrows() {
        lockSynchronously(REQUEST_1);

        CompletableFuture<Void> request2 = lockAsync(REQUEST_2);
        assertThatThrownBy(() -> unlock(REQUEST_2))
                .isInstanceOf(IllegalStateException.class);

        assertThat(lock.getCurrentHolder()).isEqualTo(REQUEST_1);
        assertNotComplete(request2);
    }

    @Test
    public void lockIsAcquiredSynchronouslyIfAvailable() {
        Future<Void> future = lock.lock(REQUEST_1);
        assertTrue(future.isDone());
    }

    @Test
    public void waitUntilAvailableCompletesSynchronouslyIfAvailable() {
        Future<Void> future = lock.waitUntilAvailable(REQUEST_1);
        assertTrue(future.isDone());
    }

    @Test
    public void waitUntilAvailableWantsUntilLockIsFree() {
        lockSynchronously(REQUEST_1);
        CompletableFuture<Void> future = lock.waitUntilAvailable(REQUEST_2);

        assertNotComplete(future);

        unlock(REQUEST_1);

        assertCompleteSuccessfully(future);
    }

    @Test
    public void waitUntilAvailableDoesNotBlockLockRequests() {
        lockSynchronously(REQUEST_1);
        lock.waitUntilAvailable(REQUEST_2);
        CompletableFuture<Void> lockRequest = lock.lock(REQUEST_3);

        assertNotComplete(lockRequest);

        unlock(REQUEST_1);

        assertCompleteSuccessfully(lockRequest);
    }

    @Test
    public void multipleWaitUntilAvailableRequestsAllCompleteWhenLockIsFree() {
        lockSynchronously(REQUEST_1);
        CompletableFuture<Void> request2 = lock.waitUntilAvailable(REQUEST_2);
        CompletableFuture<Void> request3 = lock.waitUntilAvailable(REQUEST_3);

        unlock(REQUEST_1);

        assertCompleteSuccessfully(request2);
        assertCompleteSuccessfully(request3);
    }

    private void assertCompleteSuccessfully(CompletableFuture<Void> lockRequest) {
        assertTrue(lockRequest.isDone());
        lockRequest.join();
    }

    private void assertNotComplete(CompletableFuture<Void> lockRequest) {
        assertFalse(lockRequest.isDone());
    }

    private void lockSynchronously(UUID requestId) {
        assertCompleteSuccessfully(lock.lock(requestId));
    }

    private CompletableFuture<Void> lockAsync(UUID requestId) {
        return lock.lock(requestId);
    }

    private void unlock(UUID requestId) {
        lock.unlock(requestId);
    }

}
