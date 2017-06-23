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
        assertNotLocked(future);
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

        assertLocked(future);
    }

    @Test
    public void multipleQueuedRequestsCanObtainLock() {
        lockSynchronously(REQUEST_1);
        CompletableFuture<Void> future2 = lockAsync(REQUEST_2);
        CompletableFuture<Void> future3 = lockAsync(REQUEST_3);

        unlock(REQUEST_1);

        assertLocked(future2);
        assertNotLocked(future3);

        unlock(REQUEST_2);

        assertLocked(future3);
    }

    @Test
    public void unlockByNonHolderDoesNothing() {
        lockSynchronously(REQUEST_1);
        CompletableFuture<Void> future = lockAsync(REQUEST_2);

        unlock(UUID.randomUUID());
        unlock(REQUEST_2);

        assertLocked(future);
    }

    @Test
    public void lockIsAcquiredSynchronouslyIfAvailable() {
        Future<Void> future = lock.lock(REQUEST_1);
        assertTrue(future.isDone());
    }

    private void assertLocked(CompletableFuture<Void> lockRequest) {
        assertTrue(lockRequest.isDone());
        lockRequest.join();
    }

    private void assertNotLocked(CompletableFuture<Void> lockRequest) {
        assertFalse(lockRequest.isDone());
    }

    private void lockSynchronously(UUID requestId) {
        assertLocked(lock.lock(requestId));
    }

    private CompletableFuture<Void> lockAsync(UUID requestId) {
        return lock.lock(requestId);
    }

    private void unlock(UUID requestId) {
        lock.unlock(requestId);
    }

}
