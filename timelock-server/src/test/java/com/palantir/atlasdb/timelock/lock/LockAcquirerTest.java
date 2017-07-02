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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.mockito.InOrder;

import com.google.common.collect.ImmutableList;

public class LockAcquirerTest {

    private static final UUID REQUEST_ID = UUID.randomUUID();
    private static final UUID OTHER_REQUEST_ID = UUID.randomUUID();

    private final ExclusiveLock lockA = spy(new ExclusiveLock());
    private final ExclusiveLock lockB = spy(new ExclusiveLock());
    private final ExclusiveLock lockC = spy(new ExclusiveLock());

    private final LockAcquirer lockAcquirer = new LockAcquirer();

    @Test
    public void acquiresLocksInOrder() {
        InOrder inOrder = inOrder(lockA, lockB, lockC);

        acquire(lockA, lockB, lockC);

        inOrder.verify(lockA).lock(REQUEST_ID);
        inOrder.verify(lockB).lock(REQUEST_ID);
        inOrder.verify(lockC).lock(REQUEST_ID);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void queuesAcquisitionsForHeldLocks() {
        lockA.lock(OTHER_REQUEST_ID);
        lockB.lock(OTHER_REQUEST_ID);

        CompletableFuture<HeldLocks> acquisitions = acquire(lockA, lockB);

        lockA.unlock(OTHER_REQUEST_ID);
        assertFalse(acquisitions.isDone());
        lockB.unlock(OTHER_REQUEST_ID);

        assertCompleteSuccessfully(acquisitions);
    }

    @Test
    public void doesNotStackOverflowIfLocksAreAcquiredSynchronously() {
        List<AsyncLock> locks = IntStream.range(0, 100_000)
                .mapToObj(i -> new ExclusiveLock())
                .collect(Collectors.toList());

        CompletableFuture<HeldLocks> acquisitions = acquire(locks);

        assertCompleteSuccessfully(acquisitions);
    }

    @Test
    public void doesNotStackOverflowIfManyRequestsWaitOnALock() {
        lockA.lock(REQUEST_ID);

        List<CompletableFuture<Void>> futures = IntStream.range(0, 100_000)
                .mapToObj(i -> waitFor(lockA))
                .collect(Collectors.toList());

        lockA.unlock(REQUEST_ID);

        for (int i = 0; i < futures.size(); i++) {
            assertCompleteSuccessfully(futures.get(i));
        }
    }

    @Test
    public void propagatesExceptionIfSynchronousLockAcquisitionFails() {
        CompletableFuture<Void> lockResult = new CompletableFuture<>();
        RuntimeException error = new RuntimeException("foo");
        lockResult.completeExceptionally(new RuntimeException("foo"));

        doReturn(lockResult).when(lockA).lock(any());
        CompletableFuture<HeldLocks> acquisitions = acquire(lockA);

        assertThatThrownBy(() -> acquisitions.get(1, TimeUnit.MILLISECONDS)).hasCause(error);
    }

    @Test
    public void propagatesExceptionIfAsyncLockAcquisitionFails() {
        CompletableFuture<Void> lockResult = new CompletableFuture<>();
        RuntimeException error = new RuntimeException("foo");
        lockResult.completeExceptionally(error);

        doReturn(lockResult).when(lockB).lock(any());

        lockA.lock(OTHER_REQUEST_ID);
        CompletableFuture<HeldLocks> acquisitions = acquire(lockA, lockB);
        lockA.unlock(OTHER_REQUEST_ID);

        assertThatThrownBy(() -> acquisitions.get(1, TimeUnit.MILLISECONDS)).hasCause(error);
    }

    @Test
    public void unlocksOnFailure() {
        CompletableFuture<Void> lockResult = new CompletableFuture<>();
        lockResult.completeExceptionally(new RuntimeException("foo"));

        doReturn(lockResult).when(lockC).lock(any());

        CompletableFuture<HeldLocks> acquisitions = acquire(lockA, lockB, lockC);
        assertFailed(acquisitions);

        assertNotLocked(lockA);
        assertNotLocked(lockB);
    }

    @Test
    public void returnsCorrectlyConfiguredHeldLocks() throws Exception {
        CompletableFuture<HeldLocks> acquisitions = acquire(lockA, lockB);
        HeldLocks heldLocks = acquisitions.get(1, TimeUnit.MILLISECONDS);

        assertThat(heldLocks.getLocks()).contains(lockA, lockB);
        assertThat(heldLocks.getRequestId()).isEqualTo(REQUEST_ID);
    }

    @Test
    public void waitForLocksDoesNotAcquireLocks() {
        waitFor(lockA);

        verify(lockA).waitUntilAvailable(REQUEST_ID);
        verifyNoMoreInteractions(lockA);
    }

    private CompletableFuture<Void> waitFor(AsyncLock... locks) {
        return lockAcquirer.waitForLocks(REQUEST_ID, OrderedLocks.fromOrderedList(ImmutableList.copyOf(locks)));
    }

    private CompletableFuture<HeldLocks> acquire(AsyncLock... locks) {
        return acquire(ImmutableList.copyOf(locks));
    }

    private CompletableFuture<HeldLocks> acquire(List<AsyncLock> locks) {
        return lockAcquirer.acquireLocks(REQUEST_ID, OrderedLocks.fromOrderedList(locks));
    }

    private void assertFailed(CompletableFuture<HeldLocks> acquisitions) {
        assertTrue(acquisitions.isCompletedExceptionally());
    }

    private void assertNotLocked(ExclusiveLock lock) {
        assertCompleteSuccessfully(lock.lock(UUID.randomUUID()));
    }

    private void assertCompleteSuccessfully(CompletableFuture<?> acquisitions) {
        assertTrue(acquisitions.isDone());
        acquisitions.join();
    }

}
