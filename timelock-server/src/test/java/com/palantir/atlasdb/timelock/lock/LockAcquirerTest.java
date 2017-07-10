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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.mockito.InOrder;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.FakeDelayedExecutor;

public class LockAcquirerTest {

    private static final UUID REQUEST_ID = UUID.randomUUID();
    private static final UUID OTHER_REQUEST_ID = UUID.randomUUID();

    private static final Deadline DEADLINE = Deadline.at(123L);

    private final FakeDelayedExecutor canceller = new FakeDelayedExecutor();

    private final ExclusiveLock lockA = spy(new ExclusiveLock(canceller));
    private final ExclusiveLock lockB = spy(new ExclusiveLock(canceller));
    private final ExclusiveLock lockC = spy(new ExclusiveLock(canceller));

    private final LockAcquirer lockAcquirer = new LockAcquirer();

    @Test
    public void acquiresLocksInOrder() {
        InOrder inOrder = inOrder(lockA, lockB, lockC);

        acquire(lockA, lockB, lockC);

        inOrder.verify(lockA).lock(REQUEST_ID, DEADLINE);
        inOrder.verify(lockB).lock(REQUEST_ID, DEADLINE);
        inOrder.verify(lockC).lock(REQUEST_ID, DEADLINE);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void queuesAcquisitionsForHeldLocks() {
        lockA.lock(OTHER_REQUEST_ID, DEADLINE);
        lockB.lock(OTHER_REQUEST_ID, DEADLINE);

        AsyncResult<HeldLocks> acquisitions = acquire(lockA, lockB);

        lockA.unlock(OTHER_REQUEST_ID);
        assertFalse(acquisitions.isComplete());
        lockB.unlock(OTHER_REQUEST_ID);

        assertThat(acquisitions.isCompletedSuccessfully()).isTrue();
    }

    @Test(timeout = 10_000)
    public void doesNotStackOverflowIfLocksAreAcquiredSynchronously() {
        List<AsyncLock> locks = IntStream.range(0, 10_000)
                .mapToObj(i -> new ExclusiveLock(canceller))
                .collect(Collectors.toList());

        AsyncResult<HeldLocks> acquisitions = acquire(locks);

        assertThat(acquisitions.isCompletedSuccessfully()).isTrue();
    }

    @Test(timeout = 10_000)
    public void doesNotStackOverflowIfManyRequestsWaitOnALock() {
        lockA.lock(REQUEST_ID, DEADLINE);

        List<AsyncResult<Void>> results = IntStream.range(0, 10_000)
                .mapToObj(i -> lockA.waitUntilAvailable(UUID.randomUUID(), DEADLINE))
                .collect(Collectors.toList());

        lockA.unlock(REQUEST_ID);

        for (int i = 0; i < results.size(); i++) {
            assertThat(results.get(i).isCompletedSuccessfully()).isTrue();
        }
    }

    @Test
    public void propagatesExceptionIfSynchronousLockAcquisitionFails() {
        AsyncResult<Void> lockResult = new AsyncResult<>();
        RuntimeException error = new RuntimeException("foo");
        lockResult.fail(error);

        doReturn(lockResult).when(lockA).lock(any(), any());
        AsyncResult<HeldLocks> acquisitions = acquire(lockA);

        assertThat(acquisitions.getError()).isEqualTo(error);
    }

    @Test
    public void propagatesExceptionIfAsyncLockAcquisitionFails() {
        AsyncResult<Void> lockResult = new AsyncResult<>();
        RuntimeException error = new RuntimeException("foo");
        lockResult.fail(error);

        doReturn(lockResult).when(lockB).lock(any(), any());

        lockA.lock(OTHER_REQUEST_ID, DEADLINE);
        AsyncResult<HeldLocks> acquisitions = acquire(lockA, lockB);
        lockA.unlock(OTHER_REQUEST_ID);

        assertThat(acquisitions.getError()).isEqualTo(error);
    }

    @Test
    public void unlocksOnAsyncFailure() {
        AsyncResult<Void> lockResult = new AsyncResult<>();
        lockResult.fail(new RuntimeException("foo"));

        doReturn(lockResult).when(lockC).lock(any(), any());

        AsyncResult<HeldLocks> acquisitions = acquire(lockA, lockB, lockC);
        assertThat(acquisitions.isFailed()).isTrue();

        assertNotLocked(lockA);
        assertNotLocked(lockB);
    }

    @Test
    public void unlocksOnSynchronousFailure() {
        doThrow(new RuntimeException("foo")).when(lockC).lock(any(), any());

        AsyncResult<HeldLocks> acquisitions = acquire(lockA, lockB, lockC);
        assertThat(acquisitions.isFailed()).isTrue();

        assertNotLocked(lockA);
        assertNotLocked(lockB);
    }

    @Test
    public void returnsCorrectlyConfiguredHeldLocks() throws Exception {
        HeldLocks heldLocks = acquire(lockA, lockB).get();

        assertThat(heldLocks.getLocks()).contains(lockA, lockB);
        assertThat(heldLocks.getRequestId()).isEqualTo(REQUEST_ID);
    }

    @Test
    public void waitForLocksDoesNotAcquireLocks() {
        waitFor(lockA);

        verify(lockA).waitUntilAvailable(REQUEST_ID, DEADLINE);
        verifyNoMoreInteractions(lockA);
    }

    @Test
    public void stopsAcquiringIfALockRequestTimesOut() {
        acquire(lockB);
        AsyncResult<?> result = acquire(lockA, lockB, lockC);

        canceller.tick(DEADLINE.getTimeMillis() + 1L);

        assertThat(result.isTimedOut()).isTrue();
        verify(lockC, never()).lock(any(), any());
    }

    private AsyncResult<Void> waitFor(AsyncLock... locks) {
        return lockAcquirer.waitForLocks(REQUEST_ID, OrderedLocks.fromOrderedList(ImmutableList.copyOf(locks)),
                DEADLINE);
    }

    private AsyncResult<HeldLocks> acquire(AsyncLock... locks) {
        return acquire(ImmutableList.copyOf(locks));
    }

    private AsyncResult<HeldLocks> acquire(List<AsyncLock> locks) {
        return lockAcquirer.acquireLocks(REQUEST_ID, OrderedLocks.fromOrderedList(locks), DEADLINE);
    }

    private void assertNotLocked(ExclusiveLock lock) {
        assertThat(lock.lock(UUID.randomUUID(), DEADLINE).isCompletedSuccessfully()).isTrue();
    }

}
