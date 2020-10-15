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
package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingService;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;
import org.mockito.InOrder;

public class LockAcquirerTest {

    private static final UUID REQUEST_ID = UUID.randomUUID();
    private static final UUID OTHER_REQUEST_ID = UUID.randomUUID();

    private static final LockDescriptor LOCK_DESCRIPTOR = StringLockDescriptor.of("foo");

    private static final TimeLimit TIMEOUT = TimeLimit.of(123L);

    private final DeterministicScheduler executor = new DeterministicScheduler();
    private final LeaderClock leaderClock = LeaderClock.create();

    private final ExclusiveLock lockA = spy(new ExclusiveLock(LOCK_DESCRIPTOR));
    private final ExclusiveLock lockB = spy(new ExclusiveLock(LOCK_DESCRIPTOR));
    private final ExclusiveLock lockC = spy(new ExclusiveLock(LOCK_DESCRIPTOR));

    private final LockAcquirer lockAcquirer = new LockAcquirer(
            new LockLog(new MetricRegistry(), () -> 2L),
            executor,
            leaderClock,
            mock(LockWatchingService.class));

    @Test
    public void acquiresLocksInOrder() {
        InOrder inOrder = inOrder(lockA, lockB, lockC);

        acquire(lockA, lockB, lockC);

        inOrder.verify(lockA).lock(REQUEST_ID);
        inOrder.verify(lockB).lock(REQUEST_ID);
        inOrder.verify(lockC).lock(REQUEST_ID);

        inOrder.verify(lockA).getDescriptor();
        inOrder.verify(lockB).getDescriptor();
        inOrder.verify(lockC).getDescriptor();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void queuesAcquisitionsForHeldLocks() {
        lockA.lock(OTHER_REQUEST_ID);
        lockB.lock(OTHER_REQUEST_ID);

        AsyncResult<HeldLocks> acquisitions = acquire(lockA, lockB);

        lockA.unlock(OTHER_REQUEST_ID);
        assertFalse(acquisitions.isComplete());
        lockB.unlock(OTHER_REQUEST_ID);

        assertThat(acquisitions.isCompletedSuccessfully()).isTrue();
    }

    @Test(timeout = 10_000)
    public void doesNotStackOverflowIfLocksAreAcquiredSynchronously() {
        List<AsyncLock> locks = IntStream.range(0, 10_000)
                .mapToObj(i -> new ExclusiveLock(LOCK_DESCRIPTOR))
                .collect(Collectors.toList());

        AsyncResult<HeldLocks> acquisitions = acquire(locks);

        assertThat(acquisitions.isCompletedSuccessfully()).isTrue();
    }

    @Test(timeout = 10_000)
    public void doesNotStackOverflowIfManyRequestsWaitOnALock() {
        lockA.lock(REQUEST_ID);

        List<AsyncResult<Void>> results = IntStream.range(0, 10_000)
                .mapToObj(i -> lockA.waitUntilAvailable(UUID.randomUUID()))
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

        doReturn(lockResult).when(lockA).lock(any());
        AsyncResult<HeldLocks> acquisitions = acquire(lockA);

        assertThat(acquisitions.getError()).isEqualTo(error);
    }

    @Test
    public void propagatesExceptionIfAsyncLockAcquisitionFails() {
        AsyncResult<Void> lockResult = new AsyncResult<>();
        RuntimeException error = new RuntimeException("foo");
        lockResult.fail(error);

        doReturn(lockResult).when(lockB).lock(any());

        lockA.lock(OTHER_REQUEST_ID);
        AsyncResult<HeldLocks> acquisitions = acquire(lockA, lockB);
        lockA.unlock(OTHER_REQUEST_ID);

        assertThat(acquisitions.getError()).isEqualTo(error);
    }

    @Test
    public void unlocksOnAsyncFailure() {
        AsyncResult<Void> lockResult = new AsyncResult<>();
        lockResult.fail(new RuntimeException("foo"));

        doReturn(lockResult).when(lockC).lock(any());

        AsyncResult<HeldLocks> acquisitions = acquire(lockA, lockB, lockC);
        assertThat(acquisitions.isFailed()).isTrue();

        assertNotLocked(lockA);
        assertNotLocked(lockB);
    }

    @Test
    public void unlocksOnSynchronousFailure() {
        doThrow(new RuntimeException("foo")).when(lockC).lock(any());

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

        verify(lockA).waitUntilAvailable(REQUEST_ID);
        verifyNoMoreInteractions(lockA);
    }

    @Test
    public void timesOutRequestAfterSpecifiedTime() {
        acquire(lockB);
        AsyncResult<?> result = acquire(lockA, lockB, lockC);

        executor.tick(TIMEOUT.getTimeMillis() + 1L, TimeUnit.MILLISECONDS);

        verify(lockB).timeout(REQUEST_ID);
        assertThat(result.isTimedOut()).isTrue();
    }

    @Test
    public void stopsAcquiringAndUnlocksAfterTimeout() {
        acquire(lockB);
        AsyncResult<?> result = acquire(lockA, lockB, lockC);

        executor.tick(TIMEOUT.getTimeMillis() + 1L, TimeUnit.MILLISECONDS);

        verify(lockC, never()).lock(any());
        assertNotLocked(lockA);
        assertNotLocked(lockB);
        assertNotLocked(lockC);
    }

    @Test
    public void doesNotTimeOutBeforeSpecifiedTime() {
        acquire(lockB);
        AsyncResult<?> result = acquire(lockA, lockB, lockC);

        executor.tick(TIMEOUT.getTimeMillis() - 1L, TimeUnit.MILLISECONDS);

        verify(lockB, never()).timeout(REQUEST_ID);
        assertThat(result.isTimedOut()).isFalse();
    }

    private AsyncResult<Void> waitFor(AsyncLock... locks) {
        return lockAcquirer.waitForLocks(REQUEST_ID, OrderedLocks.fromOrderedList(ImmutableList.copyOf(locks)),
                TIMEOUT);
    }

    private AsyncResult<HeldLocks> acquire(AsyncLock... locks) {
        return acquire(ImmutableList.copyOf(locks));
    }

    private AsyncResult<HeldLocks> acquire(List<AsyncLock> locks) {
        return lockAcquirer.acquireLocks(REQUEST_ID, OrderedLocks.fromOrderedList(locks), TIMEOUT);
    }

    private void assertNotLocked(ExclusiveLock lock) {
        assertThat(lock.lock(UUID.randomUUID()).isCompletedSuccessfully()).isTrue();
    }

}
