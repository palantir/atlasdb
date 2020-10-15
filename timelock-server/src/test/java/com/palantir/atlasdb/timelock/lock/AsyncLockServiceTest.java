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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingService;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

public class AsyncLockServiceTest {

    private static final UUID REQUEST_ID = UUID.randomUUID();

    private static final LockDescriptor LOCK_DESCRIPTOR = StringLockDescriptor.of("foo");

    private static final String LOCK_A = "a";
    private static final String LOCK_B = "b";
    private static final long REAPER_PERIOD_MS = LockLeaseContract.SERVER_LEASE_TIMEOUT.toMillis() / 2;

    private static final TimeLimit DEADLINE = TimeLimit.of(123L);

    private final LeaderClock leaderClock = LeaderClock.create();
    private final LockAcquirer acquirer = mock(LockAcquirer.class);
    private final LockCollection locks = mock(LockCollection.class);
    private final HeldLocksCollection heldLocks = spy(HeldLocksCollection.create(leaderClock));
    private final AwaitedLocksCollection awaitedLocks = spy(new AwaitedLocksCollection());
    private final ImmutableTimestampTracker immutableTimestampTracker = mock(ImmutableTimestampTracker.class);
    private final DeterministicScheduler reaperExecutor = new DeterministicScheduler();
    private final AsyncLockService lockService = new AsyncLockService(
            locks,
            immutableTimestampTracker,
            acquirer,
            heldLocks,
            awaitedLocks,
            mock(LockWatchingService.class),
            reaperExecutor,
            leaderClock,
            mock(LockLog.class));

    @Before
    public void before() {
        when(acquirer.acquireLocks(any(), any(), any())).thenReturn(new AsyncResult<>());
        when(acquirer.waitForLocks(any(), any(), any())).thenReturn(new AsyncResult<>());
        when(locks.getAll(any())).thenReturn(OrderedLocks.fromSingleLock(newLock()));
        when(immutableTimestampTracker.getImmutableTimestamp()).thenReturn(Optional.empty());
        when(immutableTimestampTracker.getLockFor(anyLong())).thenReturn(newLock());
    }

    @Test
    public void passesOrderedLocksToAcquirer() {
        OrderedLocks expected = orderedLocks(newLock(), newLock());
        Set<LockDescriptor> descriptors = descriptors(LOCK_A, LOCK_B);
        when(locks.getAll(descriptors)).thenReturn(expected);

        lockService.lock(REQUEST_ID, descriptors, DEADLINE);

        verify(acquirer).acquireLocks(REQUEST_ID, expected, DEADLINE);
    }

    @Test
    public void passesOrderedLocksToAcquirerWhenWaitingForLocks() {
        OrderedLocks expected = orderedLocks(newLock(), newLock());
        Set<LockDescriptor> descriptors = descriptors(LOCK_A, LOCK_B);
        when(locks.getAll(descriptors)).thenReturn(expected);

        lockService.waitForLocks(REQUEST_ID, descriptors, DEADLINE);

        verify(acquirer).waitForLocks(REQUEST_ID, expected, DEADLINE);
    }

    @Test
    public void doesNotAcquireDuplicateRequests() {
        Set<LockDescriptor> descriptors = descriptors(LOCK_A);
        lockService.lock(REQUEST_ID, descriptors, DEADLINE);
        lockService.lock(REQUEST_ID, descriptors, DEADLINE);

        verify(acquirer, times(1)).acquireLocks(any(), any(), any());
        verifyNoMoreInteractions(acquirer);
    }

    @Test
    public void doesNotWaitForDuplicateRequests() {
        Set<LockDescriptor> descriptors = descriptors(LOCK_A);
        lockService.waitForLocks(REQUEST_ID, descriptors, DEADLINE);
        lockService.waitForLocks(REQUEST_ID, descriptors, DEADLINE);

        verify(acquirer, times(1)).waitForLocks(any(), any(), any());
        verifyNoMoreInteractions(acquirer);
    }

    @Test
    public void delegatesImmutableTimestampRequestsToTracker() {
        UUID requestId = UUID.randomUUID();
        long timestamp = 123L;
        AsyncLock immutableTsLock = spy(newLock());
        when(immutableTimestampTracker.getLockFor(timestamp)).thenReturn(immutableTsLock);

        lockService.lockImmutableTimestamp(requestId, timestamp);

        verify(acquirer).acquireLocks(requestId, orderedLocks(immutableTsLock), TimeLimit.zero());
    }

    @Test
    public void schedulesReaperAtAppropriateInterval() {
        triggerNextReaperIteration();
        triggerNextReaperIteration();
        triggerNextReaperIteration();

        verify(heldLocks, times(3)).removeExpired();
    }

    @Test
    public void reaperDoesNotDieIfItEncountersAnException() {
        doThrow(new RuntimeException("test")).when(heldLocks).removeExpired();
        triggerNextReaperIteration();
        triggerNextReaperIteration();

        verify(heldLocks, times(2)).removeExpired();
    }

    @Test
    public void propagatesTimeoutExceptionIfRequestTimesOut() {
        AsyncResult<HeldLocks> timedOutResult = new AsyncResult<>();
        timedOutResult.timeout();
        when(acquirer.acquireLocks(any(), any(), any())).thenReturn(timedOutResult);

        AsyncResult<?> result = lockService.lock(REQUEST_ID, descriptors(LOCK_A), DEADLINE);

        assertThat(result.isTimedOut()).isTrue();
    }

    private static ExclusiveLock newLock() {
        return new ExclusiveLock(LOCK_DESCRIPTOR);
    }

    private static Set<LockDescriptor> descriptors(String... lockNames) {
        return Arrays.stream(lockNames)
                .map(StringLockDescriptor::of)
                .collect(Collectors.toSet());
    }

    private static OrderedLocks orderedLocks(AsyncLock... orderedLocks) {
        return OrderedLocks.fromOrderedList(ImmutableList.copyOf(orderedLocks));
    }

    private void triggerNextReaperIteration() {
        reaperExecutor.tick(REAPER_PERIOD_MS - 1, TimeUnit.MILLISECONDS);
    }

}
