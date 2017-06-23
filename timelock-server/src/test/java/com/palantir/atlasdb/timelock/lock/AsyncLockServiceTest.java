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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;

public class AsyncLockServiceTest {

    private static final UUID REQUEST_1 = UUID.randomUUID();
    private static final UUID REQUEST_2 = UUID.randomUUID();

    private static final String LOCK_A = "a";
    private static final String LOCK_B = "b";

    private final LockAcquirer acquirer = mock(LockAcquirer.class);
    private final LockCollection locks = mock(LockCollection.class);
    private final HeldLocksCollection heldLocks = spy(HeldLocksCollection.class);
    private final ImmutableTimestampTracker immutableTimestampTracker = mock(ImmutableTimestampTracker.class);
    private final DeterministicScheduler reaperExecutor = new DeterministicScheduler();

    private final AsyncLockService lockService = new AsyncLockService(
            locks, immutableTimestampTracker, acquirer, heldLocks, reaperExecutor);

    @Before
    public void before() {
        when(acquirer.acquireLocks(any(), any())).thenReturn(new CompletableFuture<>());
        when(locks.getSorted(any())).thenReturn(ImmutableList.of(new ExclusiveLock()));
        when(immutableTimestampTracker.getImmutableTimestamp()).thenReturn(Optional.empty());
        when(immutableTimestampTracker.getLockFor(anyLong())).thenReturn(new ExclusiveLock());
    }

    @Test
    public void passesOrderedLocksToAcquirer() {
        List<AsyncLock> expected = ImmutableList.of(new ExclusiveLock(), new ExclusiveLock());
        Set<LockDescriptor> descriptors = descriptors(LOCK_A, LOCK_B);
        when(locks.getSorted(descriptors)).thenReturn(expected);

        lockService.lock(REQUEST_1, descriptors);

        verify(acquirer).acquireLocks(REQUEST_1, expected);
    }

    @Test
    public void passesOrderedLocksToAcquirerWhenWaitingForLocks() {
        List<AsyncLock> expected = ImmutableList.of(new ExclusiveLock(), new ExclusiveLock());
        Set<LockDescriptor> descriptors = descriptors(LOCK_A, LOCK_B);
        when(locks.getSorted(descriptors)).thenReturn(expected);

        lockService.waitForLocks(REQUEST_1, descriptors);

        verify(acquirer).waitForLocks(REQUEST_1, expected);
    }

    @Test
    public void doesNotAcquireDuplicateRequests() {
        Set<LockDescriptor> descriptors = descriptors(LOCK_A);
        lockService.lock(REQUEST_1, descriptors);
        lockService.lock(REQUEST_1, descriptors);

        verify(acquirer, times(1)).acquireLocks(any(), any());
        verifyNoMoreInteractions(acquirer);
    }

    @Test
    public void delegatesImmutableTimestampRequestsToTracker() {
        UUID requestId = UUID.randomUUID();
        long timestamp = 123L;
        AsyncLock immutableTsLock = spy(new ExclusiveLock());
        when(immutableTimestampTracker.getLockFor(timestamp)).thenReturn(immutableTsLock);

        lockService.lockImmutableTimestamp(requestId, timestamp);

        verify(acquirer).acquireLocks(requestId, ImmutableList.of(immutableTsLock));
    }

    @Test
    public void schedulesReaper() {
        reaperExecutor.runNextPendingCommand();
        verify(heldLocks).removeExpired();
    }

    private Set<LockDescriptor> descriptors(String... lockNames) {
        return Arrays.stream(lockNames)
                .map(StringLockDescriptor::of)
                .collect(Collectors.toSet());
    }

}
