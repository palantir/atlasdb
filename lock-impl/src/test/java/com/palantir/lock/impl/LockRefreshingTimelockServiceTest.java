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

package com.palantir.lock.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequestV2;
import com.palantir.lock.v2.LockTokenV2;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.timestamp.TimestampRange;

public class LockRefreshingTimelockServiceTest {

    private static final LockTokenV2 TOKEN_1 = LockTokenV2.of(UUID.randomUUID());
    private static final LockTokenV2 TOKEN_2 = LockTokenV2.of(UUID.randomUUID());
    private static final Set<LockTokenV2> TOKENS = ImmutableSet.of(TOKEN_1, TOKEN_2);

    private static final Set<LockDescriptor> LOCKS = ImmutableSet.of(StringLockDescriptor.of("foo"));

    private final LockRefresher refresher = mock(LockRefresher.class);
    private final TimelockService delegate = mock(TimelockService.class);
    private final TimelockService timelock = new LockRefreshingTimelockService(delegate, refresher);

    private static final long TIMEOUT = 10_000;


    @Test
    public void registersImmutableTimestampLock() {
        when(delegate.lockImmutableTimestamp(any())).thenReturn(LockImmutableTimestampResponse.of(123L, TOKEN_1));
        timelock.lockImmutableTimestamp(LockImmutableTimestampRequest.create());

        verify(refresher).registerLock(TOKEN_1);
    }

    @Test
    public void registersLocks() {
        LockRequestV2 request = LockRequestV2.of(LOCKS, TIMEOUT);
        when(delegate.lock(request)).thenReturn(Optional.of(TOKEN_1));

        timelock.lock(request);

        verify(refresher).registerLock(TOKEN_1);
    }

    @Test
    public void unregistersLockBeforeUnlocking() {
        InOrder inOrder = Mockito.inOrder(refresher, delegate);

        timelock.unlock(TOKENS);

        inOrder.verify(refresher).unregisterLocks(TOKENS);
        inOrder.verify(delegate).unlock(TOKENS);
    }

    @Test
    public void refreshDelegates() {
        timelock.refreshLockLeases(TOKENS);

        verify(delegate).refreshLockLeases(TOKENS);
        verifyNoMoreInteractions(refresher);
    }

    @Test
    public void waitForLocksDelegates() {
        WaitForLocksRequest request = WaitForLocksRequest.of(LOCKS, TIMEOUT);
        timelock.waitForLocks(request);

        verify(delegate).waitForLocks(request);
        verifyNoMoreInteractions(refresher);
    }

    @Test
    public void getTimestampDelegates() {
        long timestamp = 123L;
        when(delegate.getFreshTimestamp()).thenReturn(timestamp);

        assertThat(timelock.getFreshTimestamp()).isEqualTo(timestamp);
    }

    @Test
    public void getTimestampsDelegates() {
        int numTimestamps = 5;
        TimestampRange timestamps = TimestampRange.createInclusiveRange(1L, numTimestamps);
        when(delegate.getFreshTimestamps(numTimestamps)).thenReturn(timestamps);

        assertThat(timelock.getFreshTimestamps(numTimestamps)).isEqualTo(timestamps);
    }

    @Test
    public void currentTimeMillisDelegates() {
        long time = 456L;
        when(delegate.currentTimeMillis()).thenReturn(time);

        assertThat(timelock.currentTimeMillis()).isEqualTo(time);
    }

    @Test
    public void getImmutableTimestampDelegates() {
        long immutableTs = 789L;
        when(delegate.getImmutableTimestamp()).thenReturn(immutableTs);

        assertThat(timelock.getImmutableTimestamp()).isEqualTo(immutableTs);
    }
}
