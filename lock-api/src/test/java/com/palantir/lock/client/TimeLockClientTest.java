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
package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.UUID;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.timestamp.CloseableTimestampService;
import com.palantir.timestamp.TimestampRange;

public class TimeLockClientTest {

    private static final LockToken TOKEN_1 = LockToken.of(UUID.randomUUID());
    private static final LockToken TOKEN_2 = LockToken.of(UUID.randomUUID());
    private static final ImmutableSet<LockToken> TOKENS = ImmutableSet.of(TOKEN_1, TOKEN_2);

    private static final ImmutableSet<LockDescriptor> LOCKS = ImmutableSet.of(StringLockDescriptor.of("foo"));

    private final LockRefresher refresher = mock(LockRefresher.class);
    private final CloseableTimestampService timestampService = mock(CloseableTimestampService.class);
    private final TimelockService delegate = mock(TimelockService.class);
    private final TimeLockUnlocker unlocker = mock(TimeLockUnlocker.class);
    private final TimelockService timelock = spy(new TimeLockClient(delegate, timestampService, refresher, unlocker));

    private static final long TIMEOUT = 10_000;

    @Test
    public void delegatesInitializationCheck() {
        when(delegate.isInitialized())
                .thenReturn(false)
                .thenReturn(true);
        when(timestampService.isInitialized()).thenReturn(true);

        assertFalse(timelock.isInitialized());
        assertTrue(timelock.isInitialized());
    }

    @Test
    public void registersImmutableTimestampLock() {
        when(delegate.lockImmutableTimestamp()).thenReturn(LockImmutableTimestampResponse.of(123L, TOKEN_1));
        timelock.lockImmutableTimestamp();

        verify(refresher).registerLock(TOKEN_1);
    }

    @Test
    public void registersLocks() {
        LockRequest request = LockRequest.of(LOCKS, TIMEOUT);
        when(delegate.lock(request)).thenReturn(LockResponse.successful(TOKEN_1));

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
        when(timestampService.getFreshTimestamp()).thenReturn(timestamp);

        assertThat(timelock.getFreshTimestamp()).isEqualTo(timestamp);
    }

    @Test
    public void getTimestampsDelegates() {
        int numTimestamps = 5;
        TimestampRange timestamps = TimestampRange.createInclusiveRange(1L, numTimestamps);
        when(timestampService.getFreshTimestamps(numTimestamps)).thenReturn(timestamps);

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

    @Test
    public void throwsDependencyUnavailableWhenConnectionToDelegateFails() {
        Throwable cause = new ConnectException("I couldn't connect to TimeLock");
        assertDependencyUnavailableIsThrownWhenWeCatch(cause);
    }

    @Test
    public void throwsDependencyUnavailableWhenDelegateIsUnknown() {
        Throwable cause = new UnknownHostException("I don't know how to talk to TimeLock");
        assertDependencyUnavailableIsThrownWhenWeCatch(cause);
    }

    @Test
    public void throwsDependencyUnavailableWhenDelegateIsNotCurrentLeader() {
        Throwable cause = new NotCurrentLeaderException("No TimeLock node appears to be the leader");
        assertDependencyUnavailableIsThrownWhenWeCatch(cause);
    }

    private void assertDependencyUnavailableIsThrownWhenWeCatch(Throwable cause) {
        Throwable exceptionToThrow = new RuntimeException(cause);
        when(delegate.currentTimeMillis()).thenThrow(exceptionToThrow);

        assertThatThrownBy(timelock::currentTimeMillis).isInstanceOf(AtlasDbDependencyException.class);
    }

    @Test
    public void doesNotThrowDependencyExceptionWhenDelegateFailsForSomeOtherReason() {
        when(delegate.currentTimeMillis()).thenThrow(new RuntimeException("something else happened"));

        assertThatThrownBy(timelock::currentTimeMillis).isInstanceOf(RuntimeException.class)
            .isNotInstanceOf(AtlasDbDependencyException.class);
    }

    @Test
    public void clientWithSynchronousUnlockerDelegatesToUnlock() {
        try (TimeLockClient client = TimeLockClient.withSynchronousUnlocker(timelock)) {
            UUID uuid = UUID.randomUUID();
            client.tryUnlock(ImmutableSet.of(LockToken.of(uuid)));
            verify(timelock, times(1)).unlock(ImmutableSet.of(LockToken.of(uuid)));
        }
    }
}
