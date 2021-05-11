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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

public class LockRefresherTest {

    private static final long REFRESH_INTERVAL_MILLIS = 1234L;

    private static final LockToken TOKEN_1 = LockToken.of(UUID.randomUUID());
    private static final LockToken TOKEN_2 = LockToken.of(UUID.randomUUID());
    private static final ImmutableSet<LockToken> TOKENS = ImmutableSet.of(TOKEN_1, TOKEN_2);

    private final DeterministicScheduler executor = new DeterministicScheduler();
    private final TimelockService timelock = mock(TimelockService.class);
    private final Clock clock = mock(Clock.class);
    private final LockRefresher refresher = new LockRefresher(executor, timelock, REFRESH_INTERVAL_MILLIS, clock);

    @Before
    public void setUp() {
        when(clock.instant()).thenReturn(Instant.EPOCH);
    }

    @Test
    public void continuesRefreshingLocksThatAreReturned() {
        when(timelock.refreshLockLeases(TOKENS)).thenReturn(TOKENS);
        registerLocks();

        tick();
        tick();
        verify(timelock, times(2)).refreshLockLeases(TOKENS);
    }

    @Test
    public void stopsRefreshingLockAfterItIsUnregistered() {
        registerLocks();

        refresher.unregisterLocks(ImmutableSet.of(TOKEN_2));

        tick();
        verify(timelock).refreshLockLeases(ImmutableSet.of(TOKEN_1));
    }

    @Test
    public void stopsRefreshingLockIfItIsNotRefreshed() {
        ImmutableSet<LockToken> lockTokensToRefresh = ImmutableSet.of(TOKEN_1);
        when(timelock.refreshLockLeases(TOKENS)).thenReturn(lockTokensToRefresh);
        registerLocks();

        tick();
        verify(timelock).refreshLockLeases(TOKENS);

        tick();
        verify(timelock).refreshLockLeases(lockTokensToRefresh);
    }

    @Test
    public void doesNothingIfThereAreNoLocksToRefresh() {
        tick();

        verifyNoMoreInteractions(timelock);
    }

    @Test
    public void doesNotFailIfDelegateThrows() {
        ImmutableSet<LockToken> lockTokensToRefresh = ImmutableSet.of(TOKEN_1);
        when(timelock.refreshLockLeases(any()))
                .thenThrow(new RuntimeException("test"))
                .thenReturn(lockTokensToRefresh);
        refresher.registerLocks(lockTokensToRefresh);

        tick();
        tick();
        verify(timelock, times(2)).refreshLockLeases(lockTokensToRefresh);
    }

    @Test
    public void locksWithoutSpecifiedOptionsHaveLongTenure() {
        when(timelock.refreshLockLeases(TOKENS)).thenReturn(TOKENS);
        when(clock.instant()).thenReturn(Instant.EPOCH);
        refresher.registerLocks(TOKENS);

        tick();
        verify(timelock).refreshLockLeases(TOKENS);

        when(clock.instant()).thenReturn(Instant.MAX); // O(1 billion years). AtlasDB probably won't exist then...?
        tick();
        verify(timelock, times(2)).refreshLockLeases(TOKENS);
        tick();
        verify(timelock, times(3)).refreshLockLeases(TOKENS);
    }

    @Test
    public void lockNoLongerRefreshedAfterTenureElapses() {
        Runnable callback = mock(Runnable.class);

        when(timelock.refreshLockLeases(TOKENS)).thenReturn(TOKENS);
        when(clock.instant()).thenReturn(Instant.EPOCH);
        refresher.registerLocks(
                TOKENS,
                ClientLockingOptions.builder()
                        .maximumLockTenure(Duration.ofMinutes(5))
                        .tenureExpirationCallback(callback)
                        .build());

        tick();
        verify(timelock).refreshLockLeases(TOKENS);
        verify(callback, never()).run();

        when(clock.instant()).thenReturn(Instant.EPOCH.plus(Duration.ofMinutes(3)));
        tick();
        verify(timelock, times(2)).refreshLockLeases(TOKENS);
        verify(callback, never()).run();

        when(clock.instant()).thenReturn(Instant.EPOCH.plus(Duration.ofMinutes(7)));
        tick();
        verify(timelock, times(2)).refreshLockLeases(TOKENS);
        // Callback ran once per expired token
        verify(callback, times(TOKENS.size())).run();
    }

    private void registerLocks() {
        refresher.registerLocks(TOKENS);
    }

    private void tick() {
        executor.tick(REFRESH_INTERVAL_MILLIS + 1, TimeUnit.MILLISECONDS);
    }
}
