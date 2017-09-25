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
package com.palantir.atlasdb.sweep;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;

public class SweepLocksTest {
    private SweepLocks sweepLocks;
    private LockService mockLockService = mock(LockService.class);

    @Before
    public void setUp() {
        sweepLocks = new SweepLocks(mockLockService);
    }

    @Test
    public void lockStoredInToken() throws InterruptedException {
        when(mockLockService.lock(anyString(), any())).thenReturn(new LockRefreshToken(BigInteger.ONE, 10000000000L));
        sweepLocks.lockOrRefresh();

        assertTrue(sweepLocks.haveLocks());
    }

    @Test
    public void lockClearedWhenRefreshReturnsEmpty() throws InterruptedException {
        when(mockLockService.lock(anyString(), any())).thenReturn(new LockRefreshToken(BigInteger.ONE, 10000000000L));
        sweepLocks.lockOrRefresh();

        when(mockLockService.refreshLockRefreshTokens(any())).thenReturn(ImmutableSet.of());
        sweepLocks.lockOrRefresh();

        assertFalse(sweepLocks.haveLocks());
    }

    @Test
    public void lockOrRefreshCallsLockWhenNoTokenPresent() throws InterruptedException {
        sweepLocks.lockOrRefresh();
        verify(mockLockService, atLeastOnce()).lock(any(), any());
        verifyNoMoreInteractions(mockLockService);
    }

    @Test
    public void lockOrRefreshCallsRefreshWhenTokenPresent() throws InterruptedException {
        LockRefreshToken token = new LockRefreshToken(BigInteger.ONE, 10000000000L);
        when(mockLockService.lock(anyString(), any())).thenReturn(token);
        sweepLocks.lockOrRefresh();
        verify(mockLockService, atLeastOnce()).lock(any(), any());

        sweepLocks.lockOrRefresh();
        verify(mockLockService, atLeastOnce()).refreshLockRefreshTokens(ImmutableList.of(token));
        verifyNoMoreInteractions(mockLockService);
    }

    @Test
    public void closeUnlocksToken() throws InterruptedException {
        LockRefreshToken token = new LockRefreshToken(BigInteger.ONE, 10000000000L);
        when(mockLockService.lock(anyString(), any())).thenReturn(token);
        sweepLocks.lockOrRefresh();

        sweepLocks.close();
        verify(mockLockService, atLeastOnce()).unlock(token);
    }
}
