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
package com.palantir.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.math.BigInteger;
import org.junit.Before;
import org.junit.Test;

public class SimpleLocksTest {
    private SingleLockService lockService;
    private LockService mockLockService = mock(LockService.class);
    private String lockId = "test";

    @Before
    public void setUp() {
        lockService = SingleLockService.createSingleLockService(mockLockService, lockId);
    }

    @Test
    public void lockStoredInToken() throws InterruptedException {
        when(mockLockService.lock(anyString(), any())).thenReturn(new LockRefreshToken(BigInteger.ONE, 10000000000L));
        lockService.lockOrRefresh();

        assertThat(lockService.haveLocks()).isTrue();
    }

    @Test
    public void lockClearedWhenRefreshReturnsEmpty() throws InterruptedException {
        when(mockLockService.lock(anyString(), any())).thenReturn(new LockRefreshToken(BigInteger.ONE, 10000000000L));
        lockService.lockOrRefresh();

        when(mockLockService.refreshLockRefreshTokens(any())).thenReturn(ImmutableSet.of());
        lockService.lockOrRefresh();

        assertThat(lockService.haveLocks()).isFalse();
    }

    @Test
    public void lockOrRefreshCallsLockWhenNoTokenPresent() throws InterruptedException {
        lockService.lockOrRefresh();
        verify(mockLockService, atLeastOnce()).lock(any(), any());
        verifyNoMoreInteractions(mockLockService);
    }

    @Test
    public void lockOrRefreshCallsRefreshWhenTokenPresent() throws InterruptedException {
        LockRefreshToken token = new LockRefreshToken(BigInteger.ONE, 10000000000L);
        when(mockLockService.lock(anyString(), any())).thenReturn(token);
        lockService.lockOrRefresh();
        verify(mockLockService, atLeastOnce()).lock(any(), any());

        lockService.lockOrRefresh();
        verify(mockLockService, atLeastOnce()).refreshLockRefreshTokens(ImmutableList.of(token));
        verifyNoMoreInteractions(mockLockService);
    }

    @Test
    public void closeUnlocksToken() throws InterruptedException {
        LockRefreshToken token = new LockRefreshToken(BigInteger.ONE, 10000000000L);
        when(mockLockService.lock(anyString(), any())).thenReturn(token);
        lockService.lockOrRefresh();

        lockService.close();
        verify(mockLockService, atLeastOnce()).unlock(token);
    }
}
