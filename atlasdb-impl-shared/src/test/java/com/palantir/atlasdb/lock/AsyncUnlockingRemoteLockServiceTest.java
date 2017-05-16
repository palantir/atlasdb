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

package com.palantir.atlasdb.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.RemoteLockService;

public class AsyncUnlockingRemoteLockServiceTest {

    private static final LockRefreshToken REFRESH_TOKEN = new LockRefreshToken(BigInteger.ONE, 1L);
    private static final Set<LockRefreshToken> REFRESH_TOKENS = ImmutableSet.of(REFRESH_TOKEN);

    private DeterministicScheduler executor = new DeterministicScheduler();
    private RemoteLockService delegate = mock(RemoteLockService.class);

    private AsyncUnlockingRemoteLockService lockService = new AsyncUnlockingRemoteLockService(delegate, executor);

    @Test
    public void unlockDelegatesToDelegate() {
        lockService.tryUnlockAsync(REFRESH_TOKEN);
        executor.runUntilIdle();

        verify(delegate).unlock(REFRESH_TOKEN);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void refreshDelegatesToDelegate() {
        lockService.tryRefreshLockRefreshTokensAsync(REFRESH_TOKENS, ignored -> { });
        executor.runUntilIdle();

        verify(delegate).refreshLockRefreshTokens(REFRESH_TOKENS);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void doesNotCallDelegateSynchronously() {
        lockService.tryUnlockAsync(REFRESH_TOKEN);
        lockService.tryRefreshLockRefreshTokensAsync(REFRESH_TOKENS, ignored -> { });
        verifyZeroInteractions(delegate);
    }

    @Test
    public void invokesRefreshCallbackOnSuccess() {
        when(delegate.refreshLockRefreshTokens(REFRESH_TOKENS)).thenReturn(REFRESH_TOKENS);

        MutableObject result = new MutableObject();
        lockService.tryRefreshLockRefreshTokensAsync(
                REFRESH_TOKENS, refreshed -> result.setValue(refreshed));
        executor.runUntilIdle();

        assertThat(result.getValue()).isEqualTo(REFRESH_TOKENS);
    }

    @Test
    public void doesNotInvokeRefreshCallbackOnFailure() {
        when(delegate.refreshLockRefreshTokens(REFRESH_TOKENS)).thenThrow(new RuntimeException("foo"));

        MutableBoolean wasCallbackInvoked = new MutableBoolean();
        lockService.tryRefreshLockRefreshTokensAsync(
                REFRESH_TOKENS, refreshed -> wasCallbackInvoked.setValue(true));
        executor.runUntilIdle();

        assertThat(wasCallbackInvoked.booleanValue()).isFalse();
    }

    @Test
    public void errorsDoNotPreventSubsequentCalls() {
        MutableBoolean shouldThrow = new MutableBoolean(true);
        when(delegate.unlock(any())).thenAnswer(invocation -> {
            if (shouldThrow.isTrue()) {
                throw new RuntimeException("foo");
            } else {
                return true;
            }
        });

        shouldThrow.setValue(true);
        lockService.tryUnlockAsync(REFRESH_TOKEN);
        executor.runUntilIdle();

        shouldThrow.setValue(false);
        lockService.tryUnlockAsync(REFRESH_TOKEN);
        executor.runUntilIdle();

        verify(delegate, times(2)).unlock(REFRESH_TOKEN);
    }

}
