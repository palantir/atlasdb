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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

public class AdvisoryLockPreCommitCheckTest {
    private static final LockRefreshToken TOKEN_1 = new LockRefreshToken(new BigInteger("1"), Long.MAX_VALUE);
    private static final LockRefreshToken TOKEN_2 = new LockRefreshToken(new BigInteger("2"), Long.MAX_VALUE);
    private static final List<LockRefreshToken> TOKENS = ImmutableList.of(TOKEN_1, TOKEN_2);

    private static final LockToken TOKEN_V2_1 = LockToken.of(UUID.randomUUID());
    private static final LockToken TOKEN_V2_2 = LockToken.of(UUID.randomUUID());
    private static final List<LockToken> TOKENS_V2 = ImmutableList.of(TOKEN_V2_1, TOKEN_V2_2);

    @Test
    public void checkPassesIfLegacyLocksAreRefreshed() throws InterruptedException {
        LockService happyLockService = mock(LockService.class);
        when(happyLockService.refreshLockRefreshTokens(any())).thenAnswer(invocation -> invocation.getArguments()[0]);

        AdvisoryLockPreCommitCheck.forLockServiceLocks(TOKENS, happyLockService).throwIfLocksExpired();

        // We need to check that the element contains-exactly-in-any-order the same elements
        // but eq is too strong, because the check is allowed to (and does!) wrap the tokens in a different collection.
        verifyCalledWithLegacyTokens(happyLockService);
    }

    @Test
    public void lockTokensUsedForCheckAreTheTokensPassedAtCreationTime() {
        LockService happyLockService = mock(LockService.class);
        when(happyLockService.refreshLockRefreshTokens(any())).thenAnswer(invocation -> invocation.getArguments()[0]);

        Set<LockRefreshToken> lockTokens = Sets.newHashSet(TOKENS);

        AdvisoryLockPreCommitCheck preCommitCheck =
                AdvisoryLockPreCommitCheck.forLockServiceLocks(lockTokens, happyLockService);
        lockTokens.add(new LockRefreshToken(new BigInteger("3"), Long.MAX_VALUE));
        preCommitCheck.throwIfLocksExpired();

        // We need to check that the element contains-exactly-in-any-order the same elements
        // but eq is too strong, because the check is allowed to (and does!) wrap the tokens in a different collection.
        verifyCalledWithLegacyTokens(happyLockService);
    }

    @Test
    public void checkFailsIfOnlySomeLegacyLocksAreRefreshed() {
        LockService lockService = mock(LockService.class);
        when(lockService.refreshLockRefreshTokens(any())).thenReturn(ImmutableSet.of(TOKEN_1));

        assertThatThrownBy(AdvisoryLockPreCommitCheck.forLockServiceLocks(TOKENS, lockService)::throwIfLocksExpired)
                .isInstanceOf(TransactionLockTimeoutException.class);
    }

    @Test
    public void checkFailsIfNoLegacyLocksAreRefreshed() {
        LockService sadLockService = mock(LockService.class);
        when(sadLockService.refreshLockRefreshTokens(any())).thenReturn(ImmutableSet.of());

        assertThatThrownBy(AdvisoryLockPreCommitCheck.forLockServiceLocks(TOKENS, sadLockService)::throwIfLocksExpired)
                .isInstanceOf(TransactionLockTimeoutException.class);
    }

    @Test
    public void checksDelegateAndFailsOnlyWhenChecked() {
        LockService sadLockService = mock(LockService.class);
        when(sadLockService.refreshLockRefreshTokens(any())).thenReturn(ImmutableSet.of());

        AdvisoryLockPreCommitCheck preCommitCheck = AdvisoryLockPreCommitCheck.forLockServiceLocks(
                TOKENS, sadLockService);

        verify(sadLockService, never()).refreshLockRefreshTokens(any());
        try {
            preCommitCheck.throwIfLocksExpired();
        } catch (Exception e) {
            // expected
        }
        verify(sadLockService).refreshLockRefreshTokens(any());
    }

    @Test
    public void propagatesExceptionsIfLockServiceThrows() {
        IllegalStateException exception = new IllegalStateException("something bad");
        LockService lockService = mock(LockService.class);
        when(lockService.refreshLockRefreshTokens(any())).thenThrow(exception);

        assertThatThrownBy(AdvisoryLockPreCommitCheck.forLockServiceLocks(TOKENS, lockService)::throwIfLocksExpired)
                .isInstanceOf(IllegalStateException.class)
                .isNotInstanceOf(TransactionLockTimeoutException.class);
    }

    @Test
    public void doesNotDelegateCheckIfNoLegacyLockTokensAreSupplied() {
        LockService lockService = mock(LockService.class);
        AdvisoryLockPreCommitCheck.forLockServiceLocks(ImmutableList.of(), lockService).throwIfLocksExpired();
        verifyNoMoreInteractions(lockService);
    }

    @Test
    @SuppressWarnings("unchecked") // we know throughout that we refer to Sets of LockRefreshToken
    public void checkPassesIfAsyncLocksAreRefreshed() {
        TimelockService timelockService = mock(TimelockService.class);
        when(timelockService.refreshLockLeases(any())).thenAnswer(invocation -> invocation.getArguments()[0]);

        AdvisoryLockPreCommitCheck.forAsyncLockServiceLocks(TOKENS_V2, timelockService).throwIfLocksExpired();

        // We need to check that the element contains-exactly-in-any-order the same elements
        // but eq is too strong, because the check is allowed to (and does!) wrap the tokens in a different collection.
        verify(timelockService).refreshLockLeases((Set<LockToken>) argThat(containsInAnyOrder(TOKENS_V2.toArray())));
    }

    @Test
    public void checkFailsIfOnlySomeAsyncLocksAreRefreshed() {
        TimelockService timelockService = mock(TimelockService.class);
        when(timelockService.refreshLockLeases(any())).thenReturn(ImmutableSet.of(TOKEN_V2_1));

        assertThatThrownBy(
                AdvisoryLockPreCommitCheck.forAsyncLockServiceLocks(TOKENS_V2, timelockService)::throwIfLocksExpired)
                .isInstanceOf(TransactionLockTimeoutException.class);
    }

    @Test
    public void checkFailsIfNoAsyncLocksAreRefreshed() {
        TimelockService timelockService = mock(TimelockService.class);
        when(timelockService.refreshLockLeases(any())).thenReturn(ImmutableSet.of());

        assertThatThrownBy(
                AdvisoryLockPreCommitCheck.forAsyncLockServiceLocks(TOKENS_V2, timelockService)::throwIfLocksExpired)
                .isInstanceOf(TransactionLockTimeoutException.class);
    }

    @Test
    public void propagatesExceptionsIfAsyncLockServiceThrows() {
        IllegalStateException exception = new IllegalStateException("something bad");
        TimelockService timelockService = mock(TimelockService.class);
        when(timelockService.refreshLockLeases(any())).thenThrow(exception);

        assertThatThrownBy(
                AdvisoryLockPreCommitCheck.forAsyncLockServiceLocks(TOKENS_V2, timelockService)::throwIfLocksExpired)
                .isInstanceOf(IllegalStateException.class)
                .isNotInstanceOf(TransactionLockTimeoutException.class);
    }

    @Test
    public void doesNotDelegateCheckIfNoAsyncLockTokensAreSupplied() {
        LockService lockService = mock(LockService.class);
        AdvisoryLockPreCommitCheck.forLockServiceLocks(ImmutableList.of(), lockService).throwIfLocksExpired();
        verifyNoMoreInteractions(lockService);
    }

    @SuppressWarnings("unchecked") // we know throughout that we refer to collections of LockRefreshToken
    private void verifyCalledWithLegacyTokens(LockService lockService) {
        // Note: eq is too strong, because it requires that the collection type matches the type of TOKENS
        verify(lockService).refreshLockRefreshTokens(
                (Set<LockRefreshToken>) argThat(containsInAnyOrder(TOKENS.toArray())));
    }
}
