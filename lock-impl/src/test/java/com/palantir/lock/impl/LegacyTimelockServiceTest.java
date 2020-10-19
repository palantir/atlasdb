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
package com.palantir.lock.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.palantir.lock.AtlasTimestampLockDescriptor;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;
import java.math.BigInteger;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class LegacyTimelockServiceTest {

    private static final LockClient LOCK_CLIENT = LockClient.of("foo");

    private static final long FRESH_TIMESTAMP = 5L;

    private static final LockToken LOCK_TOKEN_V2 = randomLockToken();
    private static final LockRefreshToken LOCK_REFRESH_TOKEN = toLegacyToken(LOCK_TOKEN_V2);

    private static final LockDescriptor LOCK_A = StringLockDescriptor.of("a");
    private static final LockDescriptor LOCK_B = StringLockDescriptor.of("b");

    private final TimestampService timestampService = mock(TimestampService.class);
    private final LockService lockService = mock(LockService.class);

    private static final long TIMEOUT = 10_000;

    private final LegacyTimelockService timelock = new LegacyTimelockService(timestampService, lockService, LOCK_CLIENT);

    @Before
    public void before() {
        when(timestampService.getFreshTimestamp()).thenReturn(FRESH_TIMESTAMP);
    }

    @Test
    public void freshTimestampDelegatesToTimestampService() {
        assertThat(timelock.getFreshTimestamp()).isEqualTo(FRESH_TIMESTAMP);
    }

    @Test
    public void freshTimestampsDelegatesToTimestampService() {
        int numTimestamps = 10;
        TimestampRange range = TimestampRange.createInclusiveRange(21L, 30L);
        when(timestampService.getFreshTimestamps(numTimestamps)).thenReturn(range);

        assertThat(timelock.getFreshTimestamps(numTimestamps)).isEqualTo(range);
    }

    @Test
    public void lockImmutableTimestampLocksFreshTimestamp() throws InterruptedException {
        long immutableTs = 3L;

        LockRefreshToken expectedToken = mockImmutableTsLockResponse();
        mockMinLockedInVersionIdResponse(immutableTs);

        LockImmutableTimestampResponse expectedResponse = LockImmutableTimestampResponse.of(immutableTs,
                toTokenV2(expectedToken));
        assertThat(timelock.lockImmutableTimestamp()).isEqualTo(expectedResponse);
    }

    @Test
    public void getImmutableTimestampDelegatesInProperOrder() throws InterruptedException {
        long immutableTs = 3L;

        InOrder inOrder = Mockito.inOrder(timestampService, lockService);

        mockMinLockedInVersionIdResponse(immutableTs);

        assertThat(timelock.getImmutableTimestamp()).isEqualTo(immutableTs);
        inOrder.verify(timestampService).getFreshTimestamp();
        inOrder.verify(lockService).getMinLockedInVersionId(LOCK_CLIENT.getClientId());
    }

    @Test
    public void getImmutableTimestampReturnsFreshTimestampIfMinLockedInVersionIsNull() throws InterruptedException {
        mockMinLockedInVersionIdResponse(null);

        assertThat(timelock.getImmutableTimestamp()).isEqualTo(FRESH_TIMESTAMP);
    }

    @Test
    public void lockDelegatesToLockService() throws InterruptedException {
        com.palantir.lock.LockRequest legacyRequest = com.palantir.lock.LockRequest.builder(buildLockMap(LockMode.WRITE))
                .blockForAtMost(SimpleTimeDuration.of(TIMEOUT, TimeUnit.MILLISECONDS))
                .build();

        when(lockService.lock(LockClient.ANONYMOUS.getClientId(), legacyRequest)).thenReturn(LOCK_REFRESH_TOKEN);

        assertThat(timelock.lock(LockRequest.of(ImmutableSet.of(LOCK_A, LOCK_B), TIMEOUT)))
                .isEqualTo(LockResponse.successful(LOCK_TOKEN_V2));
        verify(lockService).lock(LockClient.ANONYMOUS.getClientId(), legacyRequest);
    }

    @Test
    public void unlocksImmutableTimestampLockIfGettingMinLockedInVersionThrows() throws InterruptedException {
        Exception illegalStateException = new IllegalStateException();
        when(timestampService.getFreshTimestamp()).thenReturn(5L);
        when(lockService.lock(eq(LOCK_CLIENT.getClientId()), any())).thenReturn(LOCK_REFRESH_TOKEN);
        when(lockService.getMinLockedInVersionId(any(String.class))).thenThrow(illegalStateException);

        assertThatThrownBy(timelock::lockImmutableTimestamp).hasCause(illegalStateException);
        verify(lockService).unlock(LOCK_REFRESH_TOKEN);
    }

    @Test
    public void unlocksImmutableTimestampLockFromStartAtlasDbTransaction() throws InterruptedException {
        Exception illegalStateException = new IllegalStateException();
        when(timestampService.getFreshTimestamp())
                .thenReturn(5L) // needed because locking the immutable timestamp takes out a fresh ts
                .thenThrow(illegalStateException);
        when(lockService.lock(eq(LOCK_CLIENT.getClientId()), any())).thenReturn(LOCK_REFRESH_TOKEN);

        assertThatThrownBy(() -> timelock.startIdentifiedAtlasDbTransactionBatch(1)).isEqualTo(illegalStateException);
        verify(lockService).unlock(LOCK_REFRESH_TOKEN);
    }

    @Test
    public void waitForLocksDelegatesToLockService() throws InterruptedException {
        com.palantir.lock.LockRequest legacyRequest = com.palantir.lock.LockRequest.builder(buildLockMap(LockMode.READ)).lockAndRelease().build();

        when(lockService.lock(LockClient.ANONYMOUS.getClientId(), legacyRequest)).thenReturn(LOCK_REFRESH_TOKEN);

        timelock.waitForLocks(WaitForLocksRequest.of(ImmutableSet.of(LOCK_A, LOCK_B), TIMEOUT));
        verify(lockService).lock(LockClient.ANONYMOUS.getClientId(), legacyRequest);
    }

    @Test
    public void refreshLockLeasesDelegatesToLockService() {
        Set<LockToken> tokens = ImmutableSet.of(LOCK_TOKEN_V2);
        timelock.refreshLockLeases(tokens);

        verify(lockService).refreshLockRefreshTokens(ImmutableSet.of(LOCK_REFRESH_TOKEN));
    }

    @Test
    public void unlockDelegatesToLockService() {
        timelock.unlock(ImmutableSet.of(LOCK_TOKEN_V2));

        verify(lockService).unlock(LOCK_REFRESH_TOKEN);
    }

    @Test
    public void unlockReturnsSubsetThatWereUnlocked() {
        LockToken tokenA = randomLockToken();
        LockToken tokenB = randomLockToken();

        when(lockService.unlock(toLegacyToken(tokenA))).thenReturn(true);
        when(lockService.unlock(toLegacyToken(tokenB))).thenReturn(false);

        Set<LockToken> expected = ImmutableSet.of(tokenA);
        assertThat(timelock.unlock(ImmutableSet.of(tokenA, tokenB))).isEqualTo(expected);
    }

    private static LockToken randomLockToken() {
        return LockToken.of(UUID.randomUUID());
    }

    @Test
    public void currentTimeMillisDelegatesToLockService() {
        long time = 456L;
        when(lockService.currentTimeMillis()).thenReturn(time);

        assertThat(timelock.currentTimeMillis()).isEqualTo(time);
    }

    private void mockMinLockedInVersionIdResponse(Long immutableTs) {
        when(lockService.getMinLockedInVersionId(LOCK_CLIENT.getClientId())).thenReturn(immutableTs);
    }

    private LockRefreshToken mockImmutableTsLockResponse() throws InterruptedException {
        LockDescriptor descriptor = AtlasTimestampLockDescriptor.of(FRESH_TIMESTAMP);
        com.palantir.lock.LockRequest expectedRequest = com.palantir.lock.LockRequest.builder(ImmutableSortedMap.of(descriptor, LockMode.READ))
                .withLockedInVersionId(FRESH_TIMESTAMP).build();
        LockRefreshToken expectedToken = new LockRefreshToken(BigInteger.ONE, 123L);
        when(lockService.lock(LOCK_CLIENT.getClientId(), expectedRequest)).thenReturn(expectedToken);
        return expectedToken;
    }

    private SortedMap<LockDescriptor, LockMode> buildLockMap(LockMode mode) {
        SortedMap<LockDescriptor, LockMode> lockMap = Maps.newTreeMap();
        lockMap.put(LOCK_A, mode);
        lockMap.put(LOCK_B, mode);

        return lockMap;
    }

    private static LockRefreshToken toLegacyToken(LockToken token) {
        return LockTokenConverter.toLegacyToken(token);
    }

    private static LockToken toTokenV2(LockRefreshToken token) {
        return LockTokenConverter.toTokenV2(token);
    }

}
