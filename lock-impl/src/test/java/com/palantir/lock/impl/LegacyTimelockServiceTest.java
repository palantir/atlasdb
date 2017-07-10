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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.palantir.lock.AtlasTimestampLockDescriptor;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.LegacyTimelockService.LockRefreshTokenV2Adapter;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequestV2;
import com.palantir.lock.v2.LockTokenV2;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class LegacyTimelockServiceTest {

    private static final LockClient LOCK_CLIENT = LockClient.of("foo");

    private static final long FRESH_TIMESTAMP = 5L;

    private static final LockRefreshTokenV2Adapter LOCK_TOKEN_V2 = randomLockToken();
    private static final LockRefreshToken LOCK_REFRESH_TOKEN = LOCK_TOKEN_V2.getToken();

    private static final LockDescriptor LOCK_A = StringLockDescriptor.of("a");
    private static final LockDescriptor LOCK_B = StringLockDescriptor.of("b");

    private final TimestampService timestampService = mock(TimestampService.class);
    private final RemoteLockService lockService = mock(RemoteLockService.class);

    private static final long TIMEOUT = 10_000;

    private final TimelockService timelock = new LegacyTimelockService(timestampService, lockService, LOCK_CLIENT);

    @Before
    public void before() {
        when(timestampService.getFreshTimestamp()).thenReturn(FRESH_TIMESTAMP);
    }

    @Test
    public void freshTimestampDelegatesToTimestampService() {
        assertEquals(FRESH_TIMESTAMP, timelock.getFreshTimestamp());
    }

    @Test
    public void freshTimestampsDelegatesToTimestampService() {
        int numTimestamps = 10;
        TimestampRange range = TimestampRange.createInclusiveRange(21L, 30L);
        when(timestampService.getFreshTimestamps(numTimestamps)).thenReturn(range);

        assertEquals(range, timelock.getFreshTimestamps(numTimestamps));
    }

    @Test
    public void lockImmutableTimestampLocksFreshTimestamp() throws InterruptedException {
        long immutableTs = 3L;

        LockRefreshToken expectedToken = mockImmutableTsLockResponse();
        mockMinLockedInVersionIdResponse(immutableTs);

        LockImmutableTimestampResponse expectedResponse = LockImmutableTimestampResponse.of(immutableTs,
                new LockRefreshTokenV2Adapter(expectedToken));
        assertEquals(expectedResponse, timelock.lockImmutableTimestamp(LockImmutableTimestampRequest.create()));
    }

    @Test
    public void getImmutableTimestampDelegatesInProperOrder() throws InterruptedException {
        long immutableTs = 3L;

        InOrder inOrder = Mockito.inOrder(timestampService, lockService);

        mockMinLockedInVersionIdResponse(immutableTs);

        assertEquals(immutableTs, timelock.getImmutableTimestamp());
        inOrder.verify(timestampService).getFreshTimestamp();
        inOrder.verify(lockService).getMinLockedInVersionId(LOCK_CLIENT.getClientId());
    }

    @Test
    public void getImmutableTimestampReturnsFreshTimestampIfMinLockedInVersionIsNull() throws InterruptedException {
        mockMinLockedInVersionIdResponse(null);

        assertEquals(FRESH_TIMESTAMP, timelock.getImmutableTimestamp());
    }

    @Test
    public void lockDelegatesToLockService() throws InterruptedException {
        LockRequest legacyRequest = LockRequest.builder(buildLockMap(LockMode.WRITE)).build();

        when(lockService.lock(LockClient.ANONYMOUS.getClientId(), legacyRequest)).thenReturn(LOCK_REFRESH_TOKEN);

        assertEquals(Optional.of(LOCK_TOKEN_V2), timelock.lock(LockRequestV2.of(ImmutableSet.of(LOCK_A, LOCK_B), TIMEOUT)));
        verify(lockService).lock(LockClient.ANONYMOUS.getClientId(), legacyRequest);
    }

    @Test
    public void waitForLocksDelegatesToLockService() throws InterruptedException {
        LockRequest legacyRequest = LockRequest.builder(buildLockMap(LockMode.READ)).lockAndRelease().build();

        when(lockService.lock(LockClient.ANONYMOUS.getClientId(), legacyRequest)).thenReturn(LOCK_REFRESH_TOKEN);

        timelock.waitForLocks(WaitForLocksRequest.of(ImmutableSet.of(LOCK_A, LOCK_B), TIMEOUT));
        verify(lockService).lock(LockClient.ANONYMOUS.getClientId(), legacyRequest);
    }

    @Test
    public void refreshLockLeasesDelegatesToLockService() {
        Set<LockTokenV2> tokens = ImmutableSet.of(LOCK_TOKEN_V2);
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
        LockRefreshTokenV2Adapter tokenA = randomLockToken();
        LockRefreshTokenV2Adapter tokenB = randomLockToken();

        when(lockService.unlock(tokenA.getToken())).thenReturn(true);
        when(lockService.unlock(tokenB.getToken())).thenReturn(false);

        Set<LockTokenV2> expected = ImmutableSet.of(tokenA);
        assertEquals(expected, timelock.unlock(ImmutableSet.of(tokenA, tokenB)));
    }

    private static LockRefreshTokenV2Adapter randomLockToken() {
        LockRefreshToken token = new LockRefreshToken(BigInteger.valueOf(ThreadLocalRandom.current().nextLong()), 123L);
        return new LockRefreshTokenV2Adapter(token);
    }

    @Test
    public void currentTimeMillisDelegatesToLockService() {
        long time = 456L;
        when(lockService.currentTimeMillis()).thenReturn(time);

        assertEquals(time, timelock.currentTimeMillis());
    }

    private void mockMinLockedInVersionIdResponse(Long immutableTs) {
        when(lockService.getMinLockedInVersionId(LOCK_CLIENT.getClientId())).thenReturn(immutableTs);
    }

    private LockRefreshToken mockImmutableTsLockResponse() throws InterruptedException {
        LockDescriptor descriptor = AtlasTimestampLockDescriptor.of(FRESH_TIMESTAMP);
        LockRequest expectedRequest = LockRequest.builder(ImmutableSortedMap.of(descriptor, LockMode.READ))
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

}
