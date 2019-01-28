/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.v2.LeasableLockResponse;
import com.palantir.lock.v2.LeasableRefreshLockResponse;
import com.palantir.lock.v2.LeasableStartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.TimestampAndPartition;

public class LeasingTimelockClientTest {
    private TimelockRpcClient timelockService = mock(TimelockRpcClient.class);
    private TimelockService timelockClient;

    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());
    private static final LockToken LOCK_TOKEN_2 = LockToken.of(UUID.randomUUID());
    private static final LockResponse LOCK_RESPONSE = LockResponse.successful(LOCK_TOKEN);

    @Before
    public void setUp() {
        timelockClient = LeasingTimelockClient.create(timelockService);
    }

    @Test
    public void delegatesLockRequest() {
        LockRequest lockRequest = mock(LockRequest.class);
        when(timelockService.leasableLock(lockRequest)).thenReturn(
                LeasableLockResponse.of(LOCK_RESPONSE, getLease()));

        LockResponse lockResponse = timelockClient.lock(lockRequest);

        verify(timelockService).leasableLock(lockRequest);
        assertEquals(LOCK_RESPONSE, lockResponse);
    }

    @Test
    public void delegatesRefreshLockRequest() {
        Set<LockToken> lockTokens = ImmutableSet.of(LOCK_TOKEN, LOCK_TOKEN_2);
        when(timelockService.leasableRefreshLockLeases(lockTokens)).thenReturn(
                LeasableRefreshLockResponse.of(lockTokens, getLease()));

        Set<LockToken> refreshedTokens = timelockClient.refreshLockLeases(lockTokens);

        verify(timelockService).leasableRefreshLockLeases(refreshedTokens);
        assertEquals(lockTokens, refreshedTokens);
    }

    @Test
    public void delegatesStartAtlasdbTransactionRequest() {
        StartIdentifiedAtlasDbTransactionRequest request = mock(StartIdentifiedAtlasDbTransactionRequest.class);
        StartIdentifiedAtlasDbTransactionResponse response = startTransactionResponseWith(LOCK_TOKEN);
        when(timelockService.leasableStartIdentifiedAtlasDbTransaction(request)).thenReturn(
                LeasableStartIdentifiedAtlasDbTransactionResponse.of(response, getLease()));

        StartIdentifiedAtlasDbTransactionResponse clientResponse =
                timelockClient.startIdentifiedAtlasDbTransaction(request);

        verify(timelockService).leasableStartIdentifiedAtlasDbTransaction(request);
        assertEquals(response, clientResponse);
    }

    @Test
    public void shouldInvalidateLocalCacheAfterUnlock() {
        LockRequest lockRequest = mock(LockRequest.class);
        when(timelockService.leasableLock(lockRequest)).thenReturn(
                LeasableLockResponse.of(LOCK_RESPONSE, getLease()));
        when(timelockService.leasableRefreshLockLeases(ImmutableSet.of(LOCK_TOKEN)))
                .thenReturn(LeasableRefreshLockResponse.of(ImmutableSet.of(LOCK_TOKEN), getLease()));

        timelockClient.lock(lockRequest);
        timelockClient.unlock(ImmutableSet.of(LOCK_TOKEN));
        timelockClient.refreshLockLeases(ImmutableSet.of(LOCK_TOKEN));

        verify(timelockService).leasableRefreshLockLeases(ImmutableSet.of(LOCK_TOKEN));
    }

    @Test
    public void shouldNotCallRemoteIfLockIsNotExpired() {
        LockRequest lockRequest = mock(LockRequest.class);
        when(timelockService.leasableLock(lockRequest)).thenReturn(
                LeasableLockResponse.of(LOCK_RESPONSE, getLease()));
        when(timelockService.leasableRefreshLockLeases(any()))
                .thenReturn(LeasableRefreshLockResponse.of(ImmutableSet.of(LOCK_TOKEN), getLease()));

        timelockClient.lock(lockRequest);
        timelockClient.refreshLockLeases(ImmutableSet.of(LOCK_TOKEN));

        verify(timelockService, times(0)).leasableRefreshLockLeases(ImmutableSet.of(LOCK_TOKEN));
    }

    private StartIdentifiedAtlasDbTransactionResponse startTransactionResponseWith(LockToken lockToken) {
        return StartIdentifiedAtlasDbTransactionResponse.of(
                LockImmutableTimestampResponse.of(1L, lockToken),
                mock(TimestampAndPartition.class)
        );
    }

    private Lease getLease() {
        return Lease.of(System.nanoTime(), Duration.ofSeconds(1L));
    }
}
