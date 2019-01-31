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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.common.annotation.Immutable;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.IdentifiedTime;
import com.palantir.lock.v2.ImmutableStartIdentifiedAtlasDbTransactionResponse;
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

    private static final UUID LEADER_ID = UUID.randomUUID();

    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());
    private static final LockResponse LOCK_RESPONSE = LockResponse.successful(LOCK_TOKEN);

    @Before
    public void setUp() {
        when(timelockService.getLeaderTime()).thenReturn(IdentifiedTime.of(UUID.randomUUID(), NanoTime.now()));
        timelockClient = LeasingTimelockClient.create(timelockService);
    }

    @Test
    public void delegatesLockRequest() {
        LockRequest lockRequest = mock(LockRequest.class);
        when(timelockService.leasableLock(lockRequest)).thenReturn(
                LeasableLockResponse.of(LOCK_RESPONSE, getLease()));

        timelockClient.lock(lockRequest);

        verify(timelockService).leasableLock(lockRequest);
    }

    @Test
    public void delegatesStartAtlasdbTransactionRequest() {
        StartIdentifiedAtlasDbTransactionRequest request = mock(StartIdentifiedAtlasDbTransactionRequest.class);
        StartIdentifiedAtlasDbTransactionResponse response = startTransactionResponseWith(LOCK_TOKEN);
        when(timelockService.leasableStartIdentifiedAtlasDbTransaction(request)).thenReturn(
                LeasableStartIdentifiedAtlasDbTransactionResponse.of(response, getLease()));

        timelockClient.startIdentifiedAtlasDbTransaction(request);

        verify(timelockService).leasableStartIdentifiedAtlasDbTransaction(request);
    }

    @Test
    public void returnedTokenShouldHaveCorrectServerToken() {
        LockRequest lockRequest = mock(LockRequest.class);
        when(timelockService.leasableLock(lockRequest)).thenReturn(
                LeasableLockResponse.of(LOCK_RESPONSE, getLease()));

        LockResponse lockResponse = timelockClient.lock(lockRequest);
        LeasedLockToken leasedToken = (LeasedLockToken)lockResponse.getToken();
        assertEquals(leasedToken.serverToken(), LOCK_RESPONSE.getToken());
    }

    @Test
    public void leasedTokenShouldHaveValidLeaseForTheLeasePeriod() {
        LockRequest lockRequest = mock(LockRequest.class);
        when(timelockService.leasableLock(lockRequest)).thenReturn(
                LeasableLockResponse.of(LOCK_RESPONSE, getLease()));

        LockResponse lockResponse = timelockClient.lock(lockRequest);
        assertValid(lockResponse.getToken());
    }

    @Test
    public void unlockShouldInvalidateLease() {
        LockRequest lockRequest = mock(LockRequest.class);
        when(timelockService.leasableLock(lockRequest)).thenReturn(
                LeasableLockResponse.of(LOCK_RESPONSE, getLease()));

        LockResponse lockResponse = timelockClient.lock(lockRequest);
        LockToken token = lockResponse.getToken();
        timelockClient.unlock(ImmutableSet.of(token));

        assertInvalid(token);
    }

    @Test
    public void shouldOnlyCallIdentifiedTimeIfLeaseIsValid() {
        LeasedLockToken validToken = LeasedLockToken.of(LOCK_TOKEN, getLease());
        when(timelockService.getLeaderTime()).thenReturn(getIdentifiedTime());
        timelockClient.refreshLockLeases(ImmutableSet.of(validToken));

        verify(timelockService).getLeaderTime();
        verifyNoMoreInteractions(timelockService);
    }

    @Test
    public void shouldRefreshTheLease_invalidOnClient_validOnServer() {
        LeasedLockToken leasedLockToken = LeasedLockToken.of(LOCK_TOKEN, getLease(Duration.ZERO));
        assertInvalid(leasedLockToken);

        when(timelockService.leasableRefreshLockLeases(ImmutableSet.of(LOCK_TOKEN)))
                .thenReturn(LeasableRefreshLockResponse.of(
                        ImmutableSet.of(LOCK_TOKEN),
                        getLease()
                ));

        Set<LockToken> refreshed = timelockClient.refreshLockLeases(ImmutableSet.of(leasedLockToken));
        verify(timelockService).leasableRefreshLockLeases(ImmutableSet.of(leasedLockToken.serverToken()));

        LeasedLockToken refreshedLeasedLockToken = (LeasedLockToken) refreshed.iterator().next();
        assertValid(refreshedLeasedLockToken);
        assertValid(leasedLockToken);
        assertEquals(leasedLockToken, refreshedLeasedLockToken);
    }

    private StartIdentifiedAtlasDbTransactionResponse startTransactionResponseWith(LockToken lockToken) {
        return StartIdentifiedAtlasDbTransactionResponse.of(
                LockImmutableTimestampResponse.of(1L, lockToken),
                mock(TimestampAndPartition.class)
        );
    }

    private void assertValid(LockToken token) {
        LeasedLockToken leasedLockToken = (LeasedLockToken)token;
        assertTrue(leasedLockToken.isValid(getIdentifiedTime()));
    }

    private void assertInvalid(LockToken token) {
        LeasedLockToken leasedLockToken = (LeasedLockToken)token;
        assertFalse(leasedLockToken.isValid(getIdentifiedTime()));
    }

    private Lease getLease(Duration period) {
        return Lease.of(getIdentifiedTime(), period);
    }

    private Lease getLease() {
        return Lease.of(getIdentifiedTime(), Duration.ofSeconds(1L));
    }

    private IdentifiedTime getIdentifiedTime() {
        return IdentifiedTime.of(LEADER_ID, NanoTime.now());
    }
}
