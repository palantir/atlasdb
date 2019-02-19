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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.ImmutableIdentifiedTimeLockRequest;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.StartAtlasDbTransactionResponseV3;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.TimestampAndPartition;

@RunWith(MockitoJUnitRunner.class)
public class LeasingTimelockClientTest {
    @Mock private TimelockRpcClient timelockRpcClient;
    @Mock private LockRequest lockRequest;
    @Mock private TimestampAndPartition timestampAndPartition;

    private TimelockService timelockService;
    private static final LeadershipId LEADER_ID = LeadershipId.random();
    private static final UUID SERVICE_ID = UUID.randomUUID();
    private StartIdentifiedAtlasDbTransactionRequest startTxnRequest =
            StartIdentifiedAtlasDbTransactionRequest.createForRequestor(SERVICE_ID);

    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());
    private static final LockResponse LOCK_RESPONSE = LockResponse.successful(LOCK_TOKEN);

    @Before
    public void before() {
        when(timelockRpcClient.getLeaderTime()).thenReturn(LeaderTime.of(LeadershipId.random(), NanoTime.now()));
        timelockService = LeasingTimelockClient.create(timelockRpcClient, SERVICE_ID);
    }

    @Test
    public void lockResponeHasCorrectLeasedLock() {
        Lease lease = getLease();
        when(timelockRpcClient.lock(lockRequest)).thenReturn(
                LockResponseV2.successful(LOCK_TOKEN, lease));

        LockResponse clientResponse = timelockService.lock(lockRequest);

        verify(timelockRpcClient).lock(lockRequest);
        LeasedLockToken leasedLockToken = (LeasedLockToken) clientResponse.getToken();
        assertThat(leasedLockToken.serverToken()).isEqualTo(LOCK_TOKEN);
        assertThat(leasedLockToken.getLease()).isEqualTo(lease);
    }

    @Test
    public void shouldHandleUnsuccessfulLockResponses() {
        when(timelockRpcClient.lock(lockRequest)).thenReturn(
                LockResponseV2.timedOut());

        LockResponse clientResponse = timelockService.lock(lockRequest);
        assertThat(clientResponse.wasSuccessful()).isFalse();
    }

    @Test
    public void startAtlasdbTransactionResponseHasCorrectLeasedLock() {
        Lease lease = getLease();
        when(timelockRpcClient.startAtlasDbTransaction(startTxnRequest)).thenReturn(
                startTransactionResponseWith(LOCK_TOKEN, lease));

        StartIdentifiedAtlasDbTransactionResponse clientResponse =
                timelockService.startIdentifiedAtlasDbTransaction(
                        ImmutableIdentifiedTimeLockRequest.of(startTxnRequest.requestId()));

        verify(timelockRpcClient).startAtlasDbTransaction(startTxnRequest);

        LeasedLockToken leasedLock = (LeasedLockToken) clientResponse.immutableTimestamp().getLock();
        assertThat(leasedLock.serverToken()).isEqualTo(LOCK_TOKEN);
        assertThat(leasedLock.getLease()).isEqualTo(lease);
    }

    @Test
    public void returnedTokenShouldHaveCorrectServerToken() {
        when(timelockRpcClient.lock(lockRequest)).thenReturn(
                LockResponseV2.successful(LOCK_TOKEN, getLease()));

        LockResponse lockResponse = timelockService.lock(lockRequest);
        LeasedLockToken leasedToken = (LeasedLockToken) lockResponse.getToken();
        assertThat(LOCK_RESPONSE.getToken()).isEqualTo(leasedToken.serverToken());
    }

    @Test
    public void leasedTokenShouldHaveValidLeaseForTheLeasePeriod() {
        when(timelockRpcClient.lock(lockRequest)).thenReturn(
                LockResponseV2.successful(LOCK_TOKEN, getLease()));

        LockResponse lockResponse = timelockService.lock(lockRequest);
        assertValid(lockResponse.getToken());
    }

    @Test
    public void unlockShouldCallRemoteServer_validLeases() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        assertValid(token);
        timelockService.unlock(ImmutableSet.of(token));

        verify(timelockRpcClient).unlock(ImmutableSet.of(token.serverToken()));
    }

    @Test
    public void unlockShouldCallRemoteServer_inValidLeases() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease(Duration.ZERO));
        assertInvalid(token);
        timelockService.unlock(ImmutableSet.of(token));

        verify(timelockRpcClient).unlock(ImmutableSet.of(token.serverToken()));
    }

    @Test
    public void unlockShouldInvalidateLease() {
        LockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        timelockService.unlock(ImmutableSet.of(token));

        assertInvalid(token);
    }

    @Test
    public void shouldOnlyCallIdentifiedTimeIfLeaseIsValid() {
        LeasedLockToken validToken = LeasedLockToken.of(LOCK_TOKEN, getLease());
        when(timelockRpcClient.getLeaderTime()).thenReturn(getIdentifiedTime());
        timelockService.refreshLockLeases(ImmutableSet.of(validToken));

        verify(timelockRpcClient).getLeaderTime();
        verifyNoMoreInteractions(timelockRpcClient);
    }

    @Test
    public void shouldRefreshTheLease_invalidOnClient_validOnServer() {
        LeasedLockToken leasedLockToken = LeasedLockToken.of(LOCK_TOKEN, getLease(Duration.ZERO));
        assertInvalid(leasedLockToken);

        when(timelockRpcClient.refreshLockLeases(ImmutableSet.of(LOCK_TOKEN)))
                .thenReturn(RefreshLockResponseV2.of(
                        ImmutableSet.of(LOCK_TOKEN),
                        getLease()));

        Set<LockToken> refreshed = timelockService.refreshLockLeases(ImmutableSet.of(leasedLockToken));
        verify(timelockRpcClient).refreshLockLeases(ImmutableSet.of(leasedLockToken.serverToken()));

        LeasedLockToken refreshedLeasedLockToken = (LeasedLockToken) refreshed.iterator().next();
        assertValid(refreshedLeasedLockToken);
        assertValid(leasedLockToken);
        assertThat(refreshedLeasedLockToken).isEqualTo(leasedLockToken);
    }

    private StartAtlasDbTransactionResponseV3 startTransactionResponseWith(LockToken lockToken, Lease lease) {
        return StartAtlasDbTransactionResponseV3.of(
                LockImmutableTimestampResponse.of(1L, lockToken),
                timestampAndPartition,
                lease);
    }

    private void assertValid(LockToken token) {
        LeasedLockToken leasedLockToken = (LeasedLockToken) token;
        assertThat(leasedLockToken.isValid(getIdentifiedTime())).isTrue();
    }

    private void assertInvalid(LockToken token) {
        LeasedLockToken leasedLockToken = (LeasedLockToken) token;
        assertThat(leasedLockToken.isValid(getIdentifiedTime())).isFalse();
    }

    private Lease getLease(Duration period) {
        return Lease.of(getIdentifiedTime(), period);
    }

    private Lease getLease() {
        return Lease.of(getIdentifiedTime(), Duration.ofSeconds(1L));
    }

    private LeaderTime getIdentifiedTime() {
        return LeaderTime.of(LEADER_ID, NanoTime.now());
    }
}

