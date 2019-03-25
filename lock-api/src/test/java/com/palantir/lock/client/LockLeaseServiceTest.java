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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.ImmutablePartitionedTimestamps;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.StartAtlasDbTransactionResponseV3;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.TimestampAndPartition;

@RunWith(MockitoJUnitRunner.class)
public class LockLeaseServiceTest {
    @Mock private TimelockRpcClient timelockRpcClient;
    @Mock private LockRequest lockRequest;
    @Mock private PartitionedTimestamps partitionedTimestamps;

    private static final Duration LEASE_DURATION = Duration.ofSeconds(1);
    private static final LeadershipId LEADER_ID = LeadershipId.random();
    private static final UUID SERVICE_ID = UUID.randomUUID();

    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());
    private static final LockResponse LOCK_RESPONSE = LockResponse.successful(LOCK_TOKEN);

    private LockLeaseService lockLeaseService;
    private AtomicLong currentTime = new AtomicLong(123);
    private Supplier<NanoTime> time = Suppliers.compose(NanoTime::createForTests, currentTime::incrementAndGet);

    @Before
    public void before() {
        when(timelockRpcClient.getLeaderTime()).thenAnswer(inv -> LeaderTime.of(LEADER_ID, time.get()));
        lockLeaseService = new LockLeaseService(timelockRpcClient, SERVICE_ID);
    }

    @Test
    public void lockResponeHasCorrectLeasedLock() {
        Lease lease = getLease();
        when(timelockRpcClient.lock(any())).thenReturn(
                LockResponseV2.successful(LOCK_TOKEN, lease));

        LockResponse clientResponse = lockLeaseService.lock(lockRequest);

        verify(timelockRpcClient).lock(any());
        LeasedLockToken leasedLockToken = (LeasedLockToken) clientResponse.getToken();
        assertThat(leasedLockToken.serverToken()).isEqualTo(LOCK_TOKEN);
        assertThat(leasedLockToken.getLease()).isEqualTo(lease);
    }

    @Test
    public void shouldHandleUnsuccessfulLockResponses() {
        when(timelockRpcClient.lock(any())).thenReturn(
                LockResponseV2.timedOut());

        LockResponse clientResponse = lockLeaseService.lock(lockRequest);
        assertThat(clientResponse.wasSuccessful()).isFalse();
    }

    @Test
    public void startTransactionsResponseHasCorrectLeasedLock() {
        Lease lease = getLease();
        when(timelockRpcClient.startTransactions(any())).thenReturn(
                startTransactionsResponseWith(LOCK_TOKEN, lease));

        StartTransactionResponseV4 clientResponse =
                lockLeaseService.startTransactions(2);

        verify(timelockRpcClient).startTransactions(any());

        LeasedLockToken leasedLock = (LeasedLockToken) clientResponse.immutableTimestamp().getLock();
        assertThat(leasedLock.serverToken()).isEqualTo(LOCK_TOKEN);
        assertThat(leasedLock.getLease()).isEqualTo(lease);
    }

    @Test
    public void returnedTokenShouldHaveCorrectServerToken() {
        when(timelockRpcClient.lock(any())).thenReturn(
                LockResponseV2.successful(LOCK_TOKEN, getLease()));

        LockResponse lockResponse = lockLeaseService.lock(lockRequest);
        LeasedLockToken leasedToken = (LeasedLockToken) lockResponse.getToken();
        assertThat(LOCK_RESPONSE.getToken()).isEqualTo(leasedToken.serverToken());
    }

    @Test
    public void leasedTokenShouldHaveValidLeaseForTheLeasePeriod() {
        when(timelockRpcClient.lock(any())).thenReturn(
                LockResponseV2.successful(LOCK_TOKEN, getLease()));

        LockResponse lockResponse = lockLeaseService.lock(lockRequest);
        assertValid(lockResponse.getToken());
    }

    @Test
    public void unlockShouldCallRemoteServer_validLeases() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        assertValid(token);
        lockLeaseService.unlock(ImmutableSet.of(token));

        verify(timelockRpcClient).unlock(ImmutableSet.of(token.serverToken()));
    }

    @Test
    public void unlockShouldCallRemoteServer_invalidLeases() {
        setTime(123);
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        advance(LEASE_DURATION);
        assertInvalid(token);

        lockLeaseService.unlock(ImmutableSet.of(token));
        verify(timelockRpcClient).unlock(ImmutableSet.of(token.serverToken()));
    }

    @Test
    public void unlockShouldInvalidateLease() {
        LockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        lockLeaseService.unlock(ImmutableSet.of(token));
        assertInvalid(token);
    }

    @Test
    public void shouldOnlyCallIdentifiedTimeIfLeaseIsValid() {
        LeasedLockToken validToken = LeasedLockToken.of(LOCK_TOKEN, getLease());
        when(timelockRpcClient.getLeaderTime()).thenReturn(getIdentifiedTime());
        lockLeaseService.refreshLockLeases(ImmutableSet.of(validToken));

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

        Set<LockToken> refreshed = lockLeaseService.refreshLockLeases(ImmutableSet.of(leasedLockToken));
        verify(timelockRpcClient).refreshLockLeases(ImmutableSet.of(leasedLockToken.serverToken()));

        LeasedLockToken refreshedLeasedLockToken = (LeasedLockToken) refreshed.iterator().next();
        assertValid(refreshedLeasedLockToken);
        assertValid(leasedLockToken);
        assertThat(refreshedLeasedLockToken).isEqualTo(leasedLockToken);
    }

    private StartTransactionResponseV4 startTransactionsResponseWith(LockToken lockToken, Lease lease) {
        return StartTransactionResponseV4.of(
                LockImmutableTimestampResponse.of(1L, lockToken),
                partitionedTimestamps,
                lease);
    }

    private void setTime(long nanos) {
        time = () -> NanoTime.createForTests(nanos);
    }

    private void advance(Duration duration) {
        NanoTime advanced = time.get().plus(duration);
        time = () -> advanced;
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
        return Lease.of(getIdentifiedTime(), LEASE_DURATION);
    }

    private LeaderTime getIdentifiedTime() {
        return LeaderTime.of(LEADER_ID, time.get());
    }
}

