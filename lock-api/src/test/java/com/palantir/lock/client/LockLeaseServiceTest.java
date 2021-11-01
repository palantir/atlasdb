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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulLockResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.watch.LockWatchStateUpdate;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LockLeaseServiceTest {
    @Mock
    private NamespacedConjureTimelockService timelock;

    @Mock
    private LockRequest lockRequest;

    @Mock
    private PartitionedTimestamps partitionedTimestamps;

    private static final Duration TIMEOUT_GREATER_THAN_MAX_PERMISSIBLE_TIMEOUT =
            BlockEnforcingLockService.MAX_PERMISSIBLE_LOCK_ACQUIRE_TIMEOUT.multipliedBy(5);
    private static final Duration LEASE_DURATION = Duration.ofSeconds(1);
    private static final LeadershipId LEADER_ID = LeadershipId.random();
    private static final UUID SERVICE_ID = UUID.randomUUID();

    private static final Exception TIMEOUT_EXCEPTION = new RuntimeException(new SocketTimeoutException("timeout"));

    private static final ConjureLockToken LOCK_TOKEN = ConjureLockToken.of(UUID.randomUUID());

    private LockLeaseService lockLeaseService;
    private AtomicLong currentTime = new AtomicLong(123);
    private Supplier<NanoTime> time = Suppliers.compose(NanoTime::createForTests, currentTime::incrementAndGet);

    @Before
    public void before() {
        when(lockRequest.getAcquireTimeoutMs()).thenReturn(10L);
        when(timelock.leaderTime()).thenAnswer(_inv -> LeaderTime.of(LEADER_ID, time.get()));
        when(timelock.unlock(any())).thenAnswer(inv -> {
            ConjureUnlockRequest request = inv.getArgument(0);
            return ConjureUnlockResponse.of(request.getTokens());
        });
        lockLeaseService = new LockLeaseService(timelock, SERVICE_ID, new LegacyLeaderTimeGetter(timelock));
    }

    @Test
    public void lockResponseHasCorrectLeasedLock() {
        Lease lease = getLease();
        when(timelock.lock(any()))
                .thenReturn(ConjureLockResponse.successful(SuccessfulLockResponse.of(LOCK_TOKEN, lease)));

        LockResponse clientResponse = lockLeaseService.lock(lockRequest);

        verify(timelock).lock(any());
        LeasedLockToken leasedLockToken = (LeasedLockToken) clientResponse.getToken();
        assertThat(leasedLockToken.serverToken()).isEqualTo(LOCK_TOKEN);
        assertThat(leasedLockToken.getLease()).isEqualTo(lease);
    }

    @Test
    public void shouldHandleUnsuccessfulLockResponses() {
        when(timelock.lock(any())).thenReturn(ConjureLockResponse.unsuccessful(UnsuccessfulLockResponse.of()));

        LockResponse clientResponse = lockLeaseService.lock(lockRequest);
        assertThat(clientResponse.wasSuccessful()).isFalse();
    }

    @Test
    public void startTransactionsResponseHasCorrectLeasedLock() {
        Lease lease = getLease();
        when(timelock.startTransactions(any())).thenReturn(startTransactionsResponseWith(LOCK_TOKEN, lease));

        StartTransactionResponseV4 clientResponse = lockLeaseService.startTransactions(2);

        verify(timelock).startTransactions(any());

        LeasedLockToken leasedLock =
                (LeasedLockToken) clientResponse.immutableTimestamp().getLock();
        assertThat(leasedLock.serverToken()).isEqualTo(LOCK_TOKEN);
        assertThat(leasedLock.getLease()).isEqualTo(lease);
    }

    @Test
    public void returnedTokenShouldHaveCorrectServerToken() {
        when(timelock.lock(any()))
                .thenReturn(ConjureLockResponse.successful(SuccessfulLockResponse.of(LOCK_TOKEN, getLease())));

        LockResponse lockResponse = lockLeaseService.lock(lockRequest);
        LeasedLockToken leasedToken = (LeasedLockToken) lockResponse.getToken();
        assertThat(leasedToken.serverToken()).isEqualTo(LOCK_TOKEN);
    }

    @Test
    public void leasedTokenShouldHaveValidLeaseForTheLeasePeriod() {
        when(timelock.lock(any()))
                .thenReturn(ConjureLockResponse.successful(SuccessfulLockResponse.of(LOCK_TOKEN, getLease())));

        LockResponse lockResponse = lockLeaseService.lock(lockRequest);
        assertValid(lockResponse.getToken());
    }

    @Test
    public void lockAcquireTimeoutIsBounded() {
        when(lockRequest.getAcquireTimeoutMs()).thenReturn(TIMEOUT_GREATER_THAN_MAX_PERMISSIBLE_TIMEOUT.toMillis());
        when(timelock.lock(any()))
                .thenReturn(ConjureLockResponse.successful(SuccessfulLockResponse.of(LOCK_TOKEN, getLease())));
        LockResponse lockResponse = lockLeaseService.lock(lockRequest);
        assertValid(lockResponse.getToken());
        verify(timelock)
                .lock(argThat(req -> req.getAcquireTimeoutMs()
                        == BlockEnforcingLockService.MAX_PERMISSIBLE_LOCK_ACQUIRE_TIMEOUT.toMillis()));
    }

    @Test
    public void lockAcquireTimeoutIsBoundedAndRequestRetried() {
        when(lockRequest.getAcquireTimeoutMs()).thenReturn(TIMEOUT_GREATER_THAN_MAX_PERMISSIBLE_TIMEOUT.toMillis());
        when(timelock.lock(any()))
                .thenThrow(TIMEOUT_EXCEPTION)
                .thenReturn(ConjureLockResponse.successful(SuccessfulLockResponse.of(LOCK_TOKEN, getLease())));

        LockResponse lockResponse = lockLeaseService.lock(lockRequest);
        assertValid(lockResponse.getToken());
        verify(timelock, times(2)).lock(any());
    }

    @Test
    public void unlockShouldCallRemoteServer_validLeases() {
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        assertValid(token);
        lockLeaseService.unlock(ImmutableSet.of(token));

        verify(timelock).unlock(ConjureUnlockRequest.of(ImmutableSet.of(token.serverToken())));
    }

    @Test
    public void unlockShouldCallRemoteServer_invalidLeases() {
        setTime(123);
        LeasedLockToken token = LeasedLockToken.of(LOCK_TOKEN, getLease());
        advance(LEASE_DURATION);
        assertInvalid(token);

        lockLeaseService.unlock(ImmutableSet.of(token));
        verify(timelock).unlock(ConjureUnlockRequest.of(ImmutableSet.of(token.serverToken())));
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
        when(timelock.leaderTime()).thenReturn(getIdentifiedTime());
        lockLeaseService.refreshLockLeases(ImmutableSet.of(validToken));

        verify(timelock).leaderTime();
        verifyNoMoreInteractions(timelock);
    }

    @Test
    public void shouldRefreshTheLease_invalidOnClient_validOnServer() {
        LeasedLockToken leasedLockToken = LeasedLockToken.of(LOCK_TOKEN, getLease(Duration.ZERO));
        assertInvalid(leasedLockToken);

        when(timelock.refreshLocks(ConjureRefreshLocksRequest.of(ImmutableSet.of(LOCK_TOKEN))))
                .thenReturn(ConjureRefreshLocksResponse.of(ImmutableSet.of(LOCK_TOKEN), getLease()));

        Set<LockToken> refreshed = lockLeaseService.refreshLockLeases(ImmutableSet.of(leasedLockToken));
        verify(timelock).refreshLocks(ConjureRefreshLocksRequest.of(ImmutableSet.of(leasedLockToken.serverToken())));

        LeasedLockToken refreshedLeasedLockToken =
                (LeasedLockToken) refreshed.iterator().next();
        assertValid(refreshedLeasedLockToken);
        assertValid(leasedLockToken);
        assertThat(refreshedLeasedLockToken).isEqualTo(leasedLockToken);
    }

    private ConjureStartTransactionsResponse startTransactionsResponseWith(ConjureLockToken lockToken, Lease lease) {
        return ConjureStartTransactionsResponse.builder()
                .immutableTimestamp(LockImmutableTimestampResponse.of(1L, LockToken.of(lockToken.getRequestId())))
                .timestamps(partitionedTimestamps)
                .lease(lease)
                .lockWatchUpdate(LockWatchStateUpdate.snapshot(
                        UUID.randomUUID(), -1, Collections.emptySet(), Collections.emptySet()))
                .build();
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
