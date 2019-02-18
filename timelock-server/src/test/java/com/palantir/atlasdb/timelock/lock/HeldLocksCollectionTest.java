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
package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockToken;

public class HeldLocksCollectionTest {

    private static final UUID REQUEST_ID = UUID.randomUUID();

    private NanoTime currentTime = NanoTime.createForTests(0);
    private LeaderClock leaderClock = new LeaderClock(LeadershipId.random(), () -> currentTime);
    private final HeldLocksCollection heldLocksCollection = new HeldLocksCollection(leaderClock);


    @Test
    public void callsSupplierForNewRequest() {
        Supplier<AsyncResult<HeldLocks>> supplier = mock(Supplier.class);
        when(supplier.get()).thenReturn(new AsyncResult<>());
        heldLocksCollection.getExistingOrAcquire(REQUEST_ID, supplier);

        verify(supplier).get();
    }

    @Test
    public void doesNotCallSupplierForExistingRequest() {
        heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> new AsyncResult<>());

        Supplier<AsyncResult<HeldLocks>> supplier = mock(Supplier.class);
        heldLocksCollection.getExistingOrAcquire(REQUEST_ID, supplier);

        verifyNoMoreInteractions(supplier);
    }

    @Test
    public void tracksRequests() {
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);

        assertThat(heldLocksCollection.heldLocksById.get(REQUEST_ID)).isEqualTo(result);
    }

    @Test
    public void removesExpiredAndFailedRequests() {
        UUID nonExpiredRequest = mockNonExpiredRequest().getRequestId();
        mockExpiredRequest();
        mockFailedRequest();

        assertThat(heldLocksCollection.heldLocksById.size()).isEqualTo(3);

        heldLocksCollection.removeExpired();

        assertThat(heldLocksCollection.heldLocksById.size()).isEqualTo(1);
        assertThat(heldLocksCollection.heldLocksById.keySet().iterator().next()).isEqualTo(nonExpiredRequest);
    }

    @Test
    public void removesTimedOutRequests() {
        mockTimedOutRequest();
        assertThat(heldLocksCollection.heldLocksById.size()).isEqualTo(1);

        heldLocksCollection.removeExpired();

        assertThat(heldLocksCollection.heldLocksById.size()).isEqualTo(0);
    }

    @Test
    public void refreshReturnsSubsetOfUnlockedLocks() {
        LockToken unlockableRequest = mockRefreshableRequest();
        LockToken nonUnlockableRequest = mockNonRefreshableRequest();

        Set<LockToken> expected = ImmutableSet.of(unlockableRequest);
        Set<LockToken> actual =
                heldLocksCollection.refresh(ImmutableSet.of(unlockableRequest, nonUnlockableRequest)).value();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void unlockReturnsSubsetOfUnlockedLocks() {
        LockToken refreshableRequest = mockRefreshableRequest();
        LockToken nonRefreshableRequest = mockNonRefreshableRequest();

        Set<LockToken> expected = ImmutableSet.of(refreshableRequest);
        Set<LockToken> actual = heldLocksCollection.unlock(
                ImmutableSet.of(refreshableRequest, nonRefreshableRequest));

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void lockLeasesAreValidUntilExpiry() {
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        AsyncResult<Leased<LockToken>> asyncResult =
                heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);
        result.complete(heldLocksForId(REQUEST_ID));

        Lease lease = asyncResult.get().lease();
        assertThat(lease.isValid(leaderClock.time())).isTrue();

        setTime(currentTime.plus(LockLeaseContract.CLIENT_LEASE_TIMEOUT.minus(Duration.ofNanos(1))));
        assertThat(lease.isValid(leaderClock.time())).isTrue();
    }

    @Test
    public void lockLeasesAreInvalidAfterExpiry() {
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        AsyncResult<Leased<LockToken>> asyncResult =
                heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);
        result.complete(heldLocksForId(REQUEST_ID));

        Lease lease = asyncResult.get().lease();

        setTime(currentTime.plus(LockLeaseContract.CLIENT_LEASE_TIMEOUT));
        assertThat(lease.isValid(leaderClock.time())).isFalse();
    }

    @Test
    public void lockLeasesShouldBeInvalidatedBeforeLocksAreReaped() {
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        AsyncResult<Leased<LockToken>> asyncResult =
                heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);
        result.complete(heldLocksForId(REQUEST_ID));

        Lease lease = asyncResult.get().lease();

        setTime(currentTime.plus(LockLeaseContract.CLIENT_LEASE_TIMEOUT));
        assertThat(lease.isValid(leaderClock.time())).isFalse();
        assertLocked(REQUEST_ID);
    }

    @Test
    public void locksShouldBeReapedAndLeaseShouldBeInvalidatedAfterReapPeriod() {
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        AsyncResult<Leased<LockToken>> asyncResult =
                heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);
        result.complete(heldLocksForId(REQUEST_ID));

        Lease lease = asyncResult.get().lease();

        setTime(currentTime.plus(LockLeaseContract.SERVER_LEASE_TIMEOUT).plus(Duration.ofNanos(1)));
        assertThat(lease.isValid(leaderClock.time())).isFalse();
        assertUnlocked(REQUEST_ID);
    }

    private void assertLocked(UUID requestId) {
        heldLocksCollection.removeExpired();
        assertThat(heldLocksCollection.heldLocksById.containsKey(requestId)).isTrue();
    }

    private void assertUnlocked(UUID requestId) {
        heldLocksCollection.removeExpired();
        assertThat(heldLocksCollection.heldLocksById.containsKey(requestId)).isFalse();
    }

    private HeldLocks heldLocksForId(UUID id) {
        return new HeldLocks(new LockLog(new MetricRegistry(), () -> 2L),
                ImmutableSet.of(new ExclusiveLock(StringLockDescriptor.of("foo"))),
                id,
                leaderClock);
    }

    private void setTime(NanoTime nanoTime) {
        currentTime = nanoTime;
    }

    @Test
    public void successfulUnlockRemovesHeldLocks() {
        LockToken token = mockRefreshableRequest();

        heldLocksCollection.unlock(ImmutableSet.of(token));

        assertThat(heldLocksCollection.heldLocksById.isEmpty()).isTrue();
    }

    private LockToken mockExpiredRequest() {
        return mockHeldLocksForNewRequest(
                heldLocks -> when(heldLocks.unlockIfExpired()).thenReturn(true));
    }

    private LockToken mockNonExpiredRequest() {
        return mockHeldLocksForNewRequest(
                heldLocks -> when(heldLocks.unlockIfExpired()).thenReturn(false));
    }

    private LockToken mockRefreshableRequest() {
        return mockHeldLocksForNewRequest(
                heldLocks -> {
                    when(heldLocks.unlock()).thenReturn(true);
                    when(heldLocks.refresh()).thenReturn(true);
                });
    }

    private LockToken mockNonRefreshableRequest() {
        return mockHeldLocksForNewRequest(
                heldLocks -> {
                    when(heldLocks.unlock()).thenReturn(false);
                    when(heldLocks.refresh()).thenReturn(false);
                });
    }

    private LockToken mockFailedRequest() {
        LockToken request = LockToken.of(UUID.randomUUID());
        AsyncResult failedLocks = new AsyncResult();
        failedLocks.fail(new RuntimeException());

        heldLocksCollection.getExistingOrAcquire(request.getRequestId(), () -> failedLocks);

        return request;
    }

    private LockToken mockTimedOutRequest() {
        LockToken request = LockToken.of(UUID.randomUUID());
        AsyncResult timedOutResult = new AsyncResult();
        timedOutResult.timeout();

        heldLocksCollection.getExistingOrAcquire(request.getRequestId(), () -> timedOutResult);

        return request;
    }

    private LockToken mockHeldLocksForNewRequest(Consumer<HeldLocks> mockApplier) {
        LockToken request = LockToken.of(UUID.randomUUID());
        HeldLocks heldLocks = mock(HeldLocks.class);
        mockApplier.accept(heldLocks);

        AsyncResult<HeldLocks> completedResult = new AsyncResult<>();
        completedResult.complete(heldLocks);
        heldLocksCollection.getExistingOrAcquire(request.getRequestId(),
                () -> completedResult);

        return request;
    }

}
