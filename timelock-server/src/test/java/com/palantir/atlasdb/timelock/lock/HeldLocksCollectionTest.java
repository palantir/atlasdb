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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingService;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockToken;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.Test;

public class HeldLocksCollectionTest {

    private static final UUID REQUEST_ID = UUID.randomUUID();
    private static final UUID REQUEST_ID_2 = UUID.randomUUID();
    private static final LockDescriptor LOCK_DESCRIPTOR = StringLockDescriptor.of("foo");

    private final AtomicLong atomicLong = new AtomicLong(1);
    private Supplier<NanoTime> time = Suppliers.compose(NanoTime::createForTests, atomicLong::incrementAndGet);
    private final LeaderClock leaderClock = new LeaderClock(LeadershipId.random(), () -> time.get());
    private final HeldLocksCollection heldLocksCollection = new HeldLocksCollection(leaderClock);
    private final LockWatchingService lockWatcher = mock(LockWatchingService.class);

    @Test
    public void callsSupplierForNewRequest() {
        Supplier<AsyncResult<HeldLocks>> supplier = mock(Supplier.class);
        when(supplier.get()).thenReturn(new AsyncResult<>());
        heldLocksCollection.getExistingOrAcquire(REQUEST_ID, supplier);

        verify(supplier).get();
    }

    @Test
    public void doesNotCallSupplierForExistingRequest() {
        heldLocksCollection.getExistingOrAcquire(REQUEST_ID, AsyncResult::new);

        Supplier<AsyncResult<HeldLocks>> supplier = mock(Supplier.class);
        heldLocksCollection.getExistingOrAcquire(REQUEST_ID, supplier);

        verifyNoMoreInteractions(supplier);
    }

    @Test
    public void tracksRequests() {
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);

        assertThat(heldLocksCollection.heldLocksById).containsEntry(REQUEST_ID, result);
    }

    @Test
    public void removesExpiredAndFailedRequests() {
        UUID nonExpiredRequest = mockNonExpiredRequest().getRequestId();
        mockExpiredRequest();
        mockFailedRequest();

        assertThat(heldLocksCollection.heldLocksById).hasSize(3);

        heldLocksCollection.removeExpired();

        assertThat(heldLocksCollection.heldLocksById).hasSize(1);
        assertThat(heldLocksCollection.heldLocksById.keySet().iterator().next()).isEqualTo(nonExpiredRequest);
    }

    @Test
    public void removesTimedOutRequests() {
        mockTimedOutRequest();
        assertThat(heldLocksCollection.heldLocksById).hasSize(1);

        heldLocksCollection.removeExpired();

        assertThat(heldLocksCollection.heldLocksById).isEmpty();
    }

    @Test
    public void refreshReturnsSubsetOfUnlockedLocks() {
        LockToken unlockableRequest = mockRefreshableRequest();
        LockToken nonUnlockableRequest = mockNonRefreshableRequest();

        Set<LockToken> expected = ImmutableSet.of(unlockableRequest);
        Set<LockToken> actual = heldLocksCollection
                .refresh(ImmutableSet.of(unlockableRequest, nonUnlockableRequest))
                .value();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void unlockReturnsSubsetOfUnlockedLocks() {
        LockToken refreshableRequest = mockRefreshableRequest();
        LockToken nonRefreshableRequest = mockNonRefreshableRequest();

        Set<LockToken> expected = ImmutableSet.of(refreshableRequest);
        Set<LockToken> actual = heldLocksCollection.unlock(ImmutableSet.of(refreshableRequest, nonRefreshableRequest));

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void lockLeasesAreValidUntilExpiry() {
        setTime(123);
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        AsyncResult<Leased<LockToken>> asyncResult = heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);
        result.complete(heldLocksForId(REQUEST_ID));

        Lease lease = asyncResult.get().lease();
        assertThat(lease.isValid(leaderClock.time())).isTrue();

        advance(LockLeaseContract.CLIENT_LEASE_TIMEOUT.minus(Duration.ofNanos(1)));
        assertThat(lease.isValid(leaderClock.time())).isTrue();
    }

    @Test
    public void lockLeasesAreInvalidAfterExpiry() {
        setTime(123);
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        AsyncResult<Leased<LockToken>> asyncResult = heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);
        result.complete(heldLocksForId(REQUEST_ID));

        Lease lease = asyncResult.get().lease();

        advance(LockLeaseContract.CLIENT_LEASE_TIMEOUT);
        assertThat(lease.isValid(leaderClock.time())).isFalse();
    }

    @Test
    public void lockLeasesShouldBeInvalidatedBeforeLocksAreReaped() {
        setTime(123);
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        AsyncResult<Leased<LockToken>> asyncResult = heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);
        result.complete(heldLocksForId(REQUEST_ID));

        Lease lease = asyncResult.get().lease();

        advance(LockLeaseContract.CLIENT_LEASE_TIMEOUT);
        assertThat(lease.isValid(leaderClock.time())).isFalse();
        assertLocked(REQUEST_ID);
    }

    @Test
    public void locksShouldBeReapedAndLeaseShouldBeInvalidatedAfterReapPeriod() {
        setTime(123);
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        AsyncResult<Leased<LockToken>> asyncResult = heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);
        result.complete(heldLocksForId(REQUEST_ID));

        Lease lease = asyncResult.get().lease();

        advance(LockLeaseContract.SERVER_LEASE_TIMEOUT.plus(Duration.ofNanos(1)));
        assertThat(lease.isValid(leaderClock.time())).isFalse();
        assertUnlocked(REQUEST_ID);
    }

    @Test
    public void lockWatchingServiceIsUpdatedAfterLockIsCreatedAndReaped() {
        setTime(123);
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        AsyncResult<Leased<LockToken>> asyncResult = heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);
        result.complete(heldLocksForId(REQUEST_ID));
        verify(lockWatcher)
                .registerLock(ImmutableSet.of(LOCK_DESCRIPTOR), result.get().getToken());

        Lease lease = asyncResult.get().lease();

        advance(LockLeaseContract.SERVER_LEASE_TIMEOUT.plus(Duration.ofNanos(1)));
        assertThat(lease.isValid(leaderClock.time())).isFalse();
        assertUnlocked(REQUEST_ID);
        verify(lockWatcher).registerUnlock(ImmutableSet.of(LOCK_DESCRIPTOR));
        verifyNoMoreInteractions(lockWatcher);
    }

    @Test
    public void lockWatchingServiceIsUpdatedAfterLockIsCreatedAndUnlocked() {
        setTime(123);
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        heldLocksCollection.getExistingOrAcquire(REQUEST_ID, () -> result);
        result.complete(heldLocksForId(REQUEST_ID));
        verify(lockWatcher)
                .registerLock(ImmutableSet.of(LOCK_DESCRIPTOR), result.get().getToken());

        heldLocksCollection.unlock(ImmutableSet.of(LockToken.of(REQUEST_ID)));
        verify(lockWatcher).registerUnlock(ImmutableSet.of(LOCK_DESCRIPTOR));
        verifyNoMoreInteractions(lockWatcher);
    }

    @Test
    public void leaseShouldStartBeforeRefreshTime() {
        LockToken t1 = lockSync(REQUEST_ID);
        LockToken t2 = lockSync(REQUEST_ID_2);

        Set<LockToken> tokens = ImmutableSet.of(t1, t2);

        Leased<Set<LockToken>> refreshResult = heldLocksCollection.refresh(tokens);

        NanoTime t1RefreshTime =
                heldLocksCollection.heldLocksById.get(t1.getRequestId()).get().lastRefreshTime();
        NanoTime t2RefreshTime =
                heldLocksCollection.heldLocksById.get(t2.getRequestId()).get().lastRefreshTime();

        NanoTime minRefreshTime = t1RefreshTime.isBefore(t2RefreshTime) ? t1RefreshTime : t2RefreshTime;

        assertThat(refreshResult.lease().leaderTime().currentTime()).isLessThan(minRefreshTime);
    }

    @Test
    public void emptyRefreshResponse() {
        LockToken t1 = LockToken.of(UUID.randomUUID());
        LockToken t2 = LockToken.of(UUID.randomUUID());
        Leased<Set<LockToken>> refreshResult = heldLocksCollection.refresh(ImmutableSet.of(t1, t2));

        assertThat(refreshResult.value()).isEmpty();
        assertThat(refreshResult.lease().leaderTime().currentTime()).isLessThanOrEqualTo(time.get());
    }

    @Test
    public void successfulUnlockRemovesHeldLocks() {
        LockToken token = mockRefreshableRequest();

        heldLocksCollection.unlock(ImmutableSet.of(token));

        assertThat(heldLocksCollection.heldLocksById).isEmpty();
    }

    private LockToken lockSync(UUID requestId) {
        AsyncResult<HeldLocks> result = new AsyncResult<>();
        AsyncResult<Leased<LockToken>> acquireResult =
                heldLocksCollection.getExistingOrAcquire(requestId, () -> result);
        result.complete(heldLocksForId(requestId));
        return acquireResult.get().value();
    }

    private void assertLocked(UUID requestId) {
        heldLocksCollection.removeExpired();
        assertThat(heldLocksCollection.heldLocksById).containsKey(requestId);
    }

    private void assertUnlocked(UUID requestId) {
        heldLocksCollection.removeExpired();
        assertThat(heldLocksCollection.heldLocksById).doesNotContainKey(requestId);
    }

    private HeldLocks heldLocksForId(UUID id) {
        return HeldLocks.create(
                new LockLog(new MetricRegistry(), () -> 2L),
                ImmutableSet.of(new ExclusiveLock(LOCK_DESCRIPTOR)),
                id,
                leaderClock,
                lockWatcher);
    }

    private void advance(Duration duration) {
        NanoTime advanced = time.get().plus(duration);
        time = () -> advanced;
    }

    private void setTime(long nanos) {
        time = () -> NanoTime.createForTests(nanos);
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
        return mockHeldLocksForNewRequest(heldLocks -> {
            when(heldLocks.unlockExplicitly()).thenReturn(true);
            when(heldLocks.refresh()).thenReturn(true);
        });
    }

    private LockToken mockNonRefreshableRequest() {
        return mockHeldLocksForNewRequest(heldLocks -> {
            when(heldLocks.unlockExplicitly()).thenReturn(false);
            when(heldLocks.refresh()).thenReturn(false);
        });
    }

    private LockToken mockFailedRequest() {
        LockToken request = LockToken.of(UUID.randomUUID());
        AsyncResult<HeldLocks> failedLocks = new AsyncResult<>();
        failedLocks.fail(new RuntimeException());

        heldLocksCollection.getExistingOrAcquire(request.getRequestId(), () -> failedLocks);

        return request;
    }

    private LockToken mockTimedOutRequest() {
        LockToken request = LockToken.of(UUID.randomUUID());
        AsyncResult<HeldLocks> timedOutResult = new AsyncResult<>();
        timedOutResult.timeout();

        heldLocksCollection.getExistingOrAcquire(request.getRequestId(), () -> timedOutResult);

        return request;
    }

    private LockToken mockHeldLocksForNewRequest(Consumer<HeldLocks> mockApplier) {
        LockToken request = LockToken.of(UUID.randomUUID());
        HeldLocks heldLocks = mock(HeldLocks.class);
        mockApplier.accept(heldLocks);
        when(heldLocks.lastRefreshTime()).thenReturn(time.get());

        AsyncResult<HeldLocks> completedResult = new AsyncResult<>();
        completedResult.complete(heldLocks);
        heldLocksCollection.getExistingOrAcquire(request.getRequestId(), () -> completedResult);

        return request;
    }
}
