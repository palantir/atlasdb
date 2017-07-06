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

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.v2.LockTokenV2;

public class HeldLocksCollectionTest {

    private static final UUID REQUEST_ID = UUID.randomUUID();

    private final HeldLocksCollection heldLocksCollection = new HeldLocksCollection();

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
    public void refreshReturnsSubsetOfUnlockedLocks() {
        LockTokenV2 unlockableRequest = mockRefreshableRequest();
        LockTokenV2 nonUnlockableRequest = mockNonRefreshableRequest();

        Set<LockTokenV2> expected = ImmutableSet.of(unlockableRequest);
        Set<LockTokenV2> actual = heldLocksCollection.refresh(ImmutableSet.of(unlockableRequest, nonUnlockableRequest));

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void unlockReturnsSubsetOfUnlockedLocks() {
        LockTokenV2 refreshableRequest = mockRefreshableRequest();
        LockTokenV2 nonRefreshableRequest = mockNonRefreshableRequest();

        Set<LockTokenV2> expected = ImmutableSet.of(refreshableRequest);
        Set<LockTokenV2> actual = heldLocksCollection.unlock(
                ImmutableSet.of(refreshableRequest, nonRefreshableRequest));

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void successfulUnlockRemovesHeldLocks() {
        LockTokenV2 token = mockRefreshableRequest();

        heldLocksCollection.unlock(ImmutableSet.of(token));

        assertThat(heldLocksCollection.heldLocksById.isEmpty()).isTrue();
    }

    private LockTokenV2 mockExpiredRequest() {
        return mockHeldLocksForNewRequest(
                heldLocks -> {
                    when(heldLocks.unlockIfExpired()).thenReturn(true);
                });
    }

    private LockTokenV2 mockNonExpiredRequest() {
        return mockHeldLocksForNewRequest(
                heldLocks -> {
                    when(heldLocks.unlockIfExpired()).thenReturn(false);
                });
    }

    private LockTokenV2 mockRefreshableRequest() {
        return mockHeldLocksForNewRequest(
                heldLocks -> {
                    when(heldLocks.unlock()).thenReturn(true);
                    when(heldLocks.refresh()).thenReturn(true);
                });
    }

    private LockTokenV2 mockNonRefreshableRequest() {
        return mockHeldLocksForNewRequest(
                heldLocks -> {
                    when(heldLocks.unlock()).thenReturn(false);
                    when(heldLocks.refresh()).thenReturn(false);
                });
    }

    private LockTokenV2 mockFailedRequest() {
        LockTokenV2 request = LockTokenV2.of(UUID.randomUUID());
        AsyncResult failedLocks = new AsyncResult();
        failedLocks.fail(new RuntimeException());

        heldLocksCollection.getExistingOrAcquire(request.getRequestId(), () -> failedLocks);

        return request;
    }

    private LockTokenV2 mockHeldLocksForNewRequest(Consumer<HeldLocks> mockApplier) {
        LockTokenV2 request = LockTokenV2.of(UUID.randomUUID());
        HeldLocks heldLocks = mock(HeldLocks.class);
        mockApplier.accept(heldLocks);

        AsyncResult<HeldLocks> completedResult = new AsyncResult<>();
        completedResult.complete(heldLocks);
        heldLocksCollection.getExistingOrAcquire(request.getRequestId(),
                () -> completedResult);

        return request;
    }

}
