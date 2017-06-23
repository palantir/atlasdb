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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.junit.Test;

public class HeldLocksCollectionTest {

    private static final UUID REQUEST_ID = UUID.randomUUID();

    private final HeldLocksCollection heldLocks = new HeldLocksCollection();

    @Test
    public void callsSupplierForNewRequest() {
        Supplier<CompletableFuture<HeldLocks>> supplier = mock(Supplier.class);
        when(supplier.get()).thenReturn(new CompletableFuture<>());
        heldLocks.getExistingOrAcquire(REQUEST_ID, supplier);

        verify(supplier).get();
    }

    @Test
    public void doesNotCallSupplierForExistingRequest() {
        heldLocks.getExistingOrAcquire(REQUEST_ID, () -> new CompletableFuture<>());

        Supplier<CompletableFuture<HeldLocks>> supplier = mock(Supplier.class);
        heldLocks.getExistingOrAcquire(REQUEST_ID, supplier);

        verifyNoMoreInteractions(supplier);
    }

    @Test
    public void tracksRequests() {
        CompletableFuture<HeldLocks> future = new CompletableFuture<>();
        heldLocks.getExistingOrAcquire(REQUEST_ID, () -> future);

        assertThat(heldLocks.heldLocksById.get(REQUEST_ID)).isEqualTo(future);
    }

    @Test
    public void removesExpiredAndFailedRequests() {
        HeldLocks expiredLocks = mock(HeldLocks.class);
        when(expiredLocks.isExpired()).thenReturn(true);

        HeldLocks nonExpiredLocks = mock(HeldLocks.class);
        when(nonExpiredLocks.isExpired()).thenReturn(false);

        CompletableFuture failedLocks = new CompletableFuture();
        failedLocks.completeExceptionally(new RuntimeException());

        heldLocks.getExistingOrAcquire(UUID.randomUUID(), () -> CompletableFuture.completedFuture(expiredLocks));
        heldLocks.getExistingOrAcquire(UUID.randomUUID(), () -> CompletableFuture.completedFuture(nonExpiredLocks));
        heldLocks.getExistingOrAcquire(UUID.randomUUID(), () -> failedLocks);

        heldLocks.removeExpired();

        assertThat(heldLocks.heldLocksById.size()).isEqualTo(1);
        assertThat(heldLocks.heldLocksById.values().iterator().next().join()).isEqualTo(nonExpiredLocks);
    }

}
