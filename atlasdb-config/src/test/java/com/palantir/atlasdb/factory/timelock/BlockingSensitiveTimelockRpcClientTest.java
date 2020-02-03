/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory.timelock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.UUID;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.ImmutableStartTransactionRequestV5;
import com.palantir.lock.v2.StartTransactionRequestV5;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.WaitForLocksRequest;

public class BlockingSensitiveTimelockRpcClientTest {
    private static final String NAMESPACE = "namespace";
    private static final IdentifiedLockRequest LOCK_REQUEST = IdentifiedLockRequest.of(
            ImmutableSet.of(StringLockDescriptor.of("lock")),
            60_000);
    private static final WaitForLocksRequest WAIT_FOR_LOCKS_REQUEST = WaitForLocksRequest.of(
            ImmutableSet.of(StringLockDescriptor.of("waiting-lock")),
            60_000);
    private static final StartTransactionRequestV5 START_TRANSACTION_REQUEST
            = ImmutableStartTransactionRequestV5.builder()
            .lastKnownLockLogVersion(88)
            .numTransactions(66)
            .requestId(UUID.randomUUID())
            .requestorId(UUID.randomUUID())
            .build();

    private final TimelockRpcClient blocking = mock(TimelockRpcClient.class);
    private final TimelockRpcClient nonBlocking = mock(TimelockRpcClient.class);
    private final TimelockRpcClient sensitiveClient = new BlockingSensitiveTimelockRpcClient(blocking, nonBlocking);

    @Test
    public void lockUsesBlockingClient() {
        sensitiveClient.lock(NAMESPACE, LOCK_REQUEST);

        verify(blocking).lock(NAMESPACE, LOCK_REQUEST);
        verify(nonBlocking, never()).lock(NAMESPACE, LOCK_REQUEST);
    }

    @Test
    public void waitForLocksUsesBlockingClient() {
        sensitiveClient.waitForLocks(NAMESPACE, WAIT_FOR_LOCKS_REQUEST);

        verify(blocking).waitForLocks(NAMESPACE, WAIT_FOR_LOCKS_REQUEST);
        verify(nonBlocking, never()).waitForLocks(NAMESPACE, WAIT_FOR_LOCKS_REQUEST);
    }

    @Test
    public void startTransactionUsesNonBlockingClient() {
        sensitiveClient.startTransactionsWithWatches(NAMESPACE, START_TRANSACTION_REQUEST);

        verify(nonBlocking).startTransactionsWithWatches(NAMESPACE, START_TRANSACTION_REQUEST);
        verify(blocking, never()).startTransactionsWithWatches(NAMESPACE, START_TRANSACTION_REQUEST);
    }
}
