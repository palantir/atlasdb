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

import org.junit.Test;

import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.StringLockDescriptor;

public class BlockingSensitiveLockRpcClientTest {
    private static final String NAMESPACE = "namespace";
    private static final String LOCK_CLIENT_STRING = "client";
    private static final LockClient LOCK_CLIENT = LockClient.of(LOCK_CLIENT_STRING);
    private static final LockRequest LOCK_REQUEST = LockRequest.builder(
            ImmutableSortedMap.of(StringLockDescriptor.of("lock"), LockMode.WRITE))
            .build();

    private final LockRpcClient blocking = mock(LockRpcClient.class);
    private final LockRpcClient nonBlocking = mock(LockRpcClient.class);
    private final LockRpcClient sensitiveClient = new BlockingSensitiveLockRpcClient(blocking, nonBlocking);

    @Test
    public void lockUsesBlockingClient() throws InterruptedException {
        sensitiveClient.lock(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);

        verify(blocking).lock(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);
        verify(nonBlocking, never()).lock(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);
    }

    @Test
    public void lockWithFullLockResponseUsesBlockingClient() throws InterruptedException {
        sensitiveClient.lockWithFullLockResponse(NAMESPACE, LOCK_CLIENT, LOCK_REQUEST);

        verify(blocking).lockWithFullLockResponse(NAMESPACE, LOCK_CLIENT, LOCK_REQUEST);
        verify(nonBlocking, never()).lockWithFullLockResponse(NAMESPACE, LOCK_CLIENT, LOCK_REQUEST);
    }

    @Test
    public void lockAndGetHeldLocksUsesBlockingClient() throws InterruptedException {
        sensitiveClient.lockAndGetHeldLocks(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);

        verify(blocking).lockAndGetHeldLocks(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);
        verify(nonBlocking, never()).lockAndGetHeldLocks(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);
    }

    @Test
    public void currentTimeMillisUsesNonBlockingClient() {
        sensitiveClient.currentTimeMillis(NAMESPACE);

        verify(nonBlocking).currentTimeMillis(NAMESPACE);
        verify(blocking, never()).currentTimeMillis(NAMESPACE);

    }
}
