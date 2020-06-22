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

import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.StringLockDescriptor;
import org.junit.Test;

public class TimeoutSensitiveLockRpcClientTest {
    private static final String NAMESPACE = "namespace";
    private static final String LOCK_CLIENT_STRING = "client";
    private static final LockClient LOCK_CLIENT = LockClient.of(LOCK_CLIENT_STRING);
    private static final LockRequest LOCK_REQUEST = LockRequest.builder(
            ImmutableSortedMap.of(StringLockDescriptor.of("lock"), LockMode.WRITE))
            .build();

    private final LockRpcClient longTimeout = mock(LockRpcClient.class);
    private final LockRpcClient shortTimeout = mock(LockRpcClient.class);
    private final LockRpcClient sensitiveClient = new TimeoutSensitiveLockRpcClient(
            ImmutableShortAndLongTimeoutServices.<LockRpcClient>builder()
                    .longTimeout(longTimeout)
                    .shortTimeout(shortTimeout)
                    .build());

    @Test
    public void lockHasLongTimeout() throws InterruptedException {
        sensitiveClient.lock(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);

        verify(longTimeout).lock(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);
        verify(shortTimeout, never()).lock(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);
    }

    @Test
    public void lockWithFullLockResponseHasLongTimeout() throws InterruptedException {
        sensitiveClient.lockWithFullLockResponse(NAMESPACE, LOCK_CLIENT, LOCK_REQUEST);

        verify(longTimeout).lockWithFullLockResponse(NAMESPACE, LOCK_CLIENT, LOCK_REQUEST);
        verify(shortTimeout, never()).lockWithFullLockResponse(NAMESPACE, LOCK_CLIENT, LOCK_REQUEST);
    }

    @Test
    public void lockAndGetHeldLocksHasLongTimeout() throws InterruptedException {
        sensitiveClient.lockAndGetHeldLocks(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);

        verify(longTimeout).lockAndGetHeldLocks(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);
        verify(shortTimeout, never()).lockAndGetHeldLocks(NAMESPACE, LOCK_CLIENT_STRING, LOCK_REQUEST);
    }

    @Test
    public void currentTimeMillisHasShortTimeout() {
        sensitiveClient.currentTimeMillis(NAMESPACE);

        verify(shortTimeout).currentTimeMillis(NAMESPACE);
        verify(longTimeout, never()).currentTimeMillis(NAMESPACE);

    }
}
