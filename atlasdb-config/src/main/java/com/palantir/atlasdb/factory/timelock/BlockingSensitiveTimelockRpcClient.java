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

import java.util.Set;

import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;

/**
 * Given two proxies to the same set of underlying TimeLock servers, one configured to expect longer-running operations
 * on the server and one configured not to, routes calls appropriately.
 */
public class BlockingSensitiveTimelockRpcClient implements TimelockRpcClient {
    private final TimelockRpcClient blockingClient;
    private final TimelockRpcClient nonBlockingClient;

    public BlockingSensitiveTimelockRpcClient(
            TimelockRpcClient blockingClient,
            TimelockRpcClient nonBlockingClient) {
        this.blockingClient = blockingClient;
        this.nonBlockingClient = nonBlockingClient;
    }

    @Override
    public long getFreshTimestamp(String namespace) {
        return nonBlockingClient.getFreshTimestamp(namespace);
    }

    @Override
    public TimestampRange getFreshTimestamps(String namespace, int numTimestampsRequested) {
        return nonBlockingClient.getFreshTimestamps(namespace, numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(String namespace, IdentifiedTimeLockRequest request) {
        // Despite the name, these locks are not exclusive so we do not expect blocking.
        return nonBlockingClient.lockImmutableTimestamp(namespace, request);
    }

    @Override
    public long getImmutableTimestamp(String namespace) {
        return nonBlockingClient.getImmutableTimestamp(namespace);
    }

    @Override
    public LockResponseV2 lock(String namespace, IdentifiedLockRequest request) {
        return blockingClient.lock(namespace, request);
    }

    @Override
    public WaitForLocksResponse waitForLocks(String namespace, WaitForLocksRequest request) {
        return blockingClient.waitForLocks(namespace, request);
    }

    @Override
    public RefreshLockResponseV2 refreshLockLeases(String namespace, Set<LockToken> tokens) {
        return nonBlockingClient.refreshLockLeases(namespace, tokens);
    }

    @Override
    public Set<LockToken> unlock(String namespace, Set<LockToken> tokens) {
        return nonBlockingClient.unlock(namespace, tokens);
    }

    @Override
    public long currentTimeMillis(String namespace) {
        return nonBlockingClient.currentTimeMillis(namespace);
    }
}
