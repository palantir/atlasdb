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

import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;

import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleHeldLocksToken;

/**
 * Given two proxies to the same set of underlying remote lock servers, one configured to expect longer-running
 * operations on the server and one not to, routes calls appropriately.
 */
public class BlockingSensitiveLockRpcClient implements LockRpcClient {
    private final LockRpcClient blockingClient;
    private final LockRpcClient nonBlockingClient;

    public BlockingSensitiveLockRpcClient(
            LockRpcClient blockingClient,
            LockRpcClient nonBlockingClient) {
        this.blockingClient = blockingClient;
        this.nonBlockingClient = nonBlockingClient;
    }

    @Override
    public Optional<LockResponse> lockWithFullLockResponse(String namespace, LockClient client, LockRequest request)
            throws InterruptedException {
        return blockingClient.lockWithFullLockResponse(namespace, client, request);
    }

    @Override
    public boolean unlock(String namespace, HeldLocksToken token) {
        return nonBlockingClient.unlock(namespace, token);
    }

    @Override
    public boolean unlock(String namespace, LockRefreshToken token) {
        return nonBlockingClient.unlock(namespace, token);
    }

    @Override
    public boolean unlockSimple(String namespace, SimpleHeldLocksToken token) {
        return nonBlockingClient.unlockSimple(namespace, token);
    }

    @Override
    public boolean unlockAndFreeze(String namespace, HeldLocksToken token) {
        // TODO (jkong): It feels like this could be non-blocking but not 100% sure so going for the safe option.
        return blockingClient.unlockAndFreeze(namespace, token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(String namespace, LockClient client) {
        return nonBlockingClient.getTokens(namespace, client);
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(String namespace, Iterable<HeldLocksToken> tokens) {
        return nonBlockingClient.refreshTokens(namespace, tokens);
    }

    @Override
    public Optional<HeldLocksGrant> refreshGrant(String namespace, HeldLocksGrant grant) {
        return nonBlockingClient.refreshGrant(namespace, grant);
    }

    @Override
    public Optional<HeldLocksGrant> refreshGrant(String namespace, BigInteger grantId) {
        return nonBlockingClient.refreshGrant(namespace, grantId);
    }

    @Override
    public HeldLocksGrant convertToGrant(String namespace, HeldLocksToken token) {
        // TODO (jkong): It feels like this could be non-blocking but not 100% sure so going for the safe option.
        return blockingClient.convertToGrant(namespace, token);
    }

    @Override
    public HeldLocksToken useGrant(String namespace, LockClient client, HeldLocksGrant grant) {
        // TODO (jkong): It feels like this could be non-blocking but not 100% sure so going for the safe option.
        return blockingClient.useGrant(namespace, client, grant);
    }

    @Override
    public HeldLocksToken useGrant(String namespace, LockClient client, BigInteger grantId) {
        // TODO (jkong): It feels like this could be non-blocking but not 100% sure so going for the safe option.
        return blockingClient.useGrant(namespace, client, grantId);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(String namespace) {
        return nonBlockingClient.getMinLockedInVersionId(namespace);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(String namespace, LockClient client) {
        return nonBlockingClient.getMinLockedInVersionId(namespace, client);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(String namespace, String client) {
        return nonBlockingClient.getMinLockedInVersionId(namespace, client);
    }

    @Override
    public LockServerOptions getLockServerOptions(String namespace) {
        return nonBlockingClient.getLockServerOptions(namespace);
    }

    @Override
    public Optional<LockRefreshToken> lock(String namespace, String client, LockRequest request)
            throws InterruptedException {
        return blockingClient.lock(namespace, client, request);
    }

    @Override
    public Optional<HeldLocksToken> lockAndGetHeldLocks(String namespace, String client, LockRequest request)
            throws InterruptedException {
        return blockingClient.lockAndGetHeldLocks(namespace, client, request);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(String namespace, Iterable<LockRefreshToken> tokens) {
        return nonBlockingClient.refreshLockRefreshTokens(namespace, tokens);
    }

    @Override
    public long currentTimeMillis(String namespace) {
        return nonBlockingClient.currentTimeMillis(namespace);
    }

    @Override
    public void logCurrentState(String namespace) {
        // Even if this does take more than the non-blocking timeout, the request will fail while the server will
        // dump its logs out.
        nonBlockingClient.logCurrentState(namespace);
    }
}
