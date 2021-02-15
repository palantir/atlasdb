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

import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleHeldLocksToken;
import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;

/**
 * Given two proxies to the same set of underlying remote lock servers, one configured to expect longer-running
 * operations on the server and one not to, routes calls appropriately.
 */
public class TimeoutSensitiveLockRpcClient implements LockRpcClient {
    private final LockRpcClient longTimeoutProxy;
    private final LockRpcClient shortTimeoutProxy;

    public TimeoutSensitiveLockRpcClient(ShortAndLongTimeoutServices<LockRpcClient> services) {
        this.longTimeoutProxy = services.longTimeout();
        this.shortTimeoutProxy = services.shortTimeout();
    }

    @Override
    public Optional<LockResponse> lockWithFullLockResponse(String namespace, LockClient client, LockRequest request)
            throws InterruptedException {
        return longTimeoutProxy.lockWithFullLockResponse(namespace, client, request);
    }

    @Override
    public boolean unlock(String namespace, HeldLocksToken token) {
        return shortTimeoutProxy.unlock(namespace, token);
    }

    @Override
    public boolean unlock(String namespace, LockRefreshToken token) {
        return shortTimeoutProxy.unlock(namespace, token);
    }

    @Override
    public boolean unlockSimple(String namespace, SimpleHeldLocksToken token) {
        return shortTimeoutProxy.unlockSimple(namespace, token);
    }

    @Override
    public boolean unlockAndFreeze(String namespace, HeldLocksToken token) {
        // It feels like this could have a short timeout but not 100% sure so going for the safe option.
        return longTimeoutProxy.unlockAndFreeze(namespace, token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(String namespace, LockClient client) {
        return shortTimeoutProxy.getTokens(namespace, client);
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(String namespace, Iterable<HeldLocksToken> tokens) {
        return shortTimeoutProxy.refreshTokens(namespace, tokens);
    }

    @Override
    public Optional<HeldLocksGrant> refreshGrant(String namespace, HeldLocksGrant grant) {
        return shortTimeoutProxy.refreshGrant(namespace, grant);
    }

    @Override
    public Optional<HeldLocksGrant> refreshGrant(String namespace, BigInteger grantId) {
        return shortTimeoutProxy.refreshGrant(namespace, grantId);
    }

    @Override
    public HeldLocksGrant convertToGrant(String namespace, HeldLocksToken token) {
        // It feels like this could have a short timeout but not 100% sure so going for the safe option.
        return longTimeoutProxy.convertToGrant(namespace, token);
    }

    @Override
    public HeldLocksToken useGrant(String namespace, LockClient client, HeldLocksGrant grant) {
        // It feels like this could have a short timeout but not 100% sure so going for the safe option.
        return longTimeoutProxy.useGrant(namespace, client, grant);
    }

    @Override
    public HeldLocksToken useGrant(String namespace, LockClient client, BigInteger grantId) {
        // It feels like this could have a short timeout but not 100% sure so going for the safe option.
        return longTimeoutProxy.useGrant(namespace, client, grantId);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(String namespace) {
        return shortTimeoutProxy.getMinLockedInVersionId(namespace);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(String namespace, LockClient client) {
        return shortTimeoutProxy.getMinLockedInVersionId(namespace, client);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(String namespace, String client) {
        return shortTimeoutProxy.getMinLockedInVersionId(namespace, client);
    }

    @Override
    public LockServerOptions getLockServerOptions(String namespace) {
        return shortTimeoutProxy.getLockServerOptions(namespace);
    }

    @Override
    public Optional<LockRefreshToken> lock(String namespace, String client, LockRequest request)
            throws InterruptedException {
        return longTimeoutProxy.lock(namespace, client, request);
    }

    @Override
    public Optional<HeldLocksToken> lockAndGetHeldLocks(String namespace, String client, LockRequest request)
            throws InterruptedException {
        return longTimeoutProxy.lockAndGetHeldLocks(namespace, client, request);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(String namespace, Iterable<LockRefreshToken> tokens) {
        return shortTimeoutProxy.refreshLockRefreshTokens(namespace, tokens);
    }

    @Override
    public long currentTimeMillis(String namespace) {
        return shortTimeoutProxy.currentTimeMillis(namespace);
    }

    @Override
    public void logCurrentState(String namespace) {
        // Even if this does take more than the short timeout, the request will fail while the server will
        // dump its logs out.
        shortTimeoutProxy.logCurrentState(namespace);
    }
}
