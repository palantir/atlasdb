/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.NamespaceAgnosticLockRpcClient;
import com.palantir.lock.SimpleHeldLocksToken;
import java.math.BigInteger;
import java.util.Optional;
import java.util.Set;

public class NamespaceAgnosticLockClientAdaptor implements NamespaceAgnosticLockRpcClient {
    private final String namespace;
    private final LockRpcClient lockRpcClient;

    public NamespaceAgnosticLockClientAdaptor(String namespace, LockRpcClient lockRpcClient) {
        this.namespace = namespace;
        this.lockRpcClient = lockRpcClient;
    }

    @Override
    public Optional<LockResponse> lockWithFullLockResponse(LockClient client, LockRequest request)
            throws InterruptedException {
        return lockRpcClient.lockWithFullLockResponse(namespace, client, request);
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        return lockRpcClient.unlock(namespace, token);
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return lockRpcClient.unlock(namespace, token);
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return lockRpcClient.unlockSimple(namespace, token);
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return lockRpcClient.unlockAndFreeze(namespace, token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return lockRpcClient.getTokens(namespace, client);
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return lockRpcClient.refreshTokens(namespace, tokens);
    }

    @Override
    public Optional<HeldLocksGrant> refreshGrant(HeldLocksGrant grant) {
        return lockRpcClient.refreshGrant(namespace, grant);
    }

    @Override
    public Optional<HeldLocksGrant> refreshGrant(BigInteger grantId) {
        return lockRpcClient.refreshGrant(namespace, grantId);
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return lockRpcClient.convertToGrant(namespace, token);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return lockRpcClient.useGrant(namespace, client, grant);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return lockRpcClient.useGrant(namespace, client, grantId);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId() {
        return lockRpcClient.getMinLockedInVersionId(namespace);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(LockClient client) {
        return lockRpcClient.getMinLockedInVersionId(namespace, client);
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(String client) {
        return lockRpcClient.getMinLockedInVersionId(namespace, client);
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return lockRpcClient.getLockServerOptions(namespace);
    }

    @Override
    public Optional<LockRefreshToken> lock(String client, LockRequest request) throws InterruptedException {
        return lockRpcClient.lock(namespace, client, request);
    }

    @Override
    public Optional<HeldLocksToken> lockAndGetHeldLocks(String client, LockRequest request)
            throws InterruptedException {
        return lockRpcClient.lockAndGetHeldLocks(namespace, client, request);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return lockRpcClient.refreshLockRefreshTokens(namespace, tokens);
    }

    @Override
    public long currentTimeMillis() {
        return lockRpcClient.currentTimeMillis(namespace);
    }

    @Override
    public void logCurrentState() {
        lockRpcClient.logCurrentState(namespace);
    }
}
