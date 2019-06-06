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

package com.palantir.lock;

import java.math.BigInteger;
import java.util.Set;

import javax.annotation.Nullable;

public class LockServiceAdapter implements LockService {
    private final LockRpcClient lockRpcClient;

    public LockServiceAdapter(LockRpcClient lockRpcClient) {
        this.lockRpcClient = lockRpcClient;
    }

    @Nullable
    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        return lockRpcClient.lockWithFullLockResponse(client, request).orElse(null);
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        return lockRpcClient.unlock(token);
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return lockRpcClient.unlock(token);
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return lockRpcClient.unlockSimple(token);
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return lockRpcClient.unlockAndFreeze(token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return lockRpcClient.getTokens(client);
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return lockRpcClient.refreshTokens(tokens);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
        return lockRpcClient.refreshGrant(grant).orElse(null);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(BigInteger grantId) {
        return lockRpcClient.refreshGrant(grantId).orElse(null);
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return lockRpcClient.convertToGrant(token);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return lockRpcClient.useGrant(client, grant);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return lockRpcClient.useGrant(client, grantId);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId() {
        return lockRpcClient.getMinLockedInVersionId().orElse(null);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId(LockClient client) {
        return lockRpcClient.getMinLockedInVersionId(client).orElse(null);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId(String client) {
        return lockRpcClient.getMinLockedInVersionId(client).orElse(null);
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return lockRpcClient.getLockServerOptions();
    }

    @Nullable
    @Override
    public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
        return lockRpcClient.lock(client, request).orElse(null);
    }

    @Nullable
    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request) throws InterruptedException {
        return lockRpcClient.lockAndGetHeldLocks(client, request).orElse(null);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return lockRpcClient.refreshLockRefreshTokens(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return lockRpcClient.currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        lockRpcClient.logCurrentState();
    }
}
