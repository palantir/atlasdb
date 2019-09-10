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

import java.math.BigInteger;
import java.util.Set;

import javax.annotation.Nullable;

import com.palantir.lock.BareLockRpcClient;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleHeldLocksToken;

public class RemoteLockServiceAdapter implements LockService {
    private final BareLockRpcClient bareLockRpcClient;

    public RemoteLockServiceAdapter(BareLockRpcClient bareLockRpcClient) {
        this.bareLockRpcClient = bareLockRpcClient;
    }

    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        return bareLockRpcClient.lockWithFullLockResponse(client, request).orElse(null);
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        return bareLockRpcClient.unlock(token);
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return bareLockRpcClient.unlock(token);
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return bareLockRpcClient.unlockSimple(token);
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return bareLockRpcClient.unlockAndFreeze(token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return bareLockRpcClient.getTokens(client);
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return bareLockRpcClient.refreshTokens(tokens);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
        return bareLockRpcClient.refreshGrant(grant).orElse(null);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(BigInteger grantId) {
        return bareLockRpcClient.refreshGrant(grantId).orElse(null);
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return bareLockRpcClient.convertToGrant(token);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return bareLockRpcClient.useGrant(client, grant);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return bareLockRpcClient.useGrant(client, grantId);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId() {
        return bareLockRpcClient.getMinLockedInVersionId().orElse(null);
    }

    @Override
    public Long getMinLockedInVersionId(LockClient client) {
        return bareLockRpcClient.getMinLockedInVersionId(client).orElse(null);
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return bareLockRpcClient.getLockServerOptions();
    }

    @Nullable
    @Override
    public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
        return bareLockRpcClient.lock(client, request).orElse(null);
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request) throws InterruptedException {
        return bareLockRpcClient.lockAndGetHeldLocks(client, request).orElse(null);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return bareLockRpcClient.refreshLockRefreshTokens(tokens);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId(String client) {
        return bareLockRpcClient.getMinLockedInVersionId(client).orElse(null);
    }

    @Override
    public long currentTimeMillis() {
        return bareLockRpcClient.currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        bareLockRpcClient.currentTimeMillis();
    }
}
