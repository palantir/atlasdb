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

package com.palantir.atlasdb.timelock;

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
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleHeldLocksToken;

public class LockResource implements LockRpcClient {
    private final LockService lockService;

    public LockResource(LockService lockService) {
        this.lockService = lockService;
    }

    @Override
    public Optional<LockResponse> lockWithFullLockResponse(LockClient client, LockRequest request)
            throws InterruptedException {
        return Optional.ofNullable(lockService.lockWithFullLockResponse(client, request));
    }

    @Deprecated
    @Override
    public boolean unlock(HeldLocksToken token) {
        return lockService.unlock(token);
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return lockService.unlockSimple(token);
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return lockService.unlockAndFreeze(token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return lockService.getTokens(client);
    }

    @Deprecated
    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return lockService.refreshTokens(tokens);
    }

    @Override
    public Optional<HeldLocksGrant> refreshGrant(HeldLocksGrant grant) {
        return Optional.ofNullable(lockService.refreshGrant(grant));
    }

    @Override
    public Optional<HeldLocksGrant> refreshGrant(BigInteger grantId) {
        return Optional.ofNullable(lockService.refreshGrant(grantId));
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return lockService.convertToGrant(token);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return lockService.useGrant(client, grant);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return lockService.useGrant(client, grantId);
    }

    @Deprecated
    @Override
    public Optional<Long> getMinLockedInVersionId() {
        return Optional.ofNullable(lockService.getMinLockedInVersionId());
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(LockClient client) {
        return Optional.ofNullable(lockService.getMinLockedInVersionId(client));
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return lockService.getLockServerOptions();
    }

    @Override
    public Optional<Long> getMinLockedInVersionId(String client) {
        return Optional.ofNullable(lockService.getMinLockedInVersionId(client));
    }

    @Override
    public Optional<LockRefreshToken> lock(String client, LockRequest request) throws InterruptedException {
        return Optional.ofNullable(lockService.lock(client, request));
    }

    @Override
    public Optional<HeldLocksToken> lockAndGetHeldLocks(String client, LockRequest request)
            throws InterruptedException {
        return Optional.ofNullable(lockService.lockAndGetHeldLocks(client, request));
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return lockService.unlock(token);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return lockService.refreshLockRefreshTokens(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return lockService.currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        lockService.logCurrentState();
    }
}
