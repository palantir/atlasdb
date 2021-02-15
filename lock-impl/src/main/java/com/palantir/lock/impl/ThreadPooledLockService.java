/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.lock.impl;

import com.palantir.lock.CloseableLockService;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleHeldLocksToken;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.Semaphore;
import javax.annotation.Nullable;

public class ThreadPooledLockService implements CloseableLockService {
    private final ThreadPooledWrapper<LockService> wrapper;
    private final CloseableLockService delegate;

    public ThreadPooledLockService(CloseableLockService delegate, int localThreadPoolSize, Semaphore sharedThreadPool) {
        this.delegate = delegate;
        wrapper = new ThreadPooledWrapper<>(delegate, localThreadPoolSize, sharedThreadPool);
    }

    @Nullable
    @Override
    public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
        return wrapper.applyWithPermit(lockService -> lockService.lock(client, request));
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request) throws InterruptedException {
        return wrapper.applyWithPermit(lockService -> lockService.lockAndGetHeldLocks(client, request));
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return delegate.unlock(token);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return delegate.refreshLockRefreshTokens(tokens);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId(String client) {
        return delegate.getMinLockedInVersionId(client);
    }

    @Override
    public LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException {
        return wrapper.applyWithPermit(lockService -> lockService.lockWithFullLockResponse(client, request));
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        return delegate.unlock(token);
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return delegate.unlockSimple(token);
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return delegate.unlockAndFreeze(token);
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return delegate.getTokens(client);
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return delegate.refreshTokens(tokens);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
        return delegate.refreshGrant(grant);
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(BigInteger grantId) {
        return delegate.refreshGrant(grantId);
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return delegate.convertToGrant(token);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return delegate.useGrant(client, grant);
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return delegate.useGrant(client, grantId);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId() {
        return delegate.getMinLockedInVersionId();
    }

    @Override
    public Long getMinLockedInVersionId(LockClient client) {
        return delegate.getMinLockedInVersionId(client);
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return delegate.getLockServerOptions();
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        delegate.logCurrentState();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
