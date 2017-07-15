/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.lock.impl;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.Semaphore;

import javax.annotation.Nullable;

import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SimpleHeldLocksToken;

public class ThreadPooledLockService implements LockService, Closeable {
    private final ThreadPooledWrapper<RemoteLockService> wrapper;
    private final LockService delegate;

    public ThreadPooledLockService(LockService delegate, int localThreadPoolSize, Semaphore sharedThreadPool) {
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
    public LockResponse lockWithFullLockResponse(String client, LockRequest request) throws InterruptedException {
        return null;
    }

    @Override
    public boolean unlock(HeldLocksToken token) {
        return false;
    }

    @Override
    public boolean unlockSimple(SimpleHeldLocksToken token) {
        return false;
    }

    @Override
    public boolean unlockAndFreeze(HeldLocksToken token) {
        return false;
    }

    @Override
    public Set<HeldLocksToken> getTokens(LockClient client) {
        return null;
    }

    @Override
    public Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens) {
        return null;
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(HeldLocksGrant grant) {
        return null;
    }

    @Nullable
    @Override
    public HeldLocksGrant refreshGrant(BigInteger grantId) {
        return null;
    }

    @Override
    public HeldLocksGrant convertToGrant(HeldLocksToken token) {
        return null;
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant) {
        return null;
    }

    @Override
    public HeldLocksToken useGrant(LockClient client, BigInteger grantId) {
        return null;
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId() {
        return null;
    }

    @Override
    public Long getMinLockedInVersionId(LockClient client) {
        return null;
    }

    @Override
    public LockServerOptions getLockServerOptions() {
        return null;
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
//        delegate.close();
    }
}
