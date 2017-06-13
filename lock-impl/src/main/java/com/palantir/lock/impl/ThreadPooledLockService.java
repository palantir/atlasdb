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

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Semaphore;

import javax.annotation.Nullable;

import com.palantir.lock.CloseableRemoteLockService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;

public class ThreadPooledLockService implements CloseableRemoteLockService {
    private final ThreadPooledWrapper<RemoteLockService> wrapper;
    private final CloseableRemoteLockService delegate;

    public ThreadPooledLockService(CloseableRemoteLockService delegate, int localThreadPoolSize, Semaphore sharedThreadPool) {
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
