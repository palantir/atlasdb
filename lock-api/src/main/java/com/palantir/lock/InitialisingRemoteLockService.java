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

package com.palantir.lock;

import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ForwardingObject;

public final class InitialisingRemoteLockService extends ForwardingObject implements RemoteLockService {
    private volatile RemoteLockService delegate;

    private InitialisingRemoteLockService(RemoteLockService remoteLockService) {
        delegate = remoteLockService;
    }

    public static InitialisingRemoteLockService create() {
        return new InitialisingRemoteLockService(null);
    }

    public void initialise(RemoteLockService remoteLockService) {
        delegate = remoteLockService;
    }

    @Nullable
    @Override
    public LockRefreshToken lock(String client, LockRequest request) throws InterruptedException {
        return getDelegate().lock(client, request);
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request) throws InterruptedException {
        return getDelegate().lockAndGetHeldLocks(client, request);
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return getDelegate().unlock(token);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return getDelegate().refreshLockRefreshTokens(tokens);
    }

    @Nullable
    @Override
    public Long getMinLockedInVersionId(String client) {
        return getDelegate().getMinLockedInVersionId(client);
    }

    @Override
    public long currentTimeMillis() {
        return getDelegate().currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        getDelegate().logCurrentState();
    }

    private RemoteLockService getDelegate() {
        return (RemoteLockService) delegate();
    }

    @Override
    protected Object delegate() {
        checkInitialised();
        return delegate;
    }

    void checkInitialised() {
        if (delegate == null) {
            throw new IllegalStateException("Not initialised");
        }
    }
}
