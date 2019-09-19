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
package com.palantir.lock;

import com.google.common.collect.ForwardingObject;
import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

public abstract class ForwardingRemoteLockService extends ForwardingObject implements CloseableRemoteLockService {

    @Override
    protected abstract RemoteLockService delegate();

    @Override
    public LockRefreshToken lock(String client, LockRequest request)
            throws InterruptedException {
        return delegate().lock(client, request);
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(String client, LockRequest request)
            throws InterruptedException {
        return delegate().lockAndGetHeldLocks(client, request);
    }

    @Override
    public boolean unlock(LockRefreshToken token) {
        return delegate().unlock(token);
    }

    @Override
    public Set<LockRefreshToken> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens) {
        return delegate().refreshLockRefreshTokens(tokens);
    }

    @Override
    public Long getMinLockedInVersionId(String client) {
        return delegate().getMinLockedInVersionId(client);
    }

    @Override
    public long currentTimeMillis() {
        return delegate().currentTimeMillis();
    }

    @Override
    public void logCurrentState() {
        delegate().logCurrentState();
    }

    @Override
    public void close() throws IOException {
        if (delegate() instanceof Closeable) {
            ((Closeable) delegate()).close();
        }
    }
}
